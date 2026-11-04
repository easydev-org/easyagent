package bundled

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * WEB SEARCH, WITH SEVERAL BACKENDS — ONE PER CALL.
 *
 * WHY MORE THAN ONE. Each of these is a third party that can rate-limit, change its terms, or start
 * requiring a key; no single one is a dependency worth having. A deployment picks the one that works
 * for it, and switching is a config change rather than a release.
 *
 * EXACTLY ONE RUNS. There is no fallback chain, and that was a deliberate reversal: a chain hides
 * which source answered, makes a failing provider cost its timeout before the next is tried, and
 * turns "where did this come from" into a property of the moment. `search_provider` says which; an
 * unset value means `parallel`, not "whichever works".
 *
 * NONE OF THEM NEEDS AN API KEY TODAY, which is why they are here rather than a paid vendor. That is
 * not a promise — it is checked by calling them — so a backend that starts refusing fails loudly and
 * the deployment can pin another.
 */

// searchResult is one hit, normalized across providers.
type searchResult struct {
	URL     string
	Title   string
	Content string
	// Provider records which backend produced this, so a caller can see where an answer came from.
	Provider string
	// Published is the provider's own date, when it gives one.
	Published string
}

// defaultSearchProvider is used when the deployment sets none.
//
// IT TAKES A QUERY LIST and returns excerpts written for a model to read, which is why it is the
// default rather than the first name in a list. A deployment can pin any of the others.
const defaultSearchProvider = "parallel"

/**
 * httpClientFor returns a client that REUSES CONNECTIONS.
 *
 * Every call used to build its own `&http.Client{Timeout: ...}`, which means a fresh TCP handshake
 * and TLS negotiation per search per provider: a web search is a few hundred milliseconds of real
 * work behind a connection setup that costs as much again. The default transport pools idle
 * connections, so sharing one client is the difference between reconnecting every time and not.
 *
 * The TIMEOUT IS SET PER REQUEST, not on the client, because it is a parameter: a client-level
 * deadline would be fixed at construction and the tool exposes it as an argument.
 */
var (
	sharedClientOnce sync.Once
	sharedClient     *http.Client
)

func httpClientFor() *http.Client {
	sharedClientOnce.Do(func() {
		sharedClient = &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				// A SEARCH IS SMALL AND SHORT. Four idle connections per host is enough for the one
				// provider a call uses; more would hold sockets the next search never asks for.
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     90 * time.Second,
				// TLS handshakes are the expensive part for a provider called once per search, so they
				// are kept alive rather than renegotiated.
				ForceAttemptHTTP2:     true,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}
	})
	return sharedClient
}

// searchProvider is one backend.
type searchProvider struct {
	name string
	// search issues the query. Returning a typed reason matters: "rate limited" and "no results" look
	// identical downstream otherwise.
	search func(ctx context.Context, query string, count int, timeout time.Duration) ([]searchResult, error)
}

// searchProviders is the registry.
//
// A VARIABLE, NOT A FUNCTION, so a test can substitute recorders and observe WHICH backend ran — the
// one property of this tool that a result alone cannot prove.
var searchProviders = func() []searchProvider {
	return []searchProvider{
		{name: "parallel", search: searchParallel},
		{name: "tavily", search: searchTavily},
		{name: "exa", search: searchExa},
		{name: "firecrawl", search: searchFirecrawl},
		{name: "tinyfish", search: searchTinyfish},
	}
}

// webSearch is the tool handler.
/**
 * webSearch queries EXACTLY ONE backend — whichever `search_provider` names.
 *
 * TRYING THEM IN SEQUENCE WAS WRONG THREE WAYS. A caller that pinned `tinyfish` and silently got
 * `parallel` had no way to know; a provider that failed cost its full timeout before the next was
 * tried, so five of them could spend two minutes before returning anything; and a fallback chain
 * makes "which source produced this answer" a property of the moment rather than of the deployment.
 *
 * The default is `parallel`, the only one that takes a query list and returns excerpts written for a
 * model to read. It is a DEFAULT, not the head of a chain: if it fails, the call fails.
 */
func (e *Extension) webSearch(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	query := strings.TrimSpace(strArg(args, "query"))
	if query == "" {
		return agent.ToolOutcome{}, &toolError{msg: "web-search: missing \"query\""}
	}
	count := 8
	if f, ok := numArg(args, "count"); ok && f >= 1 {
		count = int(f)
	}
	if count > 20 {
		count = 20
	}
	timeout := 25 * time.Second
	if f, ok := numArg(args, "timeout"); ok && f >= 5 {
		timeout = time.Duration(f) * time.Second
	}

	providers := searchProviders()
	name := strings.TrimSpace(e.deps.ResolveConfig("search_provider", session, tenant))
	if name == "" {
		// THE DEFAULT IS A CHOICE, NOT A FALLBACK: the same backend every time, so two searches on one
		// deployment agree about where their answers came from.
		name = defaultSearchProvider
	}

	var picked *searchProvider
	for i := range providers {
		if providers[i].name == name {
			picked = &providers[i]
			break
		}
	}
	if picked == nil {
		names := make([]string, 0, len(providers))
		for _, p := range providers {
			names = append(names, p.name)
		}
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf(
			"web-search: unknown search_provider %q (want %s)", name, strings.Join(names, ", "))}
	}

	res, err := picked.search(ctx, query, count, timeout)
	if err != nil {
		// THE PROVIDER IS NAMED IN THE ERROR, because the fix depends on which one it was: a rate
		// limit is wait-and-retry, a timeout is try another backend, a 401 is a credential problem.
		return agent.ToolOutcome{}, &toolError{msg: fmt.Sprintf("web-search: %s failed — %v", name, err)}
	}
	return renderSearchOutcome(query, res, name), nil
}

// renderSearchOutcome writes the model-facing text plus the structured payload.
func renderSearchOutcome(query string, results []searchResult, used string) agent.ToolOutcome {
	var b strings.Builder
	fmt.Fprintf(&b, "%d result(s) for %q", len(results), query)
	if used != "" {
		fmt.Fprintf(&b, " (via %s)", used)
	}
	b.WriteString("\n")

	for i, r := range results {
		title := r.Title
		if title == "" {
			title = r.URL
		}
		fmt.Fprintf(&b, "\n[%d] %s\n%s\n", i+1, title, r.URL)
		if r.Content != "" {
			b.WriteString(r.Content)
			if !strings.HasSuffix(r.Content, "\n") {
				b.WriteString("\n")
			}
		}
		if r.Published != "" {
			fmt.Fprintf(&b, "(published: %s)\n", r.Published)
		}
	}
	if len(results) == 0 {
		// NO RESULTS FROM A PROVIDER THAT ANSWERED is a real answer, not a failure — and it reads very
		// differently from "the call failed", so it is said plainly.
		fmt.Fprintf(&b, "No results from %s. Try different wording, or pin another backend.\n", used)
	}

	rows := make([]map[string]any, 0, len(results))
	for _, r := range results {
		row := map[string]any{"url": r.URL, "provider": r.Provider}
		if r.Title != "" {
			row["title"] = r.Title
		}
		if r.Content != "" {
			row["content"] = r.Content
		}
		if r.Published != "" {
			row["published"] = r.Published
		}
		rows = append(rows, row)
	}
	return agent.ToolOutcome{
		Content: strings.TrimSpace(b.String()),
		Metadata: map[string]any{
			"provider": used, "query": query, "results": rows,
		},
	}
}

// ---- Parallel: MCP over HTTP, no key ----

// mcpEndpoint posts a JSON-RPC call and returns the `result` object.
//
// MCP SPEAKS TWO TRANSPORTS AND THE PROVIDERS DISAGREE ON WHICH: some answer with plain JSON, others
// with an SSE stream where the payload is on a `data:` line. Parsing both here means each provider
// implementation only has to describe its own arguments and result shape.
func mcpEndpoint(ctx context.Context, url, tool string, args map[string]any, headers map[string]string, timeout time.Duration) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": tool, "arguments": args},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// BOTH ACCEPTED: asking for only JSON makes an SSE-only server answer 406, and asking for only
	// SSE makes a JSON-only server send a stream nobody parses.
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// PER-REQUEST DEADLINE, because the timeout is an argument.
	reqCtx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	req = req.WithContext(reqCtx)

	resp, err := httpClientFor().Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	return extractRPCResult(raw)
}

// extractRPCResult pulls the `result` object out of either transport.
func extractRPCResult(raw []byte) (json.RawMessage, error) {
	type rpcEnvelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	parse := func(data []byte) (json.RawMessage, error) {
		data = bytes.TrimSpace(data)
		if len(data) == 0 || data[0] != '{' {
			return nil, errors.New("not a JSON object")
		}
		var env rpcEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			return nil, err
		}
		if env.Error != nil {
			return nil, errors.New(env.Error.Message)
		}
		if len(env.Result) == 0 {
			return nil, errors.New("empty result")
		}
		return env.Result, nil
	}

	// PLAIN JSON FIRST: it is the whole body when the server chose JSON.
	if res, err := parse(raw); err == nil {
		return res, nil
	}
	// THEN SSE: scan `data:` lines for the one carrying a result.
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		if res, err := parse([]byte(strings.TrimPrefix(line, "data:"))); err == nil {
			return res, nil
		}
	}
	return nil, errors.New("no result in response")
}

// searchParallel queries Parallel's MCP endpoint.
//
// IT TAKES `search_queries` AS A LIST, which is why its schema is used as-is rather than collapsed to
// one string: the endpoint issues them together, and that is the whole reason it is first in the list.
func searchParallel(ctx context.Context, query string, count int, timeout time.Duration) ([]searchResult, error) {
	res, err := mcpEndpoint(ctx, "https://search.parallel.ai/mcp", "web_search", map[string]any{
		"objective":      query,
		"search_queries": []string{query},
	}, nil, timeout)
	if err != nil {
		return nil, err
	}

	// The MCP result wraps a JSON STRING in `content[0].text`; the real payload is inside it.
	var wrapper struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(res, &wrapper); err != nil {
		return nil, fmt.Errorf("unexpected result shape: %w", err)
	}
	if len(wrapper.Content) == 0 {
		return nil, errors.New("empty content")
	}
	var payload struct {
		Results []struct {
			URL         string   `json:"url"`
			Title       string   `json:"title"`
			PublishDate string   `json:"publish_date"`
			Excerpts    []string `json:"excerpts"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(wrapper.Content[0].Text), &payload); err != nil {
		return nil, fmt.Errorf("unexpected payload: %w", err)
	}

	out := make([]searchResult, 0, len(payload.Results))
	for _, r := range payload.Results {
		if r.URL == "" {
			continue
		}
		out = append(out, searchResult{
			URL: r.URL, Title: r.Title,
			Content:  strings.Join(r.Excerpts, "\n\n"),
			Provider: "parallel", Published: r.PublishDate,
		})
		if len(out) >= count {
			break
		}
	}
	return out, nil
}

// searchTavily queries Tavily's search API.
//
// `X-Tavily-Access-Mode: keyless` is what makes it work without a key; with a key configured the
// header is omitted and the key is sent instead.
func searchTavily(ctx context.Context, query string, count int, timeout time.Duration) ([]searchResult, error) {
	body, err := json.Marshal(map[string]any{
		"query": query, "max_results": count, "search_depth": "basic",
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.tavily.com/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tavily-Access-Mode", "keyless")

	reqCtx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	req = req.WithContext(reqCtx)

	resp, err := httpClientFor().Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	var payload struct {
		Results []struct {
			URL     string `json:"url"`
			Title   string `json:"title"`
			Content string `json:"content"`
		} `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	out := make([]searchResult, 0, len(payload.Results))
	for _, r := range payload.Results {
		if r.URL == "" {
			continue
		}
		out = append(out, searchResult{URL: r.URL, Title: r.Title, Content: r.Content, Provider: "tavily"})
	}
	return out, nil
}

// searchExa queries Exa's MCP endpoint.
//
// IT ANSWERS WITH PRE-FORMATTED TEXT, not JSON: the result is a block per hit with `Title:`, `URL:`
// and `Highlights:` lines. Parsing that text is the cost of a provider that returns model-ready prose.
func searchExa(ctx context.Context, query string, count int, timeout time.Duration) ([]searchResult, error) {
	res, err := mcpEndpoint(ctx, "https://mcp.exa.ai/mcp", "web_search_exa", map[string]any{
		"query": query, "numResults": count,
	}, nil, timeout)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(res, &wrapper); err != nil {
		return nil, fmt.Errorf("unexpected result shape: %w", err)
	}
	text := ""
	for _, c := range wrapper.Content {
		if c.Text != "" {
			text = c.Text
			break
		}
	}
	if text == "" {
		return nil, errors.New("empty content")
	}
	return parseExaText(text, count), nil
}

// parseExaText reads Exa's "Title:/URL:/Highlights:" blocks.
func parseExaText(text string, count int) []searchResult {
	var out []searchResult
	for _, block := range strings.Split(text, "\n\n---\n\n") {
		var r searchResult
		r.Provider = "exa"
		var highlights []string
		inHighlights := false
		for _, line := range strings.Split(block, "\n") {
			trimmed := strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(trimmed, "Title:"):
				r.Title = strings.TrimSpace(strings.TrimPrefix(trimmed, "Title:"))
				inHighlights = false
			case strings.HasPrefix(trimmed, "URL:"):
				r.URL = strings.TrimSpace(strings.TrimPrefix(trimmed, "URL:"))
				inHighlights = false
			case strings.HasPrefix(trimmed, "Published:"):
				v := strings.TrimSpace(strings.TrimPrefix(trimmed, "Published:"))
				if v != "" && !strings.EqualFold(v, "N/A") {
					r.Published = v
				}
				inHighlights = false
			case strings.HasPrefix(trimmed, "Highlights:"):
				inHighlights = true
			case inHighlights && trimmed != "":
				highlights = append(highlights, trimmed)
			}
		}
		if r.URL == "" {
			continue
		}
		r.Content = strings.Join(highlights, "\n")
		out = append(out, r)
		if len(out) >= count {
			break
		}
	}
	return out
}

/**
 * searchTinyfish queries TinyFish's MCP endpoint.
 *
 * TWO THINGS DIFFER FROM THE OTHERS, both verified against the live endpoint:
 *   - the tool is named `search`, not `web_search` or `web_search_exa`;
 *   - the keyless mode is a HEADER (`X-TinyFish-Access-Mode: keyless`). Without it the endpoint
 *     answers 401, which reads as "this provider needs a key" when it does not.
 *
 * Its reply is a JSON string in `content[0].text`, like Parallel's — the MCP wrapper is the same
 * shape even though the payloads differ.
 */
func searchTinyfish(ctx context.Context, query string, count int, timeout time.Duration) ([]searchResult, error) {
	res, err := mcpEndpoint(ctx, "https://agent.tinyfish.ai/mcp", "search", map[string]any{
		"query": query,
	}, map[string]string{"X-TinyFish-Access-Mode": "keyless"}, timeout)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(res, &wrapper); err != nil {
		return nil, fmt.Errorf("unexpected result shape: %w", err)
	}
	if len(wrapper.Content) == 0 {
		return nil, errors.New("empty content")
	}
	var payload struct {
		Results []struct {
			URL      string `json:"url"`
			Title    string `json:"title"`
			Snippet  string `json:"snippet"`
			SiteName string `json:"site_name"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(wrapper.Content[0].Text), &payload); err != nil {
		return nil, fmt.Errorf("unexpected payload: %w", err)
	}
	out := make([]searchResult, 0, len(payload.Results))
	for _, r := range payload.Results {
		if r.URL == "" {
			continue
		}
		content := tinyfishContent(r.Snippet, r.SiteName)
		out = append(out, searchResult{
			URL: r.URL, Title: r.Title, Content: content, Provider: "tinyfish",
		})
		if len(out) >= count {
			break
		}
	}
	return out, nil
}

// searchFirecrawl queries Firecrawl's MCP endpoint.
func searchFirecrawl(ctx context.Context, query string, count int, timeout time.Duration) ([]searchResult, error) {
	res, err := mcpEndpoint(ctx, "https://mcp.firecrawl.dev/v2/mcp", "firecrawl_search", map[string]any{
		"query": query, "limit": count,
	}, nil, timeout)
	if err != nil {
		return nil, err
	}
	var wrapper struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(res, &wrapper); err != nil {
		return nil, fmt.Errorf("unexpected result shape: %w", err)
	}
	if len(wrapper.Content) == 0 {
		return nil, errors.New("empty content")
	}
	// Firecrawl DOUBLE-ENCODES: the payload is a JSON string inside the text field.
	var payload struct {
		Success bool `json:"success"`
		Data    struct {
			Web []struct {
				URL         string `json:"url"`
				Title       string `json:"title"`
				Description string `json:"description"`
			} `json:"web"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(wrapper.Content[0].Text), &payload); err != nil {
		return nil, fmt.Errorf("unexpected payload: %w", err)
	}
	out := make([]searchResult, 0, len(payload.Data.Web))
	for _, r := range payload.Data.Web {
		if r.URL == "" {
			continue
		}
		out = append(out, searchResult{
			URL: r.URL, Title: r.Title, Content: r.Description, Provider: "firecrawl",
		})
		if len(out) >= count {
			break
		}
	}
	return out, nil
}

/**
 * tinyfishContent appends the site name to a snippet, unless the snippet already names it.
 *
 * THE SITE NAME IS PART OF THE USEFUL ANSWER when the snippet is short: "go.dev" tells a reader more
 * about whether a hit is authoritative than the URL alone. Appending it twice is worse than not
 * appending it at all, hence the check.
 */
func tinyfishContent(snippet, site string) string {
	snippet = strings.TrimSpace(snippet)
	if site == "" || strings.Contains(snippet, site) {
		return snippet
	}
	return strings.TrimSpace(snippet + " (" + site + ")")
}
