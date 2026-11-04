package bundled

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

/**
 * EXA ANSWERS WITH PRE-FORMATTED TEXT, NOT JSON.
 *
 * Its MCP tool returns blocks shaped `Title:/URL:/Published:/Highlights:`, so the parser is a text
 * reader rather than a decoder. Pinned here because a silent parse failure would make Exa look like an
 * empty provider.
 */
func TestParseExaText(t *testing.T) {
	text := `Title: Tutorial: Getting started with generics
URL: https://go.dev/doc/tutorial/generics
Published: N/A
Author: N/A
Highlights:
This tutorial introduces generics.
You will learn type parameters.

---

Title: Generics in Go
URL: https://example.com/generics
Published: 2026-01-02
Highlights:
A deep dive.
`
	got := parseExaText(text, 5)
	if len(got) != 2 {
		t.Fatalf("parsed %d blocks, want 2", len(got))
	}
	if got[0].URL != "https://go.dev/doc/tutorial/generics" || got[0].Title != "Tutorial: Getting started with generics" {
		t.Fatalf("first = %+v", got[0])
	}
	if !strings.Contains(got[0].Content, "type parameters") {
		t.Errorf("highlights missing: %q", got[0].Content)
	}
	// "N/A" is a value, not a date; treating it as one puts "N/A" in front of a reader.
	if got[0].Published != "" {
		t.Errorf("published = %q, want empty for N/A", got[0].Published)
	}
	if got[1].Published != "2026-01-02" {
		t.Errorf("second published = %q", got[1].Published)
	}
	if got[0].Provider != "exa" {
		t.Errorf("provider = %q", got[0].Provider)
	}
}

// An empty or shapeless response yields nothing rather than a phantom result.
func TestParseExaTextIgnoresBlocksWithoutURL(t *testing.T) {
	if got := parseExaText("Title: only a title\n\n---\n\nURL: https://ok.example/\nTitle: fine", 5); len(got) != 1 {
		t.Fatalf("got %d results, want 1 (the block with no URL must be dropped)", len(got))
	}
}

/**
 * MCP ANSWERS OVER TWO TRANSPORTS AND THE PROVIDERS DISAGREE ON WHICH.
 *
 * Parallel and Firecrawl have both been seen returning plain JSON and an SSE stream. A client that
 * only decodes JSON fails on the stream — and one that only scans `data:` lines fails on the body.
 */
func TestExtractRPCResultHandlesBothTransports(t *testing.T) {
	t.Run("plain-json", func(t *testing.T) {
		got, err := extractRPCResult([]byte(`{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`))
		if err != nil || !strings.Contains(string(got), `"ok"`) {
			t.Fatalf("got %q, err %v", got, err)
		}
	})
	t.Run("sse", func(t *testing.T) {
		body := "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n"
		got, err := extractRPCResult([]byte(body))
		if err != nil || !strings.Contains(string(got), `"ok"`) {
			t.Fatalf("got %q, err %v", got, err)
		}
	})
	t.Run("error-is-reported", func(t *testing.T) {
		if _, err := extractRPCResult([]byte(`{"jsonrpc":"2.0","error":{"message":"bad key"}}`)); err == nil {
			t.Fatal("an RPC error was decoded as a result")
		}
	})
	t.Run("garbage", func(t *testing.T) {
		if _, err := extractRPCResult([]byte("<html>nope</html>")); err == nil {
			t.Fatal("an HTML body was decoded as a result")
		}
	})
}

/**
 * TINYFISH'S SHAPE, PINNED.
 *
 * It is the one provider whose tool is named `search` and whose keyless mode is a header; both were
 * discovered by calling the live endpoint, where a missing header answers 401 and reads as "needs a
 * key". Its payload is a JSON string inside the MCP wrapper, like Parallel's.
 */
func TestSearchTinyfishIsRegistered(t *testing.T) {
	var names []string
	for _, p := range searchProviders() {
		names = append(names, p.name)
	}
	for _, want := range []string{"parallel", "tavily", "exa", "firecrawl", "tinyfish"} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Errorf("provider %q is not registered", want)
		}
	}
}

// The site name is appended only when the snippet does not already carry it — a snippet that ends in
// "(go.dev)" twice is worse than one that says it once.
func TestTinyfishContentAppendsSiteNameOnce(t *testing.T) {
	if got := tinyfishContent("some text", "go.dev"); got != "some text (go.dev)" {
		t.Errorf("got %q", got)
	}
	if got := tinyfishContent("already mentions go.dev", "go.dev"); got != "already mentions go.dev" {
		t.Errorf("the name was appended twice: %q", got)
	}
	if got := tinyfishContent("no site", ""); got != "no site" {
		t.Errorf("an empty site name changed the text: %q", got)
	}
}

/**
 * THE PROVIDER KNOB MUST REACH THE CLIENT AS A PICKER, NOT A TEXT BOX.
 *
 * `enum_values` was declared in the manifest and dropped in three places on the way to the wire:
 * `manifestConfig` did not parse it, `ConfigKnob` did not carry it, and `ConfigField` did not expose
 * it. The knob therefore rendered as a free-text field, and the only way to choose a backend was to
 * type its name from memory and hope it was spelled the way the code expects.
 *
 * A knob whose declared values never reach the client is worse than one with none: the manifest says
 * a set of choices exists, and nothing shows them.
 */
func TestSearchProviderIsAnEnumThatReachesTheClient(t *testing.T) {
	e, err := New(&stubDeps{})
	if err != nil {
		t.Fatal(err)
	}
	var found *ConfigField
	for _, f := range e.ConfigFields() {
		if f.Name == "search_provider" {
			cp := f
			found = &cp
			break
		}
	}
	if found == nil {
		t.Fatal("search_provider is not in the declared config surface")
	}
	if found.Type != "enum" {
		t.Errorf("type = %q; a client only renders a picker for \"enum\"", found.Type)
	}
	want := []string{"parallel", "tavily", "exa", "firecrawl", "tinyfish"}
	if len(found.EnumValues) != len(want) {
		t.Fatalf("enum values = %v, want %v", found.EnumValues, want)
	}
	for i := range want {
		if found.EnumValues[i] != want[i] {
			t.Fatalf("enum values = %v, want %v", found.EnumValues, want)
		}
	}
}

/**
 * EXACTLY ONE BACKEND RUNS, AND IT IS THE CONFIGURED ONE.
 *
 * A fallback chain was wrong three ways: a caller that pinned `tinyfish` and silently got `parallel`
 * had no way to know; a failing provider cost its full timeout before the next was tried; and "which
 * source produced this answer" became a property of the moment rather than of the deployment.
 *
 * The test asserts the ORDER, not just the result: a resolver that walked the list would still return
 * a correct-looking answer from the wrong provider.
 */
func TestWebSearchRunsOnlyTheConfiguredProvider(t *testing.T) {
	var called []string
	d := &stubDeps{}
	e, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	// Replace the registry with recorders so the test observes which providers were reached, without
	// any network.
	orig := searchProviders
	t.Cleanup(func() { searchProviders = orig })
	searchProviders = func() []searchProvider {
		return []searchProvider{
			{name: "parallel", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
				called = append(called, "parallel")
				return []searchResult{{URL: "https://p.example/", Provider: "parallel"}}, nil
			}},
			{name: "tavily", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
				called = append(called, "tavily")
				return []searchResult{{URL: "https://t.example/", Provider: "tavily"}}, nil
			}},
		}
	}

	// CONFIGURED: that one runs, and only it.
	d.cfg = map[string]string{"search_provider": "tavily"}
	if _, err := e.webSearch(context.Background(), "t1", "s1", map[string]any{"query": "x"}); err != nil {
		t.Fatal(err)
	}
	if len(called) != 1 || called[0] != "tavily" {
		t.Fatalf("called %v, want only tavily", called)
	}
}

/**
 * AN UNCONFIGURED BACKEND IS A REFUSAL, NOT A DEFAULT.
 *
 * `required_config` normally hides this tool, so an unconfigured call should not arrive — but a stale
 * plan or a config cleared mid-turn can still produce one. The answer is that the tool is unavailable.
 * Substituting a backend would put results in front of a reader from a source their deployment never
 * chose, and there would be nothing anywhere saying so.
 */
func TestWebSearchRefusesWithoutANConfiguredBackend(t *testing.T) {
	called := false
	e, err := New(&stubDeps{})
	if err != nil {
		t.Fatal(err)
	}
	orig := searchProviders
	t.Cleanup(func() { searchProviders = orig })
	searchProviders = func() []searchProvider {
		return []searchProvider{{name: "parallel", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
			called = true
			return []searchResult{{URL: "https://p.example/"}}, nil
		}}}
	}

	_, err = e.webSearch(context.Background(), "t1", "s1", map[string]any{"query": "x"})
	if err == nil {
		t.Fatal("an unconfigured backend produced a result")
	}
	if called {
		t.Fatal("a provider ran even though none was configured")
	}
	if !strings.Contains(err.Error(), "not available") {
		t.Fatalf("the refusal does not say the tool is unavailable: %v", err)
	}
}

/**
 * THE MODEL IS NEVER TOLD WHICH VENDOR ANSWERED.
 *
 * It cannot act on the name — it cannot switch providers — and telling it invites commentary about the
 * tool instead of the answer. The operator sees the choice in the console; the model sees results.
 */
func TestWebSearchNeverNamesTheProvider(t *testing.T) {
	e, err := New(&stubDeps{cfg: map[string]string{"search_provider": "tinyfish"}})
	if err != nil {
		t.Fatal(err)
	}
	orig := searchProviders
	t.Cleanup(func() { searchProviders = orig })
	searchProviders = func() []searchProvider {
		return []searchProvider{{name: "tinyfish", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
			return []searchResult{{URL: "https://a.example/", Title: "A", Content: "text", Provider: "tinyfish"}}, nil
		}}}
	}

	out, err := e.webSearch(context.Background(), "t1", "s1", map[string]any{"query": "x"})
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"tinyfish", "via ", "provider"} {
		if strings.Contains(strings.ToLower(out.Content), strings.ToLower(leak)) {
			t.Errorf("the result mentions %q:\n%s", leak, out.Content)
		}
	}
	if meta, ok := out.Metadata.(map[string]any); ok {
		if _, has := meta["provider"]; has {
			t.Error("the metadata still carries a provider")
		}
	}
}

/**
 * A FAILING PINNED PROVIDER IS AN ERROR, NOT A QUIET FALLBACK.
 *
 * The whole point of pinning is to know where results came from. Falling back would answer the
 * question with a different source and never say so.
 */
func TestWebSearchDoesNotFallBackWhenTheProviderFails(t *testing.T) {
	var called []string
	e, err := New(&stubDeps{cfg: map[string]string{"search_provider": "tavily"}})
	if err != nil {
		t.Fatal(err)
	}
	orig := searchProviders
	t.Cleanup(func() { searchProviders = orig })
	searchProviders = func() []searchProvider {
		return []searchProvider{
			{name: "parallel", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
				called = append(called, "parallel")
				return []searchResult{{URL: "https://p.example/"}}, nil
			}},
			{name: "tavily", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
				called = append(called, "tavily")
				return nil, errors.New("HTTP 429: rate limited")
			}},
		}
	}

	_, err = e.webSearch(context.Background(), "t1", "s1", map[string]any{"query": "x"})
	if err == nil {
		t.Fatal("a failing pinned provider returned success")
	}
	// THE REASON STAYS, THE VENDOR GOES: "rate limited" tells the model whether to retry or rephrase;
	// the provider's name is something it cannot act on.
	if !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("the error lost the reason: %v", err)
	}
	if strings.Contains(err.Error(), "tavily") {
		t.Fatalf("the error names the vendor: %v", err)
	}
	if len(called) != 1 {
		t.Fatalf("called %v; a failure must not trigger another provider", called)
	}
}

// An unknown name is refused with the valid ones, rather than silently using the default.
func TestWebSearchRejectsUnknownProvider(t *testing.T) {
	e, _ := New(&stubDeps{cfg: map[string]string{"search_provider": "bogus"}})
	_, err := e.webSearch(context.Background(), "t1", "s1", map[string]any{"query": "x"})
	if err == nil {
		t.Fatal("an unknown provider was accepted")
	}
	// THE VALID NAMES ARE NOT LISTED TO THE MODEL either — the same reason as above. The operator
	// sees them in the config page's picker.
	if !strings.Contains(err.Error(), "not available") {
		t.Errorf("the error does not say the tool is unavailable: %v", err)
	}
	if strings.Contains(err.Error(), "bogus") {
		t.Errorf("the error echoes the configured value back to the model: %v", err)
	}
}

// A provider that answered with nothing is a real answer, said plainly — not an error, and not a
// silent empty page.
func TestWebSearchEmptyResultIsStated(t *testing.T) {
	e, _ := New(&stubDeps{cfg: map[string]string{"search_provider": "parallel"}})
	orig := searchProviders
	t.Cleanup(func() { searchProviders = orig })
	searchProviders = func() []searchProvider {
		return []searchProvider{{name: "parallel", search: func(context.Context, string, int, time.Duration) ([]searchResult, error) {
			return nil, nil
		}}}
	}
	out, err := e.webSearch(context.Background(), "t1", "s1", map[string]any{"query": "x"})
	if err != nil {
		t.Fatalf("an empty answer was treated as a failure: %v", err)
	}
	if !strings.Contains(out.Content, "No results") {
		t.Fatalf("the empty answer is not stated:\n%s", out.Content)
	}
	if strings.Contains(out.Content, "parallel") {
		t.Fatalf("the empty answer names the vendor:\n%s", out.Content)
	}
}

/**
 * THE TOOL IS HIDDEN UNTIL A BACKEND EXISTS.
 *
 * `required_config: [search_provider]` is what makes the agent never see web-search on a deployment
 * that has not chosen one. Without it, the tool is in every tool list and fails on first use — the
 * shape of "a capability that cannot work".
 *
 * This asserts the DECLARATION, because the filtering itself lives in agent.FilterBlockedByConfig and
 * is tested there; what can silently break here is the manifest forgetting to say it.
 */
func TestWebSearchRequiresAConfiguredBackend(t *testing.T) {
	m, err := ParseManifest()
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, tool := range m.Tools {
		if tool.Name != "web-search" {
			continue
		}
		found = true
		if len(tool.RequiredConfig) != 1 || tool.RequiredConfig[0] != "search_provider" {
			t.Fatalf("required_config = %v, want [search_provider]", tool.RequiredConfig)
		}
	}
	if !found {
		t.Fatal("web-search is not in the manifest")
	}

	// AND THE KNOB HAS NO DEFAULT. A default would make the tool appear everywhere, which is the same
	// failure the requirement exists to prevent.
	for _, knob := range m.Config {
		if knob.Name == "search_provider" {
			if knob.Default != "" {
				t.Fatalf("search_provider has default %q; with a default the tool is never hidden", knob.Default)
			}
			if knob.Type != "enum" || len(knob.EnumValues) == 0 {
				t.Fatalf("search_provider type = %q with %d values; the console needs both for a picker",
					knob.Type, len(knob.EnumValues))
			}
			return
		}
	}
	t.Fatal("search_provider is not declared")
}
