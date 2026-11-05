package bundled

import "strings"

/**
 * The WIRE SHAPES of the five search providers, and the adapters that turn each into `searchResult`.
 *
 * EVERY PROVIDER GETS A NAMED TYPE. They used to be anonymous structs declared inline next to the
 * `json.Unmarshal` that filled them, which made the shape invisible everywhere else: it could not be
 * referenced, could not be tested, and five providers meant five similar anonymous declarations that no
 * compiler related to each other.
 *
 * THE ADAPTER IS A PURE FUNCTION. `decode` parses bytes, `adapt` maps the parsed value onto the common
 * result. Separating them is what lets a provider's format be tested against a FIXTURE — the shape is the
 * thing that changes when a provider changes, and a test that needs the network cannot notice it.
 *
 * EACH PARSER IS DELIBERATELY STRICT ABOUT THE WRAPPER. All five answer through an MCP-style envelope whose
 * `content[0].text` holds either the payload or, for some, the payload AGAIN as a JSON string. That extra
 * layer is a property of the provider, not of the protocol, so it is handled here, once per provider, with
 * a comment saying which shape it is.
 */

// ---- MCP envelope ----

// mcpContent is the envelope every MCP tool result shares: an array of content blocks, each optionally
// carrying text. The payload lives in the first non-empty block.
type mcpContent struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

// firstText returns the first non-empty text block, or "" when there is none.
func (m mcpContent) firstText() string {
	for _, c := range m.Content {
		if c.Text != "" {
			return c.Text
		}
	}
	return ""
}

// ---- parallel ----

// parallelResult is one hit in Parallel's payload. `excerpts` is a list rather than a single string, so the
// adapter joins it — the provider returns several relevant passages per document.
type parallelResult struct {
	URL         string   `json:"url"`
	Title       string   `json:"title"`
	PublishDate string   `json:"publish_date"`
	Excerpts    []string `json:"excerpts"`
}

type parallelPayload struct {
	Results []parallelResult `json:"results"`
}

// adaptParallel maps Parallel's payload onto the common result.
func adaptParallel(p parallelPayload, count int) []searchResult {
	out := make([]searchResult, 0, len(p.Results))
	for _, r := range p.Results {
		if r.URL == "" {
			// A HIT WITH NO URL CANNOT BE OPENED, so it is not a search result. Dropping it is better than
			// emitting an entry the model will try to cite and the reader cannot follow.
			continue
		}
		out = append(out, searchResult{
			URL:       r.URL,
			Title:     r.Title,
			Content:   joinNonEmpty(r.Excerpts, "\n\n"),
			Provider:  "parallel",
			Published: r.PublishDate,
		})
		if len(out) >= count {
			break
		}
	}
	return out
}

// ---- tavily ----

type tavilyPayload struct {
	Results []struct {
		URL     string `json:"url"`
		Title   string `json:"title"`
		Content string `json:"content"`
	} `json:"results"`
}

func adaptTavily(p tavilyPayload, count int) []searchResult {
	out := make([]searchResult, 0, len(p.Results))
	for _, r := range p.Results {
		if r.URL == "" {
			continue
		}
		out = append(out, searchResult{
			URL: r.URL, Title: r.Title, Content: r.Content, Provider: "tavily",
		})
		if len(out) >= count {
			break
		}
	}
	return out
}

// ---- exa ----

// Exa does NOT answer with JSON: it returns pre-formatted text with `Title:`, `URL:`, `Published:` and
// `Highlights:` lines per block, separated by `---`. There is no struct to define, so `parseExaText` IS the
// adapter — but it is a pure function over a string, which is what makes it testable.

// ---- tinyfish ----

type tinyfishResult struct {
	URL      string `json:"url"`
	Title    string `json:"title"`
	Snippet  string `json:"snippet"`
	SiteName string `json:"site_name"`
}

type tinyfishPayload struct {
	Results []tinyfishResult `json:"results"`
}

func adaptTinyfish(p tinyfishPayload, count int) []searchResult {
	out := make([]searchResult, 0, len(p.Results))
	for _, r := range p.Results {
		if r.URL == "" {
			continue
		}
		out = append(out, searchResult{
			URL:      r.URL,
			Title:    r.Title,
			Content:  tinyfishContent(r.Snippet, r.SiteName),
			Provider: "tinyfish",
		})
		if len(out) >= count {
			break
		}
	}
	return out
}

// ---- firecrawl ----

type firecrawlPayload struct {
	Success bool `json:"success"`
	Data    struct {
		Web []struct {
			URL         string `json:"url"`
			Title       string `json:"title"`
			Description string `json:"description"`
		} `json:"web"`
	} `json:"data"`
}

func adaptFirecrawl(p firecrawlPayload, count int) []searchResult {
	out := make([]searchResult, 0, len(p.Data.Web))
	for _, r := range p.Data.Web {
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
	return out
}

// joinNonEmpty joins the parts that carry content, so an empty excerpt does not become a blank paragraph.
func joinNonEmpty(parts []string, sep string) string {
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			kept = append(kept, s)
		}
	}
	return strings.Join(kept, sep)
}
