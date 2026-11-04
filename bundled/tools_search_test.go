package bundled

import (
	"strings"
	"testing"
)

/**
 * THE FAILURE NOTE IS THE POINT.
 *
 * Several backends are tried in order, so a degraded day produces FEWER results, not an error. Without
 * a note saying which ones did not answer, the caller reads a short list as "the web has little on
 * this" — the failure is invisible precisely when it matters.
 *
 * This is not hypothetical: with one backend rate-limited, a search returned 8 results and looked
 * complete while two of three sources had refused.
 */
func TestSearchOutcomeNamesTheProvidersThatFailed(t *testing.T) {
	out := renderSearchOutcome("golang", []searchResult{
		{URL: "https://go.dev/", Title: "The Go Programming Language", Content: "Go is...", Provider: "parallel"},
	}, []providerFailure{
		{Provider: "tavily", Reason: "HTTP 429: rate limited"},
		{Provider: "exa", Reason: "request failed: timeout"},
	}, "parallel")

	for _, want := range []string{"tavily", "rate limited", "exa", "timeout", "narrower"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("the note does not mention %q:\n%s", want, out.Content)
		}
	}
	// And the provider that answered is named too, so a reader knows where it came from.
	if !strings.Contains(out.Content, "via parallel") {
		t.Errorf("the answering provider is not named:\n%s", out.Content)
	}
}

// No failures means no note: a clean search must not carry a paragraph about nothing.
func TestSearchOutcomeHasNoNoteWithoutFailures(t *testing.T) {
	out := renderSearchOutcome("golang", []searchResult{
		{URL: "https://go.dev/", Provider: "parallel"},
	}, nil, "parallel")
	if strings.Contains(out.Content, "did not answer") {
		t.Fatalf("a clean search carried a failure note:\n%s", out.Content)
	}
}

// The structured payload carries the same facts, for anything reading it machine-wise.
func TestSearchOutcomeDataCarriesFailures(t *testing.T) {
	out := renderSearchOutcome("q", []searchResult{
		{URL: "https://a.example/", Provider: "parallel"},
	}, []providerFailure{{Provider: "tavily", Reason: "HTTP 500"}}, "parallel")

	meta, ok := out.Metadata.(map[string]any)
	if !ok {
		t.Fatalf("metadata = %T", out.Metadata)
	}
	failed, ok := meta["failed"].([]map[string]any)
	if !ok || len(failed) != 1 {
		t.Fatalf("failed = %#v", meta["failed"])
	}
	if failed[0]["provider"] != "tavily" || failed[0]["reason"] != "HTTP 500" {
		t.Fatalf("failed row = %#v", failed[0])
	}
	rows, _ := meta["results"].([]map[string]any)
	if len(rows) != 1 || rows[0]["provider"] != "parallel" {
		t.Fatalf("results = %#v", meta["results"])
	}
}

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
