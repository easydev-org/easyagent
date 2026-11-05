package bundled

import (
	"encoding/json"
	"testing"
)

/**
 * THE ADAPTERS ARE TESTED AGAINST FIXTURES, NOT AGAINST THE NETWORK.
 *
 * The shape is what changes when a provider changes, and a test that needs a live endpoint cannot notice
 * it. These decode a recorded body, run the adapter, and assert the common result — so a provider that
 * renames `results` or nests one level deeper fails HERE, at build time, instead of silently producing
 * zero hits in production.
 */

func TestAdaptParallel(t *testing.T) {
	const body = `{
	  "results": [
	    {"url":"https://a.example/1","title":"First","publish_date":"2026-01-02",
	     "excerpts":["lead paragraph","second paragraph"]},
	    {"url":"https://a.example/2","title":"Second","excerpts":["only"]}
	  ]
	}`
	var p parallelPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	got := adaptParallel(p, 10)
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	if got[0].URL != "https://a.example/1" || got[0].Title != "First" {
		t.Errorf("first result = %+v", got[0])
	}
	// EXCERPTS ARE JOINED, because the provider returns several passages per document and the model needs
	// all of them to decide whether the hit is relevant.
	if got[0].Content != "lead paragraph\n\nsecond paragraph" {
		t.Errorf("content = %q, want the excerpts joined", got[0].Content)
	}
	if got[0].Published != "2026-01-02" {
		t.Errorf("published = %q", got[0].Published)
	}
	if got[0].Provider != "parallel" {
		t.Errorf("provider = %q", got[0].Provider)
	}
}

func TestAdaptTavily(t *testing.T) {
	const body = `{"results":[
	  {"url":"https://b.example/1","title":"T1","content":"body one"},
	  {"url":"","title":"no url","content":"must be dropped"}
	]}`
	var p tavilyPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	got := adaptTavily(p, 10)
	if len(got) != 1 {
		t.Fatalf("got %d results, want 1 (a hit with no url is not openable)", len(got))
	}
	if got[0].Content != "body one" || got[0].Provider != "tavily" {
		t.Errorf("result = %+v", got[0])
	}
}

func TestAdaptTinyfish(t *testing.T) {
	const body = `{"results":[
	  {"url":"https://c.example/1","title":"C1","snippet":"snip","site_name":"Example"},
	  {"url":"https://c.example/2","title":"C2","snippet":"snip2"}
	]}`
	var p tinyfishPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	got := adaptTinyfish(p, 10)
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	// The site name is folded into the content by tinyfishContent; the exact rendering is its own concern,
	// but it must not be dropped.
	if got[0].Content == "" {
		t.Error("content is empty; the snippet and site name were both lost")
	}
	if got[0].Provider != "tinyfish" {
		t.Errorf("provider = %q", got[0].Provider)
	}
}

func TestAdaptFirecrawl(t *testing.T) {
	const body = `{"success":true,"data":{"web":[
	  {"url":"https://d.example/1","title":"D1","description":"desc one"}
	]}}`
	var p firecrawlPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	got := adaptFirecrawl(p, 10)
	if len(got) != 1 || got[0].Content != "desc one" || got[0].Provider != "firecrawl" {
		t.Fatalf("result = %+v", got)
	}
}

/**
 * THE COUNT LIMIT IS APPLIED BY THE ADAPTER, not by the provider.
 *
 * A provider may ignore `count` or return more than asked; the caller asked for N and must receive at most
 * N, or the tool's own documentation is wrong.
 */
func TestAdaptersRespectCount(t *testing.T) {
	var p tavilyPayload
	p.Results = append(p.Results, struct {
		URL     string `json:"url"`
		Title   string `json:"title"`
		Content string `json:"content"`
	}{"https://x/1", "a", "b"})
	for i := 0; i < 5; i++ {
		p.Results = append(p.Results, struct {
			URL     string `json:"url"`
			Title   string `json:"title"`
			Content string `json:"content"`
		}{"https://x/" + string(rune('a'+i)), "t", "c"})
	}
	if got := adaptTavily(p, 2); len(got) != 2 {
		t.Errorf("got %d results, want 2", len(got))
	}
}

// The MCP envelope is shared, so it is parsed once. An envelope with no text block must be an error at the
// call site — not an empty result that looks like "the search found nothing".
func TestMCPContentFirstText(t *testing.T) {
	var empty mcpContent
	if got := empty.firstText(); got != "" {
		t.Errorf("empty envelope returned %q", got)
	}
	var filled mcpContent
	if err := json.Unmarshal([]byte(`{"content":[{"text":""},{"text":"payload"}]}`), &filled); err != nil {
		t.Fatal(err)
	}
	if got := filled.firstText(); got != "payload" {
		t.Errorf("firstText = %q, want payload", got)
	}
}
