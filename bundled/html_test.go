package bundled

import (
	"strings"
	"testing"
)

func TestHTMLToText(t *testing.T) {
	h := `<html><head><title>x</title><style>p{color:red}</style></head>
<body><h1>Title</h1><p>Hello <b>world</b></p><script>alert(1)</script></body></html>`
	got := htmlToText(h)
	if strings.Contains(got, "alert") || strings.Contains(got, "color:red") {
		t.Errorf("script/style leaked into text: %q", got)
	}
	if !strings.Contains(got, "Title") || !strings.Contains(got, "Hello world") {
		t.Errorf("text = %q", got)
	}
}

func TestHTMLToMarkdown(t *testing.T) {
	h := `<h2>Head</h2><p>See <a href="https://e.com">link</a> and <code>x</code>.</p>
<ul><li>one</li><li>two</li></ul><script>bad()</script>`
	got := htmlToMarkdown(h)
	if strings.Contains(got, "bad()") {
		t.Errorf("script leaked: %q", got)
	}
	for _, want := range []string{"## Head", "[link](https://e.com)", "`x`", "- one", "- two"} {
		if !strings.Contains(got, want) {
			t.Errorf("markdown missing %q:\n%s", want, got)
		}
	}
}
