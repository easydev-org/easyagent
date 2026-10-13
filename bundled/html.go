package bundled

import (
	"strings"

	"golang.org/x/net/html"
)

// skipTags are never rendered (script/style/etc.).
var skipTags = map[string]bool{
	"script": true, "style": true, "noscript": true,
	"iframe": true, "object": true, "embed": true, "head": true,
}

// blockTags end a line in the extracted text.
var blockTags = map[string]bool{
	"p": true, "div": true, "section": true, "article": true, "header": true,
	"footer": true, "ul": true, "ol": true, "li": true, "table": true,
	"tr": true, "h1": true, "h2": true, "h3": true, "h4": true, "h5": true,
	"h6": true, "br": true, "blockquote": true, "pre": true,
}

// htmlToText extracts plain text with golang.org/x/net/html, collapsing
// whitespace (the TS `extractTextFromHTML`).
func htmlToText(s string) string {
	doc, err := html.Parse(strings.NewReader(s))
	if err != nil {
		return collapseWS(stripAllTags(s))
	}
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && skipTags[n.Data] {
			return
		}
		if n.Type == html.TextNode {
			b.WriteString(n.Data)
			return
		}
		if n.Type == html.ElementNode && blockTags[n.Data] {
			b.WriteByte(' ')
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
		if n.Type == html.ElementNode && blockTags[n.Data] {
			b.WriteByte(' ')
		}
	}
	walk(doc)
	return collapseWS(b.String())
}

// htmlToMarkdown converts a subset of HTML to markdown using the parsed tree
// (a pragmatic replacement for turndown, no external JS).
func htmlToMarkdown(s string) string {
	doc, err := html.Parse(strings.NewReader(s))
	if err != nil {
		return collapseWS(stripAllTags(s))
	}
	var b strings.Builder
	writeMarkdown(&b, doc, 0)
	return collapseBlankLines(b.String())
}

func writeMarkdown(b *strings.Builder, n *html.Node, listDepth int) {
	if n.Type == html.ElementNode && skipTags[n.Data] {
		return
	}
	if n.Type == html.TextNode {
		b.WriteString(collapseInline(n.Data))
		return
	}
	if n.Type != html.ElementNode {
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			writeMarkdown(b, c, listDepth)
		}
		return
	}
	switch n.Data {
	case "h1", "h2", "h3", "h4", "h5", "h6":
		level := int(n.Data[1] - '0')
		b.WriteString("\n\n" + strings.Repeat("#", level) + " ")
		writeChildren(b, n, listDepth)
		b.WriteString("\n\n")
	case "p", "div", "section", "article", "blockquote", "pre":
		b.WriteString("\n\n")
		writeChildren(b, n, listDepth)
		b.WriteString("\n\n")
	case "br":
		b.WriteString("\n")
	case "ul", "ol":
		b.WriteString("\n")
		writeChildren(b, n, listDepth+1)
		b.WriteString("\n")
	case "li":
		b.WriteString("\n" + strings.Repeat("  ", listDepth-1) + "- ")
		writeChildren(b, n, listDepth)
	case "a":
		href := attr(n, "href")
		if href == "" {
			writeChildren(b, n, listDepth)
			return
		}
		b.WriteString("[")
		writeChildren(b, n, listDepth)
		b.WriteString("](" + href + ")")
	case "strong", "b":
		b.WriteString("**")
		writeChildren(b, n, listDepth)
		b.WriteString("**")
	case "em", "i":
		b.WriteString("*")
		writeChildren(b, n, listDepth)
		b.WriteString("*")
	case "code":
		b.WriteString("`")
		writeChildren(b, n, listDepth)
		b.WriteString("`")
	case "img":
		alt, src := attr(n, "alt"), attr(n, "src")
		b.WriteString("![" + alt + "](" + src + ")")
	default:
		writeChildren(b, n, listDepth)
	}
}

func writeChildren(b *strings.Builder, n *html.Node, listDepth int) {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		writeMarkdown(b, c, listDepth)
	}
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func collapseWS(s string) string {
	return strings.TrimSpace(strings.Join(strings.Fields(s), " "))
}

// collapseInline trims but keeps interior spacing (a text node).
func collapseInline(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func collapseBlankLines(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	blank := false
	for _, ln := range lines {
		ln = strings.TrimRight(ln, " \t")
		if strings.TrimSpace(ln) == "" {
			if blank {
				continue
			}
			blank = true
			out = append(out, "")
			continue
		}
		blank = false
		out = append(out, ln)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// stripAllTags is the fallback when parsing fails.
func stripAllTags(s string) string {
	var b strings.Builder
	depth := 0
	for _, r := range s {
		switch r {
		case '<':
			depth++
		case '>':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}
