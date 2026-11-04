package bundled

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/easydev-org/easyagent/agent"
)

const (
	webFetchMaxBytes       = 5 * 1024 * 1024
	webFetchDefaultTimeout = 30
	webFetchMaxTimeout     = 120
	webFetchUserAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
)

func acceptHeader(format string) string {
	switch format {
	case "markdown":
		return "text/markdown;q=1.0, text/x-markdown;q=0.9, text/plain;q=0.8, text/html;q=0.7, */*;q=0.1"
	case "text":
		return "text/plain;q=1.0, text/markdown;q=0.9, text/html;q=0.8, */*;q=0.1"
	case "html":
		return "text/html;q=1.0, application/xhtml+xml;q=0.9, text/plain;q=0.8, text/markdown;q=0.7, */*;q=0.1"
	}
	return "*/*"
}

func mimeFrom(contentType string) string {
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = contentType[:i]
	}
	return strings.ToLower(strings.TrimSpace(contentType))
}

func isTextualMime(mime string) bool {
	return mime == "" || strings.HasPrefix(mime, "text/") ||
		mime == "application/json" || strings.HasSuffix(mime, "+json") ||
		mime == "application/xml" || strings.HasSuffix(mime, "+xml") ||
		mime == "application/javascript" || mime == "application/x-javascript"
}

func isImageMime(mime string) bool {
	return strings.HasPrefix(mime, "image/") && mime != "image/svg+xml"
}

// webFetch fetches a URL and returns it as text / markdown / html.
func (e *Extension) webFetch(ctx context.Context, tenant, session string, args map[string]any) (agent.ToolOutcome, error) {
	locale := e.localeOf(tenant, session)
	rawURL := strings.TrimSpace(strArg(args, "url"))
	if rawURL == "" {
		return agent.ToolOutcome{}, &toolError{msg: tr(locale, "urlRequired", nil)}
	}
	format := strArg(args, "format")
	if format == "" {
		format = "markdown"
	}
	timeout := webFetchDefaultTimeout
	if f, ok := numArg(args, "timeout"); ok && f > 0 && f <= webFetchMaxTimeout {
		timeout = int(f)
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return agent.ToolOutcome{}, &toolError{msg: tr(locale, "fetchUrlScheme", nil)}
	}

	fetch := func(ua string) (string, string, error) {
		rctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(rctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return "", "", err
		}
		req.Header.Set("User-Agent", ua)
		req.Header.Set("Accept", acceptHeader(format))
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", "", err
		}
		defer resp.Body.Close()
		ct := resp.Header.Get("content-type")
		mime := mimeFrom(ct)
		if isImageMime(mime) {
			return "", "", &toolError{msg: tr(locale, "fetchUnsupportedImage", map[string]any{"mime": mime})}
		}
		if !isTextualMime(mime) {
			return "", "", &toolError{msg: tr(locale, "fetchUnsupportedFile", map[string]any{"mime": mime})}
		}
		buf, err := io.ReadAll(io.LimitReader(resp.Body, webFetchMaxBytes+1))
		if err != nil {
			return "", "", err
		}
		if len(buf) > webFetchMaxBytes {
			return "", "", &toolError{msg: tr(locale, "fetchTooLarge", map[string]any{"bytes": webFetchMaxBytes})}
		}
		return ct, string(buf), nil
	}

	ct, body, ferr := fetch(webFetchUserAgent)
	if ferr != nil {
		if _, isTool := ferr.(*toolError); !isTool {
			if ct2, body2, e2 := fetch("opencode"); e2 == nil {
				ct, body, ferr = ct2, body2, nil
			}
		}
	}
	if ferr != nil {
		return agent.ToolOutcome{}, ferr
	}
	output := convertHTML(body, ct, format)
	return agent.ToolOutcome{
		Content:  output,
		Metadata: map[string]any{"url": rawURL, "contentType": ct, "format": format},
	}, nil
}

// convertHTML converts HTML to text/markdown (plain regex-based; no deps).
func convertHTML(content, contentType, format string) string {
	if !strings.Contains(contentType, "text/html") {
		return content
	}
	switch format {
	case "markdown":
		return htmlToMarkdown(content)
	case "text":
		return htmlToText(content)
	default:
		return content
	}
}
