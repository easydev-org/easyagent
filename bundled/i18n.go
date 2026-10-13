package bundled

import (
	"fmt"
	"strings"
)

func fmtSprint(v any) string { return fmt.Sprintf("%v", v) }

// tr is a minimal localized-string lookup mirroring the TS `tr` helper. The
// bundled tools emit a small fixed set of strings; Chinese when the locale is
// zh*, English otherwise. Format params are substituted for `{name}` tokens.
func tr(locale, key string, params map[string]any) string {
	zh := strings.HasPrefix(strings.ToLower(locale), "zh")
	tmpl := ""
	if zh {
		tmpl = zhStrings[key]
	}
	if tmpl == "" {
		tmpl = enStrings[key]
	}
	if tmpl == "" {
		tmpl = key
	}
	for k, v := range params {
		tmpl = strings.ReplaceAll(tmpl, "{"+k+"}", toString(v))
	}
	return tmpl
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return strings.TrimSpace(strings.Trim(strings.ReplaceAll(strings.TrimSpace(sprint(v)), "\n", " "), `"`))
}

func sprint(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmtSprint(v)
	}
}

var enStrings = map[string]string{
	"timeWaitInterrupted":        "the wait was interrupted",
	"timeWaited":                 "Waited {n} seconds.",
	"urlRequired":                "url is required",
	"codeRequired":               "code is required",
	"promptRequired":             "prompt is required",
	"textRequired":               "text is required",
	"queryRequired":              "query is required",
	"braveKeyMissing":            "brave_api_key is not configured",
	"notAudio":                   "file {code} is not audio (mime: {mime})",
	"transcriptionNotConfigured": "model.transcription is not configured",
	"transcriptionEmpty":         "transcription returned no text",
	"modelNotConfigured":         "{cfg} is not configured (capability {capability})",
	"modelResolveFailed":         "could not resolve {cfg}: {error}",
	"generatedImages":            "Generated {n} image(s) with {model}.",
	"editedImages":               "Edited image {ref} into {n} image(s) with {model}.",
	"generatedVideos":            "Generated {n} video(s) with {model}.",
	"synthesized":                "Synthesized {kib} KiB of {mime} with {model} (file:{code}).",
	"synthesizedClone":           "Cloned voice from {ref} and synthesized {kib} KiB of {mime} with {model} (file:{code}).",
	"fileLine":                   "- file:{code} ({mime}, {bytes} bytes)",
	"transcribed":                "Transcribed {code} with {model}.",
	"fetchUrlScheme":             "only http/https URLs are supported",
	"fetchUnsupportedImage":      "unsupported image content type: {mime}",
	"fetchUnsupportedFile":       "unsupported content type: {mime}",
	"fetchTooLarge":              "response exceeds {bytes} bytes",
	"braveNoResults":             "No search results found. Please try a different query.",
	"braveTooLarge":              "response exceeds {bytes} bytes",
	"braveHttp":                  "brave search failed: {status}",
	"braveFailed":                "web search failed: {detail}",
	"todosWritten":               "Updated the session todo list ({n} items).",
}

var zhStrings = map[string]string{
	"timeWaitInterrupted":        "等待被中断",
	"timeWaited":                 "已等待 {n} 秒。",
	"urlRequired":                "缺少 url",
	"codeRequired":               "缺少 code",
	"promptRequired":             "缺少 prompt",
	"textRequired":               "缺少 text",
	"queryRequired":              "缺少 query",
	"braveKeyMissing":            "未配置 brave_api_key",
	"notAudio":                   "文件 {code} 不是音频（mime：{mime}）",
	"transcriptionNotConfigured": "未配置 model.transcription",
	"transcriptionEmpty":         "转写结果为空",
	"modelNotConfigured":         "未配置 {cfg}（能力 {capability}）",
	"modelResolveFailed":         "无法解析 {cfg}：{error}",
	"generatedImages":            "已用 {model} 生成 {n} 张图片。",
	"editedImages":               "已用 {model} 将图片 {ref} 编辑为 {n} 张。",
	"generatedVideos":            "已用 {model} 生成 {n} 个视频。",
	"synthesized":                "已用 {model} 合成 {kib} KiB 的 {mime}（file:{code}）。",
	"synthesizedClone":           "已参照 {ref} 克隆音色并用 {model} 合成 {kib} KiB 的 {mime}（file:{code}）。",
	"fileLine":                   "- file:{code}（{mime}，{bytes} 字节）",
	"transcribed":                "已用 {model} 转写 {code}。",
	"fetchUrlScheme":             "仅支持 http/https URL",
	"fetchUnsupportedImage":      "不支持的图片类型：{mime}",
	"fetchUnsupportedFile":       "不支持的内容类型：{mime}",
	"fetchTooLarge":              "响应超过 {bytes} 字节",
	"braveNoResults":             "未找到搜索结果，请换个关键词。",
	"braveTooLarge":              "响应超过 {bytes} 字节",
	"braveHttp":                  "brave 搜索失败：{status}",
	"braveFailed":                "网页搜索失败：{detail}",
	"todosWritten":               "已更新会话待办列表（{n} 项）。",
}
