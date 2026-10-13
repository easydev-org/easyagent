package agent

import (
	"strings"
	"time"
)

// NormalizeLocale lowercases and unifies separators ("zh_CN"/"ZH" -> "zh-cn").
// An empty locale becomes "en".
func NormalizeLocale(locale string) string {
	l := strings.ToLower(strings.TrimSpace(locale))
	if l == "" {
		return "en"
	}
	return strings.Replace(l, "_", "-", 1)
}

func primaryOf(tag string) string {
	if i := strings.IndexByte(tag, '-'); i >= 0 {
		return tag[:i]
	}
	return tag
}

// LanguageDirective is the language instruction injected into every turn's
// <env> block, so the model always answers and reasons in the effective agent
// language regardless of the preset's system prompt.
func LanguageDirective(locale string) string {
	switch primaryOf(NormalizeLocale(locale)) {
	case "zh":
		return "无论用户使用什么语言，你都必须始终用中文回复，并用中文进行思考。"
	default:
		return "Regardless of the language the user writes in, you must always reply and reason in English."
	}
}

// BuildEnvBlock is the per-turn <env> block appended to the model's system
// prompt (always carries the language directive).
func BuildEnvBlock(locale string, now time.Time) string {
	return strings.Join([]string{
		"<env>",
		"  Today's date: " + now.UTC().Format("2006-01-02"),
		"  " + LanguageDirective(locale),
		"</env>",
	}, "\n")
}

// PickLocalized resolves a value from a locale map: exact match -> primary-
// language fallback -> "" (so the caller falls back to its default).
func PickLocalized(m map[string]string, locale string) string {
	if len(m) == 0 {
		return ""
	}
	tag := NormalizeLocale(locale)
	if v, ok := m[tag]; ok && v != "" {
		return v
	}
	prim := primaryOf(tag)
	if v, ok := m[prim]; ok && v != "" {
		return v
	}
	for k, v := range m {
		if primaryOf(NormalizeLocale(k)) == prim && v != "" {
			return v
		}
	}
	return ""
}

// ResolveLocale picks the effective locale: session -> tenant config -> env
// default ("en"). The session value wins so a pinned language is stable.
func ResolveLocale(session, config, fallback string) string {
	if session != "" {
		return NormalizeLocale(session)
	}
	if config != "" {
		return NormalizeLocale(config)
	}
	return NormalizeLocale(fallback)
}
