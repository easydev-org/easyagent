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
	return BuildEnvBlockWithIntro(locale, now, "")
}

/**
 * BuildEnvBlockWithIntro is the turn's environment, plus the session's own description of itself.
 *
 * WHY <intro> IS AN ELEMENT AND NOT A SENTENCE. The model reads a tagged block, and a tag says what
 * the text IS: "maintainer of repo acme/app" in the middle of a paragraph could be mistaken for an
 * instruction or for something the user said. A tag also lets a reader skip it, and the block is
 * omitted entirely when the session has no intro — an empty "<intro></intro>" would prompt a model to
 * ask what it is supposed to be.
 *
 * IT GOES IN <env> BECAUSE IT CHANGES RARELY AND APPLIES TO EVERY TURN. Putting it in the system
 * prompt would mean rewriting that prompt on a role change, and `system_prompt` REPLACES the preset's
 * prompt wholesale — so a one-line edit would cost the user everything else in it.
 */
func BuildEnvBlockWithIntro(locale string, now time.Time, intro string) string {
	return BuildEnvBlockFull(locale, now, intro, nil)
}

/**
 * GroupEnv is what a session needs to know about the group it coordinates or belongs to.
 *
 * ONE LINE OF INTRO AND THE COORDINATOR, not the member list: membership is a tool call away and goes stale
 * between turns, while "what this group is for" is the thing a woken coordinator cannot recover by asking.
 */
type GroupEnv struct {
	Name        string
	Intro       string
	Coordinator string
}

/**
 * BuildEnvBlockFull is the turn's environment with everything the model knows about itself.
 *
 * THE GROUP SECTION EXISTS BECAUSE A GROUP'S PURPOSE IS NOT IN THE TRANSCRIPT. A coordinator woken by a group
 * message gets the message and the member ids; without the group's own intro it has to infer its job from the
 * names of the sessions it is writing to, which is exactly the guess that produced a coordinator answering a
 * question meant for the group. `Group.Coordinator` says which session's answer the reader expects.
 */
func BuildEnvBlockFull(locale string, now time.Time, intro string, groups []GroupEnv) string {
	lines := []string{
		"<env>",
		"  Today's date: " + now.UTC().Format("2006-01-02"),
		"  " + LanguageDirective(locale),
	}
	if intro = strings.TrimSpace(intro); intro != "" {
		lines = append(lines, "  <intro>"+intro+"</intro>")
	}
	if len(groups) > 0 {
		/**
		 * EVERY GROUP THE SESSION COORDINATES, not just one.
		 *
		 * A session may coordinate any number of groups now, and the one it is answering right now is decided by
		 * the message that woke it — which names its group. What it cannot derive is the SET: without this list it
		 * would not know that a second group exists, could not tell which group a task belongs to, and would have
		 * to guess when a tool asks it to choose.
		 *
		 * EACH ENTRY NAMES ITS OWN GROUP, so a model reading a woken message can match the two.
		 */
		lines = append(lines, "  <groups>")
		for _, g := range groups {
			if g.Name == "" {
				continue
			}
			lines = append(lines, "    <group>")
			lines = append(lines, "      <name>"+g.Name+"</name>")
			if purpose := strings.TrimSpace(g.Intro); purpose != "" {
				lines = append(lines, "      <purpose>"+purpose+"</purpose>")
			}
			if g.Coordinator != "" {
				lines = append(lines, "      <coordinator>"+g.Coordinator+"</coordinator>")
			}
			lines = append(lines, "    </group>")
		}
		lines = append(lines, "  </groups>")
	}
	lines = append(lines, "</env>")
	return strings.Join(lines, "\n")
}

// BuildEnvBlockWithGrants appends what this session is allowed to do.
//
// WHY in the prompt: without it the model learns its limits by trying and being refused,
// which costs a turn per discovery and looks to the user like the agent flailing. Naming
// the permissions up front lets it plan within them — and lets it ask for a grant instead
// of retrying.
//
// An empty grant list is stated explicitly rather than omitted: "you hold no permissions"
// is information, and a model that is not told will assume it can do anything.
func BuildEnvBlockWithGrants(locale string, now time.Time, grants []GrantLine) string {
	return BuildEnvBlockAll(locale, now, "", grants)
}

// BuildEnvBlockAll is the full block: date, language, the session's intro, the group, and the permissions.
func BuildEnvBlockAll(locale string, now time.Time, intro string, grants []GrantLine) string {
	return BuildEnvBlockWithGroup(locale, now, intro, nil, grants)
}

// BuildEnvBlockWithGroup is BuildEnvBlockAll plus what the session needs to know about its group.
func BuildEnvBlockWithGroup(locale string, now time.Time, intro string, groups []GroupEnv, grants []GrantLine) string {
	block := BuildEnvBlockFull(locale, now, intro, groups)
	if len(grants) == 0 {
		// Nothing granted: say so. Silence reads as "unrestricted" to a model.
		zh := primaryOf(NormalizeLocale(locale)) == "zh"
		note := "  This session holds no permissions: write operations will be refused until one is granted."
		if zh {
			note = "  本会话没有任何授权：在获得授权前，写操作会被拒绝。"
		}
		return strings.Replace(block, "</env>", note+"\n</env>", 1)
	}

	zh := primaryOf(NormalizeLocale(locale)) == "zh"
	head := "  This session's permissions (anything else will be refused):"
	if zh {
		head = "  本会话的授权（其余操作会被拒绝）："
	}
	lines := make([]string, 0, len(grants)+3)
	lines = append(lines, "  <permissions>", head)
	for _, g := range grants {
		line := "    " + g.ExtID + ": " + g.Capability
		if g.Params != "" {
			line += " (" + g.Params + ")"
		}
		lines = append(lines, line)
	}
	lines = append(lines, "  </permissions>")
	return strings.Replace(block, "</env>", strings.Join(lines, "\n")+"\n</env>", 1)
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
