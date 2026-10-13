package agent

import (
	"strings"
	"testing"
	"time"
)

func TestI18nLocaleAndDirective(t *testing.T) {
	if got := NormalizeLocale("ZH_CN"); got != "zh-cn" {
		t.Errorf("NormalizeLocale = %q", got)
	}
	if got := NormalizeLocale(""); got != "en" {
		t.Errorf("empty locale = %q", got)
	}
	if !strings.Contains(LanguageDirective("zh-hans"), "中文") {
		t.Errorf("zh directive = %q", LanguageDirective("zh-hans"))
	}
	if !strings.Contains(LanguageDirective("en"), "English") {
		t.Errorf("en directive = %q", LanguageDirective("en"))
	}
	env := BuildEnvBlock("zh", time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
	if !strings.Contains(env, "2026-10-09") || !strings.Contains(env, "<env>") {
		t.Errorf("env block = %q", env)
	}
	// region fallback: zh-hans -> zh
	if v := PickLocalized(map[string]string{"zh": "你好"}, "zh-hans"); v != "你好" {
		t.Errorf("PickLocalized fallback = %q", v)
	}
	if v := ResolveLocale("", "zh", "en"); v != "zh" {
		t.Errorf("ResolveLocale config = %q", v)
	}
	if v := ResolveLocale("en", "zh", "en"); v != "en" {
		t.Errorf("session must win = %q", v)
	}
}

func TestEstimateTokensAndMask(t *testing.T) {
	// 8 ASCII chars -> 2 tokens
	if got := EstimateTokens("abcdefgh"); got != 2 {
		t.Errorf("ascii tokens = %d", got)
	}
	// 3 CJK chars -> ceil(3*1.5)=5? no: ceil(3/1.5)=2
	if got := EstimateTokens("你好吗"); got != 2 {
		t.Errorf("cjk tokens = %d", got)
	}
	if MaskSecret("short") != "****" {
		t.Errorf("short mask = %q", MaskSecret("short"))
	}
	if MaskSecret("abcdefghijkl") != "abcd****ijkl" {
		t.Errorf("long mask = %q", MaskSecret("abcdefghijkl"))
	}
}

func TestCompactionSplitAndFold(t *testing.T) {
	entries := []FoldEntry{
		{ID: "u1", Role: "user", Text: "first question"},
		{ID: "a1", Role: "assistant", Text: "first answer", ToolCalls: 1, ToolInput: 10, ToolResult: 20},
		{ID: "u2", Role: "user", Text: "second question"},
		{ID: "a2", Role: "assistant", Text: "second answer"},
	}
	// A large tail budget keeps everything verbatim.
	all := SplitScan(entries, 100000, 100000)
	if len(all.Tail) != 4 || len(all.Folded) != 0 {
		t.Errorf("all-verbatim split = %+v", all)
	}
	// A tiny tail budget folds the older turns; the tail still starts at a user.
	small := SplitScan(entries, 5, 100000)
	if len(small.Tail) == 0 || small.Tail[0].Role != "user" {
		t.Errorf("tail must start at a user prompt: %+v", small.Tail)
	}
	if len(small.Folded) == 0 {
		t.Errorf("expected a folded prefix: %+v", small)
	}
	// The fold renders a deterministic Q&A transcript with a tool-call note.
	qa := FoldQA(small.Folded, nil)
	if !strings.Contains(qa, "User: first question") || !strings.Contains(qa, "[1 tool calls]") {
		t.Errorf("fold QA = %q", qa)
	}
	// A compaction checkpoint is skipped entirely.
	withCheckpoint := append([]FoldEntry{{ID: "c", Role: CompactionRole, Text: "summary"}}, entries...)
	got := SplitScan(withCheckpoint, 100000, 100000)
	for _, e := range got.Tail {
		if e.Role == CompactionRole {
			t.Errorf("compaction checkpoint leaked into the tail")
		}
	}
}

func TestProviderCapability(t *testing.T) {
	if c, err := ParseCapability(""); err != nil || c != CapText {
		t.Errorf("empty capability = %v, %v", c, err)
	}
	if c, err := ParseCapability("IMAGE"); err != nil || c != CapImage {
		t.Errorf("IMAGE = %v, %v", c, err)
	}
	if _, err := ParseCapability("bogus"); err == nil {
		t.Error("bogus capability should error")
	}
	if err := ValidateAPIType(APITypeV4); err != nil {
		t.Errorf("v4 api type: %v", err)
	}
	if err := ValidateAPIType("anthropic"); err == nil {
		t.Error("only the v4 gateway api type is supported")
	}
	if !SupportsCapability(APITypeV4, CapVideo) {
		t.Error("v4 gateway should serve video")
	}
}

func TestModelRef(t *testing.T) {
	if p, m, ok := splitModelRef("gw/deepseek-v4.1-flash"); !ok || p != "gw" || m != "deepseek-v4.1-flash" {
		t.Errorf("split = %q %q %v", p, m, ok)
	}
	if _, _, ok := splitModelRef("bare-model"); ok {
		t.Error("a bare model id must not resolve")
	}
	if _, _, ok := splitModelRef("/leading"); ok {
		t.Error("leading slash must not resolve")
	}
	if got := ModelOf("gw/m"); got != "m" {
		t.Errorf("ModelOf = %q", got)
	}
}
