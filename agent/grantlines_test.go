package agent

import (
	"strings"
	"testing"
	"time"
)

func TestFormatGrantParams(t *testing.T) {
	cases := map[string]string{
		`{"repo":"agentdev/*"}`: "repo: agentdev/*",
		`{"b":"2","a":"1"}`:     "a: 1, b: 2", // sorted, so the prompt is stable
		``:                      "",
		`{not json`:             `{not json`, // shown raw rather than hidden
		`{"n":3}`:               "n: 3",
	}
	for in, want := range cases {
		if got := formatGrantParams(in); got != want {
			t.Errorf("formatGrantParams(%q) = %q, want %q", in, got, want)
		}
	}
}

// A session with permissions gets them listed; the model then plans within them instead of
// discovering its limits by being refused.
func TestEnvBlockListsGrants(t *testing.T) {
	grants := []GrantLine{
		{ExtID: "gitea", Capability: "mr.merge", Params: "repo: agentdev/*"},
		{ExtID: "sandbox", Capability: "exec"},
	}
	got := BuildEnvBlockWithGrants("en", time.Now(), grants)
	if !strings.Contains(got, "gitea: mr.merge (repo: agentdev/*)") {
		t.Errorf("merge grant missing:\n%s", got)
	}
	if !strings.Contains(got, "sandbox: exec") {
		t.Errorf("boolean grant missing:\n%s", got)
	}
	// The env block must still be well formed: a malformed tag confuses the model about
	// where the instructions end.
	if !strings.Contains(got, "</env>") {
		t.Errorf("env block lost its closing tag:\n%s", got)
	}
}

// No permissions must be STATED. Silence reads as "unrestricted", and the model would then
// try operations that are guaranteed to be refused.
func TestEnvBlockStatesNoGrants(t *testing.T) {
	got := BuildEnvBlockWithGrants("en", time.Now(), nil)
	if !strings.Contains(got, "no permissions") {
		t.Errorf("an empty grant list must be stated, not omitted:\n%s", got)
	}

	zh := BuildEnvBlockWithGrants("zh-CN", time.Now(), nil)
	if !strings.Contains(zh, "没有任何授权") {
		t.Errorf("the note should follow the session locale:\n%s", zh)
	}
}

func TestEnvBlockGrantsFollowLocale(t *testing.T) {
	grants := []GrantLine{{ExtID: "gitea", Capability: "mr.merge"}}
	zh := BuildEnvBlockWithGrants("zh-CN", time.Now(), grants)
	if !strings.Contains(zh, "本会话的授权") {
		t.Errorf("zh heading missing:\n%s", zh)
	}
}
