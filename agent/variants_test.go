package agent

import "testing"

func TestReasoningVariants(t *testing.T) {
	if VariantsFor(false) != nil {
		t.Error("a non-reasoning model must offer no variants")
	}
	vs := VariantsFor(true)
	if len(vs) != len(ReasoningLevels) {
		t.Fatalf("variants = %+v", vs)
	}
	body, ok := ResolveVariant("high")
	if !ok || body["reasoning"] != "high" {
		t.Errorf("resolve high = %+v, %v", body, ok)
	}
	if _, ok := ResolveVariant(""); ok {
		t.Error("empty variant must not resolve")
	}
	if _, ok := ResolveVariant("bogus"); ok {
		t.Error("unknown variant must not resolve")
	}
}

/**
 * THE PICKER OFFERS FOUR CHOICES: DEFAULT, LOW, MEDIUM, HIGH.
 *
 * "Default" is the ABSENCE of a choice, so it is not in the list — the empty id is what the UI sends for it.
 * The levels below span what a person actually reasons about: cheap, balanced, thorough. The wire format
 * accepts more (none/minimal/xhigh/max), but offering them asks the user to choose between settings whose
 * difference no one can predict and whose effect they cannot check, and every extra row buries the three that
 * matter.
 */
func TestReasoningLevelsAreTheThreeThatMatter(t *testing.T) {
	want := []string{"low", "medium", "high"}
	if len(ReasoningLevels) != len(want) {
		t.Fatalf("levels = %v, want %v", ReasoningLevels, want)
	}
	for i, lvl := range want {
		if ReasoningLevels[i] != lvl {
			t.Errorf("level %d = %q, want %q", i, ReasoningLevels[i], lvl)
		}
	}
	// Every offered level must still resolve to a request fragment.
	for _, lvl := range ReasoningLevels {
		body, ok := ResolveVariant(lvl)
		if !ok || body["reasoning"] != lvl {
			t.Errorf("resolve %q = %+v, %v", lvl, body, ok)
		}
	}
	// A level that is no longer offered must not resolve: a session still carrying one would otherwise send a
	// body nobody can choose any more.
	if _, ok := ResolveVariant("xhigh"); ok {
		t.Error("a withdrawn level still resolves")
	}
	if _, ok := ResolveVariant("none"); ok {
		t.Error(`"none" is not a level; it is the absence of one`)
	}
}
