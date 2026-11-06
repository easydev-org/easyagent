package agent

import "testing"

func TestReasoningVariants(t *testing.T) {
	/**
	 * EVERY MODEL OFFERS THE SAME LEVELS, including one that does not reason.
	 *
	 * The picker used to be conditional on a per-model flag, which made the FIELD conditional — the profile page
	 * had to fetch a catalogue before it knew whether to draw a control, and the page grew a row when the answer
	 * arrived. A variant is a value merged into the request body; whether the provider acts on it is neither
	 * knowable here nor this layer's business.
	 */
	if got := VariantsFor(false); len(got) != len(ReasoningLevels) {
		t.Errorf("a non-reasoning model offered %d variants, want %d", len(got), len(ReasoningLevels))
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
