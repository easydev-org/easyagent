package bundled

import (
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * EVERY TOOL DECLARES WHAT CALLING IT DOES.
 *
 * The declaration is the only thing that can tell a user "this one is harmless" from "this one deletes a
 * repository", and a missing or wrong value is UNDETECTABLE at runtime: nothing fails, the tool just runs
 * with whatever the client assumed. So it is required and validated at parse time.
 */
func TestEveryToolDeclaresAnEffect(t *testing.T) {
	m, err := ParseManifest()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Tools) == 0 {
		t.Fatal("no tools parsed")
	}
	for _, tl := range m.Tools {
		if tl.Effect == "" {
			t.Errorf("tool %q declares no effect", tl.Name)
			continue
		}
		if !tl.Effect.Valid() {
			t.Errorf("tool %q declares effect %q, which is not none|low|medium|high", tl.Name, tl.Effect)
		}
	}
}

// A manifest with a MISSING effect must fail to parse, not default to something harmless.
func TestParseRejectsAMissingEffect(t *testing.T) {
	_, err := parseManifestBytes([]byte(`
id: probe
tools:
  - name: dangerous
    description: does something
`))
	if err == nil {
		t.Fatal("a tool with no effect parsed successfully; it would be presented as harmless")
	}
	if !strings.Contains(err.Error(), "effect") {
		t.Errorf("the error should name the missing field: %v", err)
	}
}

// And an UNKNOWN value must fail too: a typo would otherwise read as a level nobody chose.
func TestParseRejectsAnUnknownEffect(t *testing.T) {
	_, err := parseManifestBytes([]byte(`
id: probe
tools:
  - name: t
    description: d
    effect: catastrophic
`))
	if err == nil {
		t.Fatal("an unknown effect parsed successfully")
	}
}

/**
 * A PERMISSION MAY NOT LOOK SAFER THAN THE TOOL IT UNLOCKS.
 *
 * `risk` and `effect` answer different questions — "how dangerous is granting this" versus "what does the
 * call do" — but they cannot contradict each other. A high-impact tool behind a `read`-labelled permission
 * is a prompt to approve something while being told it is harmless.
 */
func TestPermissionRiskCoversItsToolEffect(t *testing.T) {
	_, err := parseManifestBytes([]byte(`
id: probe
tools:
  - name: wipe
    description: destroys things
    effect: high
    requires: [wipe.all]
permissions:
  - key: wipe.all
    label: Wipe everything
    risk: read
`))
	if err == nil {
		t.Fatal("a high-effect tool behind a risk=read permission parsed successfully")
	}
	if !strings.Contains(err.Error(), "risk") {
		t.Errorf("the error should name the mismatch: %v", err)
	}
}

// A consistent declaration parses, so the rule above is not simply rejecting everything.
func TestConsistentRiskAndEffectParse(t *testing.T) {
	_, err := parseManifestBytes([]byte(`
id: probe
tools:
  - name: wipe
    description: destroys things
    effect: high
    requires: [wipe.all]
permissions:
  - key: wipe.all
    label: Wipe everything
    risk: destructive
`))
	if err != nil {
		t.Fatalf("a consistent manifest was rejected: %v", err)
	}
}

/**
 * THE LEVELS ORDER, AND ONLY THE TOP TWO ASK FOR CONFIRMATION.
 *
 * `low` touches only the caller's own state; interrupting every such act would train the user to click
 * through the dialog, which is how a confirmation stops carrying information.
 */
func TestEffectOrderingAndConfirmation(t *testing.T) {
	order := []agent.Effect{agent.EffectNone, agent.EffectLow, agent.EffectMedium, agent.EffectHigh}
	for i := 1; i < len(order); i++ {
		if order[i-1].Rank() >= order[i].Rank() {
			t.Errorf("%q should rank below %q", order[i-1], order[i])
		}
	}
	for i, e := range order {
		want := i >= 2
		if e.NeedsConfirmation() != want {
			t.Errorf("%q NeedsConfirmation = %v, want %v", e, e.NeedsConfirmation(), want)
		}
	}
}

// Closing someone else's pull request is `medium`, not `high`: it changes state others depend on, but it
// leaves something to revert to. This is the distinction the four levels exist for.
func TestMediumMeansRevertibleButShared(t *testing.T) {
	if agent.EffectMedium.Rank() <= agent.EffectLow.Rank() {
		t.Error("medium must rank above low: it touches state others depend on")
	}
	if agent.EffectHigh.Rank() <= agent.EffectMedium.Rank() {
		t.Error("high must rank above medium: it consumes the asset")
	}
	// Both ask for confirmation; neither is silent.
	if !agent.EffectMedium.NeedsConfirmation() || !agent.EffectHigh.NeedsConfirmation() {
		t.Error("both shared-impact levels should confirm")
	}
}
