package agent

import (
	"strings"
	"testing"
	"time"
)

var envNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

/**
 * THE INTRO RIDES IN <env>, AND IT IS TAGGED.
 *
 * A tag says what the text IS. "maintainer of repo acme/app" dropped into a paragraph could be read as
 * an instruction or as something the user said; inside <intro> it can only be a description of the
 * session itself.
 */
func TestEnvBlockCarriesTheIntro(t *testing.T) {
	block := BuildEnvBlockWithIntro("en", envNow, "maintainer of repo acme/app")
	if !strings.Contains(block, "<intro>maintainer of repo acme/app</intro>") {
		t.Fatalf("intro missing or untagged:\n%s", block)
	}
	// The date and the language directive stay: this is an addition, not a replacement.
	if !strings.Contains(block, "2026-11-04") {
		t.Errorf("the date is gone:\n%s", block)
	}
	if !strings.Contains(block, LanguageDirective("en")) {
		t.Errorf("the language directive is gone:\n%s", block)
	}
}

/**
 * NO INTRO MEANS NO TAG.
 *
 * An empty `<intro></intro>` invites a model to ask what it is supposed to be — the block must say
 * nothing rather than say nothing in a way that looks like something.
 */
func TestEnvBlockOmitsAnEmptyIntro(t *testing.T) {
	for _, intro := range []string{"", "   ", "\n"} {
		block := BuildEnvBlockWithIntro("en", envNow, intro)
		if strings.Contains(block, "<intro>") {
			t.Errorf("intro %q produced a tag:\n%s", intro, block)
		}
	}
}

// Spending: whitespace is trimmed rather than preserved inside the tag.
func TestEnvBlockTrimsTheIntro(t *testing.T) {
	block := BuildEnvBlockWithIntro("en", envNow, "  CI triage  ")
	if !strings.Contains(block, "<intro>CI triage</intro>") {
		t.Fatalf("not trimmed:\n%s", block)
	}
}

/**
 * THE INTRO AND THE PERMISSIONS COEXIST.
 *
 * They are appended by two different helpers and one replaces text inside the other's closing tag, so
 * a change to either can silently drop the other. This pins the composed result.
 */
func TestEnvBlockKeepsIntroAndPermissions(t *testing.T) {
	block := BuildEnvBlockAll("en", envNow, "CI triage", []GrantLine{
		{ExtID: "gitdev", Capability: "repo.read"},
	})
	if !strings.Contains(block, "<intro>CI triage</intro>") {
		t.Errorf("intro lost when permissions are present:\n%s", block)
	}
	if !strings.Contains(block, "gitdev: repo.read") {
		t.Errorf("permissions lost when an intro is present:\n%s", block)
	}
	// One closing tag, and it is last.
	if strings.Count(block, "</env>") != 1 || !strings.HasSuffix(block, "</env>") {
		t.Errorf("the block is malformed:\n%s", block)
	}
}
