package main

import (
	"testing"
)

// Ported from the TS agent's config.test.ts: env parsing defaults, bounds and
// list splitting for the process configuration.

func TestEnvIntDefaultsAndBounds(t *testing.T) {
	cases := []struct {
		val  string
		def  int
		want int
	}{
		{"", 8081, 8081},           // unset -> default
		{"9090", 8081, 9090},       // explicit
		{"not-a-port", 8081, 8081}, // bad -> default
		{"99999", 8081, 99999},     // parsed (envInt has no upper bound)
	}
	for _, c := range cases {
		t.Setenv("EASYBUS_TEST_INT", c.val)
		if got := envInt("EASYBUS_TEST_INT", c.def); got != c.want {
			t.Errorf("envInt(%q, %d) = %d, want %d", c.val, c.def, got, c.want)
		}
	}
}

func TestEnvFloatDefaults(t *testing.T) {
	t.Setenv("EASYBUS_TEST_FLOAT", "")
	if got := envFloat("EASYBUS_TEST_FLOAT", 0.9); got != 0.9 {
		t.Errorf("unset float = %v, want 0.9", got)
	}
	t.Setenv("EASYBUS_TEST_FLOAT", "0.75")
	if got := envFloat("EASYBUS_TEST_FLOAT", 0.9); got != 0.75 {
		t.Errorf("explicit float = %v, want 0.75", got)
	}
	t.Setenv("EASYBUS_TEST_FLOAT", "bogus")
	if got := envFloat("EASYBUS_TEST_FLOAT", 0.9); got != 0.9 {
		t.Errorf("bad float = %v, want the default 0.9", got)
	}
}

// splitList trims blanks and drops empties (DISABLED_TOOLS parsing).
func TestSplitList(t *testing.T) {
	got := splitList("bundled.subsession-create, bundled.mail-send ,,")
	want := []string{"bundled.subsession-create", "bundled.mail-send"}
	if len(got) != len(want) {
		t.Fatalf("splitList = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("splitList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if splitList("") != nil {
		t.Error("empty string should yield nil")
	}
	if splitList("  ") != nil {
		t.Error("whitespace-only should yield nil")
	}
}
