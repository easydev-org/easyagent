package store

import "testing"

// TestCreateSessionAlwaysHasPreset pins the invariant: EVERY session row
// carries a non-empty preset, whatever path created it. An empty preset means
// "no system prompt", which is never what a caller wants — it was reachable
// through fork (inheriting an empty parent preset) and the subsession path.
func TestCreateSessionAlwaysHasPreset(t *testing.T) {
	d := newTestDB(t)

	cases := []struct {
		name   string
		preset string
	}{
		{"empty", ""},
		{"explicit", "reviewer"},
	}
	for _, c := range cases {
		if err := d.CreateSession("t", SessionInput{Name: c.name, Preset: c.preset}); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		row, ok, err := d.GetSession("t", c.name)
		if err != nil || !ok {
			t.Fatalf("%s: get: %v %v", c.name, ok, err)
		}
		if row.Preset == "" {
			t.Fatalf("%s: preset is EMPTY, want a default", c.name)
		}
		if c.preset == "" && row.Preset != DefaultPreset {
			t.Errorf("%s: preset = %q, want %q", c.name, row.Preset, DefaultPreset)
		}
		if c.preset != "" && row.Preset != c.preset {
			t.Errorf("%s: preset = %q, want %q (explicit must win)", c.name, row.Preset, c.preset)
		}
	}
}
