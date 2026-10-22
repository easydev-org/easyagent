package store

import "testing"

// TestReplacePartsKeepsLateParts is the regression test for a tool card with an
// empty Content area.
//
// A step's parts arrive in waves: reasoning, then text, then the tool call, then
// its result. They used to be written once, guarded by "is the message row new?" —
// so everything after the first wave was dropped, and tool_results (always last)
// never reached the database. The console then rendered an empty Content block
// while the result was sitting in the event stream.
func TestReplacePartsKeepsLateParts(t *testing.T) {
	d := newTestDB(t)
	if _, err := d.InsertMessage("t1", "m1", "assistant", nil, ""); err != nil {
		t.Fatal(err)
	}

	// First wave: reasoning + a tool call.
	if err := d.ReplaceParts("t1", "m1", []Part{
		{Type: "reasoning", Data: `{"text":"thinking"}`},
		{Type: "tool", Data: `{"id":"c1","name":"sandbox-job-start"}`},
	}); err != nil {
		t.Fatal(err)
	}

	// Second wave: the result lands later.
	if err := d.ReplaceParts("t1", "m1", []Part{
		{Type: "reasoning", Data: `{"text":"thinking"}`},
		{Type: "tool", Data: `{"id":"c1","name":"sandbox-job-start"}`},
		{Type: "tool_result", Data: `{"tool_use_id":"c1","content":"tick1\ntick2"}`},
	}); err != nil {
		t.Fatal(err)
	}

	parts, err := d.PartsByMessages("t1", []string{"m1"})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, p := range parts {
		kinds = append(kinds, p.Type)
	}
	if len(kinds) != 3 || kinds[2] != "tool_result" {
		t.Fatalf("parts = %v, want the late tool_result to survive", kinds)
	}
	// And no duplicates: replacing, not appending.
	if kinds[0] != "reasoning" || kinds[1] != "tool" {
		t.Fatalf("parts = %v, want a single copy of each in order", kinds)
	}
}
