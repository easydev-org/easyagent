package store

import "testing"

/**
 * THE BACKFILL NUMBERS MESSAGES THAT PREDATE `seq`.
 *
 * AutoMigrate adds the column and nothing else, so every existing row would start at 0 — and a
 * positional read filtering on seq would then treat those messages as "before the beginning" and
 * skip them. History would look SHORTER than it is, with no error anywhere.
 *
 * The test reproduces that state directly: insert with seq forced to 0, then run Migrate and check
 * the numbers came out as positions.
 */
func TestBackfillNumbersPreexistingMessages(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t", SessionInput{Name: "s", Model: "m"}); err != nil {
		t.Fatal(err)
	}
	// Build a 3-message chain, then WIPE the seqs to imitate rows written before the column existed.
	for i, id := range []string{"a", "b", "c"} {
		var prev *string
		if i > 0 {
			prev = ptr([]string{"a", "b", "c"}[i-1])
		}
		if _, err := d.InsertMessage("t", id, "user", prev, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.AppendMessageTip("t", "s", "c"); err != nil {
		t.Fatal(err)
	}
	if err := d.g.Model(&Message{}).Where("tenant = ?", "t").Update("seq", 0).Error; err != nil {
		t.Fatal(err)
	}

	if err := d.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	for i, id := range []string{"a", "b", "c"} {
		m, ok, err := d.GetMessage("t", id)
		if err != nil || !ok {
			t.Fatalf("%s: %v", id, err)
		}
		if m.Seq != int64(i+1) {
			t.Errorf("%s seq = %d, want %d", id, m.Seq, i+1)
		}
	}
}

// Running it twice must not change anything: a normal boot does one cheap count.
func TestBackfillIsIdempotent(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t", SessionInput{Name: "s", Model: "m"}); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b"} {
		var prev *string
		if i > 0 {
			prev = ptr("a")
		}
		if _, err := d.InsertMessage("t", id, "user", prev, ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.AppendMessageTip("t", "s", "b"); err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	m, _, _ := d.GetMessage("t", "a")
	if m.Seq != 1 {
		t.Fatalf("a second migrate renumbered: %d", m.Seq)
	}
}

/**
 * MESSAGES NO TIP CAN REACH ARE NOT NUMBERED.
 *
 * An undone message is unreachable from every session, so numbering it would make a seq range
 * include messages no conversation contains. The backfill walks tips, so it leaves them alone.
 */
func TestBackfillSkipsAbandonedMessages(t *testing.T) {
	d := newTestDB(t)
	if err := d.CreateSession("t", SessionInput{Name: "s", Model: "m"}); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b", "c"} {
		var prev *string
		if i > 0 {
			prev = ptr([]string{"a", "b", "c"}[i-1])
		}
		if _, err := d.InsertMessage("t", id, "user", prev, ""); err != nil {
			t.Fatal(err)
		}
	}
	// Undo to "b": "c" is now unreachable.
	if _, err := d.AppendMessageTip("t", "s", "b"); err != nil {
		t.Fatal(err)
	}
	if err := d.g.Model(&Message{}).Where("tenant = ?", "t").Update("seq", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(); err != nil {
		t.Fatal(err)
	}
	c, _, _ := d.GetMessage("t", "c")
	if c.Seq != 0 {
		t.Fatalf("an unreachable message was numbered: %d", c.Seq)
	}
	a, _, _ := d.GetMessage("t", "a")
	if a.Seq != 1 {
		t.Fatalf("reachable message a seq = %d, want 1", a.Seq)
	}
}
