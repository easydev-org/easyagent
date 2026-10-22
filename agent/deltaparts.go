package agent

// deltaParts accumulates a step's streamed text, grouped by PART id.
//
// WHY this exists: deltas were stored as ONE PART PER FRAME. A step that streamed
// 163 reasoning fragments was persisted as 163 separate reasoning parts — so the
// console showed a new block per fragment. The text was never wrong; the grouping
// was, and the console was faithfully rendering what storage said.
//
// The grouping key is the delta's `id`, the same key the live client uses: deltas
// sharing an id are one part, a new id starts a new part. A delta with no id
// (older producers) joins the current part, which keeps output readable instead of
// exploding it.
type deltaParts struct {
	ids   []string
	texts []string
}

// add folds one delta in.
func (d *deltaParts) add(id, text string) {
	if text == "" {
		return
	}
	// An id-less delta belongs to the part being built: there is nothing to
	// distinguish it from the previous frame, and starting a new part on every
	// frame is exactly the bug this type exists to prevent.
	if len(d.texts) > 0 && (id == "" || d.ids[len(d.ids)-1] == id) {
		d.texts[len(d.texts)-1] += text
		return
	}
	d.ids = append(d.ids, id)
	d.texts = append(d.texts, text)
}

// Texts returns the parts in order.
func (d *deltaParts) Texts() []string { return d.texts }
