package server

// Frame is the durable, cross-replica session-event wire shape.
//
// At comes from the PRODUCER, not the reader, and that is why it is here: a page refresh has no
// `seq` in memory, so it resumes from the last MESSAGE id it holds, and the server turns that into
// a point in time. Stamping the time at replay would make every frame look equally new, which is
// exactly the information the anchor needs.
type Frame struct {
	Event  string         `json:"event"`
	Params map[string]any `json:"params"`
	Eid    string         `json:"eid"`
	Seq    int64          `json:"seq"`
	// At is Unix milliseconds when the frame entered the stream (0 when unknown).
	At int64 `json:"at,omitempty"`
}
