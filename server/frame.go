package server

// Frame is the durable, cross-replica session-event wire shape.
type Frame struct {
	Event  string         `json:"event"`
	Params map[string]any `json:"params"`
	Eid    string         `json:"eid"`
	Seq    int64          `json:"seq"`
}
