package store

// DefaultPreset is the built-in fallback preset id. It mirrors
// server.DefaultPreset, but lives here (not in `server`) so the store can
// enforce "every session carries a preset" without an import cycle.
const DefaultPreset = "default"
