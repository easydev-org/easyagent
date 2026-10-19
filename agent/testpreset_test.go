package agent

// stubPresets is a minimal PresetProvider for tests whose subject is NOT the
// preset system (turn execution, lease loss, compaction). Since every session
// now carries a preset, Plan resolves it — so those tests need a provider that
// knows the built-in "default".
type stubPresets struct{}

func (stubPresets) Preset(tenant, id string) (PresetRow, bool) {
	if id == "default" || id == "test" {
		return PresetRow{ID: id, SystemPrompt: "you are a test assistant"}, true
	}
	return PresetRow{}, false
}
