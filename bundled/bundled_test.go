package bundled

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/llm"
)

// stubDeps is a minimal Deps for tool tests.
type stubDeps struct {
	cfg   map[string]string
	blobs map[string]struct {
		meta map[string]any
		data []byte
	}
	mails []string

	// Conversation state, for the history tools. Zero value = an empty session, which is what most
	// tests want.
	chain      []ChainEntry
	parts      []MessagePartRow
	tips       map[string]string
	chainByTip map[string][]ChainEntry
	existing   map[string]bool
}

func (d *stubDeps) SessionVariable(string, string, string) (string, bool) { return "", false }
func (d *stubDeps) ResolveConfig(name, _, _ string) string                { return d.cfg[name] }
func (d *stubDeps) ResolveTextModel(string, string) (*llm.Client, string, error) {
	return nil, "", nil
}
func (d *stubDeps) ResolveGenerative(string, string, string) (*llm.Client, string, error) {
	return nil, "", nil
}
func (d *stubDeps) BlobGet(_, code string) (map[string]any, []byte, bool) {
	if b, ok := d.blobs[code]; ok {
		return b.meta, b.data, true
	}
	return nil, nil, false
}
func (d *stubDeps) IngestBlob(_, _, name string, data []byte) (string, string, string, error) {
	return "code1234567890ab", "image/png", name, nil
}
func (d *stubDeps) SessionGroup(string, string) string { return "" }
func (d *stubDeps) SessionExists(_ string, sid string) bool {
	if d.existing != nil {
		if ok, seen := d.existing[sid]; seen {
			return ok
		}
		return false
	}
	return sid == "parent" || sid == "child"
}
func (d *stubDeps) SessionTip(_ string, sid string) string {
	if d.tips != nil {
		return d.tips[sid]
	}
	if len(d.chain) > 0 {
		return d.chain[0].ID
	}
	return ""
}
func (d *stubDeps) GroupsOfSession(string, string) ([]GroupRow, error) { return nil, nil }
func (d *stubDeps) IsGroupMember(string, string, string) bool          { return false }
func (d *stubDeps) GroupMemberProfiles(string, string) ([]GroupMemberRow, error) {
	return nil, nil
}
func (d *stubDeps) GroupCoordinator(string, string) (string, error)                 { return "", nil }
func (d *stubDeps) CoordinatedGroup(string, string) (string, error)                 { return "", nil }
func (d *stubDeps) CoordinatedGroupOrMembership(string, string) string              { return "" }
func (d *stubDeps) AddGroupMemberOnly(string, string, string) error                 { return nil }
func (d *stubDeps) RevokeCapability(string, string, string, string) error           { return nil }
func (d *stubDeps) CreateGroupForSession(string, string, string) error              { return nil }
func (d *stubDeps) PostGroupMessage(string, string, string, []string, string) error { return nil }
func (d *stubDeps) ForkSession(string, string, string, []string) (string, error)    { return "", nil }
func (d *stubDeps) DeleteSessionRow(string, string) error                           { return nil }
func (d *stubDeps) MessageChain(_ string, tip string, _ int) []ChainEntry {
	if d.chainByTip != nil {
		return d.chainByTip[tip]
	}
	return d.chain
}
func (d *stubDeps) MessageParts(string, []string) []MessagePartRow { return d.parts }
func (d *stubDeps) TodosReplace(string, string, []TodoRow) error   { return nil }

// NO JOBS IN THE STUB. A test that needs them sets jobs; the default is the ordinary case, which
// is a conversation that started none.

func TestManifestToolsAndNoTimeWait(t *testing.T) {
	m, err := ParseManifest()
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "bundled" {
		t.Errorf("id = %q", m.ID)
	}
	if len(m.Tools) == 0 {
		t.Fatal("no tools parsed")
	}
	names := map[string]bool{}
	for _, tool := range m.Tools {
		names[tool.Name] = true
	}
	// time-wait is intentionally excluded from the Go bundled extension.
	if names["time-wait"] {
		t.Error("time-wait must NOT be in the Go bundled extension")
	}
	for _, want := range []string{"todo-write", "image-generate", "audio-transcribe", "tts-generate", "group-create", "spawn", "group-list", "group-members", "group-send"} {
		if !names[want] {
			t.Errorf("missing tool %q", want)
		}
	}
	// i18n descriptions carried through.
	for _, tool := range m.Tools {
		if tool.Name == "todo-write" && tool.Descriptions["zh"] == "" {
			t.Error("todo-write zh description missing")
		}
	}
}

// EVERY DECLARED TOOL MUST HAVE A HANDLER.
//
// The manifest is what the model SEES; the handlers map is what actually runs. A tool in one and
// not the other is advertised and then fails at call time — the model believes it has a capability
// it does not, calls it, and reads back "unknown tool".
//
// It happened: `job-list` and `job-wait` were removed from the code but left in the manifest, so
// ListExtensions offered them in production. The old test only asserted that SOME tools existed,
// which cannot notice a leftover.
func TestEveryDeclaredToolHasAHandler(t *testing.T) {
	m, err := ParseManifest()
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(&stubDeps{})
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, tool := range m.Tools {
		declared[tool.Name] = true
		if _, ok := e.handlers[tool.Name]; !ok {
			t.Errorf("manifest declares %q but no handler implements it; the model would call it and get 'unknown tool'", tool.Name)
		}
	}
	for name := range e.handlers {
		if !declared[name] {
			t.Errorf("handler %q is not declared in the manifest, so the model can never call it", name)
		}
	}
}

func TestExecuteTodoWriteAndUnknown(t *testing.T) {
	e, err := New(&stubDeps{})
	if err != nil {
		t.Fatal(err)
	}
	out, err := e.Execute(context.Background(), "t1", "s1", "c1", "todo-write",
		`{"todos":[{"content":"a","status":"pending","priority":"high"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if out.Content == "" {
		t.Error("expected a todo-write confirmation")
	}
	// unknown tool -> ErrUnknownTool (so a composite falls through).
	if _, err := e.Execute(context.Background(), "t1", "s1", "c2", "ghost", "{}"); !agent.IsUnknownTool(err) {
		t.Errorf("unknown tool should be ErrUnknownTool, got %v", err)
	}
	// qualified name accepted.
	if _, err := e.Execute(context.Background(), "t1", "s1", "c3", "bundled-todo-write", `{"todos":[]}`); err != nil {
		t.Errorf("qualified name should resolve: %v", err)
	}
}

/**
 * THERE IS NO DIRECT MESSAGING ANY MORE.
 *
 * `mail-send` delivered to a session's mailbox with nothing recording that the two sessions were
 * related: unaddressable from outside, invisible to anyone else, and a second routing rule next to the
 * channel one. It was replaced by `group-send`, which only reaches a member of a group the caller
 * coordinates — so the group IS the relation, and the subscription decides delivery.
 *
 * This asserts the removal. A leftover handler would be a hole in the rule "every session-to-session
 * message goes through a group", and the model would find it.
 */
func TestNoDirectMessagingTool(t *testing.T) {
	d := &stubDeps{}
	e, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	for name := range e.handlers {
		if name == "mail-send" || name == "subsession-create" || name == "delegate" {
			t.Errorf("handler %q still exists; session-to-session messaging must go through a group", name)
		}
	}
	if _, err := e.Execute(context.Background(), "t1", "parent", "c4", "mail-send", `{"to":"child","text":"hi"}`); err == nil {
		t.Error("mail-send still executes")
	}
}
