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
func (d *stubDeps) SessionGroup(string, string) string        { return "" }
func (d *stubDeps) SessionExists(string, sid string) bool     { return sid == "parent" || sid == "child" }
func (d *stubDeps) SessionTip(string, string) string          { return "" }
func (d *stubDeps) SessionsInGroup(string, string) []string   { return nil }
func (d *stubDeps) ForkSession(string, string, string) error  { return nil }
func (d *stubDeps) DeleteSessionRow(string, string) error     { return nil }
func (d *stubDeps) DeleteSessionMailbox(string, string) error { return nil }
func (d *stubDeps) PublishMailbox(_, session, _ string, _ any, _ string) error {
	d.mails = append(d.mails, session)
	return nil
}
func (d *stubDeps) MessageChain(string, string, int) []ChainEntry  { return nil }
func (d *stubDeps) MessageParts(string, []string) []MessagePartRow { return nil }
func (d *stubDeps) TodosReplace(string, string, []TodoRow) error   { return nil }

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
	for _, want := range []string{"todo-write", "image-generate", "audio-transcribe", "tts-generate", "subsession-create", "mail-send"} {
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

func TestExecuteTodoWriteAndUnknown(t *testing.T) {
	e, err := New(&stubDeps{})
	if err != nil {
		t.Fatal(err)
	}
	out, err := e.Execute(context.Background(), "t1", "s1", "todo-write",
		`{"todos":[{"content":"a","status":"pending","priority":"high"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if out.Content == "" {
		t.Error("expected a todo-write confirmation")
	}
	// unknown tool -> ErrUnknownTool (so a composite falls through).
	if _, err := e.Execute(context.Background(), "t1", "s1", "ghost", "{}"); !agent.IsUnknownTool(err) {
		t.Errorf("unknown tool should be ErrUnknownTool, got %v", err)
	}
	// qualified name accepted.
	if _, err := e.Execute(context.Background(), "t1", "s1", "bundled-todo-write", `{"todos":[]}`); err != nil {
		t.Errorf("qualified name should resolve: %v", err)
	}
}

func TestMailSendDelivers(t *testing.T) {
	d := &stubDeps{}
	e, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	out, err := e.Execute(context.Background(), "t1", "parent", "mail-send", `{"to":"child","text":"hi"}`)
	if err != nil {
		t.Fatal(err)
	}
	if out.Content == "" || len(d.mails) != 1 || d.mails[0] != "child" {
		t.Errorf("mail-send: out=%q mails=%v", out.Content, d.mails)
	}
}
