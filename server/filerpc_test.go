package server

import (
	"context"
	"testing"

	"github.com/easydev-org/easybus-sdk/bus"
	"github.com/easydev-org/easybus-sdk/extension"
	"github.com/easydev-org/easybus-sdk/natsrun"
	"github.com/easydev-org/easybus-sdk/protocol"
	"github.com/easydev-org/easybus-sdk/transport/nats"

	abcbus "github.com/easydev-org/easyagent/bus"
)

// TestFileRPCRoundTrip proves the agent serves file.ingest / file.get for a
// DB-less extension: the extension uploads bytes via the transient object store,
// the agent stores them (db + blob) and returns a file:<code>, and a get
// returns the bytes back through the same path.
func TestFileRPCRoundTrip(t *testing.T) {
	srv, err := natsrun.Start(natsrun.Config{Port: 0})
	if err != nil {
		t.Skipf("nats-server unavailable: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	// The agent side: a real server (db + blob) + its file RPC loops.
	ts, _, s := newTestServerWithSrv(t)
	_ = ts
	agentBus, err := abcbus.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agentBus.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stop, err := s.ServeFileRPC(ctx, agentBus.Transport())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)

	// The extension side: a plain bus connection.
	eb, err := nats.Connect(srv.URL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eb.Close() })

	// Ingest: put bytes in the object store, ask the agent to persist them.
	obj := protocol.NewID() + ".bin"
	data := []byte("hello-from-extension")
	if err := eb.ObjectPut(ctx, protocol.TenantObjectName("acme", obj), data); err != nil {
		t.Fatal(err)
	}
	code, mime, err := extension.IngestFileViaAgent(ctx, eb, "acme", "greeting.txt", data, "sess-1")
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if code == "" {
		t.Fatal("no code returned")
	}
	if mime == "" {
		t.Error("no mime derived")
	}

	// Get: fetch it back through the agent.
	meta, got, err := extension.GetFileViaAgent(ctx, eb, "acme", code)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("roundtrip mismatch: %q", got)
	}
	if meta.Name != "greeting.txt" {
		t.Errorf("name = %q", meta.Name)
	}
	_ = bus.PublishOpts{}
}
