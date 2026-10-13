package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	"github.com/easydev-org/easyagent/agent"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
	"github.com/easydev-org/easyagent/llm"
	"github.com/easydev-org/easyagent/store"
)

// mockV4Gateway streams a fixed v4 script.
func mockV4Gateway(t *testing.T, script ...string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		for _, p := range script {
			fmt.Fprintf(w, "data: %s\n\n", p)
			if fl != nil {
				fl.Flush()
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
}

func newPromptServer(t *testing.T, client *llm.Client) (*httptest.Server, *store.DB) {
	t.Helper()
	db, err := store.Open(store.SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	hub := NewEventHub()
	presets := agent.NewPresets(agent.NewMemoryKV())
	presets.SeedSystem("default", agent.PresetRow{ID: "default", SystemPrompt: "You are a helpful assistant."})
	rt := agent.NewRuntime(agent.RuntimeDeps{
		DB: db, Client: client, Lease: agent.NewMemoryLease(),
		EmitFor: hub.Emitter, MaxTurns: 3, Presets: agent.PresetStore{P: presets},
	})
	srv := New(Deps{
		DB: db, Runtime: rt, Hub: hub,
		Config:   Config{AuthMode: "none", DefaultTenant: "default"},
		Presets:  presets,
		ConfigKV: agent.NewConfig(agent.NewMemoryKV()),
	})
	mux := http.NewServeMux()
	cs := connect.NewServer(srv.AuthInterceptor())
	srv.Register(cs)
	connecthttp.Mount(mux, cs, connecthttp.WithReadMaxBytes(0))
	ts := httptest.NewUnstartedServer(mux)
	proto := new(http.Protocols)
	proto.SetHTTP1(true)
	proto.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = proto
	ts.Start()
	t.Cleanup(ts.Close)
	return ts, db
}

func TestPromptStreamsTurnAndPersists(t *testing.T) {
	gw := mockV4Gateway(t,
		`{"type":"reasoning-delta","id":"r","delta":"think"}`,
		`{"type":"text-delta","id":"t","delta":"Hello "}`,
		`{"type":"text-delta","id":"t","delta":"world"}`,
		`{"type":"finish","finishReason":{"unified":"stop"},"usage":{"inputTokens":{"total":4},"outputTokens":{"total":2}}}`,
	)
	defer gw.Close()

	ts, db := newPromptServer(t, llm.New(llm.ProviderCredentials{BaseURL: gw.URL, APIKey: "k"}, nil))
	// auth=none: any client works
	c := agentv1connect.NewAgentServiceClient(
		connect.NewClient(connecthttp.NewTransport(&http.Client{}, ts.URL, connecthttp.WithReadMaxBytes(0))),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := c.CreateSession(ctx, &agentv1.CreateSessionRequest{Name: "s1", Model: "gw/m"}); err != nil {
		t.Fatal(err)
	}
	stream, err := c.Prompt(ctx, &agentv1.PromptRequest{Id: "s1", Prompt: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	var text string
	accepted := false
	for {
		res, err := stream.Receive()
		if err != nil {
			break
		}
		events = append(events, res.Event)
		if res.Event == "accepted" {
			accepted = true
		}
		if res.Event == "text-delta" {
			text += res.Params["text"]
		}
		if res.Event == "turn-complete" {
			break
		}
	}
	if !accepted {
		t.Error("Prompt must emit `accepted` first")
	}
	if text != "Hello world" {
		t.Errorf("streamed text = %q", text)
	}
	if !contains(events, "message-added") || !contains(events, "text-delta") || !contains(events, "turn-complete") {
		t.Errorf("events = %v", events)
	}

	// The turn persisted: user + assistant in the chain, and usage recorded.
	tip, _ := db.SessionTip("default", "s1")
	chain, _ := db.Chain("default", tip, 10, "")
	if len(chain) != 2 || chain[0].Role != "user" || chain[1].Role != "assistant" {
		t.Fatalf("chain = %+v", chain)
	}
	row, _, _ := db.GetSession("default", "s1")
	if row.LastInputTokens != 4 || row.LastOutputTokens != 2 {
		t.Errorf("usage = %d/%d", row.LastInputTokens, row.LastOutputTokens)
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
