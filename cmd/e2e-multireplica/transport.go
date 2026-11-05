package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
)

// watchMsg is one frame of WatchSessions, reduced to what this binary asserts on.
type watchMsg struct {
	snapshot bool
	upserts  []string
	groups   int
}

func idsOf(ss []*agentv1.Session) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, s.GetId())
	}
	return out
}

func awaitSnapshot(events <-chan watchMsg, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("the stream closed")
			}
			if ev.snapshot {
				return nil
			}
		case <-deadline:
			return fmt.Errorf("timed out")
		}
	}
}

func awaitSessionUpsert(events <-chan watchMsg, id string, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("the stream closed")
			}
			for _, got := range ev.upserts {
				if got == id {
					return nil
				}
			}
		case <-deadline:
			return fmt.Errorf("timed out")
		}
	}
}

// clients builds the pair of clients for one replica.
//
// THE CONNECT CLIENT OWNS THE STREAM FRAMING. A server-streaming body is a sequence of length-prefixed frames, so
// a JSON decoder over the raw bytes reads a header as if it were a message — which is exactly how the first
// version of this binary failed, with "the stream closed" and nothing else to go on.
func newClients(base, token string) (agentv1connect.AgentServiceClient, agentv1connect.AdminServiceClient) {
	hc := &http.Client{
		// Streaming responses stay open; a client timeout would cut them off mid-frame.
		Timeout: 0,
	}
	interceptor := func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			if token != "" {
				// THE HEADER IS SET ON A DERIVED CONTEXT; `:=` would shadow `ctx` and the next() below would
				// carry the original, sending an unauthenticated request that the server rejects with 401 —
				// indistinguishable from a bad token.
				var info *connect.CallInfo
				ctx, info = connect.NewClientContext(ctx)
				info.RequestHeader().Set("Authorization", "Bearer "+token)
			}
			return next(ctx, spec)
		}
	}
	transport := connecthttp.NewTransport(hc, base, connecthttp.WithReadMaxBytes(0))
	return agentv1connect.NewAgentServiceClient(connect.NewClient(transport, interceptor)),
		agentv1connect.NewAdminServiceClient(connect.NewClient(transport, interceptor))
}

// login mints a session token from the admin service.
func login(ctx context.Context, adm agentv1connect.AdminServiceClient, user, pass string) (string, error) {
	res, err := adm.Login(ctx, &agentv1.LoginRequest{Username: user, Token: pass})
	if err != nil {
		return "", err
	}
	if res.SessionToken == "" {
		return "", fmt.Errorf("no sessionToken in the reply")
	}
	return res.SessionToken, nil
}

// probeHealth checks the health listener.
//
// THE RPC ADDRESS IS NOT ALWAYS THE HEALTH ADDRESS: the agent can serve `/healthz` on its own port, and the CI
// job does exactly that. The port is derived from the RPC one by the same offset the job uses, so a caller only
// has to say where the RPCs are.
func probeHealth(ctx context.Context, base string) error {
	for _, url := range []string{
		strings.TrimSuffix(base, "/") + "/healthz",
		healthAddrFor(base) + "/healthz",
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			continue
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		_ = res.Body.Close()
		if res.StatusCode == http.StatusOK {
			return nil
		}
	}
	return fmt.Errorf("health: no listener answered")
}

// healthAddrFor derives the health address from the RPC address.
//
// THE TWO PORTS COME IN PAIRS, ten apart, which is the convention the CI job uses for its two replicas
// (18080/18081, 18090/18091). A deployment that mounts the health handler on the RPC port is covered by the
// direct attempt in probeHealth, so this only has to handle the split case.
func healthAddrFor(base string) string {
	u := strings.TrimSuffix(base, "/")
	if i := strings.LastIndex(u, ":"); i >= 0 {
		var port int
		if _, err := fmt.Sscanf(u[i+1:], "%d", &port); err == nil {
			return u[:i] + fmt.Sprintf(":%d", port+1)
		}
	}
	return u
}
