// Command e2e-multireplica proves that two agent processes sharing one broker and
// one database behave as ONE service.
//
// WHY A SEPARATE BINARY RATHER THAN A GO TEST. The thing under test is not a
// function: it is the interaction between two OS processes, a real broker and a
// shared store. A unit test cannot start them, and `go test` gives no place to
// say "these two failures are the same symptom seen from different sides".
//
// WHY THE CONNECT CLIENT AND NOT curl. A server-streaming RPC is framed (a flag
// byte and a length per message), so a plain JSON decoder over the raw body
// reads the frame header as if it were the message and gives up. The generated
// client owns that framing; using it is also what keeps this binary honest about
// the protocol a real console speaks.
//
// WHAT IT CHECKS, and each one is a way a multi-replica deployment can be
// silently wrong:
//
//  1. BOTH REPLICAS BOOT against the same store. A schema-owning replica that
//     refuses to start when another already migrated is a deployment that only
//     ever runs one pod.
//
//  2. A SESSION CREATED ON A IS VISIBLE ON B, immediately. The list is served
//     from the shared store, so this catches a per-replica cache.
//
//  3. A LIST CHANGE PUBLISHED ON A REACHES A WATCHER ON B. This is the one that
//     was actually broken: nudges were pushed into one process's subscriber map,
//     so a preview written on A never reached a client connected to B.
//
//  4. A GROUP MESSAGE POSTED ON A IS READABLE ON B, with its transcript preview
//     — the group half of the same defect.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"github.com/easydev-org/easyagent/gen/agent/v1/agentv1connect"
)

func main() {
	a := newReplica("A", mustEnv("REPLICA_A_ADDR"))
	b := newReplica("B", mustEnv("REPLICA_B_ADDR"))

	if err := run(a, b); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK: the two replicas behaved as one service")
}

func run(a, b replica) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// A TOKEN, WHEN THE DEPLOYMENT REQUIRES ONE. CI runs with `auth: none`, where an empty bearer is correct; a
	// real deployment rejects that with 401, so the credential is optional and comes from the environment.
	if user := os.Getenv("E2E_LOGIN_USER"); user != "" {
		tok, err := login(ctx, a.adm, user, os.Getenv("E2E_LOGIN_PASS"))
		if err != nil {
			return fmt.Errorf("login: %w", err)
		}
		a.setToken(tok)
		b.setToken(tok)
	}

	// ---- 1. both boot ----
	for _, r := range []replica{a, b} {
		if err := r.waitHealthy(ctx); err != nil {
			return fmt.Errorf("replica %s never became healthy: %w", r.name, err)
		}
	}
	fmt.Println("ok: both replicas are healthy")

	// ---- 2. a session created on A is visible on B ----
	//
	// THE TENANT IS PART OF THE TOKEN, so both replicas must be configured with the same default tenant. They
	// are: one store, one tenant.
	sid, err := a.createSession(ctx, "cross-replica-check")
	if err != nil {
		return fmt.Errorf("create session on A: %w", err)
	}
	if err := b.waitSessionVisible(ctx, sid); err != nil {
		return fmt.Errorf("a session created on A is not visible on B: %w", err)
	}
	fmt.Println("ok: a session created on A is visible on B")

	// ---- 3. a list change on A reaches a watcher on B ----
	//
	// THIS IS THE REGRESSION THIS BINARY EXISTS FOR. The watch is opened on B; the change is made on A. A nudge
	// delivered only within the process that wrote it leaves this waiting until it times out.
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	events := b.watchSessions(watchCtx)

	// Drain the snapshot first: it arrives immediately and says nothing about cross-replica delivery.
	if err := awaitSnapshot(events, 20*time.Second); err != nil {
		return fmt.Errorf("B's watch never sent its snapshot: %w", err)
	}

	second, err := a.createSession(ctx, "cross-replica-live")
	if err != nil {
		return fmt.Errorf("create second session on A: %w", err)
	}
	if err := awaitSessionUpsert(events, second, 25*time.Second); err != nil {
		return fmt.Errorf("B's watcher never heard about a session created on A "+
			"(this is the cross-replica list defect): %w", err)
	}
	fmt.Println("ok: a list change on A reaches a watcher on B")

	// ---- 4. a group message posted on A is readable on B ----
	if err := crossReplicaGroup(ctx, a, b, sid); err != nil {
		return err
	}

	return nil
}

// crossReplicaGroup posts into a group on one replica and reads it on the other.
//
// BOTH HALVES MATTER. Storing on A and reading on B proves the transcript is shared; the preview proves the LIST
// row is derived from that shared store rather than from a per-replica cache.
func crossReplicaGroup(ctx context.Context, a, b replica, coord string) error {
	const name = "cross-replica-group"
	if err := a.createGroup(ctx, name, coord, "shared transcript check"); err != nil {
		return fmt.Errorf("create group on A: %w", err)
	}

	const body = "posted on A, must be visible on B"
	if err := a.sendGroupMessage(ctx, name, body); err != nil {
		return fmt.Errorf("post to the group on A: %w", err)
	}

	if err := b.waitGroupMessage(ctx, name, body); err != nil {
		return fmt.Errorf("a group message posted on A is not readable on B: %w", err)
	}
	if err := b.waitGroupPreview(ctx, name, body); err != nil {
		return fmt.Errorf("B's group list does not show the message's preview: %w", err)
	}
	fmt.Println("ok: a group message posted on A is readable on B, preview included")
	return nil
}

// ---- replica ----

// replica is one agent process, addressed through the generated connect client.
type replica struct {
	name string
	base string
	tok  string
	cli  agentv1connect.AgentServiceClient
	adm  agentv1connect.AdminServiceClient
}

func newReplica(name, base string) replica {
	r := replica{name: name, base: base}
	r.cli, r.adm = newClients(base, "")
	return r
}

// setToken installs a credential and rebuilds the clients around it.
//
// A TOKEN ARRIVES AFTER CONSTRUCTION (login happens once, shared by both replicas), so the clients are rebuilt
// when one is known. The alternative — an interceptor reading a mutable field — is one more moving part for the
// same result.
func (r *replica) setToken(tok string) {
	r.tok = tok
	r.cli, r.adm = newClients(r.base, tok)
}

func (r replica) createSession(ctx context.Context, name string) (string, error) {
	res, err := r.cli.CreateSession(ctx, &agentv1.CreateSessionRequest{
		// `locale: zh` pins the language at creation, which is what the console does.
		Name: name, Locale: "zh",
	})
	if err != nil {
		return "", err
	}
	if res.SessionId == "" {
		return "", fmt.Errorf("no sessionId in the reply")
	}
	return res.SessionId, nil
}

func (r replica) createGroup(ctx context.Context, name, coordinator, intro string) error {
	_, err := r.cli.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: name, Coordinator: coordinator, Intro: intro, CreatedBy: "user",
	})
	return err
}

func (r replica) sendGroupMessage(ctx context.Context, group, text string) error {
	_, err := r.cli.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		Group: group, Text: text,
	})
	return err
}

func (r replica) waitSessionVisible(ctx context.Context, id string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := r.cli.ListSessions(ctx, &agentv1.ListSessionsRequest{})
		if err == nil {
			for _, s := range res.Sessions {
				if s.Id == id {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out")
}

func (r replica) waitGroupMessage(ctx context.Context, group, body string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := r.cli.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{Group: group})
		if err == nil {
			for _, m := range res.Messages {
				if m.Body == body {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out")
}

func (r replica) waitGroupPreview(ctx context.Context, group, want string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := r.cli.ListGroups(ctx, &agentv1.ListGroupsRequest{})
		if err == nil {
			for _, g := range res.Groups {
				if g.Name == group && g.LastMessagePreview == want {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for preview %q", want)
}

// watchSessions opens the streaming RPC and emits one message at a time.
//
// THE CLIENT OWNS THE FRAMING. Reading the raw body would hand the decoder a frame header instead of a message —
// which is exactly how the first version of this binary failed, with "the stream closed" and no clue why.
func (r replica) watchSessions(ctx context.Context) <-chan watchMsg {
	out := make(chan watchMsg, 16)
	stream, err := r.cli.WatchSessions(ctx, &agentv1.WatchSessionsRequest{})
	if err != nil {
		close(out)
		return out
	}
	go func() {
		defer close(out)
		for {
			m, err := stream.Receive()
			if err != nil {
				return
			}
			select {
			case out <- watchMsg{
				snapshot: m.Snapshot,
				upserts:  idsOf(m.Upserts),
				groups:   len(m.Groups),
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (r replica) waitHealthy(ctx context.Context) error {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The health server is a separate listener; the RPC address is not it.
		if err := probeHealth(ctx, r.base); err == nil {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timed out")
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", k)
		os.Exit(2)
	}
	return v
}
