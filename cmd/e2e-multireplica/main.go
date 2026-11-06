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
	"strings"
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

	// ---- 5. a turn runs once, not once per replica ----
	//
	// THE PROPERTY THAT MAKES TWO REPLICAS A SERVICE rather than two services. It needs a model, so it runs only
	// where one is configured (E2E_TURN=true); the CI job sets it because a deployment without this check can
	// answer every prompt twice and still pass everything above.
	if os.Getenv("E2E_TURN") != "" {
		if err := assertTurnRunsOnce(ctx, a, b); err != nil {
			return err
		}
	} else {
		fmt.Println("skip: the once-per-turn check needs a model (set E2E_TURN=1)")
	}

	return nil
}

// crossReplicaGroup posts into a group on one replica and reads it on the other.
//
// BOTH HALVES MATTER. Storing on A and reading on B proves the transcript is shared; the preview proves the LIST
// row is derived from that shared store rather than from a per-replica cache.
func crossReplicaGroup(ctx context.Context, a, b replica, coord string) error {
	/**
	 * A GROUP IS ADDRESSED BY ID NOW, so the id has to come back from the create call.
	 *
	 * The memory of "the group I made" is the id, exactly as a console keeps it: the name is a label a person may
	 * change, and every later call — post, read, preview — must survive that.
	 */
	id, err := a.createGroup(ctx, "cross-replica-group", coord, "shared transcript check")
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("create group on A: %w", err)
	}
	if id == "" {
		// AN EARLIER RUN LEFT IT BEHIND, so the id comes from the list. This is the one place the binary resolves a
		// name to an id, and it is at the same edge a console does it.
		got, lerr := a.findGroupID(ctx, "cross-replica-group")
		if lerr != nil {
			return fmt.Errorf("resolve the existing group: %w", lerr)
		}
		id = got
	}

	/**
	 * THE BODY CARRIES A NONCE, because the group may already exist.
	 *
	 * A previous run leaves its group and transcript behind — this binary does not delete what it creates, and
	 * deleting a group also drops its transcript and its subscriptions, which is more cleanup than a check
	 * needs. A unique body makes "the message posted on A is readable on B" mean THIS message, rather than a
	 * row from an earlier run that would pass the assertion no matter what.
	 */
	body := fmt.Sprintf("posted on A at %d, must be visible on B", time.Now().UnixNano())
	if err := a.sendGroupMessage(ctx, id, body); err != nil {
		return fmt.Errorf("post to the group on A: %w", err)
	}

	if err := b.waitGroupMessage(ctx, id, body); err != nil {
		return fmt.Errorf("a group message posted on A is not readable on B: %w", err)
	}
	if err := b.waitGroupPreview(ctx, id, body); err != nil {
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
	/**
	 * A MODEL IS NAMED EXPLICITLY, because creation now REFUSES a session that could not run a turn.
	 *
	 * THIS BINARY CHECKS REPLICATION, NOT MODEL RESOLUTION. Leaving the field empty would make it depend on the
	 * deployment having a provider registered and a default configured — a second thing to get wrong, in a job
	 * whose subject is the list and the transcript. The model only has to exist as a string here: the assertions
	 * never run a turn (that is the separate `E2E_TURN` check, which needs a real one).
	 */
	res, err := r.cli.CreateSession(ctx, &agentv1.CreateSessionRequest{
		// `locale: zh` pins the language at creation, which is what the console does.
		Name: name, Locale: "zh", Model: "e2e/model",
	})
	if err != nil {
		return "", err
	}
	if res.SessionId == "" {
		return "", fmt.Errorf("no sessionId in the reply")
	}
	return res.SessionId, nil
}

// createGroup makes a group and returns its ID — which is what every later call needs.
func (r replica) createGroup(ctx context.Context, name, coordinator, intro string) (string, error) {
	res, err := r.cli.CreateGroup(ctx, &agentv1.CreateGroupRequest{
		Name: name, Coordinator: coordinator, Intro: intro, CreatedBy: "user",
	})
	if err != nil {
		return "", err
	}
	return res.Group.GetId(), nil
}

// findGroupID resolves a display name to an id, the way a console does once at the edge.
func (r replica) findGroupID(ctx context.Context, name string) (string, error) {
	res, err := r.cli.ListGroups(ctx, &agentv1.ListGroupsRequest{})
	if err != nil {
		return "", err
	}
	for _, g := range res.Groups {
		if g.Name == name {
			return g.Id, nil
		}
	}
	return "", fmt.Errorf("no group named %q", name)
}

func (r replica) sendGroupMessage(ctx context.Context, groupID, text string) error {
	_, err := r.cli.SendGroupMessage(ctx, &agentv1.SendGroupMessageRequest{
		GroupId: groupID, Text: text,
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

func (r replica) waitGroupMessage(ctx context.Context, groupID, body string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := r.cli.ListGroupMessages(ctx, &agentv1.ListGroupMessagesRequest{GroupId: groupID})
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

func (r replica) waitGroupPreview(ctx context.Context, groupID, want string) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := r.cli.ListGroups(ctx, &agentv1.ListGroupsRequest{})
		if err == nil {
			for _, g := range res.Groups {
				if g.Id == groupID && g.LastMessagePreview == want {
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

// isAlreadyExists reports a create that lost to an earlier run.
//
// MATCHED ON THE CONNECT CODE, not on the message: the code is part of the contract and the wording is not.
func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already_exists")
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", k)
		os.Exit(2)
	}
	return v
}
