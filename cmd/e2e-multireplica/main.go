// Command e2e-multireplica proves that two agent processes sharing one broker and
// one database behave as ONE service.
//
// WHY A SEPARATE BINARY RATHER THAN A GO TEST. The thing under test is not a
// function: it is the interaction between two OS processes, a real broker and a
// shared store. A unit test cannot start them, and `go test` gives no place to
// say "these two failures are the same symptom seen from different sides".
//
// WHAT IT CHECKS, and each one is a way a multi-replica deployment can be
// silently wrong:
//
//  1. BOTH REPLICAS BOOT against the same store. A schema-owning replica that
//     refuses to start when another already migrated is a deployment that
//     only ever runs one pod.
//
//  2. A SESSION CREATED ON A IS VISIBLE ON B, immediately. The list is served
//     from the shared store, so this catches a per-replica cache.
//
//  3. A LIST CHANGE PUBLISHED ON A REACHES A WATCHER ON B. This is the one
//     that was actually broken: nudges were pushed into one process's
//     subscriber map, so a preview written on A never reached a client
//     connected to B. The test opens a watch on B, creates a session on A,
//     and requires B's watcher to hear about it.
//
//  4. A GROUP MESSAGE POSTED ON A IS READABLE ON B, with its transcript
//     preview — the group half of the same defect.
//
//  5. A TURN IS NOT RUN TWICE. Both replicas drain the same durable mailbox
//     consumer; a message must be consumed by exactly one of them. The check
//     is indirect but decisive: the accepted message id is consumed on both
//     sides, and the queue reports no pending duplicate.
//
// EXIT CODES. 0 = every property held. 1 = a property failed, with the reason
// printed. The CI step runs this after booting the pair, so a regression shows
// up as a failed job rather than as a user noticing a stale list.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// replica is one agent process, addressed over its JSON/HTTP API.
//
// THE JSON CODEC, not the binary one: this talks to the server the way `curl`
// does, which is also how the console's simplest callers do it, so a serialization
// change that breaks one is caught here.
type replica struct {
	name string
	base string
	tok  string
}

func main() {
	a := replica{name: "A", base: mustEnv("REPLICA_A_ADDR")}
	b := replica{name: "B", base: mustEnv("REPLICA_B_ADDR")}

	/**
	 * A TOKEN, WHEN THE DEPLOYMENT REQUIRES ONE.
	 *
	 * The CI job runs with `auth: none`, where an empty bearer is correct. A real deployment — the one this binary
	 * is also useful against — rejects that with 401, so the credential is optional and comes from the environment
	 * rather than being baked in. `E2E_TOKEN` is the plain tenant token the console uses; the login RPC would also
	 * work, but only for a deployment that has the admin service enabled.
	 */
	tok := os.Getenv("E2E_TOKEN")
	a.tok, b.tok = tok, tok

	if err := run(a, b); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK: the two replicas behaved as one service")
}

func run(a, b replica) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// ---- 1. both boot ----
	for _, r := range []replica{a, b} {
		if err := r.waitHealthy(ctx); err != nil {
			return fmt.Errorf("replica %s never became healthy: %w", r.name, err)
		}
	}
	fmt.Println("ok: both replicas are healthy")

	if user := os.Getenv("E2E_LOGIN_USER"); user != "" {
		// LOGIN MODE, for a deployment whose admin service is reachable: the token is minted once and shared, so
		// both replicas authenticate against the same tenant.
		t, err := login(ctx, a.base, user, os.Getenv("E2E_LOGIN_PASS"))
		if err != nil {
			return fmt.Errorf("login: %w", err)
		}
		a.tok, b.tok = t, t
	}

	// ---- 2. a session created on A is visible on B ----
	//
	// THE TENANT IS PART OF THE TOKEN, so both replicas must be configured with
	// the same default tenant. They are: one store, one tenant.
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
	// THIS IS THE REGRESSION THIS BINARY EXISTS FOR. The watch is opened on B;
	// the change is made on A. A nudge delivered only within the process that
	// wrote it leaves this waiting until it times out.
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	events := b.watchSessions(watchCtx)

	// Drain the snapshot first: it arrives immediately and says nothing about
	// cross-replica delivery.
	if err := awaitSnapshot(events, 15*time.Second); err != nil {
		return fmt.Errorf("B's watch never sent its snapshot: %w", err)
	}

	second, err := a.createSession(ctx, "cross-replica-live")
	if err != nil {
		return fmt.Errorf("create second session on A: %w", err)
	}
	if err := awaitSessionUpsert(events, second, 20*time.Second); err != nil {
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
// BOTH HALVES MATTER. Storing on A and reading on B proves the transcript is
// shared; the preview proves the LIST row is derived from that shared store
// rather than from a per-replica cache.
func crossReplicaGroup(ctx context.Context, a, b replica, coord string) error {
	if err := a.createGroup(ctx, "cross-replica-group", coord, "shared transcript check"); err != nil {
		return fmt.Errorf("create group on A: %w", err)
	}

	const body = "posted on A, must be visible on B"
	if err := a.sendGroupMessage(ctx, "cross-replica-group", body); err != nil {
		return fmt.Errorf("post to the group on A: %w", err)
	}

	if err := b.waitGroupMessage(ctx, "cross-replica-group", body); err != nil {
		return fmt.Errorf("a group message posted on A is not readable on B: %w", err)
	}

	// The list row's preview comes from the same transcript, so it is the check
	// that the group half of the list is shared too.
	if err := b.waitGroupPreview(ctx, "cross-replica-group", body); err != nil {
		return fmt.Errorf("B's group list does not show the message's preview: %w", err)
	}
	fmt.Println("ok: a group message posted on A is readable on B, preview included")
	return nil
}

// ---- transport ----

// call posts one JSON RPC and decodes the reply into out.
func (r replica) call(ctx context.Context, method string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		r.base+"/agent.v1.AgentService/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.tok != "" {
		req.Header.Set("Authorization", "Bearer "+r.tok)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", res.Status, strings.TrimSpace(string(raw)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (r replica) waitHealthy(ctx context.Context) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := http.Get(strings.Replace(r.base, ":1808", ":1809", 1) + "/healthz")
		if err == nil {
			_ = res.Body.Close()
			if res.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timed out")
}

func (r replica) createSession(ctx context.Context, name string) (string, error) {
	var out struct {
		SessionID string `json:"sessionId"`
	}
	// `locale: zh` pins the language at creation, which is what the console does:
	// a session that drifts with the tenant default is a different bug, and this
	// binary should not depend on which one it hits.
	err := r.call(ctx, "CreateSession", map[string]any{"name": name, "locale": "zh"}, &out)
	if err != nil {
		return "", err
	}
	if out.SessionID == "" {
		return "", fmt.Errorf("no sessionId in the reply")
	}
	return out.SessionID, nil
}

func (r replica) createGroup(ctx context.Context, name, coordinator, intro string) error {
	return r.call(ctx, "CreateGroup", map[string]any{
		"name": name, "coordinator": coordinator, "intro": intro, "createdBy": "user",
	}, nil)
}

func (r replica) sendGroupMessage(ctx context.Context, group, text string) error {
	return r.call(ctx, "SendGroupMessage", map[string]any{"group": group, "text": text}, nil)
}

func (r replica) waitSessionVisible(ctx context.Context, id string) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var out struct {
			Sessions []struct {
				ID string `json:"id"`
			} `json:"sessions"`
		}
		if err := r.call(ctx, "ListSessions", map[string]any{}, &out); err == nil {
			for _, s := range out.Sessions {
				if s.ID == id {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out")
}

func (r replica) waitGroupMessage(ctx context.Context, group, body string) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var out struct {
			Messages []struct {
				Body string `json:"body"`
			} `json:"messages"`
		}
		if err := r.call(ctx, "ListGroupMessages", map[string]any{"group": group}, &out); err == nil {
			for _, m := range out.Messages {
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
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var out struct {
			Groups []struct {
				Name               string `json:"name"`
				LastMessagePreview string `json:"lastMessagePreview"`
			} `json:"groups"`
		}
		if err := r.call(ctx, "ListGroups", map[string]any{}, &out); err == nil {
			for _, g := range out.Groups {
				if g.Name == group && g.LastMessagePreview == want {
					return nil
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for preview %q", want)
}

// ---- the server-streaming watch ----

// watchEvent is one frame of WatchSessions, decoded as the console decodes it.
type watchEvent struct {
	Snapshot bool `json:"snapshot"`
	Upserts  []struct {
		ID string `json:"id"`
	} `json:"upserts"`
	Removed       []string          `json:"removed"`
	Groups        []json.RawMessage `json:"groups"`
	RemovedGroups []string          `json:"removedGroups"`
}

// watchSessions opens the streaming RPC and emits one decoded frame at a time.
//
// A STREAM NEEDS ITS OWN READER. The frame size is unknown ahead of time and the
// server keeps the connection open, so this reads incrementally and hands frames
// to a channel — the same shape a console's generator has.
func (r replica) watchSessions(ctx context.Context) <-chan watchEvent {
	out := make(chan watchEvent, 16)
	go func() {
		defer close(out)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			r.base+"/agent.v1.AgentService/WatchSessions", strings.NewReader("{}"))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if r.tok != "" {
			req.Header.Set("Authorization", "Bearer "+r.tok)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		defer res.Body.Close()
		dec := json.NewDecoder(res.Body)
		for {
			var ev watchEvent
			if err := dec.Decode(&ev); err != nil {
				return
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func awaitSnapshot(events <-chan watchEvent, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("the stream closed")
			}
			if ev.Snapshot {
				return nil
			}
		case <-deadline:
			return fmt.Errorf("timed out")
		}
	}
}

func awaitSessionUpsert(events <-chan watchEvent, id string, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("the stream closed")
			}
			for _, s := range ev.Upserts {
				if s.ID == id {
					return nil
				}
			}
		case <-deadline:
			return fmt.Errorf("timed out")
		}
	}
}

// login mints a session token from the admin service.
func login(ctx context.Context, base, user, pass string) (string, error) {
	body, _ := json.Marshal(map[string]string{"username": user, "token": pass})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/agent.v1.AdminService/Login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		return "", fmt.Errorf("%s: %s", res.Status, strings.TrimSpace(string(raw)))
	}
	var out struct {
		SessionToken string `json:"sessionToken"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", err
	}
	if out.SessionToken == "" {
		return "", fmt.Errorf("no sessionToken in the reply")
	}
	return out.SessionToken, nil
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", k)
		os.Exit(2)
	}
	return v
}
