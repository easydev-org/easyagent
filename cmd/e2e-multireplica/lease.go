package main

import (
	"context"
	"fmt"
	"time"

	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
)

/**
 * A TURN RUNS ONCE, NOT ONCE PER REPLICA.
 *
 * This is the property that makes two replicas a service rather than two services. Both replicas drain the same
 * durable mailbox consumer and both receive the wake; the session LEASE decides which one actually runs the turn.
 * Without it, a prompt delivered while two replicas are up would be answered twice — and the duplicate would be
 * indistinguishable from a model repeating itself.
 *
 * HOW IT IS TESTED. Two prompts for one session are sent AT THE SAME TIME, one through each replica, so both
 * replica-local wake paths fire. Then the transcript is read and the assistant's final answers are counted. Two
 * answers means the lease lost; one means it held.
 *
 * THE COUNT IS ON FINAL TEXT, not on messages: a turn with tool calls writes several assistant steps, and a step
 * boundary is not a second answer. Only steps that carry text are counted, which is what a reader sees.
 */
func assertTurnRunsOnce(ctx context.Context, a, b replica) error {
	// A FRESH SESSION PER RUN, so a transcript left by an earlier run cannot make this pass.
	name := fmt.Sprintf("lease-check-%d", time.Now().UnixNano())
	sid, err := a.createSession(ctx, name)
	if err != nil {
		return fmt.Errorf("create the lease-check session: %w", err)
	}

	const prompt = "Reply with exactly: ACK"
	// BOTH AT ONCE. Sending them in sequence would let the first turn finish before the second arrived, which
	// tests nothing: the race is on the WAKE, not on the prompt.
	errs := make(chan error, 2)
	for _, r := range []replica{a, b} {
		go func(r replica) {
			errs <- r.prompt(ctx, sid, prompt)
		}(r)
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			// A REFUSED PROMPT IS A DIFFERENT FAILURE: report it rather than counting answers and blaming the
			// lease for something that never ran.
			return fmt.Errorf("prompt %d refused: %w", i+1, err)
		}
		fmt.Printf("prompt %d accepted\n", i+1)
	}

	// The turn is asynchronous; give it room to finish without polling in a tight loop.
	deadline := time.Now().Add(45 * time.Second)
	var answers int
	for time.Now().Before(deadline) {
		answers, err = a.countAnswers(ctx, sid)
		if err != nil {
			return err
		}
		if answers >= 2 {
			break // already wrong; no reason to wait for more
		}
		time.Sleep(500 * time.Millisecond)
	}
	if answers != 1 {
		return fmt.Errorf("the session has %d final answers, want exactly 1 — the turn ran once per replica", answers)
	}
	fmt.Println("ok: a turn runs once, not once per replica")
	return nil
}

// prompt sends one user turn and waits for it to be accepted.
//
// IT DOES NOT WAIT FOR THE ANSWER. The point is to have two wakes race; waiting for the first to complete would
// serialize them and hide the race entirely.
func (r replica) prompt(ctx context.Context, sid, text string) error {
	stream, err := r.cli.Prompt(ctx, &agentv1.PromptRequest{Id: sid, Prompt: text})
	if err != nil {
		return err
	}
	/**
	 * ACCEPTED IS ENOUGH. The stream would keep going for the whole turn, and both prompts are meant to race on
	 * the WAKE — waiting for a first answer here would serialize them and hide exactly what is being tested.
	 *
	 * The server keeps the stream open in a goroutine-free loop; leaving early is what a console does when the
	 * reader navigates away, and the turn continues regardless.
	 */
	for {
		ev, err := stream.Receive()
		if err != nil {
			return nil
		}
		if ev.Event == "accepted" {
			return nil
		}
	}
}

// countAnswers counts the assistant steps that carry final text.
func (r replica) countAnswers(ctx context.Context, sid string) (int, error) {
	res, err := r.cli.ListMessages(ctx, &agentv1.ListMessagesRequest{Id: sid, Limit: 200})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range res.Messages {
		if m.Role != "assistant" {
			continue
		}
		for _, p := range m.Parts {
			/**
			 * A TOOL STEP HAS NO TEXT PART, so counting text parts counts answers.
			 *
			 * The payload is the part's JSON document; an empty text part would be `{"text":""}`, which this
			 * skips by requiring more than that. Streamed reasoning is a different part type and is not counted.
			 */
			if p.Type == "text" && len(p.Data) > len(`{"text":""}`) {
				n++
				break
			}
		}
	}
	return n, nil
}
