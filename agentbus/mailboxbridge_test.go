package agentbus

import (
	"testing"
	"time"

	"github.com/easydev-org/easybus-sdk/extension"
)

/**
 * THE PAYLOAD OF A MAILBOX MESSAGE IS ITS `payload` FIELD, NOT THE WHOLE ENVELOPE.
 *
 * `PublishMailboxEvent` sends {id, type, payload, source}. A bridge that enqueued the whole object
 * would put the envelope's own bookkeeping in front of the model as part of the message it is
 * supposed to read — a wake that says `map[id:... type:trigger payload:map[messageId:...]]` instead
 * of the message.
 */
func TestExtensionMailPartsUnwrapsTheEnvelope(t *testing.T) {
	body := map[string]any{"messageId": "job-done-x", "text": "<job ...>"}
	typ, payload, source := extensionMailParts(map[string]any{
		"id": "m1", "type": "trigger", "payload": body, "source": "sandbox",
	})
	if typ != "trigger" {
		t.Errorf("type = %q, want trigger", typ)
	}
	if source != "sandbox" {
		t.Errorf("source = %q, want sandbox", source)
	}
	m, ok := payload.(map[string]any)
	if !ok || m["text"] != body["text"] {
		t.Fatalf("payload was not unwrapped: %#v", payload)
	}
}

/**
 * A PUBLISHER THAT NAMES NO TYPE GETS "event".
 *
 * The SDK does the same when `PublishMailboxEvent` is called with an empty type, and the two must
 * agree: "event" is context-only and does not wake a turn, which is the safe default for a message
 * whose sender did not say what it wants.
 */
func TestExtensionMailPartsDefaultsToEvent(t *testing.T) {
	typ, payload, _ := extensionMailParts(map[string]any{"type": "", "payload": "hi"})
	if typ != "event" {
		t.Errorf("type = %q, want event", typ)
	}
	if payload != "hi" {
		t.Errorf("payload = %#v, want hi", payload)
	}
}

func TestExtensionMailPartsPassesThroughANonMapPayload(t *testing.T) {
	// A PAYLOAD THAT IS NOT AN OBJECT is the message itself. Rejecting it would drop a message some
	// publisher considered well-formed, and the body is still readable.
	typ, payload, source := extensionMailParts("just a string")
	if typ != "event" || source != "" {
		t.Errorf("type=%q source=%q", typ, source)
	}
	if payload != "just a string" {
		t.Errorf("payload = %#v", payload)
	}
}

// THE BRIDGE REFUSES WITHOUT A QUEUE, rather than accepting messages it cannot store.
//
// Falling through would leave a message on the subject and nothing in the table — the exact
// silent-loss shape the bridge exists to remove.
func TestBridgeNeedsAQueue(t *testing.T) {
	b := &Bus{}
	if _, err := b.BridgeExtensionMail(t.Context()); err == nil {
		t.Fatal("a bus with no queue must not pretend to bridge")
	}
}

/**
 * A DELIVERED MESSAGE MUST WAKE THE SESSION, NOT WAIT FOR THE BACKSTOP.
 *
 * The bridge used to enqueue and stop, on the theory that "the row existing is what wakes it". Nobody
 * watches the queue: `Wake` happens when a `wake.<session>` message arrives, or when the 60-second
 * backstop scans the whole bucket. So an extension's completion notice — "the job finished" — sat
 * unread for up to a minute, on a path whose entire purpose is to report that it finished.
 *
 * It worked before v0.13.0 by accident: the publish landed on `mailbox.<session>` and ConsumeWake was
 * listening there, so delivery carried its own nudge. Splitting the wake onto its own subject removed
 * that side effect and left nothing in its place.
 */
func TestBridgeWakesTheSessionAfterEnqueue(t *testing.T) {
	b := startBus(t).WithMailboxQueue(newMemQueue())

	// The runtime's consumer, as main wires it: a wake message reaches the handler with the address.
	woken := make(chan string, 4)
	stop, err := b.ConsumeWake(t.Context(), func(tenant, session string) error {
		woken <- tenant + "/" + session
		return nil
	})
	if err != nil {
		t.Fatalf("consume wake: %v", err)
	}
	defer stop()

	if _, err := b.BridgeExtensionMail(t.Context()); err != nil {
		t.Fatalf("bridge: %v", err)
	}
	// Let the subscriptions settle before publishing; the bridge's own is flushed on create, but the
	// consume side is a separate pull consumer.
	time.Sleep(300 * time.Millisecond)

	// An extension announces a completion the way extensions do.
	ext := extension.New(b.Transport(), extension.Config{ID: "sandbox"})
	if err := ext.PublishMailboxEvent(t.Context(), "t1", "sess-1", "trigger",
		map[string]any{"text": "<job done>"}, "sandbox"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-woken:
		if got != "t1/sess-1" {
			t.Fatalf("woke %q, want t1/sess-1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the bridge enqueued but never woke the session; the notice would sit until the backstop")
	}
}
