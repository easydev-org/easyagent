package bus

import (
	"testing"
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
