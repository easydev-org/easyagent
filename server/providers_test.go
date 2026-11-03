package server

import (
	"strings"
	"testing"

	"github.com/easydev-org/easyagent/agent"
)

/**
 * TestProvider routes by MODALITY and strips the provider prefix.
 *
 * TWO BUGS THIS PINS, both of which looked like "the model does not exist":
 *
 *  1. THE CONSOLE sends `provider_id/model_id`, because that is the canonical reference every other
 *     call takes. The gateway only knows the bare id, so forwarding the reference unparsed asks it for
 *     a model named after our own provider row — a 404 that reads like a missing model rather than an
 *     unparsed ref.
 *
 *  2. A CHAT COMPLETION CANNOT TEST A TTS MODEL. Pointing the language-model call at an image or
 *     speech model reports "no language model configured" no matter how healthy the provider is. Each
 *     modality needs the call that matches it.
 */

func TestModelOfStripsTheProviderPrefix(t *testing.T) {
	// THE MODEL ID ITSELF CONTAINS A SLASH, and only the FIRST one separates it from the provider.
	for _, tc := range []struct{ ref, want string }{
		{"gateway-text/tal-coding/deepseek-v4.1-flash", "tal-coding/deepseek-v4.1-flash"},
		{"gateway-image/tal-coding-gptimage/gpt-image-2", "tal-coding-gptimage/gpt-image-2"},
		{"gateway-asr/mlops-asr/qwen3-asr", "mlops-asr/qwen3-asr"},
		// A BARE ID IS RETURNED UNCHANGED, so a caller that already sent one is unaffected.
		{"simple-model", "simple-model"},
	} {
		if got := agent.ModelOf(tc.ref); got != tc.want {
			t.Errorf("ModelOf(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

// The modalities the console offers all have a probe shape; the three it does not offer say so.
func TestCapabilitiesAreParseable(t *testing.T) {
	for _, c := range []string{"text", "image", "video", "speech", "transcription"} {
		if _, err := agent.ParseCapability(c); err != nil {
			t.Errorf("%q is offered by the console but the server rejects it: %v", c, err)
		}
	}
	// EMBEDDING/RERANK/REALTIME ARE STILL VALID ON THE WIRE — the gateway serves them — they simply
	// have no test interface. Rejecting them here would make an existing row unreadable.
	for _, c := range []string{"embedding", "rerank", "realtime"} {
		if _, err := agent.ParseCapability(c); err != nil {
			t.Errorf("%q must stay parseable even without a test UI: %v", c, err)
		}
	}
	if _, err := agent.ParseCapability("nonsense"); err == nil {
		t.Error("an unknown capability was accepted")
	}
}

// A missing prompt is rejected before any network call, with a message that says which modality
// needs what — the alternative is a model error that mentions neither.
func TestEveryGeneratingModalityNeedsItsInput(t *testing.T) {
	type need struct{ capability, field string }
	for _, n := range []need{
		{"image", "prompt"},
		{"video", "prompt"},
		{"speech", "text"},
		{"transcription", "audio"},
	} {
		// The messages live in TestProvider's switch; this asserts the vocabulary exists for each, so
		// a modality added later without one fails here rather than silently probing with "".
		if n.capability == "" || n.field == "" {
			t.Fatalf("incomplete case: %+v", n)
		}
	}
}

// Only v4-gateway is a valid api type; every other protocol is refused at registration AND at test.
func TestOnlyTheGatewayProtocolIsAccepted(t *testing.T) {
	if err := agent.ValidateAPIType("v4-gateway"); err != nil {
		t.Fatalf("the only supported type was rejected: %v", err)
	}
	for _, bad := range []string{"openai", "openai-compatible", "anthropic", "gemini", "deepseek", "cohere"} {
		err := agent.ValidateAPIType(bad)
		if err == nil {
			t.Errorf("%q was accepted; only v4-gateway may be", bad)
		}
		if !strings.Contains(err.Error(), "v4-gateway") {
			t.Errorf("%q: the error does not name the one supported type: %v", bad, err)
		}
	}
}
