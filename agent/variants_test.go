package agent

import "testing"

func TestReasoningVariants(t *testing.T) {
	if VariantsFor(false) != nil {
		t.Error("a non-reasoning model must offer no variants")
	}
	vs := VariantsFor(true)
	if len(vs) != len(ReasoningLevels) {
		t.Fatalf("variants = %+v", vs)
	}
	body, ok := ResolveVariant("high")
	if !ok || body["reasoning"] != "high" {
		t.Errorf("resolve high = %+v, %v", body, ok)
	}
	if _, ok := ResolveVariant(""); ok {
		t.Error("empty variant must not resolve")
	}
	if _, ok := ResolveVariant("bogus"); ok {
		t.Error("unknown variant must not resolve")
	}
}
