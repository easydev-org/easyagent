package llm

import "testing"

func TestIsContextOverflow(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"This model's maximum context length is 8192 tokens", true},
		{"prompt is too long: 300000 tokens > 200000 maximum", true},
		{"Input exceeds the context limit (200000 tokens). Please shorten the input.", true},
		{"400 status code (no body)", true},
		{"413 status code (no body)", true},
		{"Rate limit reached for gpt-4o", false},
		{"Throttling error: too many requests", false},
		{"invalid api key", false},
		{"service unavailable: please retry", false},
	}
	for _, c := range cases {
		if got := IsContextOverflow(c.msg); got != c.want {
			t.Errorf("IsContextOverflow(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}
