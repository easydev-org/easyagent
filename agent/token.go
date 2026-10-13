package agent

// EstimateTokens is a cheap, deterministic heuristic (NOT a real tokenizer):
// CJK chars ~1.5 chars/token, everything else ~4 chars/token. Good enough for
// compaction budget gating.
func EstimateTokens(text string) int {
	var cjk, other int
	for _, r := range text {
		if isCJK(r) {
			cjk++
		} else {
			other++
		}
	}
	// CJK ~1.5 chars/token => ceil(cjk*2/3); others ~4 chars/token => ceil(other/4).
	return ceilDiv(cjk, 2, 3) + ceilDiv(other, 1, 4)
}

func isCJK(r rune) bool {
	return (r >= 0x3040 && r <= 0x30ff) || // kana
		(r >= 0x3400 && r <= 0x4dbf) || // CJK ext A
		(r >= 0x4e00 && r <= 0x9fff) || // CJK unified
		(r >= 0xf900 && r <= 0xfaff) || // CJK compat
		(r >= 0xff00 && r <= 0xffef) // fullwidth
}

// ceilDiv returns ceil(n * num / den).
func ceilDiv(n, num, den int) int {
	return (n*num + den - 1) / den
}

// MaskSecret is the display mask for a stored secret (provider api keys). Long
// secrets show `<first4>****<last4>`; short ones mask fully. The masked value
// doubles as the edit-time sentinel: saving the mask round-trips the stored key
// untouched.
func MaskSecret(secret string) string {
	if len(secret) <= 8 {
		return "****"
	}
	return secret[:4] + "****" + secret[len(secret)-4:]
}
