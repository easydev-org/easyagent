package llm

import "regexp"

// Context-overflow detection: a curated, multi-provider wording table plus
// exclusions, ported from the TS agent's context-overflow.ts. A match is what
// triggers compaction + a single transparent retry of the step.
var overflowPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)prompt is too long`),
	regexp.MustCompile(`(?i)request_too_large`),
	regexp.MustCompile(`(?i)input is too long for requested model`),
	regexp.MustCompile(`(?i)exceeds the context window`),
	regexp.MustCompile(`(?i)exceeds (?:the )?(?:model'?s )?maximum context length`),
	regexp.MustCompile(`(?i)input token count.*exceeds the maximum`),
	regexp.MustCompile(`(?i)tokens in request more than max tokens allowed`),
	regexp.MustCompile(`(?i)maximum prompt length is \d+`),
	regexp.MustCompile(`(?i)reduce the length of the messages`),
	regexp.MustCompile(`(?i)maximum context length is \d+ tokens`),
	regexp.MustCompile(`(?i)exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?`),
	regexp.MustCompile(`(?i)input \(\d+ tokens\) is longer than the model'?s context length`),
	regexp.MustCompile(`(?i)exceeds the limit of \d+`),
	regexp.MustCompile(`(?i)exceeds the available context size`),
	regexp.MustCompile(`(?i)greater than the context length`),
	regexp.MustCompile(`(?i)context window exceeds limit`),
	regexp.MustCompile(`(?i)exceeded model token limit`),
	regexp.MustCompile(`(?i)context[_ ]length[_ ]exceeded`),
	regexp.MustCompile(`(?i)request entity too large`),
	regexp.MustCompile(`(?i)context length is only \d+ tokens`),
	regexp.MustCompile(`(?i)input length.*exceeds.*context length`),
	regexp.MustCompile(`(?i)prompt too long; exceeded (?:max )?context length`),
	regexp.MustCompile(`(?i)too large for model with \d+ maximum context length`),
	regexp.MustCompile(`(?i)model_context_window_exceeded`),
	regexp.MustCompile(`(?i)too many tokens`),
	regexp.MustCompile(`(?i)token limit exceeded`),
	regexp.MustCompile(`(?i)input exceeds the context limit`),
	regexp.MustCompile(`(?i)exceed(?:s|ed)? the model context limit`),
	// A bodyless 400/413 from an OpenAI-compatible gateway.
	regexp.MustCompile(`(?i)^4(00|13)\s*(status code)?\s*\(no body\)`),
}

// Wording that looks like overflow but is really a rate limit / outage: never
// compact on these (compaction is irreversible).
var overflowExclusions = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^(throttling error|service unavailable):`),
	regexp.MustCompile(`(?i)rate limit`),
	regexp.MustCompile(`(?i)too many requests`),
}

// IsContextOverflow reports whether a provider error message indicates the
// request exceeded the model's context window.
func IsContextOverflow(message string) bool {
	for _, re := range overflowExclusions {
		if re.MatchString(message) {
			return false
		}
	}
	for _, re := range overflowPatterns {
		if re.MatchString(message) {
			return true
		}
	}
	return false
}

// IsContextOverflowError is the error-typed convenience form.
func IsContextOverflowError(err error) bool {
	if err == nil {
		return false
	}
	return IsContextOverflow(err.Error())
}
