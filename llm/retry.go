package llm

import (
	"context"
	"errors"
	"math"
	mrand "math/rand"
	"net/http"
	"strings"
	"time"
)

// Retry policy defaults (mirror the TS agent's llm-retry.ts).
const (
	RetryInitialDelay  = 2 * time.Second
	RetryBackoffFactor = 2.0
	RetryJitterFactor  = 0.25
	RetryMaxDelay      = 30 * time.Second
	// RetryMaxDelayHeader caps a header-supplied delay (32-bit ms ceiling).
	RetryMaxDelayHeader = 2147483647 * time.Millisecond
)

// RetryPolicy configures the bounded, visible retry loop.
type RetryPolicy struct {
	// MaxRetries is the number of retries AFTER the first attempt.
	MaxRetries int
	// InitialDelay overrides RetryInitialDelay (tests).
	InitialDelay time.Duration
	// Rand is injectable for deterministic tests; nil => math/rand.
	Rand func() float64
}

func (p RetryPolicy) initial() time.Duration {
	if p.InitialDelay > 0 {
		return p.InitialDelay
	}
	return RetryInitialDelay
}

// Backoff returns the delay before retry attempt (1-based), honoring a
// Retry-After header when present, else exponential + jitter, capped at 30s.
func (p RetryPolicy) Backoff(attempt int, err error, random float64) time.Duration {
	if d, ok := RetryAfter(err); ok {
		if d > RetryMaxDelayHeader {
			return RetryMaxDelayHeader
		}
		return d
	}
	base := float64(p.initial()) * math.Pow(RetryBackoffFactor, float64(attempt-1))
	jittered := base + base*RetryJitterFactor*random
	// Clamp in float space BEFORE converting: a large attempt overflows
	// math.Pow into +Inf, and converting that to time.Duration wraps negative.
	if !(jittered < float64(RetryMaxDelay)) {
		return RetryMaxDelay
	}
	return time.Duration(math.Ceil(jittered))
}

// IsRetryable reports whether an error escaped the SDK's own budgets but is
// worth retrying at the turn level (408/409/429/5xx + transport failures).
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.StatusCode != 0 {
			return isRetryableStatus(apiErr.StatusCode)
		}
	}
	msg := strings.ToLower(err.Error())
	for _, p := range retryablePatterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

func isRetryableStatus(status int) bool {
	return status == 408 || status == 409 || status == 429 || status >= 500
}

var retryablePatterns = []string{
	"terminated", "fetch failed", "failed to fetch",
	"socket hang up", "connection reset", "connection refused", "connection lost",
	"econnreset", "econnrefused", "etimedout", "epipe", "eai_again",
	"reset before headers", "upstream connect", "network error", "network_error",
	"request timeout", "response timeout", "connection timeout", "stream timeout",
	"read timeout", "i/o timeout", "timeout awaiting response",
}

// RetryAfter reads a Retry-After / retry-after-ms response header, if any.
func RetryAfter(err error) (time.Duration, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Header == nil {
		return 0, false
	}
	h := apiErr.Header
	if ms := h.Get("retry-after-ms"); ms != "" {
		if d, perr := time.ParseDuration(ms + "ms"); perr == nil && d >= 0 {
			return d, true
		}
	}
	if ra := h.Get("retry-after"); ra != "" {
		if secs, perr := time.ParseDuration(ra + "s"); perr == nil && secs >= 0 {
			return secs, true
		}
		if t, perr := http.ParseTime(ra); perr == nil {
			if d := time.Until(t); d > 0 {
				return d, true
			}
		}
	}
	return 0, false
}

// RetryingStream runs Stream with a bounded, capped, VISIBLE retry loop. onRetry
// is invoked before each sleep so the caller can publish a durable `retry`
// event (the whole point: the AI SDK's own retry loops are uncapped and silent).
// Partial output from a failed attempt is discarded (a fresh Stream is opened).
func (c *Client) RetryingStream(
	ctx context.Context,
	req ChatRequest,
	pol RetryPolicy,
	onRetry func(attempt int, delay time.Duration, err error),
) (<-chan Event, error) {
	first, err := c.Stream(ctx, req)
	if err != nil {
		return nil, err // build error: not retryable
	}
	rnd := pol.Rand
	if rnd == nil {
		rnd = mrand.Float64
	}
	out := make(chan Event, 16)
	go func() {
		defer close(out)
		cur := first
		attempt := 0
		for {
			var lastErr error
			retry := false
			for ev := range cur {
				if ev.Type == EventError {
					if IsRetryable(ev.Err) && attempt < pol.MaxRetries && ctx.Err() == nil {
						lastErr = ev.Err
						retry = true
						break
					}
					send(out, ctx, ev)
					return
				}
				if !send(out, ctx, ev) {
					return
				}
				if ev.Type == EventFinishStep {
					return
				}
			}
			if !retry {
				return
			}
			attempt++
			delay := pol.Backoff(attempt, lastErr, rnd())
			if onRetry != nil {
				onRetry(attempt, delay, lastErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			next, err := c.Stream(ctx, req)
			if err != nil {
				send(out, ctx, Event{Type: EventError, Err: err})
				return
			}
			cur = next
		}
	}()
	return out, nil
}
