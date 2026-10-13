package llm

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestBackoffNoHeaderExponentialCapped(t *testing.T) {
	p := RetryPolicy{InitialDelay: time.Second}
	// attempt 1: 1s * (1 + 0.25*0) = 1s
	if got := p.Backoff(1, nil, 0); got != time.Second {
		t.Errorf("attempt1 = %v, want 1s", got)
	}
	// attempt 2: 2s base; with random=1 => 2s + 0.5s = 2.5s
	if got := p.Backoff(2, nil, 1); got != 2500*time.Millisecond {
		t.Errorf("attempt2 = %v, want 2.5s", got)
	}
	// a huge attempt is capped at RetryMaxDelay (30s)
	if got := p.Backoff(50, nil, 1); got != RetryMaxDelay {
		t.Errorf("attempt50 = %v, want %v", got, RetryMaxDelay)
	}
}

func TestBackoffHonorsRetryAfterHeader(t *testing.T) {
	err := &APIError{
		StatusCode: 429,
		Header: http.Header{
			"Retry-After": []string{"7"},
		},
	}
	p := RetryPolicy{InitialDelay: time.Second}
	if got := p.Backoff(1, err, 0); got != 7*time.Second {
		t.Errorf("retry-after = %v, want 7s", got)
	}
}

func TestRetryAfterMilliseconds(t *testing.T) {
	err := &APIError{
		StatusCode: 503,
		Header: http.Header{
			"Retry-After-Ms": []string{"1500"},
		},
	}
	d, ok := RetryAfter(err)
	if !ok || d != 1500*time.Millisecond {
		t.Errorf("RetryAfter = %v, %v; want 1500ms, true", d, ok)
	}
}

func TestRetryAfterAbsent(t *testing.T) {
	if _, ok := RetryAfter(errors.New("boom")); ok {
		t.Error("plain error should have no Retry-After")
	}
}

func TestIsRetryableStatuses(t *testing.T) {
	for _, s := range []int{408, 409, 429, 500, 502, 503} {
		if !IsRetryable(&APIError{StatusCode: s}) {
			t.Errorf("status %d should be retryable", s)
		}
	}
	for _, s := range []int{400, 401, 403, 404, 422} {
		if IsRetryable(&APIError{StatusCode: s}) {
			t.Errorf("status %d should NOT be retryable", s)
		}
	}
}
