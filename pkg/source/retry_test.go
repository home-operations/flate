package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// connReset builds an error shaped like the real failure that motivated
// this code: `Head "<url>": read tcp ...: connection reset by peer`,
// wrapped through url.Error and a fmt %w like oras/flate produce.
func connReset() error {
	return fmt.Errorf("OCIRepository flux-system/x resolve 1.0.0: %w",
		&url.Error{
			Op:  "Head",
			URL: "https://reg.example/v2/x/manifests/1.0.0",
			Err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: os.NewSyscallError("read", syscall.ECONNRESET),
			},
		})
}

// timeoutErr is a net.Error reporting a timeout (e.g. dial/IO deadline).
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestRetryable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection reset", connReset(), true},
		{"connection refused",
			&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, true},
		{"dial timeout", fmt.Errorf("dial: %w", timeoutErr{}), true},
		{"unexpected eof", fmt.Errorf("copy layer: %w", io.ErrUnexpectedEOF), true},
		// A typo'd host resolves to NXDOMAIN — a net.Error, but Timeout()
		// is false, so it must NOT be retried.
		{"dns not found", &net.DNSError{Err: "no such host", Name: "bad.host", IsNotFound: true}, false},
		// io.EOF is too ambiguous (legitimate stream end) to treat as
		// transient — only ErrUnexpectedEOF qualifies.
		{"plain eof", io.EOF, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"input error", fmt.Errorf("%w: bad ref", manifest.ErrInput), false},
		{"missing secret", manifest.ErrMissingSecret, false},
		{"source skipped", manifest.ErrSourceSkipped, false},
		// A registry 404 (wrong path/tag) surfaces as a plain status
		// error — unrecognized, so default-deny keeps it a hard failure.
		{"http 404", errors.New("unexpected status: 404 Not Found"), false},
		// Permanent classes are checked first, so they win even when a
		// transient leaf (here a timeout net.Error) coexists in the chain.
		{"permanent wins over transient leaf",
			fmt.Errorf("%w: %w", context.DeadlineExceeded, timeoutErr{}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := retryable(tc.err); got != tc.want {
				t.Errorf("retryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestBackoff_ExponentialClamped(t *testing.T) {
	t.Parallel()
	r := retryFetcher{cfg: RetryConfig{MinWait: 100 * time.Millisecond, MaxWait: time.Second, Jitter: 0}}
	want := []time.Duration{
		100 * time.Millisecond, // 2^0
		200 * time.Millisecond, // 2^1
		400 * time.Millisecond, // 2^2
		800 * time.Millisecond, // 2^3
		time.Second,            // 2^4 = 1600ms, clamped to MaxWait
		time.Second,            // clamped
	}
	for attempt, w := range want {
		if got := r.backoff(attempt); got != w {
			t.Errorf("backoff(%d) = %v, want %v", attempt, got, w)
		}
	}
}

func TestBackoff_JitterWithinBounds(t *testing.T) {
	t.Parallel()
	const jitter = 0.2
	r := retryFetcher{cfg: RetryConfig{MinWait: 100 * time.Millisecond, MaxWait: time.Hour, Jitter: jitter}}
	for attempt := range 5 {
		base := float64(100*time.Millisecond) * math.Pow(2, float64(attempt))
		lo := time.Duration(base * (1 - jitter))
		hi := time.Duration(base * (1 + jitter))
		for range 100 {
			if got := r.backoff(attempt); got < lo || got >= hi {
				t.Fatalf("backoff(%d) = %v, want within [%v, %v)", attempt, got, lo, hi)
			}
		}
	}
}

// fakeFetcher returns errs[i] on the i-th call (the last entry repeats
// once exhausted, or success with art once past len(errs)).
type fakeFetcher struct {
	calls int
	errs  []error
	art   *store.SourceArtifact
}

func (f *fakeFetcher) Fetch(_ context.Context, _ manifest.BaseManifest) (*store.SourceArtifact, error) {
	i := f.calls
	f.calls++
	if i < len(f.errs) {
		return nil, f.errs[i]
	}
	return f.art, nil
}

func repeatErr(e error, n int) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = e
	}
	return out
}

func testObj() manifest.BaseManifest {
	return stubObj{}
}

type stubObj struct{}

func (stubObj) Named() manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "x"}
}

// fastRetry is a config with negligible waits so tests don't sleep.
func fastRetry(attempts int) RetryConfig {
	return RetryConfig{Attempts: attempts, MinWait: time.Microsecond, MaxWait: time.Microsecond, Jitter: 0}
}

func TestWithRetry_DisabledIsNoOp(t *testing.T) {
	t.Parallel()
	// Attempts <= 1 must not wrap: a transient error returns after one call.
	inner := &fakeFetcher{errs: repeatErr(connReset(), 5)}
	got := WithRetry(inner, RetryConfig{Attempts: 1})
	if _, err := got.Fetch(context.Background(), testObj()); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("err = %v, want ECONNRESET", err)
	}
	if inner.calls != 1 {
		t.Errorf("calls = %d, want 1 (retry disabled)", inner.calls)
	}
}

func TestRetryFetcher_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	want := &store.SourceArtifact{}
	inner := &fakeFetcher{errs: []error{connReset(), connReset()}, art: want}
	art, err := WithRetry(inner, fastRetry(4)).Fetch(context.Background(), testObj())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art != want {
		t.Errorf("art = %v, want the success artifact", art)
	}
	if inner.calls != 3 {
		t.Errorf("calls = %d, want 3 (2 transient failures + 1 success)", inner.calls)
	}
}

func TestRetryFetcher_FailFastOnPermanent(t *testing.T) {
	t.Parallel()
	// A wrong path is a permanent (input) error: it must be fetched once,
	// never retried — the exact concern this design guards against.
	permanent := fmt.Errorf("%w: no OCIRepository named %q", manifest.ErrInput, "typo")
	inner := &fakeFetcher{errs: repeatErr(permanent, 5)}
	_, err := WithRetry(inner, fastRetry(4)).Fetch(context.Background(), testObj())
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("err = %v, want ErrInput", err)
	}
	if inner.calls != 1 {
		t.Errorf("calls = %d, want 1 (permanent error must not retry)", inner.calls)
	}
}

func TestRetryFetcher_ExhaustsAttempts(t *testing.T) {
	t.Parallel()
	inner := &fakeFetcher{errs: repeatErr(connReset(), 10)}
	_, err := WithRetry(inner, fastRetry(4)).Fetch(context.Background(), testObj())
	if !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("err = %v, want ECONNRESET", err)
	}
	if inner.calls != 4 {
		t.Errorf("calls = %d, want 4 (Attempts)", inner.calls)
	}
}

func TestRetryFetcher_ContextCancelStops(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the first attempt completes
	inner := &fakeFetcher{errs: repeatErr(connReset(), 10)}
	// Huge MinWait: if cancellation weren't honored the test would hang.
	cfg := RetryConfig{Attempts: 5, MinWait: time.Hour, MaxWait: time.Hour}
	_, err := WithRetry(inner, cfg).Fetch(ctx, testObj())
	if !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("err = %v, want the last fetch error (ECONNRESET)", err)
	}
	if inner.calls != 1 {
		t.Errorf("calls = %d, want 1 (ctx cancel stops the backoff)", inner.calls)
	}
}
