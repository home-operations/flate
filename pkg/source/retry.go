package source

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"syscall"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// RetryConfig tunes the bounded, classified retry applied uniformly to
// every source Fetcher via WithRetry. It's the single retry layer in
// flate's fetch path: the per-kind HTTP clients (oras for OCI, go-git,
// minio for Bucket) are deliberately left non-retrying so retries
// happen exactly once, here, with one predictable policy across kinds.
//
// Attempts is the TOTAL number of tries (the first call plus retries),
// so Attempts <= 1 disables retry entirely and WithRetry returns the
// wrapped Fetcher untouched. Backoff between tries is exponential
// (MinWait * 2^n) clamped to MaxWait, then spread by ±Jitter.
type RetryConfig struct {
	// Attempts is the total tries per fetch, including the first.
	// <= 1 disables retry.
	Attempts int
	// MinWait is the backoff floor (and the first retry's base delay).
	MinWait time.Duration
	// MaxWait clamps the exponential backoff ceiling.
	MaxWait time.Duration
	// Jitter is the fraction ([0,1]) of randomized spread applied to
	// each backoff so a fleet of fetches don't retry in lockstep.
	Jitter float64
}

func (c RetryConfig) enabled() bool { return c.Attempts > 1 }

// WithRetry wraps f so transient fetch failures are retried per cfg.
// When retry is disabled (Attempts <= 1) f is returned unwrapped, so
// the decorator adds zero overhead on the default-off path. Jitter is
// clamped to [0,1] here so a misconfigured flag can't produce negative
// backoff durations.
func WithRetry(f Fetcher, cfg RetryConfig) Fetcher {
	if !cfg.enabled() {
		return f
	}
	cfg.Jitter = max(0, min(1, cfg.Jitter))
	return retryFetcher{inner: f, cfg: cfg}
}

type retryFetcher struct {
	inner Fetcher
	cfg   RetryConfig
}

// Fetch calls the wrapped Fetcher, retrying only when retryable reports
// the error is a transient wire condition. Permanent errors (bad path,
// missing secret, auth, not-found) return on the first try — so a user
// error is never re-fetched. The loop is context-aware: a cancelled or
// deadline-exceeded ctx stops further attempts and returns the last
// fetch result rather than masking the real cause with ctx.Err().
func (r retryFetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	var (
		art *store.SourceArtifact
		err error
	)
	for attempt := 0; ; attempt++ {
		art, err = r.inner.Fetch(ctx, obj)
		if err == nil || attempt >= r.cfg.Attempts-1 || !retryable(err) {
			return art, err
		}
		wait := r.backoff(attempt)
		slog.Debug("source: retrying fetch after transient error",
			"id", obj.Named().String(),
			"attempt", attempt+1,
			"max_attempts", r.cfg.Attempts,
			"wait", wait,
			"err", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return art, err
		case <-timer.C:
		}
	}
}

// backoff returns the delay before the (attempt+1)-th try: an
// exponential MinWait*2^attempt clamped to MaxWait, then spread by
// ±Jitter. Mirrors oras-go's ExponentialBackoff shape so the OCI path
// feels the same as it did when retry lived in the HTTP transport.
func (r retryFetcher) backoff(attempt int) time.Duration {
	d := float64(r.cfg.MinWait) * math.Pow(2, float64(attempt))
	if hi := float64(r.cfg.MaxWait); hi > 0 { // MaxWait==0 means no ceiling
		d = min(d, hi)
	}
	if r.cfg.Jitter > 0 {
		d = d*(1-r.cfg.Jitter) + rand.Float64()*(2*r.cfg.Jitter*d) //nolint:gosec // non-crypto jitter
	}
	return time.Duration(d)
}

// retryable classifies a fetch error as a transient wire condition
// worth another try. It is deliberately conservative — default deny —
// so anything it doesn't positively recognize as transient fails fast,
// exactly as the fetch path behaved before retry existed. That's what
// keeps a user error (a typo'd path → 404 / NXDOMAIN, a bad ref, a
// missing secret) from being re-fetched pointlessly.
//
// The checks are kind-agnostic: they match standard net/syscall/io
// errors that surface identically whether the failure came from oras,
// go-git, or minio, so one classifier covers every source kind without
// importing those packages' bespoke error types.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	// Permanent classes win over any transient-looking leaf in the same
	// chain, so they're checked first. These are user/input errors and
	// caller-driven cancellation a retry can't fix.
	switch {
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, manifest.ErrInput),
		errors.Is(err, manifest.ErrObjectNotFound),
		errors.Is(err, manifest.ErrMissingSecret),
		errors.Is(err, manifest.ErrSourceSkipped):
		return false
	}
	// A dial/IO timeout is transient. net.DNSError for a typo'd host is
	// also a net.Error, but its Timeout() is false (IsNotFound/permanent),
	// so it correctly falls through to the default-deny below.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	// Connection reset (the common transient registry blip), connection
	// refused (registry momentarily down), and a truncated response body
	// are all worth another attempt.
	switch {
	case errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNREFUSED),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	}
	return false
}
