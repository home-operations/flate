package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"syscall"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote/errcode"
)

// RetryConfig bounds producer attempts within one acquisition.
type RetryConfig struct {
	// Attempts includes the first try; values <= 1 disable retries.
	Attempts int
	// MinWait defaults to 750 milliseconds when nonpositive.
	MinWait time.Duration
	// MaxWait defaults to 15 minutes when nonpositive.
	MaxWait time.Duration
	// Jitter is a fraction in [0,1]; non-finite values disable jitter.
	Jitter float64
}

// Policy is the caller's immutable freshness and retry policy.
type Policy struct {
	// MaxAge bounds freshness; Forever retains immutable pins.
	MaxAge time.Duration
	// Retry applies only to producer failures.
	Retry RetryConfig
}

// TerminalError prevents an outer acquisition from repeating a completed sequence.
type TerminalError struct {
	// Err retains the typed cause and its diagnostic text.
	Err error
	// Attempted distinguishes producer exhaustion from a suppressed non-attempt.
	Attempted bool
}

// Error preserves the underlying diagnostic.
func (e *TerminalError) Error() string { return e.Err.Error() }

// Unwrap exposes the domain sentinel and the underlying cause.
func (e *TerminalError) Unwrap() []error { return []error{manifest.ErrFlux, e.Err} }

type attemptCeilingKey struct{}

func (c *Cache) acquire(ctx context.Context, key acquireKey, policy Policy, produce func(context.Context, io.Writer) (ocispec.Descriptor, error)) (*entry, string, error) {
	if key.id == "" {
		var content struct {
			Archive any
			Blob    any
			Git     any
		}
		switch {
		case key.archive:
			content.Archive = struct {
				Content      any
				Ignore       string
				WithDefaults bool
			}{key.Content, key.Ignore, key.WithDefaults}
		case key.git != nil:
			content.Git = key.git.key
		default:
			content.Blob = struct{ Content any }{key.Content}
		}
		key.id = manifest.Fingerprint(content)
		if key.id == "" {
			return nil, "", &CacheError{fmt.Errorf("%w: acquisition content cannot be fingerprinted", manifest.ErrInput)}
		}
	}
	policy.Retry.Attempts = max(1, policy.Retry.Attempts)
	if ceiling, _ := ctx.Value(attemptCeilingKey{}).(bool); ceiling {
		policy.Retry.Attempts = 1
	}
	for attempt := 1; ; attempt++ {
		remaining := policy
		remaining.Retry.Attempts -= attempt - 1
		e, err := c.acquireOnce(ctx, key, remaining, produce)
		if err == nil {
			return e, key.id, nil
		}
		if _, ok := errors.AsType[*TerminalError](err); ok {
			return nil, "", err
		}
		if _, ok := err.(*producerError); !ok || !retryable(err) {
			return nil, "", err
		}
		delay := policy.Retry.backoff(attempt - 1)
		slog.Debug("cache: acquisition failed, retrying", "key", key.id, "attempt", attempt, "delay", delay, "error", err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return nil, "", &TerminalError{Err: errors.Join(err, ctx.Err()), Attempted: true}
		}
	}
}

func (c RetryConfig) backoff(attempt int) time.Duration {
	ceiling := c.MaxWait
	if ceiling <= 0 {
		ceiling = 15 * time.Minute
	}
	delay := c.MinWait
	if delay <= 0 {
		delay = 750 * time.Millisecond
	}
	delay = min(delay, ceiling)
	for range attempt {
		if delay >= ceiling-delay {
			delay = ceiling
			break
		}
		delay *= 2
	}
	if c.Jitter <= 0 || math.IsNaN(c.Jitter) || math.IsInf(c.Jitter, 0) {
		return delay
	}
	jitter := min(c.Jitter, 1)
	spread := float64(delay) * (1 - jitter + rand.Float64()*2*jitter) //nolint:gosec // Non-cryptographic retry jitter.
	if spread >= float64(ceiling) {
		return ceiling
	}
	return time.Duration(spread)
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[*TerminalError](err); ok {
		return false
	}
	if _, ok := errors.AsType[*fs.PathError](err); ok {
		return false
	}
	switch {
	case errors.Is(err, io.ErrShortWrite), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, manifest.ErrInput), errors.Is(err, manifest.ErrObjectNotFound),
		errors.Is(err, manifest.ErrMissingSecret), errors.Is(err, manifest.ErrSourceSkipped),
		permanentStatus(err):
		return false
	}
	if status, ok := errors.AsType[*errcode.ErrorResponse](err); ok {
		return status.StatusCode == http.StatusTooManyRequests || status.StatusCode >= 500 && status.StatusCode < 600
	}
	if network, ok := errors.AsType[net.Error](err); ok && network.Timeout() {
		return true
	}
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, io.ErrUnexpectedEOF)
}

func permanentStatus(err error) bool {
	if status, ok := err.(*errcode.ErrorResponse); ok {
		return status.StatusCode != http.StatusTooManyRequests && (status.StatusCode < 500 || status.StatusCode >= 600)
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		if slices.ContainsFunc(wrapped.Unwrap(), permanentStatus) {
			return true
		}
	case interface{ Unwrap() error }:
		return permanentStatus(wrapped.Unwrap())
	}
	return false
}
