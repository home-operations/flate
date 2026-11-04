package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

// connReset models wrapped OCI transport failures.
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

// timeoutError is a [net.Error] reporting a timeout (e.g. dial/IO deadline).
type timeoutError struct{}

func (timeoutError) Error() string { return "i/o timeout" }

func (timeoutError) Timeout() bool { return true }

func (timeoutError) Temporary() bool { return true }

func TestRetryable(t *testing.T) {
	t.Parallel()

	cases := []testRetryableRetryCase{
		{"nil", nil, false},
		{"skipped joined reset", errors.Join(manifest.ErrSourceSkipped, connReset()), false},
		{"cancelled joined timeout", errors.Join(context.Canceled, timeoutError{}), false},
		{
			"local file joined reset",
			errors.Join(&fs.PathError{Op: "write", Path: "sink", Err: syscall.ECONNRESET}, connReset()),
			false,
		},
		{"short write joined reset", errors.Join(io.ErrShortWrite, connReset()), false},
		{"429", &fetcherr.ResponseError{StatusCode: 429}, true},
		{"503", &fetcherr.ResponseError{StatusCode: 503}, true},
		{
			"joined statuses",
			errors.Join(
				&fetcherr.ResponseError{StatusCode: 503},
				&fetcherr.ResponseError{StatusCode: 404},
				connReset(),
			),
			false,
		},
		{"input joined timeout", errors.Join(manifest.ErrInput, timeoutError{}), false},
		{"missing secret joined reset", errors.Join(manifest.ErrMissingSecret, connReset()), false},
		{"not found joined timeout", errors.Join(manifest.ErrObjectNotFound, timeoutError{}), false},
		{"terminal non-attempt", &TerminalError{Err: connReset()}, false},
		{"terminal attempt", &TerminalError{Err: connReset(), Attempted: true}, false},
		{"connection reset", connReset(), true},
		{
			"connection refused",
			&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, true,
		},
		{"dial timeout", fmt.Errorf("dial: %w", timeoutError{}), true},
		{"unexpected eof", fmt.Errorf("copy layer: %w", io.ErrUnexpectedEOF), true},
		// A typo'd host resolves to NXDOMAIN — a [net.Error], but Timeout()
		// is false, so it must NOT be retried.
		{"dns not found", &net.DNSError{Err: "no such host", Name: "bad.host", IsNotFound: true}, false},
		// io.EOF is too ambiguous (legitimate stream end) to treat as
		// transient — only ErrUnexpectedEOF qualifies.
		{"plain eof", io.EOF, false},
		{"context canceled", context.Canceled, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"input error", manifest.NewError(manifest.ErrInput, nil, "bad ref"), false},
		{"missing secret", manifest.ErrMissingSecret, false},
		{"source skipped", manifest.ErrSourceSkipped, false},
		// A registry 404 (wrong path/tag) surfaces as a plain status
		// error — unrecognized, so default-deny keeps it a hard failure.
		{"http 404", errors.New("unexpected status: 404 Not Found"), false},
		// Permanent classes are checked first, so they win even when a
		// transient leaf (here a timeout net.Error) coexists in the chain.
		{
			"permanent wins over transient leaf",
			fmt.Errorf("%w: %w", context.DeadlineExceeded, timeoutError{}), false,
		},
	}
	for _, tc := range []struct {
		status int
		want   bool
	}{
		{200, false},
		{300, false},
		{400, false},
		{401, false},
		{403, false},
		{404, false},
		{429, true},
		{499, false},
		{500, true},
		{503, true},
		{599, true},
		{600, false},
	} {
		cause := &fetcherr.ResponseError{StatusCode: tc.status, Op: "GET"}
		cases = append(
			cases,
			testRetryableRetryCase{fmt.Sprintf("status-%d", tc.status), fmt.Errorf("edge: %w", cause), tc.want},
		)
		if !tc.want {
			for i, transient := range []error{&fetcherr.ResponseError{StatusCode: 503, Op: "GET"}, timeoutError{}} {
				cases = append(
					cases,
					testRetryableRetryCase{
						fmt.Sprintf("terminal-%d/first/%d", tc.status, i),
						errors.Join(cause, transient),
						false,
					},
					testRetryableRetryCase{
						fmt.Sprintf("terminal-%d/last/%d", tc.status, i),
						errors.Join(transient, fmt.Errorf("nested: %w", cause)),
						false,
					},
				)
			}
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); testRetryableCase(t, testRetryableCaseInputs{tc: &tc}) })
	}
}

func TestBackoff_ExponentialClamped(t *testing.T) {
	t.Parallel()
	r := RetryConfig{MinWait: 100 * time.Millisecond, MaxWait: time.Second, Jitter: 0}
	want := []time.Duration{
		100 * time.Millisecond, // 2^0
		200 * time.Millisecond, // 2^1
		400 * time.Millisecond, // 2^2
		800 * time.Millisecond, // 2^3
		time.Second,            // 2^4 = 1600ms, clamped to MaxWait
		time.Second,            // clamped
	}
	for attempt, w := range want {
		{
			got := r.backoff(attempt)
			assert.ErrorIf(t, got != w, "backoff(%d) = %v, want %v", attempt, got, w)
		}
	}
}

func TestBackoff_JitterWithinBounds(t *testing.T) {
	t.Parallel()
	const jitter = 0.2
	r := RetryConfig{MinWait: 100 * time.Millisecond, MaxWait: time.Hour, Jitter: jitter}
	for attempt := range 5 {
		base := float64(100*time.Millisecond) * math.Pow(2, float64(attempt))
		lo := time.Duration(base * (1 - jitter))
		hi := time.Duration(base * (1 + jitter))
		for range 100 {
			{
				got := r.backoff(attempt)
				assert.FatalIf(t, got < lo || got >= hi, "backoff(%d) = %v, want within [%v, %v)", attempt, got, lo, hi)
			}
		}
	}
}

func fastRetry(attempts int) RetryConfig {
	return RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
}

func TestAcquire_RetrySequence(t *testing.T) {
	t.Parallel()
	t.Run("persistence", func(t *testing.T) { t.Parallel(); testAcquireRetrySequencePersistence(t) })

	for _, tc := range []struct {
		name                     string
		attempts, failures, want int
		failure                  error
	}{
		{"zero", 0, 4, 1, connReset()},
		{"one", 1, 4, 1, connReset()},
		{"success", 4, 2, 3, connReset()},
		{"exhaustion", 3, 4, 3, connReset()},
		{"permanent", 3, 4, 1, errors.Join(manifest.ErrInput, connReset())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAcquireRetrySequenceCase(t, &tc)
		},
		)
	}
}

func TestAcquire_NestedTerminal(t *testing.T) {
	t.Parallel()
	t.Run("due-ceiling", func(t *testing.T) { t.Parallel(); testAcquireNestedTerminalDueCeiling(t) })

	for _, attempted := range []bool{false, true} {
		t.Run(strconv.FormatBool(attempted), func(t *testing.T) {
			t.Parallel()
			testAcquireNestedTerminalCase(t, &attempted)
		},
		)
	}
	for _, attempts := range []int{1, 3} {
		t.Run(fmt.Sprintf("child-%d", attempts), func(t *testing.T) {
			t.Parallel()
			testAcquireNestedTerminalCase2(t, &attempts)
		},
		)
	}
}

func TestAcquire_RetryFreshArchive(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(strconv.FormatBool(disk), func(t *testing.T) {
			t.Parallel()
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			calls := 0
			var payloadTar bytes.Buffer
			testutil.WriteTar(t, &payloadTar, map[string]string{"file": "complete"}, 0o644)
			payload := payloadTar.Bytes()
			input, _, _, err := c.Acquire(
				t.Context(),
				ArchiveKey{Content: "partial"},
				Policy{Retry: fastRetry(3)},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					calls++
					if calls == 1 {
						_, _ = w.Write(payload[:len(payload)/2])
						return ProducerResult{}, connReset()
					}
					return producer(payload, &ocispec.Descriptor{})(ctx, w)
				},
			)
			assert.FatalIf(t, err != nil || calls != 2, "calls=%d, error=%v", calls, err)
			data, err := fs.ReadFile(input, "file")
			assert.FatalIf(t, err != nil || string(data) != "complete", "data=%q, error=%v", data, err)
		})
	}
}

func TestAcquire_PolicyIsolation(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse-%t", concurrency, reverse), func(t *testing.T) {
				t.Parallel()
				testAcquirePolicyIsolationCase(
					t,
					&concurrency, &reverse,
				)
			},
			)
		}
	}
}

func TestAcquire_BackoffReleasesKey(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		done := make(chan error, 1)
		calls := 0
		go func() {
			_, _, _, err := c.Blob(
				t.Context(),
				BlobKey{Content: "shared"},
				Policy{MaxAge: Forever, Retry: RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour}},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					calls++
					return ProducerResult{}, connReset()
				},
			)
			done <- err
		}()
		synctest.Wait()
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "shared"},
			Policy{MaxAge: Forever},
			producer([]byte("other fill"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		{
			err := <-done
			assert.FatalIf(t, err != nil || calls != 1, "calls=%d, error=%v", calls, err)
		}
	})
}

func TestAcquire_RetryCancellation(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			t.Parallel()
			testAcquireRetryCancellationCase(t, &concurrency)
		},
		)
	}
}

func TestBackoff_NormalizationAndSaturation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		cfg     RetryConfig
		attempt int
		want    time.Duration
	}{
		{"default", RetryConfig{}, 0, 750 * time.Millisecond},
		{"default ceiling", RetryConfig{}, 1000, 15 * time.Minute},
		{"max count", RetryConfig{}, math.MaxInt, 15 * time.Minute},
		{"negative count", RetryConfig{}, -1, 750 * time.Millisecond},
		{"negative bounds", RetryConfig{MinWait: -1, MaxWait: -1}, 0, 750 * time.Millisecond},
		{"inverted bounds", RetryConfig{MinWait: time.Hour, MaxWait: time.Second}, 0, time.Second},
		{"saturation", RetryConfig{MinWait: time.Duration(math.MaxInt64/2 + 1), MaxWait: time.Duration(math.MaxInt64)}, 1, time.Duration(math.MaxInt64)},
		{"nan", RetryConfig{Jitter: math.NaN()}, 0, 750 * time.Millisecond},
		{"infinite", RetryConfig{Jitter: math.Inf(1)}, 0, 750 * time.Millisecond},
		{"negative jitter", RetryConfig{Jitter: -1}, 0, 750 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.backoff(tc.attempt); got != tc.want {
				t.Fatalf("delay=%v, want %v", got, tc.want)
			}
		})
	}
	cfg := RetryConfig{MinWait: time.Duration(math.MaxInt64/2 + 1), MaxWait: time.Duration(math.MaxInt64), Jitter: 2}
	for range 1000 {
		{
			got := cfg.backoff(1)
			assert.FatalIf(t, got < 0 || got > cfg.MaxWait, "overflow: %v", got)
		}
	}
}

func TestGit_RetryDiscardsGeneration(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(strconv.FormatBool(disk), func(t *testing.T) {
			t.Parallel()
			testGitRetryDiscardsGenerationCase(t, &disk)
		},
		)
	}
}

func TestAcquire_ReaderFailureCancelsAttempt(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				t.Parallel()
				testAcquireReaderFailureCancelsAttemptCase(
					t,
					&concurrency, &backing,
				)
			},
			)
		}
	}
}

func TestAcquire_BackoffResets(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		for range 2 {
			calls := 0
			start := time.Now()
			_, _, _, err := c.Blob(
				t.Context(),
				BlobKey{Content: "reset"},
				Policy{Retry: RetryConfig{Attempts: 3}},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					calls++
					if calls == 1 {
						return ProducerResult{}, connReset()
					}
					return producer(nil, &ocispec.Descriptor{})(ctx, w)
				},
			)
			assert.NoError(t, err)

			if calls != 2 || time.Since(start) != 750*time.Millisecond {
				t.Fatalf("calls=%d, wait=%v", calls, time.Since(start))
			}
		}
	})
}

func TestAcquire_CleanupFailureStopsRetry(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, kind := range []string{"blob", "archive", "git"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, kind), func(t *testing.T) {
				t.Parallel()
				testAcquireCleanupFailureStopsRetryCase(
					t,
					&concurrency, &kind,
				)
			},
			)
		}
	}
}

func testAcquireRetrySequencePersistence(t *testing.T) {
	t.Helper()

	for _, disk := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			cause    error
			attempts int
			record   bool
		}{
			{"terminal", &TerminalError{Err: connReset(), Attempted: true}, 1, true},
			{"unsupported timeout", timeoutError{}, 3, false},
			{"ambiguous eof", fmt.Errorf("layer: %w", io.ErrUnexpectedEOF), 3, false},
			{"input", errors.Join(connReset(), fmt.Errorf("invalid: %w", manifest.ErrInput)), 1, false},
			{"missing credentials", errors.Join(connReset(), manifest.ErrMissingSecret), 1, false},
			{"cancellation", errors.Join(connReset(), context.Canceled), 1, false},
			{"deadline cancellation", errors.Join(connReset(), context.DeadlineExceeded), 1, false},
			{"short write", errors.Join(connReset(), io.ErrShortWrite), 1, false},
			{"local", errors.Join(connReset(), &fs.PathError{Op: "write", Path: "sink", Err: fs.ErrPermission}), 1, false},
			{"nested non-attempt", &TerminalError{Err: errors.Join(connReset(), &TerminalError{Err: connReset()}), Attempted: true}, 1, false},
		} {
			t.Run(fmt.Sprintf("%t/%s", disk, tc.name), func(t *testing.T) {
				t.Parallel()
				testAcquireRetrySequencePersistenceCase(
					t,
					testAcquireRetrySequencePersistenceCaseInputs{disk: &disk, tc: &tc},
				)
			},
			)
		}
	}
}

func testAcquireRetrySequenceCase(t *testing.T, tc *struct {
	name     string
	attempts int
	failures int
	want     int
	failure  error
},
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", New)
		calls := 0
		r, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "retry"},
			Policy{Retry: fastRetry(tc.attempts)},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				if ctx.Err() != nil {
					t.Error(ctx.Err())
				}
				if calls <= tc.failures {
					_, _ = io.WriteString(w, "partial")
					return ProducerResult{}, tc.failure
				}
				_, err := io.WriteString(w, "complete")
				return ProducerResult{}, err
			},
		)
		if calls != tc.want {
			t.Fatalf("calls=%d, want %d", calls, tc.want)
		}
		if tc.failures < tc.want {
			assert.NoError(t, err)

			body, readErr := fs.ReadFile(r, "content")
			assert.FatalIf(t, string(body) != "complete" || readErr != nil, "body=%q, error=%v", body, readErr)
		} else {
			assert.FatalIf(
				t,
				!errors.Is(err, tc.failure) || !errors.Is(err, manifest.ErrFlux),
				"error identity: %v",
				err,
			)
			terminal, ok := errors.AsType[*TerminalError](err)
			assert.FatalIf(
				t,
				ok != retryable(tc.failure) || ok && !terminal.Attempted,
				"terminal=%+v, error=%v",
				terminal,
				err,
			)
		}
	})
}

func testAcquireNestedTerminalDueCeiling(t *testing.T) {
	t.Helper()

	for _, callers := range []int{2, 4} {
		for _, child := range []string{"missing", "due", "fresh"} {
			t.Run(fmt.Sprintf("%d/%s", callers, child), func(t *testing.T) {
				t.Parallel()
				testAcquireNestedTerminalDueCeilingCase(
					t,
					testAcquireNestedTerminalDueCeilingCaseInputs{callers: &callers, child: &child},
				)
			},
			)
		}
	}
}

func testAcquireNestedTerminalCase(t *testing.T, attempted *bool,
) {
	t.Helper()

	c := cachetest.New(t, "", New)
	cause := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	terminal := &TerminalError{Err: cause, Attempted: (*attempted)}
	calls := 0
	_, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "outer"},
		Policy{Retry: fastRetry(3)},
		func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
			calls++
			_, _, _, err := c.Blob(
				ctx,
				BlobKey{Content: "inner"},
				Policy{Retry: fastRetry(3)},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					return ProducerResult{}, terminal
				},
			)
			return ProducerResult{}, err
		},
	)
	got, ok := errors.AsType[*TerminalError](err)
	assert.FatalIf(t, !ok || got != terminal || calls != 1 || err.Error() != cause.Error() ||
		!errors.Is(err, manifest.ErrFlux) ||
		!errors.Is(err, cause), "terminal=%+v, error=%v, calls=%d", got, err, calls)
}

func testAcquireNestedTerminalCase2(t *testing.T, attempts *int,
) {
	t.Helper()

	c := cachetest.New(t, "", New)
	outer, inner := 0, 0
	_, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "outer"},
		Policy{Retry: fastRetry(3)},
		func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
			outer++
			_, _, _, err := c.Blob(
				ctx,
				BlobKey{Content: "inner"},
				Policy{Retry: fastRetry((*attempts))},
				func(context.Context, ProducerInput) (ProducerResult, error) {
					inner++
					return ProducerResult{}, connReset()
				},
			)
			return ProducerResult{}, err
		},
	)
	assert.FatalIf(
		t,
		outer != 1 || inner != (*attempts) || !errors.Is(err, syscall.ECONNRESET),
		"calls=%d/%d, error=%v",
		outer,
		inner,
		err,
	)
}

type testRetryableRetryCase struct {
	name string
	err  error
	want bool
}
type testRetryableCaseInputs struct {
	tc *testRetryableRetryCase
}

func testRetryableCase(t *testing.T, testCaseInput testRetryableCaseInputs) {
	t.Helper()

	if got := retryable(testCaseInput.tc.err); got != testCaseInput.tc.want {
		t.Errorf("retryable(%v) = %v, want %v", testCaseInput.tc.err, got, testCaseInput.tc.want)
	}
}
