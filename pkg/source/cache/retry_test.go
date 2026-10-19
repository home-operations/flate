package cache

import (
	"archive/tar"
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
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

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

// timeoutErr is a net.Error reporting a timeout (e.g. dial/IO deadline).
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestRetryable(t *testing.T) {
	t.Parallel()
	type retryCase struct {
		name string
		err  error
		want bool
	}
	cases := []retryCase{
		{"nil", nil, false},
		{"skipped joined reset", errors.Join(manifest.ErrSourceSkipped, connReset()), false},
		{"cancelled joined timeout", errors.Join(context.Canceled, timeoutErr{}), false},
		{"local file joined reset", errors.Join(&fs.PathError{Op: "write", Path: "sink", Err: syscall.ECONNRESET}, connReset()), false},
		{"short write joined reset", errors.Join(io.ErrShortWrite, connReset()), false},
		{"429", &fetcherr.ResponseError{StatusCode: 429}, true},
		{"503", &fetcherr.ResponseError{StatusCode: 503}, true},
		{"joined statuses", errors.Join(&fetcherr.ResponseError{StatusCode: 503}, &fetcherr.ResponseError{StatusCode: 404}, connReset()), false},
		{"input joined timeout", errors.Join(manifest.ErrInput, timeoutErr{}), false},
		{"missing secret joined reset", errors.Join(manifest.ErrMissingSecret, connReset()), false},
		{"not found joined timeout", errors.Join(manifest.ErrObjectNotFound, timeoutErr{}), false},
		{"terminal non-attempt", &TerminalError{Err: connReset()}, false},
		{"terminal attempt", &TerminalError{Err: connReset(), Attempted: true}, false},
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
	for _, tc := range []struct {
		status int
		want   bool
	}{
		{200, false}, {300, false}, {400, false}, {401, false}, {403, false}, {404, false},
		{429, true}, {499, false}, {500, true}, {503, true}, {599, true}, {600, false},
	} {
		cause := &fetcherr.ResponseError{StatusCode: tc.status, Op: "GET"}
		cases = append(cases, retryCase{fmt.Sprintf("status-%d", tc.status), fmt.Errorf("edge: %w", cause), tc.want})
		if !tc.want {
			for i, transient := range []error{&fetcherr.ResponseError{StatusCode: 503, Op: "GET"}, timeoutErr{}} {
				cases = append(cases,
					retryCase{fmt.Sprintf("terminal-%d/first/%d", tc.status, i), errors.Join(cause, transient), false},
					retryCase{fmt.Sprintf("terminal-%d/last/%d", tc.status, i), errors.Join(transient, fmt.Errorf("nested: %w", cause)), false})
			}
		}
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
		if got := r.backoff(attempt); got != w {
			t.Errorf("backoff(%d) = %v, want %v", attempt, got, w)
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
			if got := r.backoff(attempt); got < lo || got >= hi {
				t.Fatalf("backoff(%d) = %v, want within [%v, %v)", attempt, got, lo, hi)
			}
		}
	}
}

func fastRetry(attempts int) RetryConfig {
	return RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
}

func TestAcquire_RetrySequence(t *testing.T) {
	t.Parallel()
	t.Run("persistence", func(t *testing.T) {
		t.Parallel()
		for _, disk := range []bool{false, true} {
			for _, tc := range []struct {
				name     string
				cause    error
				attempts int
				record   bool
			}{
				{"terminal", &TerminalError{Err: connReset(), Attempted: true}, 1, true},
				{"unsupported timeout", timeoutErr{}, 3, false},
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
					synctest.Test(t, func(t *testing.T) {
						root := ""
						if disk {
							root = t.TempDir()
						}
						c := cachetest.New(t, root, New)
						calls := 0
						for i := range 2 {
							_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "eligibility"}, Policy{Retry: fastRetry(3)}, func(_ context.Context, w ProducerInput) (ProducerResult, error) {
								calls++
								_, _ = io.WriteString(w, "partial")
								return ProducerResult{}, tc.cause
							})
							want := tc.attempts * (i + 1)
							if disk && tc.record {
								want = 1
							}
							if calls != want || err == nil || !errors.Is(err, manifest.ErrFlux) {
								t.Fatalf("calls=%d, want=%d, error=%v", calls, want, err)
							}
							if !disk || !tc.record || i == 0 {
								if !errors.Is(err, tc.cause) {
									t.Fatalf("live cause lost: %v", err)
								}
							}
						}
						if disk {
							desc, err := c.store.Resolve(t.Context(), manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"eligibility"}}))
							if tc.record && (err != nil || desc.MediaType != ocispec.MediaTypeImageManifest) || !tc.record && !errors.Is(err, errdef.ErrNotFound) {
								t.Fatalf("eligibility descriptor=%+v, error=%v", desc, err)
							}
							files, err := os.ReadDir(filepath.Join(c.root.Name(), ociDir))
							if err != nil {
								t.Fatal(err)
							}
							for _, file := range files {
								if strings.HasPrefix(file.Name(), "acquire-") {
									t.Fatalf("failed sink remains: %s", file.Name())
								}
							}
						}
					})
				})
			}
		}
	})

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
			synctest.Test(t, func(t *testing.T) {
				c := cachetest.New(t, "", New)
				calls := 0
				r, _, _, err := c.Blob(t.Context(), BlobKey{Content: "retry"}, Policy{Retry: fastRetry(tc.attempts)}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
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
				})
				if calls != tc.want {
					t.Fatalf("calls=%d, want %d", calls, tc.want)
				}
				if tc.failures < tc.want {
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(r)
					closeErr := r.Close()
					if string(body) != "complete" || readErr != nil || closeErr != nil {
						t.Fatalf("body=%q, errors=%v/%v", body, readErr, closeErr)
					}
				} else {
					if !errors.Is(err, tc.failure) || !errors.Is(err, manifest.ErrFlux) {
						t.Fatalf("error identity: %v", err)
					}
					terminal, ok := errors.AsType[*TerminalError](err)
					if ok != retryable(tc.failure) || ok && !terminal.Attempted {
						t.Fatalf("terminal=%+v, error=%v", terminal, err)
					}
				}
			})
		})
	}
}

func TestAcquire_NestedTerminal(t *testing.T) {
	t.Parallel()
	t.Run("due-ceiling", func(t *testing.T) {
		t.Parallel()
		for _, callers := range []int{2, 4} {
			for _, child := range []string{"missing", "due", "fresh"} {
				t.Run(fmt.Sprintf("%d/%s", callers, child), func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						c := cachetest.New(t, t.TempDir(), New)
						fail := func(context.Context, ProducerInput) (ProducerResult, error) {
							return ProducerResult{}, connReset()
						}
						r, _, outerID, err := c.Blob(t.Context(), BlobKey{Content: "outer"}, Policy{}, producer(nil, ocispec.Descriptor{}))
						if err != nil {
							t.Fatal(err)
						}
						if err := r.Close(); err != nil {
							t.Fatal(err)
						}
						for _, id := range []string{"outer", "inner"} {
							if id == "inner" && child != "due" {
								continue
							}
							if _, _, _, err := c.Blob(t.Context(), BlobKey{Content: id}, Policy{}, fail); !errors.Is(err, syscall.ECONNRESET) {
								t.Fatal(err)
							}
						}
						time.Sleep(750 * time.Millisecond)
						if child == "fresh" {
							if _, _, _, err := c.Blob(t.Context(), BlobKey{Content: "inner"}, Policy{}, fail); !errors.Is(err, syscall.ECONNRESET) {
								t.Fatal(err)
							}
						}
						before, err := c.store.Resolve(t.Context(), outerID)
						if err != nil {
							t.Fatal(err)
						}
						unlock, err := c.locks.Acquire(t.Context(), outerID)
						if err != nil {
							t.Fatal(err)
						}
						done := make(chan error, callers)
						outer, inner := 0, 0
						for range callers {
							go func() {
								_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "outer"}, Policy{Retry: fastRetry(5)}, func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
									outer++
									_, _, _, err := c.Blob(ctx, BlobKey{Content: "inner"}, Policy{Retry: fastRetry(7)}, func(context.Context, ProducerInput) (ProducerResult, error) {
										inner++
										return ProducerResult{}, connReset()
									})
									return ProducerResult{}, err
								})
								done <- err
							}()
						}
						synctest.Wait()
						unlock()
						for range callers {
							err := <-done
							terminal, ok := errors.AsType[*TerminalError](err)
							if !ok || !errors.Is(err, syscall.ECONNRESET) || child == "fresh" && terminal.Attempted {
								t.Fatalf("nested error=%v", err)
							}
						}
						wantInner := 1
						if child == "fresh" {
							wantInner = 0
						}
						if outer != 1 || inner != wantInner {
							t.Fatalf("producer calls=%d/%d", outer, inner)
						}
						after, err := c.store.Resolve(t.Context(), outerID)
						if err != nil {
							t.Fatal(err)
						}
						o, err := c.readOutcome(t.Context(), after)
						wantCount := uint32(2)
						if child == "fresh" {
							wantCount = 1
							if before.Digest != after.Digest {
								t.Fatal("cached child renewed outer outcome")
							}
						}
						if err != nil || o == nil || o.record.Count != wantCount {
							t.Fatalf("outer outcome=%+v, error=%v", o, err)
						}
					})
				})
			}
		}
	})

	for _, attempted := range []bool{false, true} {
		t.Run(fmt.Sprint(attempted), func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, "", New)
			cause := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
			terminal := &TerminalError{Err: cause, Attempted: attempted}
			calls := 0
			_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "outer"}, Policy{Retry: fastRetry(3)}, func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
				calls++
				_, _, _, err := c.Blob(ctx, BlobKey{Content: "inner"}, Policy{Retry: fastRetry(3)}, func(context.Context, ProducerInput) (ProducerResult, error) {
					return ProducerResult{}, terminal
				})
				return ProducerResult{}, err
			})
			got, ok := errors.AsType[*TerminalError](err)
			if !ok || got != terminal || calls != 1 || err.Error() != cause.Error() || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, cause) {
				t.Fatalf("terminal=%+v, error=%v, calls=%d", got, err, calls)
			}
		})
	}
	for _, attempts := range []int{1, 3} {
		t.Run(fmt.Sprintf("child-%d", attempts), func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, "", New)
			outer, inner := 0, 0
			_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "outer"}, Policy{Retry: fastRetry(3)}, func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
				outer++
				_, _, _, err := c.Blob(ctx, BlobKey{Content: "inner"}, Policy{Retry: fastRetry(attempts)}, func(context.Context, ProducerInput) (ProducerResult, error) {
					inner++
					return ProducerResult{}, connReset()
				})
				return ProducerResult{}, err
			})
			if outer != 1 || inner != attempts || !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("calls=%d/%d, error=%v", outer, inner, err)
			}
		})
	}
}

func TestAcquire_RetryFreshArchive(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprint(disk), func(t *testing.T) {
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
			input, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "partial"}, Policy{Retry: fastRetry(3)}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				if calls == 1 {
					_, _ = w.Write(payload[:len(payload)/2])
					return ProducerResult{}, connReset()
				}
				return producer(payload, ocispec.Descriptor{})(ctx, w)
			})
			if err != nil || calls != 2 {
				t.Fatalf("calls=%d, error=%v", calls, err)
			}
			data, err := fs.ReadFile(input, "file")
			if err != nil || string(data) != "complete" {
				t.Fatalf("data=%q, error=%v", data, err)
			}
		})
	}
}

func TestAcquire_PolicyIsolation(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse-%t", concurrency, reverse), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					c := cachetest.New(t, "", New)
					started := make(chan struct{})
					release := make(chan struct{})
					done := make(chan error, concurrency)
					calls := make([]int, concurrency)
					launch := func(i int) {
						go func() {
							attempts := 3
							if i == 0 {
								attempts = 1
							}
							_, _, _, err := c.Blob(t.Context(), BlobKey{Content: "shared"}, Policy{MaxAge: time.Hour, Retry: fastRetry(attempts)}, func(context.Context, ProducerInput) (ProducerResult, error) {
								calls[i]++
								if (i == 0 && !reverse || i == concurrency-1 && reverse) && calls[i] == 1 {
									close(started)
									<-release
								}
								return ProducerResult{}, connReset()
							})
							done <- err
						}()
					}
					first := 0
					if reverse {
						first = concurrency - 1
					}
					launch(first)
					<-started
					for i := range concurrency {
						if i != first {
							launch(i)
						}
					}
					cancelCtx, cancel := context.WithCancel(t.Context())
					cancelled := make(chan error, 1)
					cancelCalls := 0
					go func() {
						_, _, _, err := c.Blob(cancelCtx, BlobKey{Content: "shared"}, Policy{Retry: fastRetry(3)}, func(context.Context, ProducerInput) (ProducerResult, error) {
							cancelCalls++
							return ProducerResult{}, nil
						})
						cancelled <- err
					}()
					synctest.Wait()
					cancel()
					if err := <-cancelled; !errors.Is(err, context.Canceled) || cancelCalls != 0 {
						t.Fatalf("canceled waiter: calls=%d, error=%v", cancelCalls, err)
					}
					close(release)
					for range concurrency {
						if err := <-done; !errors.Is(err, syscall.ECONNRESET) {
							t.Fatal(err)
						}
					}
					for i, got := range calls {
						want := 3
						if i == 0 {
							want = 1
						}
						if got != want {
							t.Fatalf("caller %d: calls=%d, want %d", i, got, want)
						}
					}
				})
			})
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
			r, _, _, err := c.Blob(t.Context(), BlobKey{Content: "shared"}, Policy{MaxAge: Forever, Retry: RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour}}, func(context.Context, ProducerInput) (ProducerResult, error) {
				calls++
				return ProducerResult{}, connReset()
			})
			if r != nil {
				err = errors.Join(err, r.Close())
			}
			done <- err
		}()
		synctest.Wait()
		r, _, _, err := c.Blob(t.Context(), BlobKey{Content: "shared"}, Policy{MaxAge: Forever}, producer([]byte("other fill"), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil || calls != 1 {
			t.Fatalf("calls=%d, error=%v", calls, err)
		}
	})
}

func TestAcquire_RetryCancellation(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := cachetest.New(t, "", New)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, concurrency)
				calls := make([]int, concurrency)
				for i := range concurrency {
					go func() {
						_, _, _, err := c.Blob(ctx, BlobKey{Content: fmt.Sprintf("cancel-%d", i)}, Policy{Retry: RetryConfig{Attempts: 3, MinWait: time.Hour}}, func(context.Context, ProducerInput) (ProducerResult, error) {
							calls[i]++
							return ProducerResult{}, connReset()
						})
						done <- err
					}()
				}
				synctest.Wait()
				cancel()
				for range concurrency {
					err := <-done
					terminal, ok := errors.AsType[*TerminalError](err)
					if !ok || !terminal.Attempted || !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.ECONNRESET) || !errors.Is(err, manifest.ErrFlux) {
						t.Errorf("retry cancellation: %v", err)
					}
				}
				for i := range concurrency {
					_, _, _, err := c.Blob(ctx, BlobKey{Content: fmt.Sprintf("cancel-%d", i)}, Policy{Retry: fastRetry(3)}, func(context.Context, ProducerInput) (ProducerResult, error) {
						calls[i]++
						return ProducerResult{}, nil
					})
					if calls[i] != 1 || !errors.Is(err, context.Canceled) {
						t.Errorf("pre-cancel calls=%d, error=%v", calls[i], err)
					}
				}
			})
		})
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
		if got := cfg.backoff(1); got < 0 || got > cfg.MaxWait {
			t.Fatalf("overflow: %v", got)
		}
	}
}

func TestGit_RetryDiscardsGeneration(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprint(disk), func(t *testing.T) {
			t.Parallel()
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			calls := 0
			g, _, err := c.Git(t.Context(), GitKey{Content: "partial-git", RecurseSubmodules: true}, Policy{MaxAge: Forever, Retry: fastRetry(2)}, func(_ context.Context, _ storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
				calls++
				if _, err := worktree.Stat("partial"); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("attempt inherited partial worktree: %v", err)
				}
				if calls == 1 {
					f, err := worktree.Create("partial")
					if err != nil {
						return plumbing.ZeroHash, err
					}
					if err := f.Close(); err != nil {
						return plumbing.ZeroHash, err
					}
					return plumbing.ZeroHash, connReset()
				}
				if disk {
					entries, err := os.ReadDir(filepath.Dir(filepath.Join(c.root.Name(), gitObjectsPath(""))))
					if err != nil || len(entries) != 1 {
						t.Fatalf("failed generation remains: %v, %v", entries, err)
					}
				}
				return plumbing.NewHash(strings.Repeat("1", 40)), nil
			})
			if err != nil || g == nil || calls != 2 {
				t.Fatalf("generation=%v, calls=%d, error=%v", g, calls, err)
			}
			held, _, err := c.Git(t.Context(), GitKey{Content: "partial-git", RecurseSubmodules: true}, Policy{MaxAge: Forever}, func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
				calls++
				return plumbing.ZeroHash, nil
			})
			if err != nil || held != g || calls != 2 {
				t.Fatalf("warm generation=%v, calls=%d, error=%v", held, calls, err)
			}
		})
	}
}

func TestAcquire_ReaderFailureCancelsAttempt(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				t.Parallel()
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				synctest.Test(t, func(t *testing.T) {
					canceled, release := make(chan struct{}), make(chan struct{})
					done := make(chan error, concurrency)
					calls := make([]int, concurrency)
					for i := range concurrency {
						go func() {
							_, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: fmt.Sprintf("invalid-%d", i)}, Policy{Retry: fastRetry(3)}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
								calls[i]++
								_, _ = w.Write(bytes.Repeat([]byte("x"), 512))
								<-ctx.Done()
								canceled <- struct{}{}
								<-release
								return ProducerResult{}, errors.Join(ctx.Err(), connReset())
							})
							done <- err
						}()
					}
					for range concurrency {
						<-canceled
					}
					synctest.Wait()
					completed := 0
					select {
					case <-done:
						completed++
						t.Error("reader failure returned before producer completion")
					default:
					}
					close(release)
					for range concurrency - completed {
						err := <-done
						if _, ok := errors.AsType[*CacheError](err); !ok || !errors.Is(err, tar.ErrHeader) {
							t.Errorf("reader error=%v", err)
						}
					}
					for i, count := range calls {
						if count != 1 {
							t.Errorf("producer %d: calls=%d", i, count)
						}
					}
				})
			})
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
			r, _, _, err := c.Blob(t.Context(), BlobKey{Content: "reset"}, Policy{Retry: RetryConfig{Attempts: 3}}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				if calls == 1 {
					return ProducerResult{}, connReset()
				}
				return producer(nil, ocispec.Descriptor{})(ctx, w)
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
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
				started, release := make(chan struct{}), make(chan struct{})
				done := make(chan error, concurrency)
				calls := make([]int, concurrency)
				failure := connReset()
				for i := range concurrency {
					c, err := New(t.Context(), t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
							t.Error(err)
						}
					})
					go func() {
						fail := func() error {
							calls[i]++
							started <- struct{}{}
							<-release
							return errors.Join(failure, c.root.Close())
						}
						var err error
						switch kind {
						case "git":
							_, _, err = c.Git(t.Context(), GitKey{Content: "cleanup"}, Policy{Retry: fastRetry(3)}, func(context.Context, storage.Storer, billy.Filesystem) (plumbing.Hash, error) {
								return plumbing.ZeroHash, fail()
							})
						case "archive":
							_, _, _, err = c.Acquire(t.Context(), ArchiveKey{Content: "cleanup"}, Policy{Retry: fastRetry(3)}, func(context.Context, ProducerInput) (ProducerResult, error) {
								return ProducerResult{}, fail()
							})
						case "blob":
							_, _, _, err = c.Blob(t.Context(), BlobKey{Content: "cleanup"}, Policy{Retry: fastRetry(3)}, func(context.Context, ProducerInput) (ProducerResult, error) {
								return ProducerResult{}, fail()
							})
						}
						done <- err
					}()
				}
				for range concurrency {
					<-started
				}
				close(release)
				for range concurrency {
					err := <-done
					cacheErr, ok := errors.AsType[*CacheError](err)
					if !ok || !errors.Is(err, failure) || !errors.Is(err, os.ErrClosed) || !errors.Is(err, manifest.ErrFlux) {
						t.Errorf("cleanup error=%v", err)
						continue
					}
					joined, ok := cacheErr.Err.(interface{ Unwrap() []error })
					if !ok || len(joined.Unwrap()) < 2 || !errors.Is(joined.Unwrap()[0], failure) {
						t.Errorf("operation must precede cleanup: %v", err)
					} else {
						for _, cleanup := range joined.Unwrap()[1:] {
							if errors.Is(cleanup, failure) {
								t.Errorf("operation duplicated in cleanup: %v", err)
							}
						}
					}
				}
				for i, count := range calls {
					if count != 1 {
						t.Errorf("producer %d retried after cleanup failure: %d", i, count)
					}
				}
			})
		}
	}
}
