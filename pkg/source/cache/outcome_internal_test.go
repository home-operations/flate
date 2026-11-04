package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type outcomeCustomWrapperError struct{ error }

func (e outcomeCustomWrapperError) Unwrap() error { return e.error }

type outcomeCustomJoinError struct{ children []error }

func (e outcomeCustomJoinError) Error() string { return "custom join" }

func (e outcomeCustomJoinError) Unwrap() []error { return e.children }

type outcomeCycleError struct{}

func (e outcomeCycleError) Error() string { return "cycle" }

func (e outcomeCycleError) Unwrap() error { return e }

func outcomeFailure(err error) error {
	return &TerminalError{Err: newProducerError(err), Attempted: true}
}

func TestOutcome_TypedRoundTrip(t *testing.T) {
	t.Parallel()
	transport := outcomeCause{
		Kind: "wrap",
		Children: []outcomeCause{
			{
				Kind: "url",
				Op:   "Head",
				Children: []outcomeCause{
					{
						Kind:    "net",
						Op:      "read",
						Network: "tcp",
						Children: []outcomeCause{
							{Kind: "syscall", Op: "read", Children: []outcomeCause{{Kind: "reset"}}},
						},
					},
				},
			},
		},
	}
	transport = outcomeCause{Kind: "wrap", Children: []outcomeCause{transport}}
	machine := outcomeCause{Kind: "machine", Children: []outcomeCause{transport}}
	cause := connReset()
	u, _ := errors.AsType[*url.Error](cause)
	u.URL = "https://fixture-secret:machine-canary@reg.example/private?token=credential-canary"
	cause = fmt.Errorf("cause diagnostic canary: %w", cause)
	checkTransport := func(t *testing.T, err error) {
		t.Helper()
		{
			innerU, ok := errors.AsType[*url.Error](err)
			assert.FatalIf(t, !ok || innerU.Op != "Head" || innerU.URL != "", "URL wrapper: %+v", innerU)
		}
		{
			n, ok := errors.AsType[*net.OpError](
				err,
			)
			assert.FatalIf(t, !ok || n.Op != "read" || n.Net != "tcp" || n.Source != nil ||
				n.Addr != nil, "net wrapper: %+v", n)
		}
		{
			s, ok := errors.AsType[*os.SyscallError](err)
			assert.FatalIf(t, !ok || s.Syscall != "read", "syscall wrapper: %+v", s)
		}
	}
	cases := []struct {
		name        string
		cause       error
		sentinel    error
		check       func(*testing.T, error)
		shape       outcomeCause
		machinePath []int
		diagnostic  string
	}{
		{name: "real nested fmt transport", cause: connReset(), sentinel: syscall.ECONNRESET, check: checkTransport},
		{
			name:     "classified cause only",
			cause:    manifest.NewError(nil, connReset(), "fixture-secret"),
			sentinel: syscall.ECONNRESET,
			check:    checkTransport,
		},
		{
			name:     "classified category only",
			cause:    errors.Join(manifest.NewError(manifest.ErrFlux, nil, "fixture-secret"), connReset()),
			sentinel: syscall.ECONNRESET,
			check: func(t *testing.T, err error) {
				t.Helper()
				checkTransport(t, err)
				terminal, _ := errors.AsType[*TerminalError](err)
				assert.FatalIf(t, !errors.Is(terminal.Err, manifest.ErrFlux), "lost producer classification: %v", err)
			},
		},
		{
			name:     "classified transport",
			cause:    manifest.NewError(manifest.ErrFlux, connReset(), "fixture-secret"),
			sentinel: syscall.ECONNRESET,
			check: func(t *testing.T, err error) {
				t.Helper()
				assert.FatalIf(
					t,
					strings.Contains(err.Error(), "flux error") || strings.Contains(err.Error(), "fixture-secret"),
					"replay prose: %v",
					err,
				)
			},
		},
		{
			name:     "classified joined causes",
			cause:    manifest.NewError(manifest.ErrFlux, errors.Join(connReset(), syscall.ECONNREFUSED), ""),
			sentinel: syscall.ECONNREFUSED,
		},
		{name: "refused", cause: syscall.ECONNREFUSED, sentinel: syscall.ECONNREFUSED},
		{
			name:     "timeout",
			cause:    &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT},
			sentinel: syscall.ETIMEDOUT,
		},
		{
			name:     "IO deadline",
			cause:    &url.Error{Op: "Get", Err: os.ErrDeadlineExceeded},
			sentinel: os.ErrDeadlineExceeded,
		},
		{
			name:     "transport EOF",
			cause:    &url.Error{Op: "Get", Err: fmt.Errorf("read: %w", io.ErrUnexpectedEOF)},
			sentinel: io.ErrUnexpectedEOF,
		},
		{name: "net EOF", cause: &net.OpError{Op: "read", Err: io.ErrUnexpectedEOF}, sentinel: io.ErrUnexpectedEOF},
		{
			name: "DNS",
			cause: &net.DNSError{
				Err:         "fixture-secret",
				Name:        "fixture-secret",
				Server:      "fixture-secret",
				IsTimeout:   true,
				IsTemporary: true,
				IsNotFound:  true,
				UnwrapErr:   os.ErrDeadlineExceeded,
			},
			sentinel: os.ErrDeadlineExceeded,
			check: func(t *testing.T, err error) {
				t.Helper()
				{
					d, ok := errors.AsType[*net.DNSError](
						err,
					)
					assert.FatalIf(t, !ok || !d.Timeout() || !d.IsTemporary || !d.IsNotFound || d.Name != "" ||
						d.Server != "", "DNS wrapper: %+v", d)
				}
			},
		},
		{name: "DNS without inner cause", cause: &net.DNSError{IsTimeout: true}},
		{
			name: "ORAS response",
			cause: &fetcherr.ResponseError{
				StatusCode: 503,
				Op:         http.MethodGet,
				Message:    "fixture-secret",
				Err: &errcode.ErrorResponse{
					Method:     http.MethodGet,
					URL:        &url.URL{Host: "fixture-secret"},
					StatusCode: 503,
					Errors: errcode.Errors{
						{Code: errcode.ErrorCodeBlobUnknown, Message: "fixture-secret", Detail: "fixture-secret"},
						{Code: errcode.ErrorCodeManifestUnknown},
					},
				},
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				r, ok := errors.AsType[*errcode.ErrorResponse](err)
				assert.FatalIf(
					t,
					!ok || r.StatusCode != http.StatusServiceUnavailable || r.URL != nil || r.Method != http.MethodGet ||
						len(r.Errors) != 2 ||
						r.Errors[0].Code != errcode.ErrorCodeBlobUnknown ||
						r.Errors[0].Message != "" ||
						r.Errors[0].Detail != nil,
					"response wrapper: %+v",
					r,
				)
			},
		},
		{
			name:  "ORAS rate limit",
			cause: &fetcherr.ResponseError{StatusCode: 429, Err: &errcode.ErrorResponse{StatusCode: 429}},
		},
		{
			name:  "neutral response",
			cause: &fetcherr.ResponseError{StatusCode: 599, Op: http.MethodHead, Message: "fixture-secret"},
		},
		{
			name:  "neutral redacted operation",
			cause: &fetcherr.ResponseError{StatusCode: 500, Op: "fixture-secret", Message: "fixture-secret"},
		},
		{
			name: "ORAS code value",
			cause: &fetcherr.ResponseError{
				StatusCode: 503,
				Err:        errcode.Error{Code: errcode.ErrorCodeManifestUnknown, Message: "fixture-secret"},
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				{
					_, ok := errors.AsType[errcode.Error](err)
					assert.FatalIf(t, !ok, "%s", "value code identity lost")
				}
				{
					_, ok := errors.AsType[*errcode.Error](err)
					assert.FatalIf(t, ok, "%s", "value code became pointer")
				}
			},
		},
		{
			name: "ORAS code pointer",
			cause: &fetcherr.ResponseError{
				StatusCode: 503,
				Err: &errcode.Error{
					Code:    errcode.ErrorCodeBlobUnknown,
					Message: "fixture-secret",
					Detail:  "fixture-secret",
				},
			},
			check: func(t *testing.T, err error) {
				t.Helper()
				{
					code, ok := errors.AsType[*errcode.Error](
						err,
					)
					assert.FatalIf(t, !ok || code.Code != errcode.ErrorCodeBlobUnknown || code.Message != "" ||
						code.Detail != nil, "pointer code identity: %+v", code)
				}
				{
					_, ok := errors.AsType[errcode.Error](err)
					assert.FatalIf(t, ok, "%s", "pointer code became value")
				}
			},
		},
		{name: "join", cause: errors.Join(connReset(), syscall.ECONNREFUSED), sentinel: syscall.ECONNREFUSED},
		{
			name:     "fmt multiple",
			cause:    fmt.Errorf("fixture-secret: %w %w", connReset(), syscall.ETIMEDOUT),
			sentinel: syscall.ETIMEDOUT,
		},
		{
			name: "machine", cause: MachineCredentialErr(cause), sentinel: syscall.ECONNRESET,
			shape: machine, diagnostic: "machine credential acquisition failed",
		},
		{
			name:        "format outside machine",
			cause:       fmt.Errorf("fixture-secret: %w", MachineCredentialErr(cause)),
			sentinel:    syscall.ECONNRESET,
			shape:       outcomeCause{Kind: "wrap", Children: []outcomeCause{machine}},
			machinePath: []int{0},
			diagnostic:  "remote acquisition: machine credential acquisition failed",
		},
		{
			name:     "machine inside join",
			cause:    errors.Join(syscall.ECONNREFUSED, MachineCredentialErr(cause)),
			sentinel: syscall.ECONNRESET,
			shape: outcomeCause{
				Kind:     "join",
				Children: []outcomeCause{{Kind: "refused"}, machine},
			},
			machinePath: []int{1},
			diagnostic:  syscall.ECONNREFUSED.Error() + "\nmachine credential acquisition failed",
		},
		{
			name:     "machine around nested terminal",
			cause:    MachineCredentialErr(outcomeFailure(cause)),
			sentinel: syscall.ECONNRESET,
			shape: outcomeCause{
				Kind: "machine",
				Children: []outcomeCause{
					{
						Kind:     "terminal",
						Children: []outcomeCause{{Kind: "producer", Children: []outcomeCause{transport}}},
					},
				},
			},
			diagnostic: "machine credential acquisition failed",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOutcomeTypedRoundTripCase(t, &u, &tc)
		},
		)
	}
}

func TestOutcome_Exclusions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
	}{
		{"cancellation", context.Canceled},
		{"caller deadline", context.DeadlineExceeded},
		{"input", manifest.ErrInput},
		{"missing object", manifest.ErrObjectNotFound},
		{"missing credential", manifest.ErrMissingSecret},
		{"skipped source", manifest.ErrSourceSkipped},
		{"path IO", &fs.PathError{Op: "write", Path: "fixture-secret", Err: syscall.ECONNRESET}},
		{"cache IO", &CacheError{syscall.ECONNRESET}},
		{"short write", io.ErrShortWrite},
		{"integrity", content.ErrMismatchedDigest},
		{"permanent HTTP", &fetcherr.ResponseError{StatusCode: 404}},
		{"terminal HTTP", &fetcherr.ResponseError{StatusCode: 600}},
		{
			"unknown code",
			&fetcherr.ResponseError{
				StatusCode: 503,
				Err:        &errcode.ErrorResponse{StatusCode: 503, Errors: errcode.Errors{{Code: "fixture-secret"}}},
			},
		},
		{
			"invalid digest code",
			&fetcherr.ResponseError{
				StatusCode: 503,
				Err: &errcode.ErrorResponse{
					StatusCode: 503,
					Errors:     errcode.Errors{{Code: errcode.ErrorCodeDigestInvalid}},
				},
			},
		},
		{"raw ORAS response", &errcode.ErrorResponse{StatusCode: 503}},
		{"raw ORAS code", errcode.Error{Code: errcode.ErrorCodeBlobUnknown}},
		{"neutral unsupported child", &fetcherr.ResponseError{StatusCode: 503, Err: errors.New("fixture-secret")}},
		{
			"neutral wrapped unsupported child",
			&fetcherr.ResponseError{StatusCode: 503, Err: outcomeCustomWrapperError{connReset()}},
		},
		{"neutral cancellation", &fetcherr.ResponseError{StatusCode: 503, Err: context.Canceled}},
		{"neutral caller deadline", &fetcherr.ResponseError{StatusCode: 503, Err: context.DeadlineExceeded}},
		{"neutral input", &fetcherr.ResponseError{StatusCode: 503, Err: manifest.ErrInput}},
		{"neutral EOF without transport", &fetcherr.ResponseError{StatusCode: 503, Err: io.ErrUnexpectedEOF}},
		{"neutral cycle", &fetcherr.ResponseError{StatusCode: 503, Err: outcomeCycleError{}}},
		{"non-timeout DNS", &net.DNSError{IsNotFound: true}},
		{"DNS cancellation", &net.DNSError{IsTimeout: true, UnwrapErr: context.DeadlineExceeded}},
		{"custom timeout", timeoutError{}},
		{"custom wrapper", outcomeCustomWrapperError{connReset()}},
		{"machine unsupported child", MachineCredentialErr(outcomeCustomWrapperError{connReset()})},
		{
			"machine rejected credentials",
			MachineCredentialErr(&fetcherr.ResponseError{StatusCode: http.StatusForbidden}),
		},
		{"machine canceled child", MachineCredentialErr(context.Canceled)},
		{"machine cached child", MachineCredentialErr(&TerminalError{Err: connReset(), Attempted: false})},
		{"custom join", outcomeCustomJoinError{[]error{connReset(), syscall.ECONNREFUSED}}},
		{"custom neutral wrapper", outcomeCustomWrapperError{&fetcherr.ResponseError{StatusCode: 503}}},
		{"cached child", &TerminalError{Err: connReset(), Attempted: false}},
		{"ambiguous EOF", io.ErrUnexpectedEOF},
		{"formatting EOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF)},
		{"sibling transport EOF", errors.Join(connReset(), io.ErrUnexpectedEOF)},
		{"custom transport EOF", outcomeCustomWrapperError{&url.Error{Err: io.ErrUnexpectedEOF}}},
		{"cycle", outcomeCycleError{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOutcomeExclusionsCase(t, &tc)
		},
		)
	}
	assert.FatalIf(
		t,
		newOutcome(connReset(), time.Now().UTC(), 1, &ocispec.Descriptor{}) != nil,
		"%s",
		"non-terminal recorded",
	)

	for _, n := range []int{outcomeDepthLimit - 3, outcomeDepthLimit - 2} {
		err := error(syscall.ECONNRESET)
		cause := outcomeCause{Kind: "reset"}
		for range n {
			err = fmt.Errorf("context: %w", err)
			cause = outcomeCause{Kind: "wrap", Children: []outcomeCause{cause}}
		}
		got := newOutcome(outcomeFailure(err), time.Now().UTC(), 1, &ocispec.Descriptor{})
		if (got != nil) != (n == outcomeDepthLimit-3) {
			t.Fatalf("depth limit: wraps=%d accepted=%t", n, got != nil)
		}
		codec := outcomeCodecState{}
		_, ok := codec.decode(&outcomeCause{
			Kind:     "terminal",
			Children: []outcomeCause{{Kind: "producer", Children: []outcomeCause{cause}}},
		}, 0,
			1,
		)
		assert.FatalIf(t, ok != (n == outcomeDepthLimit-3), "decode depth limit: wraps=%d accepted=%t", n, ok)
	}
	for _, n := range []int{outcomeNodeLimit - 3, outcomeNodeLimit - 2} {
		children := make([]error, n)
		causes := make([]outcomeCause, n)
		for i := range children {
			children[i] = syscall.ECONNRESET
			causes[i] = outcomeCause{Kind: "reset"}
		}
		got := newOutcome(outcomeFailure(errors.Join(children...)), time.Now().UTC(), 1, &ocispec.Descriptor{})
		if (got != nil) != (n == outcomeNodeLimit-3) {
			t.Fatalf("node limit: children=%d accepted=%t", n, got != nil)
		}
		codec := outcomeCodecState{}
		_, ok := codec.decode(&outcomeCause{
			Kind: "terminal",
			Children: []outcomeCause{
				{Kind: "producer", Children: []outcomeCause{{Kind: "join", Children: causes}}},
			},
		}, 0,
			1,
		)
		assert.FatalIf(t, ok != (n == outcomeNodeLimit-3), "decode node limit: children=%d accepted=%t", n, ok)
	}
	codes := make(errcode.Errors, outcomeNodeLimit)
	for i := range codes {
		codes[i].Code = errcode.ErrorCodeBlobUnknown
	}
	assert.FatalIf(t, newOutcome(
		outcomeFailure(
			&fetcherr.ResponseError{StatusCode: 503, Err: &errcode.ErrorResponse{StatusCode: 503, Errors: codes}},
		),
		time.Now().UTC(),
		1, &ocispec.Descriptor{},
	) != nil, "%s", "oversize ORAS cause tree accepted")
}

func TestOutcome_CanceledCallerIsNotRecorded(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	ctx, cancel := context.WithCancel(t.Context())
	_, _, _, err := c.Blob(
		ctx,
		BlobKey{Content: "cancel-record"},
		&Policy{},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			cancel()
			return ProducerResult{}, connReset()
		},
	)
	assert.FatalIf(t, err == nil, "%s", "want acquisition error")

	{
		_, err := c.store.Resolve(
			t.Context(),
			manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"cancel-record"}}),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			errdef.ErrNotFound,
		), "outcome recorded for canceled caller: %v", err)
	}
}

func TestOutcome_MachineCooldown(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		c := cachetest.New(t, root, New)
		key := BlobKey{Content: "machine-cooldown"}
		policy := Policy{Retry: fastRetry(2), Machine: true}
		calls := 0
		produce := func(context.Context, ProducerInput) (ProducerResult, error) {
			calls++
			return ProducerResult{}, connReset()
		}
		_, _, _, err := c.Blob(t.Context(), key, &policy, produce)
		{
			terminal, ok := errors.AsType[*TerminalError](err)
			assert.FatalIf(
				t,
				!ok || !terminal.Attempted || calls != 2,
				"initial attempt: calls=%d, error=%v",
				calls,
				err,
			)
		}
		{
			closeErr2 := c.Close()
			assert.NoError(t, closeErr2)
		}

		c = cachetest.New(t, root, New)
		_, _, _, err = c.Blob(t.Context(), key, &policy, produce)
		{
			terminal, ok := errors.AsType[*TerminalError](err)
			assert.FatalIf(
				t,
				!ok || terminal.Attempted || calls != 2,
				"cooldown replay: calls=%d, error=%v",
				calls,
				err,
			)
		}
		assert.FatalIf(
			t,
			!errors.Is(err, syscall.ECONNRESET) || !errors.Is(err, manifest.ErrFlux),
			"cooldown cause: %v",
			err,
		)
		{
			_, ok := errors.AsType[*url.Error](err)
			assert.FatalIf(t, !ok, "%s", "cooldown lost URL cause")
		}
		{
			_, ok := errors.AsType[*net.OpError](err)
			assert.FatalIf(t, !ok, "%s", "cooldown lost network cause")
		}
		{
			_, ok := errors.AsType[*os.SyscallError](err)
			assert.FatalIf(t, !ok, "%s", "cooldown lost syscall cause")
		}

		var count func(error) int
		count = func(err error) int {
			n := 0
			if _, ok := err.(*machineError); ok { //nolint:errorlint // Count concrete nodes, including duplicate wrappers, rather than wrapped matches.
				n++
			}
			switch e := err.(type) { //nolint:errorlint // Count each concrete node exactly once.
			case interface{ Unwrap() []error }:
				for _, child := range e.Unwrap() {
					n += count(child)
				}
			case interface{ Unwrap() error }:
				n += count(e.Unwrap())
			}
			return n
		}
		assert.FatalIf(t, count(MachineCredentialErr(err)) != 1, "%s", "replayed outcome gained a machine wrapper")

		{
			machine, ok := errors.AsType[*machineError](
				err,
			)
			assert.FatalIf(t, !ok || machine.Error() != "machine credential acquisition failed" ||
				err.Error() != machine.Error(), "cooldown masking: %v", err)
		}
	})
}
