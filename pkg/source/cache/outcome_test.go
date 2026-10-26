package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"
)

type outcomeCustomWrapper struct{ error }

func (e outcomeCustomWrapper) Unwrap() error { return e.error }

type outcomeCustomJoin struct{ children []error }

func (e outcomeCustomJoin) Error() string   { return "custom join" }
func (e outcomeCustomJoin) Unwrap() []error { return e.children }

type outcomeCycle struct{}

func (e outcomeCycle) Error() string { return "cycle" }
func (e outcomeCycle) Unwrap() error { return e }

func outcomeFailure(err error) error {
	return &TerminalError{Err: &producerError{err}, Attempted: true}
}

func TestOutcome_TypedRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		cause    error
		sentinel error
		check    func(*testing.T, error)
	}{
		{"real nested fmt transport", connReset(), syscall.ECONNRESET, func(t *testing.T, err error) {
			t.Helper()
			if u, ok := errors.AsType[*url.Error](err); !ok || u.Op != "Head" || u.URL != "" {
				t.Fatalf("URL wrapper: %+v", u)
			}
			if n, ok := errors.AsType[*net.OpError](err); !ok || n.Op != "read" || n.Net != "tcp" || n.Source != nil || n.Addr != nil {
				t.Fatalf("net wrapper: %+v", n)
			}
			if s, ok := errors.AsType[*os.SyscallError](err); !ok || s.Syscall != "read" {
				t.Fatalf("syscall wrapper: %+v", s)
			}
		}},
		{"refused", syscall.ECONNREFUSED, syscall.ECONNREFUSED, nil},
		{"timeout", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ETIMEDOUT}, syscall.ETIMEDOUT, nil},
		{"IO deadline", &url.Error{Op: "Get", Err: os.ErrDeadlineExceeded}, os.ErrDeadlineExceeded, nil},
		{"transport EOF", &url.Error{Op: "Get", Err: fmt.Errorf("read: %w", io.ErrUnexpectedEOF)}, io.ErrUnexpectedEOF, nil},
		{"net EOF", &net.OpError{Op: "read", Err: io.ErrUnexpectedEOF}, io.ErrUnexpectedEOF, nil},
		{"DNS", &net.DNSError{Err: "fixture-secret", Name: "fixture-secret", Server: "fixture-secret", IsTimeout: true, IsTemporary: true, IsNotFound: true, UnwrapErr: os.ErrDeadlineExceeded}, os.ErrDeadlineExceeded, func(t *testing.T, err error) {
			t.Helper()
			if d, ok := errors.AsType[*net.DNSError](err); !ok || !d.Timeout() || !d.IsTemporary || !d.IsNotFound || d.Name != "" || d.Server != "" {
				t.Fatalf("DNS wrapper: %+v", d)
			}
		}},
		{"DNS without inner cause", &net.DNSError{IsTimeout: true}, nil, nil},
		{"ORAS response", &fetcherr.ResponseError{StatusCode: 503, Op: http.MethodGet, Message: "fixture-secret", Err: &errcode.ErrorResponse{Method: http.MethodGet, URL: &url.URL{Host: "fixture-secret"}, StatusCode: 503, Errors: errcode.Errors{{Code: errcode.ErrorCodeBlobUnknown, Message: "fixture-secret", Detail: "fixture-secret"}, {Code: errcode.ErrorCodeManifestUnknown}}}}, nil, func(t *testing.T, err error) {
			t.Helper()
			r, ok := errors.AsType[*errcode.ErrorResponse](err)
			if !ok || r.StatusCode != http.StatusServiceUnavailable || r.URL != nil || r.Method != http.MethodGet || len(r.Errors) != 2 || r.Errors[0].Code != errcode.ErrorCodeBlobUnknown || r.Errors[0].Message != "" || r.Errors[0].Detail != nil {
				t.Fatalf("response wrapper: %+v", r)
			}
		}},
		{"ORAS rate limit", &fetcherr.ResponseError{StatusCode: 429, Err: &errcode.ErrorResponse{StatusCode: 429}}, nil, nil},
		{"neutral response", &fetcherr.ResponseError{StatusCode: 599, Op: http.MethodHead, Message: "fixture-secret"}, nil, nil},
		{"neutral redacted operation", &fetcherr.ResponseError{StatusCode: 500, Op: "fixture-secret", Message: "fixture-secret"}, nil, nil},
		{"ORAS code value", &fetcherr.ResponseError{StatusCode: 503, Err: errcode.Error{Code: errcode.ErrorCodeManifestUnknown, Message: "fixture-secret"}}, nil, func(t *testing.T, err error) {
			t.Helper()
			if _, ok := errors.AsType[errcode.Error](err); !ok {
				t.Fatal("value code identity lost")
			}
			if _, ok := errors.AsType[*errcode.Error](err); ok {
				t.Fatal("value code became pointer")
			}
		}},
		{"ORAS code pointer", &fetcherr.ResponseError{StatusCode: 503, Err: &errcode.Error{Code: errcode.ErrorCodeBlobUnknown, Message: "fixture-secret", Detail: "fixture-secret"}}, nil, func(t *testing.T, err error) {
			t.Helper()
			if code, ok := errors.AsType[*errcode.Error](err); !ok || code.Code != errcode.ErrorCodeBlobUnknown || code.Message != "" || code.Detail != nil {
				t.Fatalf("pointer code identity: %+v", code)
			}
			if _, ok := errors.AsType[errcode.Error](err); ok {
				t.Fatal("pointer code became value")
			}
		}},
		{"join", errors.Join(connReset(), syscall.ECONNREFUSED), syscall.ECONNREFUSED, nil},
		{"fmt multiple", fmt.Errorf("fixture-secret: %w %w", connReset(), syscall.ETIMEDOUT), syscall.ETIMEDOUT, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			live := outcomeFailure(tc.cause)
			o := newOutcome(live, time.Now().UTC(), 1, ocispec.Descriptor{})
			if o == nil {
				t.Fatal("supported live failure rejected")
			}
			desc, err := c.publishOutcome(t.Context(), "failure", o, live)
			if err != nil || desc.Digest == "" {
				t.Fatalf("publish: %+v, %v", desc, err)
			}
			got, err := c.readOutcome(t.Context(), desc)
			if err != nil || got == nil {
				t.Fatalf("read: %+v, %v", got, err)
			}
			replay := got.replay()
			if status, ok := errors.AsType[*fetcherr.ResponseError](tc.cause); ok {
				decoded, ok := errors.AsType[*fetcherr.ResponseError](replay)
				if !ok || decoded.StatusCode != status.StatusCode || decoded.Op != outcomeOp(status.Op) || decoded.Message != "" {
					t.Fatalf("neutral response: %+v", decoded)
				}
				root := got.record.Cause
				if root.Kind != "terminal" || len(root.Children) != 1 || root.Children[0].Kind != "producer" || len(root.Children[0].Children) != 1 || root.Children[0].Children[0].Kind != "status" {
					t.Fatalf("neutral response shape: %+v", root)
				}
			}
			if terminal, ok := errors.AsType[*TerminalError](replay); !ok || terminal.Attempted || !errors.Is(replay, manifest.ErrFlux) {
				t.Fatalf("replay terminal identity: %v", replay)
			}
			if tc.sentinel != nil && !errors.Is(replay, tc.sentinel) {
				t.Fatalf("lost sentinel: %v", replay)
			}
			if tc.check != nil {
				tc.check(t, replay)
			}
			config, data, ok := o.encode()
			if !ok || bytes.Contains(config, []byte("fixture-secret")) || bytes.Contains(data, []byte("fixture-secret")) || strings.Contains(replay.Error(), "fixture-secret") {
				t.Fatal("persisted diagnostics contain fixture secret")
			}
			terminal, _ := errors.AsType[*TerminalError](live)
			producer, ok := errors.AsType[*producerError](terminal.Err)
			if !terminal.Attempted || !ok || !errors.Is(producer.error, tc.cause) {
				t.Fatal("live cause changed")
			}
		})
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
		{"unknown code", &fetcherr.ResponseError{StatusCode: 503, Err: &errcode.ErrorResponse{StatusCode: 503, Errors: errcode.Errors{{Code: "fixture-secret"}}}}},
		{"invalid digest code", &fetcherr.ResponseError{StatusCode: 503, Err: &errcode.ErrorResponse{StatusCode: 503, Errors: errcode.Errors{{Code: errcode.ErrorCodeDigestInvalid}}}}},
		{"raw ORAS response", &errcode.ErrorResponse{StatusCode: 503}},
		{"raw ORAS code", errcode.Error{Code: errcode.ErrorCodeBlobUnknown}},
		{"neutral unsupported child", &fetcherr.ResponseError{StatusCode: 503, Err: errors.New("fixture-secret")}},
		{"neutral wrapped unsupported child", &fetcherr.ResponseError{StatusCode: 503, Err: outcomeCustomWrapper{connReset()}}},
		{"neutral cancellation", &fetcherr.ResponseError{StatusCode: 503, Err: context.Canceled}},
		{"neutral caller deadline", &fetcherr.ResponseError{StatusCode: 503, Err: context.DeadlineExceeded}},
		{"neutral input", &fetcherr.ResponseError{StatusCode: 503, Err: manifest.ErrInput}},
		{"neutral EOF without transport", &fetcherr.ResponseError{StatusCode: 503, Err: io.ErrUnexpectedEOF}},
		{"neutral cycle", &fetcherr.ResponseError{StatusCode: 503, Err: outcomeCycle{}}},
		{"non-timeout DNS", &net.DNSError{IsNotFound: true}},
		{"DNS cancellation", &net.DNSError{IsTimeout: true, UnwrapErr: context.DeadlineExceeded}},
		{"custom timeout", timeoutErr{}},
		{"custom wrapper", outcomeCustomWrapper{connReset()}},
		{"custom join", outcomeCustomJoin{[]error{connReset(), syscall.ECONNREFUSED}}},
		{"custom neutral wrapper", outcomeCustomWrapper{&fetcherr.ResponseError{StatusCode: 503}}},
		{"cached child", &TerminalError{Err: connReset(), Attempted: false}},
		{"ambiguous EOF", io.ErrUnexpectedEOF},
		{"formatting EOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF)},
		{"sibling transport EOF", errors.Join(connReset(), io.ErrUnexpectedEOF)},
		{"custom transport EOF", outcomeCustomWrapper{&url.Error{Err: io.ErrUnexpectedEOF}}},
		{"cycle", outcomeCycle{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, err := range []error{tc.err, errors.Join(connReset(), fmt.Errorf("nested: %w", tc.err)), fmt.Errorf("outer: %w", errors.Join(tc.err, connReset()))} {
				if got := newOutcome(outcomeFailure(err), time.Now().UTC(), 1, ocispec.Descriptor{}); got != nil {
					t.Fatalf("excluded tree accepted: %T", tc.err)
				}
			}
		})
	}
	if newOutcome(connReset(), time.Now().UTC(), 1, ocispec.Descriptor{}) != nil {
		t.Fatal("non-terminal recorded")
	}
	for _, n := range []int{outcomeDepthLimit - 3, outcomeDepthLimit - 2} {
		err := error(syscall.ECONNRESET)
		cause := outcomeCause{Kind: "reset"}
		for range n {
			err = fmt.Errorf("context: %w", err)
			cause = outcomeCause{Kind: "wrap", Children: []outcomeCause{cause}}
		}
		got := newOutcome(outcomeFailure(err), time.Now().UTC(), 1, ocispec.Descriptor{})
		if (got != nil) != (n == outcomeDepthLimit-3) {
			t.Fatalf("depth limit: wraps=%d accepted=%t", n, got != nil)
		}
		codec := outcomeCodecState{}
		_, ok := codec.decode(outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "producer", Children: []outcomeCause{cause}}}}, 0, 1)
		if ok != (n == outcomeDepthLimit-3) {
			t.Fatalf("decode depth limit: wraps=%d accepted=%t", n, ok)
		}
	}
	for _, n := range []int{outcomeNodeLimit - 3, outcomeNodeLimit - 2} {
		children := make([]error, n)
		causes := make([]outcomeCause, n)
		for i := range children {
			children[i] = syscall.ECONNRESET
			causes[i] = outcomeCause{Kind: "reset"}
		}
		got := newOutcome(outcomeFailure(errors.Join(children...)), time.Now().UTC(), 1, ocispec.Descriptor{})
		if (got != nil) != (n == outcomeNodeLimit-3) {
			t.Fatalf("node limit: children=%d accepted=%t", n, got != nil)
		}
		codec := outcomeCodecState{}
		_, ok := codec.decode(outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "producer", Children: []outcomeCause{{Kind: "join", Children: causes}}}}}, 0, 1)
		if ok != (n == outcomeNodeLimit-3) {
			t.Fatalf("decode node limit: children=%d accepted=%t", n, ok)
		}
	}
	codes := make(errcode.Errors, outcomeNodeLimit)
	for i := range codes {
		codes[i].Code = errcode.ErrorCodeBlobUnknown
	}
	if newOutcome(outcomeFailure(&fetcherr.ResponseError{StatusCode: 503, Err: &errcode.ErrorResponse{StatusCode: 503, Errors: codes}}), time.Now().UTC(), 1, ocispec.Descriptor{}) != nil {
		t.Fatal("oversize ORAS cause tree accepted")
	}

}

func TestOutcome_CanceledCallerIsNotRecorded(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	ctx, cancel := context.WithCancel(t.Context())
	_, _, _, err := c.Blob(ctx, BlobKey{Content: "cancel-record"}, Policy{}, func(context.Context, ProducerInput) (ProducerResult, error) {
		cancel()
		return ProducerResult{}, connReset()
	})
	if err == nil {
		t.Fatal("want acquisition error")
	}
	if _, err := c.store.Resolve(t.Context(), manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"cancel-record"}})); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("outcome recorded for canceled caller: %v", err)
	}
}

func TestOutcome_Time(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		count uint32
		delay time.Duration
	}{{1, 750 * time.Millisecond}, {2, 1500 * time.Millisecond}, {3, 3 * time.Second}, {11, 768 * time.Second}, {12, 15 * time.Minute}, {math.MaxInt32 + 1, 15 * time.Minute}, {math.MaxUint32, 15 * time.Minute}} {
		deadline, valid := outcomeDeadline(at, tc.count)
		if !valid || deadline.Sub(at) != tc.delay {
			t.Fatalf("count %d: %s, %t", tc.count, deadline.Sub(at), valid)
		}
	}
	if nextOutcomeCount(0) != 1 || nextOutcomeCount(1) != 2 || nextOutcomeCount(math.MaxUint32) != math.MaxUint32 {
		t.Fatal("failure count did not saturate")
	}
	for _, tc := range []struct {
		name  string
		at    time.Time
		count uint32
	}{
		{"zero", time.Time{}, 1}, {"zero count", at, 0},
		{"year zero", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"year overflow", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"deadline overflow", time.Date(9999, 12, 31, 23, 59, 59, 500000000, time.UTC), 1},
		{"non-UTC", at.In(time.FixedZone("offset", 3600)), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, valid := outcomeDeadline(tc.at, tc.count); valid {
				t.Fatal("invalid time accepted")
			}
		})
	}
	for _, tc := range []struct {
		name         string
		observations []time.Duration
		fresh        []bool
		valid        []bool
	}{
		{"boundary", []time.Duration{0, 749 * time.Millisecond, 750 * time.Millisecond, time.Second}, []bool{true, true, false, false}, []bool{true, true, true, true}},
		{"future", []time.Duration{-1, 0, time.Second}, []bool{false, false, false}, []bool{false, false, false}},
		{"rollback after completion", []time.Duration{500 * time.Millisecond, 499 * time.Millisecond, 600 * time.Millisecond}, []bool{true, false, false}, []bool{true, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := newOutcome(outcomeFailure(connReset()), at, 1, ocispec.Descriptor{})
			before := o.record
			for i, delta := range tc.observations {
				fresh, valid := o.observe(at.Add(delta))
				if fresh != tc.fresh[i] || valid != tc.valid[i] {
					t.Fatalf("observe %s: %t/%t", delta, fresh, valid)
				}
			}
			if diff := cmp.Diff(before, o.record); diff != "" {
				t.Fatalf("observation renewed record (-want +got):\n%s", diff)
			}
		})
	}
}

func pushOutcomeFixture(t *testing.T, c *Cache, config, data []byte) ocispec.Descriptor {
	t.Helper()
	for _, blob := range []struct {
		media string
		data  []byte
	}{{outcomeConfig, config}, {ocispec.MediaTypeImageManifest, data}} {
		desc := content.NewDescriptorFromBytes(blob.media, blob.data)
		if err := c.store.Push(t.Context(), desc, bytes.NewReader(blob.data)); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
			t.Fatal(err)
		}
	}
	return content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, data)
}

func TestOutcome_ReadBoundsAndIntegrity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"exact bounds", "config one over", "manifest one over", "negative size", "missing config", "missing manifest", "config digest", "manifest digest", "config short", "config trailing", "missing layer"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, ocispec.Descriptor{})
			config, data, _ := o.encode()
			var m ocispec.Manifest
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			config = append(config, bytes.Repeat([]byte(" "), outcomeConfigLimit-len(config))...)
			if scenario == "config one over" {
				config = append(config, ' ')
			}
			m.Config = content.NewDescriptorFromBytes(outcomeConfig, config)
			if scenario == "missing layer" {
				m.Layers = []ocispec.Descriptor{content.NewDescriptorFromBytes("application/octet-stream", []byte("absent"))}
			}
			data, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, bytes.Repeat([]byte(" "), outcomeManifestLimit-len(data))...)
			if scenario == "manifest one over" {
				data = append(data, ' ')
			}
			desc := pushOutcomeFixture(t, c, config, data)
			path := blobPath(m.Config.Digest)
			if err := c.root.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := c.root.Chmod(blobPath(desc.Digest), 0o600); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "negative size":
				desc.Size = -1
			case "missing manifest":
				path = blobPath(desc.Digest)
				fallthrough
			case "missing config":
				if err := c.root.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "config digest":
				if err := c.root.WriteFile(path, bytes.Repeat([]byte("x"), len(config)), 0o600); err != nil {
					t.Fatal(err)
				}
			case "manifest digest":
				if err := c.root.WriteFile(blobPath(desc.Digest), bytes.Repeat([]byte("x"), len(data)), 0o600); err != nil {
					t.Fatal(err)
				}
			case "config short":
				if err := c.root.WriteFile(path, config[:len(config)-1], 0o600); err != nil {
					t.Fatal(err)
				}
			case "config trailing":
				if err := c.root.WriteFile(path, append(config, 'x'), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := c.readOutcome(t.Context(), desc)
			switch scenario {
			case "exact bounds":
				if err != nil || got == nil {
					t.Fatalf("exact caps rejected: %+v, %v", got, err)
				}
			case "config one over", "manifest one over", "negative size":
				if err != nil || got != nil {
					t.Fatalf("out-of-bounds read: %+v, %v", got, err)
				}
			default:
				if _, ok := errors.AsType[*CacheError](err); !ok || got != nil || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("integrity failure lost: %+v, %v", got, err)
				}
			}
		})
	}
}

func TestOutcome_InvalidRecords(t *testing.T) {
	t.Parallel()
	code := outcomeCause{Kind: "code", Code: errcode.ErrorCodeBlobUnknown}
	codes := outcomeCause{Kind: "codes", Children: []outcomeCause{code}}
	invalidCauses := map[string]outcomeCause{
		"status client":            {Kind: "status", Status: 404},
		"status terminal":          {Kind: "status", Status: 600},
		"status children":          {Kind: "status", Status: 503, Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"status unknown child":     {Kind: "status", Status: 503, Children: []outcomeCause{{Kind: "unknown"}}},
		"status unknown operation": {Kind: "status", Status: 503, Op: "fixture-secret"},
		"raw response":             {Kind: "response", Status: 503},
		"raw codes":                codes,
		"leaf children":            {Kind: "reset", Children: []outcomeCause{{Kind: "flux"}}},
		"leaf network":             {Kind: "reset", Network: "tcp"},
		"leaf status":              {Kind: "reset", Status: 503},
		"leaf code":                {Kind: "reset", Code: errcode.ErrorCodeBlobUnknown},
		"leaf temporary":           {Kind: "reset", Temporary: true},
		"leaf not found":           {Kind: "reset", NotFound: true},
		"empty join":               {Kind: "join"},
		"wrap children":            {Kind: "wrap", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"DNS children":             {Kind: "dns", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"net unknown network":      {Kind: "net", Network: "fixture-secret", Children: []outcomeCause{{Kind: "reset"}}},
		"URL unknown operation":    {Kind: "url", Op: "fixture-secret", Children: []outcomeCause{{Kind: "reset"}}},
		"sibling transport EOF":    {Kind: "join", Children: []outcomeCause{{Kind: "url", Children: []outcomeCause{{Kind: "reset"}}}, {Kind: "eof"}}},
	}
	for name, cause := range map[string]outcomeCause{
		"response not codes":     {Kind: "response", Status: 503, Children: []outcomeCause{code}},
		"response wrapped codes": {Kind: "response", Status: 503, Children: []outcomeCause{{Kind: "wrap", Children: []outcomeCause{codes}}}},
		"response children":      {Kind: "response", Status: 503, Children: []outcomeCause{codes, codes}},
		"codes pointer":          {Kind: "codes", Children: []outcomeCause{{Kind: "code-pointer", Code: errcode.ErrorCodeBlobUnknown}}},
		"codes wrapped value":    {Kind: "codes", Children: []outcomeCause{{Kind: "wrap", Children: []outcomeCause{code}}}},
		"codes noncode":          {Kind: "codes", Children: []outcomeCause{{Kind: "reset"}}},
		"unknown code":           {Kind: "code", Code: "fixture-secret"},
		"code children":          {Kind: "code", Code: errcode.ErrorCodeBlobUnknown, Children: []outcomeCause{{Kind: "flux"}}},
	} {
		invalidCauses[name] = outcomeCause{Kind: "status", Status: 503, Children: []outcomeCause{cause}}
	}
	scenarios := []string{"version", "count", "completion", "unknown field", "unknown cause field", "trailing JSON", "unknown cause", "cause field", "too deep", "bare EOF", "no terminal", "no remote", "format", "schema", "media", "two layers", "subject", "config media", "layer is outcome"}
	for _, scenario := range append(scenarios, slices.Sorted(maps.Keys(invalidCauses))...) {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, ocispec.Descriptor{})
			_, data, _ := o.encode()
			var m ocispec.Manifest
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatal(err)
			}
			if cause, ok := invalidCauses[scenario]; ok {
				o.record.Cause = outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "join", Children: []outcomeCause{{Kind: "reset"}, cause}}}}
			}
			switch scenario {
			case "version":
				o.record.Version++
			case "count":
				o.record.Count = 0
			case "completion":
				o.record.Completed = time.Time{}
			case "unknown cause":
				o.record.Cause.Kind = "fixture-secret"
			case "cause field":
				o.record.Cause.Op = "fixture-secret"
			case "too deep":
				for range outcomeDepthLimit {
					o.record.Cause = outcomeCause{Kind: "wrap", Children: []outcomeCause{o.record.Cause}}
				}
			case "bare EOF":
				o.record.Cause = outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "eof"}}}
			case "no terminal":
				o.record.Cause = outcomeCause{Kind: "reset"}
			case "no remote":
				o.record.Cause = outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "flux"}}}
			case "format":
				m.ArtifactType = "unknown"
			case "schema":
				m.SchemaVersion = 1
			case "media":
				m.MediaType = "unknown"
			case "two layers":
				m.Layers = []ocispec.Descriptor{{}, {}}
			case "subject":
				m.Subject = &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: "sha256:abc", Size: 1}
			case "layer is outcome":
				m.Layers = []ocispec.Descriptor{content.NewDescriptorFromBytes(ocispec.MediaTypeImageManifest, data)}
			}
			config, err := json.Marshal(o.record)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "unknown field" {
				config = append([]byte(`{"unknown":"fixture-secret",`), config[1:]...)
			}
			if scenario == "unknown cause field" {
				config = bytes.Replace(config, []byte(`"kind":`), []byte(`"unknown":"fixture-secret","kind":`), 1)
			}
			if scenario == "trailing JSON" {
				config = append(config, []byte(`{}`)...)
			}
			m.Config = content.NewDescriptorFromBytes(outcomeConfig, config)
			if scenario == "config media" {
				m.Config.MediaType = "unknown"
			}
			data, err = json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			desc := pushOutcomeFixture(t, c, config, data)
			if got, err := c.readOutcome(t.Context(), desc); got != nil || err != nil {
				t.Fatalf("invalid record accepted: %+v, %v", got, err)
			}
			id := manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"invalid"}})
			if err := c.store.Tag(t.Context(), desc, id); err != nil {
				t.Fatal(err)
			}
			calls := 0
			r, _, _, err := c.Blob(t.Context(), BlobKey{Content: "invalid"}, Policy{MaxAge: Forever}, func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				return producer([]byte("replacement"), ocispec.Descriptor{})(ctx, w)
			})
			if err != nil || calls != 1 {
				t.Fatalf("invalid record suppressed acquisition: calls=%d, error=%v", calls, err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOutcome_PublicationFailure(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"tag", "push", "existing config corrupt"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			live := outcomeFailure(connReset())
			o := newOutcome(live, time.Now().UTC(), 1, ocispec.Descriptor{})
			switch scenario {
			case "tag":
				index := filepath.Join(c.root.Name(), indexPath())
				if err := os.Remove(index); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(index, 0o750); err != nil {
					t.Fatal(err)
				}
			case "push":
				if err := c.root.RemoveAll("oci/blobs"); err != nil {
					t.Fatal(err)
				}
				if err := c.root.WriteFile("oci/blobs", []byte("blocked"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "existing config corrupt":
				config, data, _ := o.encode()
				pushOutcomeFixture(t, c, config, data)
				desc := content.NewDescriptorFromBytes(outcomeConfig, config)
				if err := c.root.Chmod(blobPath(desc.Digest), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := c.root.WriteFile(blobPath(desc.Digest), []byte("corrupt"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.publishOutcome(t.Context(), "failure", o, live); err == nil || !errors.Is(err, syscall.ECONNRESET) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("publication lost live failure: %v", err)
			} else if _, ok := errors.AsType[*CacheError](err); !ok {
				t.Fatalf("publication not CacheError: %v", err)
			}
		})
	}
}

func TestOutcome_WriteBounds(t *testing.T) {
	t.Parallel()
	for _, size := range []int{outcomeConfigLimit, outcomeConfigLimit + 1} {
		t.Run(fmt.Sprintf("record/%d", size), func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			o := &outcome{record: outcomeRecord{Version: 1, Completed: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Count: 1, Cause: outcomeCause{Kind: "terminal", Children: []outcomeCause{{Kind: "join"}}}}}
			for range 6 {
				child := outcomeCause{Kind: "reset"}
				for range 12 {
					child = outcomeCause{Kind: "net", Op: "recvfrom", Network: "tcp6", Children: []outcomeCause{child}}
				}
				o.record.Cause.Children[0].Children = append(o.record.Cause.Children[0].Children, child)
			}
			encodedSize := func() int {
				data, err := json.Marshal(o.record)
				if err != nil {
					t.Fatal(err)
				}
				return len(data)
			}
			var trim func(*outcomeCause)
			trim = func(n *outcomeCause) {
				if n.Kind == "net" && encodedSize()-size >= len(`,"op":"recvfrom"`) {
					n.Op = ""
				}
				if n.Kind == "net" && encodedSize() > size {
					n.Network = "tcp"
				}
				for i := range n.Children {
					trim(&n.Children[i])
				}
			}
			trim(&o.record.Cause)
			if encodedSize() != size {
				t.Fatalf("fixture size = %d, want %d", encodedSize(), size)
			}
			before := sweepSnapshot(t, filepath.Join(c.root.Name(), blobDir()))
			desc, err := c.publishOutcome(t.Context(), "bounded", o, connReset())
			if err != nil {
				t.Fatal(err)
			}
			if size == outcomeConfigLimit {
				if desc.Digest == "" {
					t.Fatal("exact config rejected")
				}
			} else {
				if desc.Digest != "" {
					t.Fatal("oversize config published")
				}
				if diff := cmp.Diff(before, sweepSnapshot(t, filepath.Join(c.root.Name(), blobDir()))); diff != "" {
					t.Fatalf("partial outcome written before bounds checked (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestOutcome_LastGoodRedaction(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	r, _, id, err := c.Blob(t.Context(), BlobKey{Content: "last-good"}, Policy{}, producer([]byte("held"), ocispec.Descriptor{Annotations: map[string]string{"fixture-secret": "fixture-secret", prefix + "chart-url": "https://fixture-secret"}}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	last, err := c.store.Resolve(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, last)
	for n := uint32(1); n <= 4; n++ {
		o.record.Count = n
		desc, err := c.publishOutcome(t.Context(), id, o, connReset())
		if err != nil {
			t.Fatal(err)
		}
		config, data, _ := o.encode()
		if bytes.Contains(config, []byte("fixture-secret")) || bytes.Contains(data, []byte("fixture-secret")) {
			t.Fatal("last-good annotations leaked")
		}
		o, err = c.readOutcome(t.Context(), desc)
		if err != nil || o == nil || o.lastGood.Digest != last.Digest || len(o.lastGood.Annotations) != 0 {
			t.Fatalf("direct payload: %+v, %v", o, err)
		}
	}
	if data, err := io.ReadAll(r); err != nil || string(data) != "held" {
		t.Fatalf("held blob: %q, %v", data, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	desc, err := c.store.Resolve(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-24 * time.Hour)
	if err := c.root.Chtimes(blobPath(desc.Digest), old, old); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	res, err := Sweep(t.Context(), c.root.Name(), SweepOpts{MaxAge: time.Hour})
	if err != nil || len(res.Errors) != 0 || len(res.Removed) != 1 || res.Removed[0] != id {
		t.Fatalf("retire last edge: %+v, %v", res, err)
	}
	if _, err := os.Stat(filepath.Join(c.root.Name(), blobPath(last.Digest))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("retired payload remains: %v", err)
	}
}

func TestOutcome_AcquisitionValidity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"future", "rollback", "missing", "corrupt", "saturation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := t.TempDir()
				c := cachetest.New(t, root, New)
				o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, ocispec.Descriptor{})
				switch scenario {
				case "future":
					o.record.Completed = time.Now().UTC().Add(time.Hour)
				case "rollback":
					o.observed = time.Now().UTC().Add(time.Second)
				case "saturation":
					o.record.Completed = time.Now().UTC().Add(-15 * time.Minute)
					o.record.Count = math.MaxUint32
				}
				id := manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"validity"}})
				desc, err := c.publishOutcome(t.Context(), id, o, o.replay())
				if err != nil {
					t.Fatal(err)
				}
				switch scenario {
				case "rollback":
					c.entries.Store(id, &entry{stored: desc, outcome: o})
				case "missing":
					if err := c.root.Remove(blobPath(desc.Digest)); err != nil {
						t.Fatal(err)
					}
				case "corrupt":
					if err := c.root.Chmod(blobPath(desc.Digest), 0o600); err != nil {
						t.Fatal(err)
					}
					if err := c.root.WriteFile(blobPath(desc.Digest), []byte("invalid"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "future" || scenario == "saturation" {
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = cachetest.New(t, root, New)
				}
				calls := 0
				_, _, _, err = c.Blob(t.Context(), BlobKey{Content: "validity"}, Policy{MaxAge: Forever, Retry: fastRetry(2)}, func(context.Context, ProducerInput) (ProducerResult, error) {
					calls++
					return ProducerResult{}, connReset()
				})
				if scenario == "missing" || scenario == "corrupt" {
					if _, ok := errors.AsType[*CacheError](err); !ok || calls != 0 {
						t.Fatalf("broken persistence concealed: calls=%d, error=%v", calls, err)
					}
					return
				}
				wantCalls, wantCount := 2, uint32(1)
				if scenario == "saturation" {
					wantCalls, wantCount = 1, math.MaxUint32
				}
				if calls != wantCalls || !errors.Is(err, syscall.ECONNRESET) {
					t.Fatalf("validity calls=%d, want=%d, error=%v", calls, wantCalls, err)
				}
				desc, err = c.store.Resolve(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				next, err := c.readOutcome(t.Context(), desc)
				if err != nil || next == nil || next.record.Count != wantCount || !next.record.Completed.Equal(time.Now()) {
					t.Fatalf("validity outcome=%+v, error=%v", next, err)
				}
			})
		})
	}
}
