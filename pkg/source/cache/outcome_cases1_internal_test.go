package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

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
	assert.FatalIf(
		t,
		nextOutcomeCount(0) != 1 || nextOutcomeCount(1) != 2 || nextOutcomeCount(math.MaxUint32) != math.MaxUint32,
		"%s",
		"failure count did not saturate",
	)

	for _, tc := range []struct {
		name  string
		at    time.Time
		count uint32
	}{
		{"zero", time.Time{}, 1},
		{"zero count", at, 0},
		{"year zero", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"year overflow", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"deadline overflow", time.Date(9999, 12, 31, 23, 59, 59, 500000000, time.UTC), 1},
		{"non-UTC", at.In(time.FixedZone("offset", 3600)), 1},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); testOutcomeTimeCase(t, &tc) })
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
			testOutcomeTimeCase2(t, &at, &tc)
		},
		)
	}
}

func pushOutcomeFixture(t *testing.T, c *Cache, config, data []byte) ocispec.Descriptor {
	t.Helper()
	for _, blob := range []struct {
		media string
		data  []byte
	}{{outcomeConfig, config}, {ocispec.MediaTypeImageManifest, data}} {
		desc := content.NewDescriptorFromBytes(blob.media, blob.data)
		if err := c.store.Push(
			t.Context(),
			desc,
			bytes.NewReader(blob.data),
		); err != nil &&
			!errors.Is(err, errdef.ErrAlreadyExists) {
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
			testOutcomeReadBoundsAndIntegrityCase(t, &scenario)
		},
		)
	}
}

func TestOutcome_InvalidRecords(t *testing.T) {
	t.Parallel()
	code := outcomeCause{Kind: "code", Code: errcode.ErrorCodeBlobUnknown}
	codes := outcomeCause{Kind: "codes", Children: []outcomeCause{code}}
	invalidCauses := map[string]outcomeCause{
		"status client":   {Kind: "status", Status: 404},
		"status terminal": {Kind: "status", Status: 600},
		"status children": {
			Kind:     "status",
			Status:   503,
			Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}},
		},
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
		"machine missing child":    {Kind: "machine"},
		"machine extra child":      {Kind: "machine", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"machine operation":        {Kind: "machine", Op: "read", Children: []outcomeCause{{Kind: "reset"}}},
		"machine network":          {Kind: "machine", Network: "tcp", Children: []outcomeCause{{Kind: "reset"}}},
		"machine status":           {Kind: "machine", Status: 503, Children: []outcomeCause{{Kind: "reset"}}},
		"machine code": {
			Kind:     "machine",
			Code:     errcode.ErrorCodeBlobUnknown,
			Children: []outcomeCause{{Kind: "reset"}},
		},
		"machine temporary":     {Kind: "machine", Temporary: true, Children: []outcomeCause{{Kind: "reset"}}},
		"machine not found":     {Kind: "machine", NotFound: true, Children: []outcomeCause{{Kind: "reset"}}},
		"empty join":            {Kind: "join"},
		"wrap children":         {Kind: "wrap", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"DNS children":          {Kind: "dns", Children: []outcomeCause{{Kind: "reset"}, {Kind: "refused"}}},
		"net unknown network":   {Kind: "net", Network: "fixture-secret", Children: []outcomeCause{{Kind: "reset"}}},
		"URL unknown operation": {Kind: "url", Op: "fixture-secret", Children: []outcomeCause{{Kind: "reset"}}},
		"sibling transport EOF": {
			Kind:     "join",
			Children: []outcomeCause{{Kind: "url", Children: []outcomeCause{{Kind: "reset"}}}, {Kind: "eof"}},
		},
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
	scenarios := []string{
		"version",
		"count",
		"completion",
		"unknown field",
		"unknown cause field",
		"trailing JSON",
		"unknown cause",
		"cause field",
		"too deep",
		"bare EOF",
		"no terminal",
		"no remote",
		"format",
		"schema",
		"media",
		"two layers",
		"subject",
		"config media",
		"layer is outcome",
	}
	for _, scenario := range append(scenarios, slices.Sorted(maps.Keys(invalidCauses))...) {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomeInvalidRecordsCase(
				t,
				&invalidCauses, &scenario,
			)
		},
		)
	}
}

func TestOutcome_PublicationFailure(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"tag", "push", "existing config corrupt"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomePublicationFailureCase(t, &scenario)
		},
		)
	}
}

func TestOutcome_WriteBounds(t *testing.T) {
	t.Parallel()
	for _, size := range []int{outcomeConfigLimit, outcomeConfigLimit + 1} {
		t.Run(fmt.Sprintf("record/%d", size), func(t *testing.T) {
			t.Parallel()
			testOutcomeWriteBoundsCase(t, &size)
		},
		)
	}
}

func TestOutcome_LastGoodRedaction(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	r, _, id, err := c.Blob(
		t.Context(),
		BlobKey{Content: "last-good"},
		Policy{},
		producer(
			[]byte("held"), &ocispec.Descriptor{
				Annotations: map[string]string{
					"fixture-secret":     "fixture-secret",
					prefix + "chart-url": "https://fixture-secret",
				},
			},
		),
	)
	assert.NoError(t, err)

	last, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &last)
	for n := uint32(1); n <= 4; n++ {
		o.record.Count = n
		desc, publishOutcomeErr := c.publishOutcome(t.Context(), id, o, connReset())
		assert.NoError(t, publishOutcomeErr)

		config, data, _ := o.encode()
		assert.FatalIf(
			t,
			bytes.Contains(config, []byte("fixture-secret")) || bytes.Contains(data, []byte("fixture-secret")),
			"%s",
			"last-good annotations leaked",
		)

		o, publishOutcomeErr = c.readOutcome(t.Context(), &desc)
		assert.FatalIf(
			t,
			publishOutcomeErr != nil || o == nil || o.lastGood.Digest != last.Digest ||
				len(o.lastGood.Annotations) != 0,
			"direct payload: %+v, %v",
			o,
			publishOutcomeErr,
		)
	}
	{
		data, readFileErr := fs.ReadFile(r, "content")
		assert.FatalIf(t, readFileErr != nil || string(data) != "held", "held blob: %q, %v", data, readFileErr)
	}

	desc, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	old := time.Now().Add(-24 * time.Hour)
	{
		chtimesErr := c.root.Chtimes(blobPath(desc.Digest), old, old)
		assert.NoError(t, chtimesErr)
	}
	{
		closeErr3 := c.Close()
		assert.NoError(t, closeErr3)
	}

	res, err := Sweep(t.Context(), c.root.Name(), SweepOpts{MaxAge: time.Hour})
	assert.FatalIf(
		t,
		err != nil || len(res.Errors) != 0 || len(res.Removed) != 1 || res.Removed[0] != id,
		"retire last edge: %+v, %v",
		res,
		err,
	)
	{
		_, err := os.Stat(filepath.Join(c.root.Name(), blobPath(last.Digest)))
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "retired payload remains: %v", err)
	}
}

func TestOutcome_AcquisitionValidity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"future", "rollback", "missing", "corrupt", "saturation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomeAcquisitionValidityCase(t, &scenario)
		},
		)
	}
}

func TestMachineCredentialErr_Idempotent(t *testing.T) {
	t.Parallel()
	assert.FatalIf(t, MachineCredentialErr(nil) != nil, "%s", "nil became a failure")

	cause := &fs.PathError{Op: "open", Path: "credential-canary", Err: fs.ErrPermission}
	machine := MachineCredentialErr(cause)
	terminal := &TerminalError{Err: machine, Attempted: true}
	got := MachineCredentialErr(terminal)
	assert.FatalIf(t, !errors.Is(got, terminal) || !errors.Is(got, cause) ||
		strings.Contains(got.Error(), "canary"), "classification lost terminal or typed cause: %v", got)
}

func TestOutcome_ClassifiedAcquisitionReplay(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "classified-cooldown"}
	calls := 0
	produce := func(context.Context, ProducerInput) (ProducerResult, error) {
		calls++
		return ProducerResult{}, manifest.NewError(
			manifest.ErrFlux,
			&fetcherr.ResponseError{StatusCode: 503, Op: http.MethodGet},
			"",
		)
	}
	_, _, _, live := c.Blob(t.Context(), key, Policy{}, produce)
	assert.FatalIf(t, live == nil || live.Error() != "HTTP 503" || calls != 1, "live: %v calls=%d", live, calls)
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	c = cachetest.New(t, root, New)
	_, _, _, replay := c.Blob(t.Context(), key, Policy{}, produce)
	assert.FatalIf(
		t,
		replay == nil || replay.Error() != live.Error() || calls != 1 || !errors.Is(replay, manifest.ErrFlux),
		"replay: %v calls=%d",
		replay,
		calls,
	)
	{
		terminal, ok := errors.AsType[*TerminalError](replay)
		assert.FatalIf(t, !ok || terminal.Attempted, "%s", "replay retried production")
	}
	{
		response, ok := errors.AsType[*fetcherr.ResponseError](
			replay,
		)
		assert.FatalIf(t, !ok || response.StatusCode != http.StatusServiceUnavailable ||
			response.Op != http.MethodGet, "%s", "lost typed response")
	}
}

func testOutcomeTypedRoundTripCase(t *testing.T, u **url.Error,
	tc *struct {
		name        string
		cause       error
		sentinel    error
		check       func(*testing.T, error)
		shape       outcomeCause
		machinePath []int
		diagnostic  string
	},
) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	live := outcomeFailure(tc.cause)
	o := newOutcome(live, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), 1, &ocispec.Descriptor{})
	assert.FatalIf(t, o == nil, "%s", "supported live failure rejected")

	desc, err := c.publishOutcome(t.Context(), "failure", o, live)
	assert.FatalIf(t, err != nil || desc.Digest == "", "publish: %+v, %v", desc, err)
	{
		closeErr := c.Close()
		assert.NoError(t, closeErr)
	}

	c = cachetest.New(t, root, New)
	got, err := c.readOutcome(t.Context(), &desc)
	assert.FatalIf(t, err != nil || got == nil, "read: %+v, %v", got, err)
	replay := got.replay()
	if status, ok := errors.AsType[*fetcherr.ResponseError](tc.cause); ok {
		decoded, ok := errors.AsType[*fetcherr.ResponseError](replay)
		assert.FatalIf(
			t,
			!ok || decoded.StatusCode != status.StatusCode || decoded.Op != outcomeOp(status.Op) ||
				decoded.Message != "",
			"neutral response: %+v",
			decoded,
		)
		root := got.record.Cause
		assert.FatalIf(
			t,
			root.Kind != "terminal" || len(root.Children) != 1 || root.Children[0].Kind != "producer" ||
				len(root.Children[0].Children) != 1 ||
				root.Children[0].Children[0].Kind != "status",
			"neutral response shape: %+v",
			root,
		)
	}
	{
		terminal, ok := errors.AsType[*TerminalError](
			replay,
		)
		assert.FatalIf(t, !ok || terminal.Attempted ||
			!errors.Is(replay, manifest.ErrFlux), "replay terminal identity: %v", replay)
	}
	assert.FatalIf(
		t, tc.
			sentinel != nil && !errors.Is(replay, tc.sentinel), "lost sentinel: %v",
		replay,
	)
	testOutcomeTypedRoundTripCaseTail(
		t,
		&testOutcomeTypedRoundTripCaseTailState{u: u, tc: tc, live: &live, o: o, got: got, replay: &replay},
	)
}

func testOutcomeTypedRoundTripCaseCompletion(t *testing.T, captures *testOutcomeTypedRoundTripCaseCompletionInputs) {
	t.Helper()
	if (*captures.tc).check != nil {
		(*captures.tc).
			check(t, (*captures.replay))
	}
	config, data, ok := (*captures.o).encode()
	assert.FatalIf(t, !ok || bytes.Contains(config, []byte("fixture-secret")) ||
		bytes.Contains(data, []byte("fixture-secret")) ||
		strings.Contains(
			(*captures.replay).Error(),
			"fixture-secret",
		), "%s", "persisted diagnostics contain fixture secret")

	if (*captures.tc).shape.Kind != "" {
		for _, forbidden := range []string{"fixture-secret", "machine-canary", "credential-canary", (*(*captures.u)).URL, "https://", "cause diagnostic canary", "OCIRepository", syscall.ECONNRESET.Error()} {
			assert.FatalIf(
				t,
				bytes.Contains(config, []byte(forbidden)) || bytes.Contains(data, []byte(forbidden)) ||
					strings.Contains((*captures.replay).Error(), forbidden),
				"machine diagnostics leaked %q",
				forbidden,
			)
		}
		again, manifestAgain, encodeOk := (*captures.o).encode()
		assert.FatalIf(
			t,
			!encodeOk || !bytes.Equal(config, again) || !bytes.Equal(data, manifestAgain),
			"%s",
			"failure encoding is nondeterministic",
		)
	}
	terminal, _ := errors.AsType[*TerminalError]((*captures.live))
	producer, ok := errors.AsType[*producerError](terminal.Err)
	assert.FatalIf(
		t,
		!terminal.Attempted || !ok || !errors.Is(producer.error, (*captures.tc).cause),
		"%s",
		"live cause changed",
	)
}

func testOutcomeExclusionsCase(t *testing.T, tc *struct {
	name string
	err  error
},
) {
	t.Helper()

	for _, err := range []error{tc.err, errors.Join(connReset(), fmt.Errorf("nested: %w", tc.err)), fmt.Errorf("outer: %w", errors.Join(tc.err, connReset()))} {
		if got := newOutcome(outcomeFailure(err), time.Now().UTC(), 1, &ocispec.Descriptor{}); got != nil {
			t.Fatalf("excluded tree accepted: %T", tc.err)
		}
	}
}

func testOutcomeTimeCase(t *testing.T, tc *struct {
	name  string
	at    time.Time
	count uint32
},
) {
	t.Helper()
	{
		_, valid := outcomeDeadline(tc.at, tc.count)
		assert.FatalIf(t, valid, "%s", "invalid time accepted")
	}
}

func testOutcomeTimeCase2(t *testing.T, at *time.Time,
	tc *struct {
		name         string
		observations []time.Duration
		fresh        []bool
		valid        []bool
	},
) {
	t.Helper()

	o := newOutcome(outcomeFailure(connReset()), (*at), 1, &ocispec.Descriptor{})
	before := o.record
	for i, delta := range tc.observations {
		fresh, valid := o.observe(at.Add(delta))
		assert.FatalIf(
			t,
			fresh != tc.fresh[i] || valid != tc.valid[i],
			"observe %s: %t/%t",
			delta,
			fresh,
			valid,
		)
	}
	{
		diff := cmp.Diff(before, o.record)
		assert.FatalIf(t, diff != "", "observation renewed record (-want +got):\n%s", diff)
	}
}

type testOutcomeTypedRoundTripCaseCompletionInputs struct {
	tc **struct {
		name        string
		cause       error
		sentinel    error
		check       func(*testing.T, error)
		shape       outcomeCause
		machinePath []int
		diagnostic  string
	}
	replay *error
	o      **outcome
	u      ***url.Error
	live   *error
}

type testOutcomeTypedRoundTripCaseTailState struct {
	u  **url.Error
	tc *struct {
		name        string
		cause       error
		sentinel    error
		check       func(*testing.T, error)
		shape       outcomeCause
		machinePath []int
		diagnostic  string
	}
	live   *error
	o      *outcome
	got    *outcome
	replay *error
}

func testOutcomeTypedRoundTripCaseTail(t *testing.T, scope *testOutcomeTypedRoundTripCaseTailState) {
	t.Helper()
	if scope.tc.shape.Kind != "" {
		want := outcomeCause{
			Kind:     "terminal",
			Children: []outcomeCause{{Kind: "producer", Children: []outcomeCause{scope.tc.shape}}},
		}
		{
			diff := cmp.Diff(want, scope.got.record.Cause)
			assert.FatalIf(t, diff != "", "persisted cause (-want +got):\n%s", diff)
		}
		terminal, ok := (*scope.replay).(*TerminalError)
		assert.FatalIf(t, !ok, "replay root: %T", (*scope.replay))
		producer, ok := terminal.Err.(*producerError)
		if !ok {
			t.Fatalf("terminal child: %T", terminal.Err)
		}
		node, shape := producer.error, scope.tc.shape
		for _, index := range scope.tc.machinePath {
			assert.FatalIf(t, shape.Kind == "wrap" && reflect.TypeOf(node) != fmtWrapType ||
				shape.Kind == "join" && reflect.TypeOf(node) != joinType, "machine parent identity: %T", node)
			switch parent := node.(type) {
			case interface{ Unwrap() error }:
				node = parent.Unwrap()
			case interface{ Unwrap() []error }:
				node = parent.Unwrap()[index]
			default:
				t.Fatalf("machine parent: %T", node)
			}
			shape = shape.Children[index]
		}
		machine, ok := node.(*machineError)
		assert.FatalIf(t, !ok || machine.Error() != "machine credential acquisition failed" ||
			(*scope.replay).Error() != scope.tc.diagnostic, "machine masking or position: %T, %v", node, (*scope.replay))
		child := machine.Unwrap()
		if shape.Children[0].Kind == "terminal" {
			terminal, ok := child.(*TerminalError)
			assert.FatalIf(t, !ok || terminal.Attempted, "machine terminal child: %T, %v", child, child)
			producer, ok := terminal.Err.(*producerError)
			if !ok {
				t.Fatalf("nested terminal child: %T", terminal.Err)
			}
			child = producer.error
		}
		assert.FatalIf(t, reflect.TypeOf(child) != fmtWrapType, "machine transport child: %T", child)
		{
			innerU2, ok := errors.AsType[*url.Error](machine)
			assert.FatalIf(t, !ok || innerU2.Op != "Head" || innerU2.URL != "", "machine URL: %+v", innerU2)
		}
		{
			n, ok := errors.AsType[*net.OpError](
				machine,
			)
			assert.FatalIf(t, !ok || n.Op != "read" || n.Net != "tcp" || n.Source != nil ||
				n.Addr != nil, "machine network: %+v", n)
		}
		{
			s, ok := errors.AsType[*os.SyscallError](machine)
			assert.FatalIf(t, !ok || s.Syscall != "read", "machine syscall: %+v", s)
		}
	}
	testOutcomeTypedRoundTripCaseCompletion(
		t,
		&testOutcomeTypedRoundTripCaseCompletionInputs{
			tc:     &scope.tc,
			replay: &(*scope.replay),
			o:      &scope.o,
			u:      &scope.u,
			live:   &(*scope.live),
		},
	)
}
