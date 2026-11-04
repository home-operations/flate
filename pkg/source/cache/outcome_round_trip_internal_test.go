package cache

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

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
		&testOutcomeTypedRoundTripCaseTailState{u: &u, tc: &tc, live: &live, o: &o, got: &got, replay: &replay},
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
	u  ***url.Error
	tc **struct {
		name        string
		cause       error
		sentinel    error
		check       func(*testing.T, error)
		shape       outcomeCause
		machinePath []int
		diagnostic  string
	}
	live   *error
	o      **outcome
	got    **outcome
	replay *error
}

func testOutcomeTypedRoundTripCaseTail(t *testing.T, scope *testOutcomeTypedRoundTripCaseTailState) {
	t.Helper()
	if (*scope.tc).shape.Kind != "" {
		want := outcomeCause{
			Kind:     "terminal",
			Children: []outcomeCause{{Kind: "producer", Children: []outcomeCause{(*scope.tc).shape}}},
		}
		{
			diff := cmp.Diff(want, (*scope.got).record.Cause)
			assert.FatalIf(t, diff != "", "persisted cause (-want +got):\n%s", diff)
		}
		//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
		terminal, ok := (*scope.replay).(*TerminalError)
		assert.FatalIf(t, !ok, "replay root: %T", (*scope.replay))
		//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
		producer, ok := terminal.Err.(*producerError)
		assert.FatalIf(t, !ok, "terminal child: %T", terminal.Err)
		node, shape := producer.error, (*scope.tc).shape
		for _, index := range (*scope.tc).machinePath {
			assert.FatalIf(t, shape.Kind == "wrap" && reflect.TypeOf(node) != fmtWrapType ||
				shape.Kind == "join" && reflect.TypeOf(node) != joinType, "machine parent identity: %T", node)
			//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
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
		//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
		machine, ok := node.(*machineError)
		assert.FatalIf(t, !ok || machine.Error() != "machine credential acquisition failed" ||
			(*scope.replay).Error() != (*scope.tc).diagnostic, "machine masking or position: %T, %v", node, (*scope.replay))
		child := machine.Unwrap()
		if shape.Children[0].Kind == "terminal" {
			//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
			terminal, ok := child.(*TerminalError)
			assert.FatalIf(t, !ok || terminal.Attempted, "machine terminal child: %T, %v", child, child)
			//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
			producer, ok := terminal.Err.(*producerError)
			assert.FatalIf(t, !ok, "nested terminal child: %T", terminal.Err)
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
			tc:     scope.tc,
			replay: scope.replay,
			o:      scope.o,
			u:      scope.u,
			live:   scope.live,
		},
	)
}
