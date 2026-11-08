package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFinishSelected_JoinsWriterErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []bool{false, true} {
		t.Run(strconv.FormatBool(test), func(t *testing.T) {
			t.Parallel()
			productErr, reportErr, renderErr := errors.New(
				"product closed",
			), errors.New(
				"report closed",
			), errors.New(
				"render failed",
			)
			side := &selection.Result{
				Warnings: []manifest.Warning{{Category: manifest.WarnEmptyScan, Message: "advisory"}},
			}
			inv := &selectedInvocation{
				groups: []selectedGroup{
					{cluster: ".", sides: []*selection.Result{side, side}, err: renderErr, remainder: renderErr},
				},
			}
			payload := selectedPayload{
				err:      productErr,
				warnings: [][]manifest.Warning{{{Category: manifest.WarnDiffSuppressed, Message: "payload warning"}}},
			}
			if test {
				payload.err = nil
				payload.rosters = []output.TestReport{{}}
			}
			writer := &countFailWriter{err: reportErr}
			err := finishSelected(
				&selectedRequest{
					roots:  []manifest.NamedResource{{Kind: manifest.KindKustomization}},
					stdout: failingWriter{err: productErr},
					stderr: writer,
					test:   test,
					pair:   true,
				},
				inv,
				&payload,
			)
			for _, want := range []error{productErr, reportErr, renderErr} {
				assert.FatalIf(t, !errors.Is(err, want), "lost %v: %v", want, err)
			}
			assert.FatalIf(t, writer.writes < 2, "report stopped after %d writes", writer.writes)
		})
	}
}

func TestFinishSelected_FailureContext(t *testing.T) {
	t.Parallel()
	for _, pair := range []bool{false, true} {
		t.Run(fmt.Sprintf("pair_%t", pair), func(t *testing.T) {
			t.Parallel()
			root := testutil.KustomizationID("ns", "root")
			missing := testutil.KustomizationID("ns", "missing")
			leaf := testutil.KustomizationID("ns", "leaf")
			dependent := testutil.KustomizationID("ns", "dependent")
			labels := []string{"single"}
			if pair {
				labels = []string{"orig", "current"}
			}
			var sides []*selection.Result
			var rows []string
			for _, label := range labels {
				if sides == nil {
					sides = make([]*selection.Result, 0, len(labels))
				}
				sides = append(sides, &selection.Result{
					Failed: map[manifest.NamedResource]store.StatusInfo{
						root: {Status: store.StatusFailed, Message: label + " failure\nunchanged details"},
					},
					Blocked: map[manifest.NamedResource][]manifest.NamedResource{leaf: {root}, dependent: {missing}},
					Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {leaf}, missing: {dependent}},
				})
				prefix := ""
				if pair {
					prefix = label + " snapshot: "
				}
				if rows == nil {
					rows = make([]string, 0, len(labels))
				}
				rows = append(rows, "  ✗  "+prefix+"Kustomization  ns/root  "+label+" failure\n"+
					"         unchanged details\n"+
					"         "+prefix+"blocks ns/leaf\n"+
					"  ✗  "+prefix+"Kustomization  ns/missing  not found\n"+
					"         "+prefix+"required by ns/dependent")
			}
			var stderr bytes.Buffer
			err := finishSelected(
				&selectedRequest{pair: pair, stdout: io.Discard, stderr: &stderr},
				&selectedInvocation{
					groups: []selectedGroup{{cluster: ".", sides: sides, err: errors.New("render failed")}},
				},
				&selectedPayload{},
			)
			{
				_, reported := errors.AsType[reportedError](err)
				assert.FatalIf(t, !reported, "failure was not marked reported: %v", err)
			}
			want := "\n" + strings.Join(
				rows,
				"\n\n",
			) + fmt.Sprintf(
				"\n\n  ✗ %d failed · %d blocked\n",
				2*len(sides),
				2*len(sides),
			)
			assert.Equal(t, stderr.String(), want)
		})
	}
}

func TestFinishSelected_ReportOrder(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("ns", "same")
	var combined bytes.Buffer
	stdout, stderr := &recordingStream{all: &combined}, &recordingStream{all: &combined}
	failure := store.StatusInfo{Status: store.StatusFailed, Message: "primary cause"}
	side := &selection.Result{
		Failed:   map[manifest.NamedResource]store.StatusInfo{id: failure},
		Causes:   map[manifest.NamedResource][]manifest.NamedResource{id: nil},
		Warnings: []manifest.Warning{{Resource: id, Message: "selected warning"}},
	}
	payload := selectedPayload{rosters: []output.TestReport{
		{Cases: []output.Case{{ID: id, Outcome: output.OutcomeFailed, Reason: failure.Message}}, Failed: 1},
		{Cases: []output.Case{{ID: id, Outcome: output.OutcomePassed}}, Passed: 1},
	}, write: func() error {
		_, err := io.WriteString(stdout, "payload\n")
		return err
	}}
	err := finishSelected(
		&selectedRequest{roots: []manifest.NamedResource{id}, stdout: stdout, stderr: stderr, test: true},
		&selectedInvocation{
			groups: []selectedGroup{
				{cluster: "alpha", sides: []*selection.Result{side}, err: errors.New("selected failed")},
				{cluster: "omega", sides: []*selection.Result{{}}},
			},
		},
		&payload,
	)
	assert.FatalIf(t, err == nil, "%v", "test failure became successful")
	out := combined.String()
	last := -1
	for _, token := range []string{"payload", "primary cause", "omega: ns/same", "selected warning", "alpha: ✗ 0 passed", "omega: ✓ 1 passed", "Invocation totals", "✗ 1 passed · 1 failed"} {
		at := strings.Index(out, token)
		assert.FatalIf(t, at <= last, "%q out of order: %s", token, out)
		last = at
	}
	assert.Equal(t, strings.Count(out, "primary cause"), 1)
	assert.Equal(t, strings.Contains(stdout.String(), "warning"), false)
	assert.Equal(t, strings.Contains(stderr.String(), "Invocation totals"), false)
}

func TestFinishSelected_UnpresentedCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("unpresented cause")
	var stderr bytes.Buffer
	err := finishSelected(&selectedRequest{stdout: io.Discard, stderr: &stderr},
		&selectedInvocation{groups: []selectedGroup{{cluster: "alpha", err: cause}}}, &selectedPayload{})
	assert.FatalIf(t, !errors.Is(err, cause), "lost cause: %v", err)
	{
		_, reported := errors.AsType[reportedError](err)
		assert.FatalIf(
			t,
			reported || stderr.Len() != 0,
			"unwritten diagnostics marked reported: %v, %s",
			err,
			stderr.String(),
		)
	}
}

func TestFinishSelected_PhaseWriterErrors(t *testing.T) {
	t.Parallel()
	for _, phase := range []struct {
		name, token string
		test        bool
	}{
		{name: "payload", token: "phase payload"},
		{name: "warning", token: "phase advisory"},
		{name: "summary", token: "alpha: ✗ 1 failed"},
		{name: "test verdict", token: "Invocation totals", test: true},
	} {
		t.Run(phase.name, func(t *testing.T) {
			t.Parallel()
			writeErr, renderErr := errors.New("phase write failed"), errors.New("render failed")
			writer := &phaseFailWriter{token: phase.token, err: writeErr}
			id := testutil.KustomizationID("ns", "root")
			side := &selection.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					id: {Status: store.StatusFailed, Message: "phase primary cause"},
				},
				Causes:   map[manifest.NamedResource][]manifest.NamedResource{id: nil},
				Warnings: []manifest.Warning{{Resource: id, Message: "phase advisory"}},
			}
			inv := &selectedInvocation{groups: []selectedGroup{
				{cluster: "alpha", sides: []*selection.Result{side}, err: renderErr},
				{cluster: "omega", sides: []*selection.Result{{}}},
			}}
			payload := selectedPayload{write: func() error {
				_, err := io.WriteString(writer, "phase payload\n")
				return err
			}}
			if phase.test {
				payload.rosters = []output.TestReport{{Failed: 1}, {Passed: 1}}
			}
			err := finishSelected(&selectedRequest{stdout: writer, stderr: writer}, inv, &payload)
			assert.FatalIf(
				t,
				!writer.failed || !errors.Is(err, writeErr) || !errors.Is(err, renderErr),
				"%s lost a write or render failure: failed=%v err=%v",
				phase.name,
				writer.failed,
				err,
			)
			assert.Equal(t, strings.Count(writer.String(), "phase primary cause"), 1)
			if phase.name == "summary" || phase.test {
				assert.Equal(t, strings.Count(writer.String(), "phase payload"), 1)
				assert.Equal(t, strings.Count(writer.String(), "phase advisory"), 1)
			}
		})
	}
}
