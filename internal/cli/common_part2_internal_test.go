package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinishSelected_SuppliedAdvisoryUnion(t *testing.T) {
	t.Parallel()
	ordinary := manifest.Warning{
		Resource: testutil.HelmReleaseID("apps", "demo"),
		Category: manifest.WarnStaleValues,
		Message:  "ordinary",
		Detail:   []string{"kept"},
		Count:    3,
	}
	other := ordinary
	other.Message, other.Detail = "another ordinary", []string{"another", "kept"}
	inv := &selectedInvocation{}
	for _, cluster := range []string{"alpha", "omega"} {
		advisories := []manifest.Warning{
			{
				Category: manifest.WarnSuppliedPrecedence,
				Message:  "repository wins",
				Detail:   []string{cluster, "common", "first", "last"},
				Count:    1,
			},
		}
		inv.groups = append(inv.groups, selectedGroup{cluster: cluster, sides: []*selection.Selection{
			{Warnings: []manifest.Warning{ordinary, other}},
			{Warnings: []manifest.Warning{ordinary, other}, ClusterWarnings: advisories},
		}})
	}
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		assert.NoError(
			t,
			finishSelected(&selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr}, inv, &selectedPayload{}),
		)
		text := stderr.String()
		for _, token := range []string{"warnings (10)"} {
			assert.Equal(t, strings.Count(text, token), 1)
		}
		assert.Equal(t, strings.Count(text, "[SuppliedPrecedence]"), 2)
		assert.Equal(t, strings.Count(text, "[StaleValues]"), 8)
		finishSelectedSuppliedAdvisoryUnionLoop(t, struct {
			text     *string
			ordinary *struct {
				Resource manifest.NamedResource
				Subject  manifest.DiagnosticIdentity
				Related  []manifest.NamedResource
				Category string
				Message  string
				Detail   []string
				Count    int
			}
			other *struct {
				Resource manifest.NamedResource
				Subject  manifest.DiagnosticIdentity
				Related  []manifest.NamedResource
				Category string
				Message  string
				Detail   []string
				Count    int
			}
		}{text: &text, ordinary: (*struct {
			Resource manifest.NamedResource
			Subject  manifest.DiagnosticIdentity
			Related  []manifest.NamedResource
			Category string
			Message  string
			Detail   []string
			Count    int
		})(&ordinary), other: (*struct {
			Resource manifest.NamedResource
			Subject  manifest.DiagnosticIdentity
			Related  []manifest.NamedResource
			Category string
			Message  string
			Detail   []string
			Count    int
		})(&other)})
		assert.FatalIf(
			t,
			strings.Contains(text, "Cluster:") || strings.Contains(text, output.GlyphFail),
			"advisories changed report framing or verdict: %s",
			text,
		)
		if run == 1 {
			assert.Equal(t, text, first)
		}
		first = text
		for _, group := range inv.groups {
			for _, side := range group.sides {
				assert.Diff(t, side.Warnings, []manifest.Warning{ordinary, other})
			}
		}
	}
	fatal := errors.New("fatal render failure")
	var stderr bytes.Buffer
	err := finishSelected(
		&selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr},
		inv,
		&selectedPayload{err: fatal},
	)
	assert.FatalIf(
		t,
		!errors.Is(err, fatal) || !strings.Contains(stderr.String(), "another ordinary (×3)"),
		"warnings changed fatal result or lost messages: %v\n%s",
		err,
		stderr.String(),
	)
}

func TestRunSelected_RemainderAndPayloadErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []bool{false, true} {
		t.Run(strconv.FormatBool(test), func(t *testing.T) {
			t.Parallel()
			o, err := completedOrchestrator(t)
			assert.NoError(t, err)
			id := testutil.KustomizationID("apps", "selected")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					id: {Status: store.StatusFailed, Message: "selected render failed"},
				},
			}
			resourceErr := &orchestrator.FailuresError{Message: "aggregate render failure"}
			closeErr, writeErr := errors.New("shared cleanup failed"), errors.New("payload write failed")
			runs := []orchestrator.Rendered{{Orchestrator: o, Result: res, Err: resourceErr}}
			roots := []manifest.NamedResource{id}
			sides, selectedErr, remainder := orchestrator.Select(
				runs,
				errors.Join(
					fmt.Errorf("head tree: %w", errors.Join(resourceErr, context.Canceled)),
					fmt.Errorf("cache close: %w", closeErr),
				),
				roots,
				nil,
			)
			assert.FatalIf(t, len(sides) != 1 || selectedErr == nil, "completed run selection: %v", selectedErr)
			var stdout, stderr bytes.Buffer
			_, payloadErr := failingWriter{err: writeErr}.Write([]byte("partial"))
			payload := selectedPayload{err: payloadErr}
			if test {
				payload.rosters = []output.TestReport{
					{Cases: []output.Case{{ID: id, Outcome: output.OutcomePassed}}, Passed: 1, Matched: 1},
				}
			}
			got := finishSelected(
				&selectedRequest{roots: roots, stdout: &stdout, stderr: &stderr, test: test},
				&selectedInvocation{
					groups: []selectedGroup{{cluster: ".", sides: sides, err: selectedErr, remainder: remainder}},
				},
				&payload,
			)
			assert.FatalIf(
				t,
				!errors.Is(got, closeErr) || !errors.Is(got, writeErr) || !errors.Is(got, context.Canceled) ||
					!errors.Is(got, selectedErr),
				"lost selected, cleanup or payload error: %v",
				got,
			)
			if _, reported := errors.AsType[reportedError](got); !reported {
				assert.NoError(t, (output.Encoder{Writer: &stderr}).Errors(got))
			}
			for _, message := range []string{"selected render failed", closeErr.Error(), writeErr.Error(), context.Canceled.Error()} {
				assert.Equal(t, strings.Count(stderr.String(), message), 1)
			}
		})
	}
}

func TestRenderKinds_Capabilities(t *testing.T) {
	t.Parallel()
	var want []string
	for info := range manifest.Kinds() {
		if info.Render {
			want = append(want, info.Kind)
		}
	}
	assert.Diff(t, renderKinds, want)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiagnosticOnlyComparison(t *testing.T) {
	root := writeFixture(t)
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "work.txt", "before")
	pin := testutil.GitCommitAll(
		t,
		repo,
		"base",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("work.txt")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "work.txt", "after")
	testutil.GitCommitAll(t, repo, "head", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	oid := entry.Hash.String()
	assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
	testutil.WriteFile(t, root, ".git/index", "invalid index")
	for _, command := range [][]string{{"diff", "all"}, {"diff", "images"}, {"test", "ks", "named"}, {"build", "all"}, {"get", "all"}} {
		t.Run(strings.Join(command, "/"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append(slices.Clone(command), "--path", root, "--base", pin.String(), "--concurrency", "2")
			code := Run(args, &stdout, &stderr)
			assert.FatalIf(
				t,
				code != 1 || stdout.Len() != 0 || strings.Count(stderr.String(), "Git status unavailable") != 1 ||
					strings.Count(stderr.String(), "missing objects") != 1 ||
					strings.Contains(
						stderr.String(),
						`no Kustomization named`,
					),
				"diagnostic-only %v = exit %d, stdout %q, stderr %q",
				command,
				code,
				stdout.String(),
				stderr.String(),
			)
		})
	}
	for _, tc := range []struct {
		name       string
		test, pair bool
	}{{name: "data"}, {name: "test", test: true}, {name: "diff", pair: true}} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			rootID := testutil.KustomizationID("", "")
			if tc.test {
				rootID.Name = "named"
			}
			err := runSelected(t.Context(), selectedRequest{
				flags: commonFlags{path: root, base: pin.String(), concurrency: 2},
				roots: []manifest.NamedResource{rootID},
				pair:  tc.pair, stdout: &stdout, stderr: &stderr, test: tc.test,
			}, func(*selectedInvocation) selectedPayload { called = true; return selectedPayload{} })
			assert.FatalIf(
				t,
				err == nil || called || !strings.Contains(stderr.String(), "Git status unavailable"),
				"incomplete input invoked product=%v: %v, %s",
				called,
				err,
				stderr.String(),
			)
		})
	}
}

func TestRenderTrees_InputWarningOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	warning := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "before failure", Count: 1}
	runs, err := orchestrator.RenderTrees(
		t.Context(),
		[]orchestrator.Tree{
			{
				ID:    manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path:  root,
				Input: &store.SourceArtifact{LocalPath: root, Warnings: []manifest.Warning{warning}},
			},
		},
		orchestrator.Config{Concurrency: 2},
	)
	assert.FatalIf(t, len(runs) != 1, "missing diagnostic run: %v", err)
	run := runs[0]
	assert.FatalIf(t, run.Orchestrator != nil || run.Result == nil || !errors.Is(run.Err, manifest.ErrInput) ||
		len(run.Result.Manifests)+len(run.Result.Failed) != 0, "failed bootstrap exposed content: %+v", run)
	assert.Diff(t, run.Result.Warnings, []manifest.Warning{warning})
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unavailable_%t", unavailable), func(t *testing.T) {
			t.Parallel()
			renderTreesInputWarningOnlyCase(t, (*struct {
				Resource manifest.NamedResource
				Subject  manifest.DiagnosticIdentity
				Related  []manifest.NamedResource
				Category string
				Message  string
				Detail   []string
				Count    int
			})(&warning), &root, &unavailable)
		})
	}
}

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
			side := &selection.Selection{
				Warnings: []manifest.Warning{{Category: manifest.WarnEmptyScan, Message: "advisory"}},
			}
			inv := &selectedInvocation{
				groups: []selectedGroup{
					{cluster: ".", sides: []*selection.Selection{side, side}, err: renderErr, remainder: renderErr},
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

type countFailWriter struct {
	writes int
	err    error
}

func (w *countFailWriter) Write([]byte) (int, error) { w.writes++; return 0, w.err }

func outputByParent(batches []manifest.DocumentBatch) map[manifest.NamedResource][]map[string]any {
	out := make(map[manifest.NamedResource][]map[string]any, len(batches))
	for _, batch := range batches {
		if batch.Parent != nil {
			out[batch.Parent.Named()] = batch.Documents
		}
	}
	return out
}

func completedOrchestrator(tb testing.TB) (*orchestrator.Orchestrator, error) {
	tb.Helper()
	runs, err := orchestrator.RenderTrees(
		tb.Context(),
		[]orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: tb.TempDir()}},
		orchestrator.Config{Concurrency: 2},
	)
	if err != nil {
		return nil, err
	}
	if len(runs) != 1 || runs[0].Orchestrator == nil {
		tb.Fatal("missing completed store")
	}
	return runs[0].Orchestrator, nil
}

func TestRun_ClusterReport_KindCollisionInventory(t *testing.T) {
	t.Parallel()
	widget := manifest.NamedResource{Group: "one.example", Kind: "Widget", Namespace: "ns", Name: "same"}
	other := widget
	other.Group = "two.example"
	gadget := manifest.NamedResource{Group: "one.example", Kind: "Gadget", Namespace: "ns", Name: "root"}
	victims := make([]manifest.NamedResource, 7)
	for i := range victims {
		victims[i] = manifest.NamedResource{Kind: "Leaf", Namespace: "ns", Name: fmt.Sprintf("leaf%d", i)}
	}
	victims[6] = manifest.NamedResource{Group: "hidden.example", Kind: "Gadget", Name: "hidden"}
	warning := manifest.Warning{
		Resource: other, Category: "Inventory", Count: 3,
		Message: "Widget.one.example/ns/opaque stays unchanged", Detail: []string{"Gadget.hidden.example/ns/reference"},
	}
	alpha := &selection.Selection{
		Failed: map[manifest.NamedResource]store.StatusInfo{
			widget: {
				Status:  store.StatusFailed,
				Message: "widget failed",
			},
			gadget: {Status: store.StatusFailed, Message: "gadget failed"},
		},
		Causes: map[manifest.NamedResource][]manifest.NamedResource{widget: nil, gadget: victims},
	}
	omega := &selection.Selection{Warnings: []manifest.Warning{warning}}
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		assert.NoError(t, finishSelected(
			&selectedRequest{stderr: &stderr, stdout: io.Discard},
			&selectedInvocation{
				groups: []selectedGroup{
					{cluster: "alpha", sides: []*selection.Selection{alpha}},
					{cluster: "omega", sides: []*selection.Selection{omega}},
				},
			},
			&selectedPayload{},
		))
		out := stderr.String()
		for _, want := range []string{
			"Widget.one.example  alpha: ns/same", "Widget.two.example omega: ns/same: " + warning.Message,
			"Gadget  alpha: ns/root", "alpha: ns/leaf5 +1 more", warning.Detail[0], "(×3)",
		} {
			assert.FatalIf(t, !strings.Contains(out, want), "missing %q: %s", want, out)
		}
		assert.FatalIf(t, strings.Contains(out, "Gadget.one.example") || strings.Contains(out, "hidden.example  ") ||
			strings.Contains(out, "Cluster:"), "hidden subjects changed the inventory: %s", out)
		if run == 0 {
			first = out
		} else {
			assert.Equal(t, out, first)
		}
	}
	assert.Diff(t, omega.Warnings, []manifest.Warning{warning})
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
			var sides []*selection.Selection
			var rows []string
			for _, label := range labels {
				if sides == nil {
					sides = make([]*selection.Selection, 0, len(labels))
				}
				sides = append(sides, &selection.Selection{
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
	side := &selection.Selection{
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
				{cluster: "alpha", sides: []*selection.Selection{side}, err: errors.New("selected failed")},
				{cluster: "omega", sides: []*selection.Selection{{}}},
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

type recordingStream struct {
	bytes.Buffer

	all *bytes.Buffer
}

func (w *recordingStream) Write(p []byte) (int, error) {
	_, _ = w.all.Write(p)
	return w.Buffer.Write(p)
}

func (w *recordingStream) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

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
			side := &selection.Selection{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					id: {Status: store.StatusFailed, Message: "phase primary cause"},
				},
				Causes:   map[manifest.NamedResource][]manifest.NamedResource{id: nil},
				Warnings: []manifest.Warning{{Resource: id, Message: "phase advisory"}},
			}
			inv := &selectedInvocation{groups: []selectedGroup{
				{cluster: "alpha", sides: []*selection.Selection{side}, err: renderErr},
				{cluster: "omega", sides: []*selection.Selection{{}}},
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

type phaseFailWriter struct {
	bytes.Buffer

	token  string
	err    error
	failed bool
}

func (w *phaseFailWriter) Write(p []byte) (int, error) {
	if !w.failed && bytes.Contains(p, []byte(w.token)) {
		w.failed = true
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func (w *phaseFailWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

func finishSelectedSuppliedAdvisoryUnionLoop(t *testing.T, loopInputs struct {
	text     *string
	ordinary *struct {
		Resource manifest.NamedResource
		Subject  manifest.DiagnosticIdentity
		Related  []manifest.NamedResource
		Category string
		Message  string
		Detail   []string
		Count    int
	}
	other *struct {
		Resource manifest.NamedResource
		Subject  manifest.DiagnosticIdentity
		Related  []manifest.NamedResource
		Category string
		Message  string
		Detail   []string
		Count    int
	}
},
) {
	t.Helper()

	for _, cluster := range []string{"alpha", "omega"} {
		for _, row := range []string{
			"[SuppliedPrecedence] " + cluster + ": repository wins\n      " + cluster + ", common, first, last\n",
		} {
			assert.Equal(t, strings.Count((*loopInputs.text), row), 1)
		}
		for _, context := range []string{"orig snapshot", "current snapshot"} {
			for warningIndex := range []manifest.Warning{(*loopInputs.ordinary), (*loopInputs.other)} {
				warning := []manifest.Warning{(*loopInputs.ordinary), (*loopInputs.other)}[warningIndex]
				row := "[StaleValues] " + context + ": HelmRelease " + cluster + ": apps/demo: " + warning.Message + " (×3)\n      " + strings.Join(
					warning.Detail,
					", ",
				) + "\n"
				assert.Equal(t, strings.Count((*loopInputs.text), row), 1)
			}
		}
	}
}
