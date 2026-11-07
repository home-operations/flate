package testrunner

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_AllPass(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.AddObject(&manifest.HelmRelease{Name: "demo", Namespace: "apps"})
	s.UpdateStatus(
		testutil.KustomizationID("flux-system", "apps"),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
	)
	s.UpdateStatus(
		testutil.HelmReleaseID("apps", "demo"),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
	)
	raw := &manifest.RawObject{
		Kind:       manifest.KindKustomization,
		APIVersion: "foreign.example/v1",
		Namespace:  "apps",
		Name:       "raw",
	}
	s.AddObject(raw)

	rep := runSelected(t, o)
	assert.ErrorIf(t, rep.AnyFailed() || rep.Passed != 2, "expected 2 passed, got %+v", rep)
	var b bytes.Buffer
	{
		err := writeTestReport(output.Encoder{Writer: &b}, &rep)
		assert.FatalIf(t, err != nil, "Write: %v", err)
	}
	assert.ErrorIf(t, !strings.Contains(b.String(), "2 passed"), "missing summary: %s", b.String())
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_OneFailed(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.UpdateStatus(
		testutil.KustomizationID("flux-system", "apps"),
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"},
	)

	rep := runSelected(t, o)
	assert.ErrorIf(t, !rep.AnyFailed(), "expected failure: %+v", rep)
	if rep.Cases[0].Reason != "boom" {
		t.Errorf("reason: %q", rep.Cases[0].Reason)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SkippedReasonReportedAsSkipped(t *testing.T) {
	for _, tc := range []struct {
		name          string
		reason        store.StatusReason
		message, want string
		skipped       bool
	}{
		{name: "source skipped", reason: store.ReasonSkipped, message: "source GitRepository.source.toolkit.fluxcd.io/flux-system/sealed missing auth", want: "source GitRepository.source.toolkit.fluxcd.io/flux-system/sealed missing auth", skipped: true},
		{name: "nested skip", reason: store.ReasonSkipped, message: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth", want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth", skipped: true},
		{name: "boundary whitespace", reason: store.ReasonSkipped, message: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth", want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth", skipped: true},
		{name: "explanatory prefix", reason: store.ReasonSkipped, message: "skipped: missing auth", want: "skipped: missing auth", skipped: true},
		{name: "empty skip", reason: store.ReasonSkipped, skipped: true},
		{name: "unchanged", reason: store.ReasonUnchanged, message: "different words", want: "unchanged", skipped: true},
		{name: "suspended", reason: store.ReasonSuspended, want: "suspended", skipped: true},
		{name: "misleading skip", message: "skipped: diagnostic"},
		{name: "misleading unchanged", message: "unchanged"},
		{name: "misleading suspended", message: "suspended"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, s := newRunner(t)
			ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system"}
			s.AddObject(ks)
			s.UpdateStatus(
				ks.Named(),
				store.StatusInfo{Status: store.StatusReady, Reason: tc.reason, Message: tc.message},
			)
			rep := runSelected(t, o)
			assert.FatalIf(
				t,
				rep.AnyFailed() || rep.Matched != 1 || len(rep.Cases) != 1 || (rep.Skipped == 1) != tc.skipped ||
					(rep.Passed == 1) == tc.skipped,
				"unexpected typed outcome report: %+v",
				rep,
			)
			assert.Equal(t, rep.Cases[0].Reason, tc.want)
			assert.Diff(t, runSelected(t, o), rep)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NoStatus(t *testing.T) {
	o, s := newRunner(t)
	obj := &manifest.Kustomization{Name: "x", Namespace: "ns"}
	s.AddObject(obj)
	rep := runSelected(t, o)
	assert.FatalIf(
		t,
		!rep.AnyFailed() || rep.Matched != 1 || rep.Failed != 1 || len(rep.Cases) != 1,
		"expected one failure for no-status case: %+v",
		rep,
	)
	assert.Equal(t, rep.Cases[0].Reason, "no status reported")
	_, exists := s.GetStatus(obj.Named())
	assert.Equal(t, exists, false)
	assert.Equal(t, len(s.FailedResources()), 0)
	for _, obj := range []manifest.BaseManifest{
		&manifest.GitRepository{Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace},
		&manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindKustomization, Name: "foreign", Namespace: "apps"},
	} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			o, s := newRunner(t)
			s.AddObject(obj)
			rep := runSelected(t, o, obj.Named())
			assert.FatalIf(
				t,
				rep.Matched != 0 || rep.Failed != 0 || len(rep.Cases) != 0,
				"object without prepared reporting status entered the report: %+v",
				rep,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SelectedNamespace(t *testing.T) {
	o, s := newRunner(t)
	alpha := testutil.KustomizationID("alpha", "apps")
	beta := testutil.KustomizationID("beta", "apps")
	s.AddObject(&manifest.Kustomization{Name: alpha.Name, Namespace: alpha.Namespace})
	s.AddObject(&manifest.Kustomization{Name: beta.Name, Namespace: beta.Namespace})
	s.UpdateStatus(alpha, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(beta, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})

	rep := runSelected(t, o, testutil.KustomizationID("alpha", ""))
	assert.ErrorIf(
		t,
		rep.Passed != 1 || len(rep.Cases) != 1 || rep.Cases[0].ID != alpha,
		"selected namespace report = %+v, want only %s",
		rep,
		alpha,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DeterministicOrderAndMatchCount(t *testing.T) {
	o, s := newRunner(t)
	for _, id := range []manifest.NamedResource{
		testutil.KustomizationID("z", "last"),
		testutil.KustomizationID("a", "first"),
		testutil.KustomizationID("m", "middle"),
	} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		s.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	}
	for _, name := range []string{"extra-z", "extra-a"} {
		rs := &manifest.ResourceSet{Name: name, Namespace: "ns"}
		s.AddObject(rs)
		s.UpdateStatus(rs.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"})
	}

	foreign := manifest.NamedResource{
		Group:     "example.test",
		Kind:      manifest.KindResourceSet,
		Namespace: "ns",
		Name:      "extra-z",
	}
	s.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       foreign.Kind,
			Namespace:  foreign.Namespace,
			Name:       foreign.Name,
		},
	)
	s.UpdateStatus(foreign, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "foreign failure"})

	rep := runSelected(t, o, testutil.KustomizationID("", ""))

	assert.FatalIf(t, rep.Matched != 3, "Matched = %d, want 3", rep.Matched)
	got := []manifest.NamedResource{rep.Cases[0].ID, rep.Cases[1].ID, rep.Cases[2].ID}
	want := []manifest.NamedResource{
		testutil.KustomizationID("a", "first"),
		testutil.KustomizationID("m", "middle"),
		testutil.KustomizationID("z", "last"),
	}
	for i := range want {
		assert.FatalIf(t, got[i] != want[i], "case order = %v, want %v", got, want)
	}
	assert.FatalIf(
		t,
		len(rep.Cases) != 6 || rep.Failed != 3 || rep.Cases[3].ID != foreign || rep.Cases[4].ID.Name != "extra-a" ||
			rep.Cases[5].ID.Name != "extra-z",
		"diagnostic cases must follow the roster in identity order: %+v",
		rep.Cases,
	)
}

// TestRun_BlockedByPrimaryFailure pins the cascade: a failure with recorded
// blockers classifies as Blocked (not Failed), is kept out of the roster Cases,
// and lands in Blocked as a "blocked by <root>" case naming the primary failure
// it traces to — the verdict counting it as blocked, the run still non-zero.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BlockedByPrimaryFailure(t *testing.T) {
	o, s := newRunner(t)
	root := testutil.KustomizationID("flux-system", "cluster-apps")
	child := testutil.KustomizationID("media", "plex")
	s.AddObject(&manifest.Kustomization{Name: root.Name, Namespace: root.Namespace})
	s.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
	s.UpdateStatus(
		root,
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "kustomize build boom"},
	) // primary
	s.UpdateFailure(child, &manifest.DependencyFailedError{Parent: child, Failed: []manifest.NamedResource{root}})

	rep := runSelected(t, o)
	assert.FatalIf(
		t,
		rep.Failed != 1 || len(rep.Blocked) != 1,
		"want 1 failed (root) + 1 blocked (child), got %+v",
		rep,
	)
	assert.ErrorIf(t, !rep.AnyFailed(), "%v", "a blocked cascade must flip the run to failed")
	for _, c := range rep.Cases {
		assert.ErrorIf(t, c.ID == child, "blocked child must not get a roster Cases row: %+v", c)
	}
	// The root is a primary failure on its own row, so it carries no "(not found)".
	want := []output.Case{{ID: child, Outcome: output.OutcomeBlocked, Reason: "blocked by flux-system/cluster-apps"}}
	assert.FatalIf(t, !reflect.DeepEqual(rep.Blocked, want), "Blocked = %+v, want %+v", rep.Blocked, want)

	var b bytes.Buffer
	_ = writeTestReport(output.Encoder{Writer: &b}, &rep)
	out := b.String()
	assert.ErrorIf(
		t,
		!strings.Contains(out, "1 failed") || !strings.Contains(out, "1 blocked"),
		"verdict should separate failed and blocked:\n%s",
		out,
	)
	// The blocked child gets its own row naming the root cause, so the user can
	// see WHAT is blocked and WHY without it counting as a primary failure.
	assert.ErrorIf(
		t,
		!strings.Contains(out, "media/plex") || !strings.Contains(out, "blocked by flux-system/cluster-apps"),
		"blocked child should render naming its root:\n%s",
		out,
	)
}

// TestRun_SurfacesNonRosterFailure pins the consolidation contract: a run-time
// failure whose kind isn't in the roster (here a ResourceSet whose template
// won't parse) must still appear as a failed row and count in the verdict.
// Diagnostic failures outside the roster do not count as matched roots.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SurfacesNonRosterFailure(t *testing.T) {
	o, s := newRunner(t)
	ks := testutil.KustomizationID("flux-system", "apps")
	rs := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "broken-rs",
	}
	s.AddObject(&manifest.Kustomization{Name: ks.Name, Namespace: ks.Namespace})
	s.AddObject(&manifest.ResourceSet{Name: rs.Name, Namespace: rs.Namespace})
	s.UpdateStatus(ks, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(
		rs,
		store.StatusInfo{
			Status:  store.StatusFailed,
			Reason:  "",
			Message: `parse template: function "nope" not defined`,
		},
	)

	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "flux-system",
		Name:      "same",
	}
	s.RecordEmitted(
		ks,
		&store.Emission{
			Complete: true,
			Children: []store.Child{
				{Obj: &manifest.HelmChartSource{Name: chart.Name, Namespace: chart.Namespace}, Generated: true},
			},
		},
		nil,
	)
	s.UpdateStatus(chart, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "synthetic chart failure"})
	foreignChart := chart
	foreignChart.Group = "example.test"
	s.AddObject(
		&manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       foreignChart.Kind,
			Namespace:  foreignChart.Namespace,
			Name:       foreignChart.Name,
		},
	)
	s.UpdateStatus(
		foreignChart,
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "foreign chart failure"},
	)

	rep := runSelected(t, o)
	assert.FatalIf(
		t,
		rep.Matched != 1 || rep.Passed != 1 || rep.Failed != 2,
		"want 1 matched, 1 passed (KS) + 2 diagnostic failures, got %+v",
		rep,
	)
	foreignFound := false
	var found *output.Case
	for i := range rep.Cases {
		assert.ErrorIf(t, rep.Cases[i].ID == chart, "%v", "synthetic real chart failure entered diagnostic cases")
		if rep.Cases[i].ID == foreignChart {
			foreignFound = rep.Cases[i].Reason == "foreign chart failure"
		}
		if rep.Cases[i].ID == rs {
			found = &rep.Cases[i]
		}
	}
	assert.ErrorIf(t, !foreignFound, "foreign chart failure hidden by kind-only exemption: %+v", rep.Cases)
	assert.FatalIf(t, found == nil, "ResourceSet failure not surfaced as a case: %+v", rep.Cases)
	assert.ErrorIf(
		t,
		found.Outcome != output.OutcomeFailed || !strings.Contains(found.Reason, "nope"),
		"ResourceSet case = %+v, want failed with the parse error",
		found,
	)
	var b bytes.Buffer
	_ = writeTestReport(output.Encoder{Writer: &b}, &rep)
	{
		out := b.String()
		assert.ErrorIf(
			t,
			!strings.Contains(out, "broken-rs") || !strings.Contains(out, "nope"),
			"roster must show the non-roster failure inline:\n%s",
			out,
		)
	}

	s.AddObject(&manifest.HelmChartSource{Name: chart.Name, Namespace: chart.Namespace})
	s.UpdateStatus(chart, store.StatusInfo{Status: store.StatusFailed, Message: "synthetic chart failure"})
	explicit := runSelected(t, o, manifest.NamedResource{Group: chart.Group, Kind: chart.Kind, Name: chart.Name})
	assert.FatalIf(t, explicit.Matched != 1 || len(explicit.Cases) != 2 || explicit.Cases[0].ID != chart ||
		explicit.Cases[0].Reason != "synthetic chart failure", "explicit chart must retain its selected row before the foreign diagnostic: %+v", explicit)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SameNameDiagnosticDoesNotMatchRequestedKind(t *testing.T) {
	o, s := newRunner(t)
	hr := &manifest.HelmRelease{Name: "missing", Namespace: "ns"}
	s.AddObject(hr)
	s.UpdateStatus(hr.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"})
	rep := runSelected(t, o, testutil.KustomizationID("", hr.Name))
	assert.FatalIf(
		t,
		rep.Matched != 0 || rep.Failed != 1 || len(rep.Cases) != 1 || rep.Cases[0].ID != hr.Named(),
		"same-name diagnostic must not count as a requested root: %+v",
		rep,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BlockedRootReason(t *testing.T) {
	for _, tc := range []struct {
		name       string
		root       manifest.BaseManifest
		namespace  string
		wantFailed int
		chain      bool
		wantReason string
	}{
		{
			name:       "synthetic HelmChart",
			root:       &manifest.HelmChartSource{Name: "apps-demo", Namespace: "apps"},
			wantReason: "blocked by apps/apps-demo: fetch artifact: 404 Not Found",
		},
		{
			name:       "generated chart dependency chain",
			root:       &manifest.HelmChartSource{Name: "apps-demo", Namespace: "apps"},
			chain:      true,
			wantFailed: 1,
			wantReason: "blocked by flux-system/repository",
		},
		{
			name:       "filtered-out OCIRepository",
			root:       &manifest.OCIRepository{Name: "source", Namespace: "flux-system"},
			namespace:  "apps",
			wantReason: "blocked by flux-system/source: fetch artifact: 404 Not Found",
		},
		{
			name:       "reported OCIRepository",
			root:       &manifest.OCIRepository{Name: "source", Namespace: "flux-system"},
			wantFailed: 1,
			wantReason: "blocked by flux-system/source",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, s := newRunner(t)
			hr := testutil.HelmReleaseID("apps", "demo")
			root := tc.root.Named()
			s.AddObject(&manifest.HelmRelease{Name: hr.Name, Namespace: hr.Namespace})
			if tc.root.Named().Kind == manifest.KindHelmChart {
				s.RecordEmitted(
					hr,
					&store.Emission{Complete: true, Children: []store.Child{{Obj: tc.root, Generated: true}}},
					nil,
				)
			} else {
				s.AddObject(tc.root)
			}
			s.UpdateFailure(hr, &manifest.DependencyFailedError{Parent: hr, Failed: []manifest.NamedResource{root}})
			s.UpdateStatus(
				root,
				store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "fetch artifact: 404 Not Found"},
			)
			if tc.chain {
				repository := &manifest.HelmRepository{Name: "repository", Namespace: "flux-system"}
				s.AddObject(repository)
				s.UpdateStatus(
					repository.Named(),
					store.StatusInfo{Status: store.StatusFailed, Message: "fetch artifact: 404 Not Found"},
				)
				s.UpdateFailure(
					root,
					&manifest.DependencyFailedError{Parent: root, Failed: []manifest.NamedResource{repository.Named()}},
				)
			}

			rep := runSelected(t, o, testutil.HelmReleaseID(tc.namespace, ""))
			assert.FatalIf(
				t,
				rep.Matched != 1 || rep.Failed != tc.wantFailed || len(rep.Blocked) != 1 || !rep.AnyFailed(),
				"want %d failed + 1 blocked, got %+v",
				tc.wantFailed,
				rep,
			)
			want := []output.Case{{ID: hr, Outcome: output.OutcomeBlocked, Reason: tc.wantReason}}
			assert.FatalIf(t, !reflect.DeepEqual(rep.Blocked, want), "Blocked = %+v, want %+v", rep.Blocked, want)
			var b bytes.Buffer
			assert.NoError(t, writeTestReport(output.Encoder{Writer: &b}, &rep))
			{
				out := b.String()
				assert.ErrorIf(t, !strings.Contains(out, "apps/demo") ||
					strings.Count(out, "fetch artifact: 404 Not Found") != 1 ||
					strings.Contains(
						out,
						"flux error:",
					), "report must show the root error exactly once without sentinel prefixes:\n%s", out)
			}
		})
	}
}

// TestRun_BlockedByMissingDep covers a resource blocked on a dependency that was
// never loaded: its reason names the root with a "(not found)" marker and it
// counts as blocked even though no primary failure exists, keeping the run
// non-zero.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BlockedByMissingDep(t *testing.T) {
	o, s := newRunner(t)
	child := testutil.KustomizationID("volsync-system", "kopiur-repository")
	missing := testutil.KustomizationID("kopiur-system", "kopiur")
	s.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
	s.UpdateFailure(child, &manifest.DependencyFailedError{Parent: child, Failed: []manifest.NamedResource{missing}})

	rep := runSelected(t, o)
	assert.FatalIf(t, rep.Failed != 0 || len(rep.Blocked) != 1, "want 0 failed + 1 blocked, got %+v", rep)
	assert.ErrorIf(t, !rep.AnyFailed(), "%v", "a blocked-only run must still be non-zero")
	want := []output.Case{
		{ID: child, Outcome: output.OutcomeBlocked, Reason: "blocked by kopiur-system/kopiur (not found)"},
	}
	assert.FatalIf(t, !reflect.DeepEqual(rep.Blocked, want), "Blocked = %+v, want %+v", rep.Blocked, want)
	var b bytes.Buffer
	_ = writeTestReport(output.Encoder{Writer: &b}, &rep)
	assert.ErrorIf(
		t,
		!strings.Contains(b.String(), "not found"),
		"missing root should render (not found):\n%s",
		b.String(),
	)
}

// TestRun_BlockedByCycle pins that a cross-kind dependency cycle (which never
// escapes to a primary/missing root) still reports every victim: the count is
// keyed off the blocked set, not off root resolution, so a cycle can't silently
// drop a failure and flip the run green. Each victim falls back to naming its
// immediate blocker.
//
//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_BlockedByCycle(t *testing.T) {
	o, s := newRunner(t)
	ks := testutil.KustomizationID("flux-system", "ks-a")
	hr := testutil.HelmReleaseID("app", "hr-b")
	s.AddObject(&manifest.Kustomization{Name: ks.Name, Namespace: ks.Namespace})
	s.AddObject(&manifest.HelmRelease{Name: hr.Name, Namespace: hr.Namespace})
	// A ⇄ B: each blocked on the other, both terminally failed — a closed cycle.
	s.UpdateFailure(ks, &manifest.DependencyFailedError{Parent: ks, Failed: []manifest.NamedResource{hr}})
	s.UpdateFailure(hr, &manifest.DependencyFailedError{Parent: hr, Failed: []manifest.NamedResource{ks}})

	rep := runSelected(t, o)
	assert.FatalIf(
		t,
		rep.Failed != 0 || len(rep.Blocked) != 2,
		"want 0 failed + 2 blocked (neither victim dropped), got %+v",
		rep,
	)
	assert.ErrorIf(t, !rep.AnyFailed(), "%v", "a cyclic block must still flip the run to failed, not pass green")
	// Each cycle victim names its immediate blocker; both
	// blockers carry a status, so neither is tagged "(not found)".
	want := []output.Case{
		{ID: hr, Outcome: output.OutcomeBlocked, Reason: "blocked by flux-system/ks-a"},
		{ID: ks, Outcome: output.OutcomeBlocked, Reason: "blocked by app/hr-b"},
	}
	assert.FatalIf(t, !reflect.DeepEqual(rep.Blocked, want), "Blocked = %+v, want %+v", rep.Blocked, want)
}

func newRunner(t *testing.T) (*orchestrator.Orchestrator, *store.Store) {
	t.Helper()
	runs, err := orchestrator.RenderTrees(
		t.Context(),
		[]orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: t.TempDir()}},
		orchestrator.Config{Concurrency: 2},
	)
	for _, run := range runs {
		t.Cleanup(run.Result.Close)
	}
	assert.NoError(t, err)
	assert.FatalIf(t, len(runs) != 1 || runs[0].Orchestrator == nil, "%v", "missing completed store")
	o := runs[0].Orchestrator

	s := o.Store()
	return o, s
}

func runSelected(t *testing.T, o *orchestrator.Orchestrator, roots ...manifest.NamedResource) output.TestReport {
	t.Helper()
	if len(roots) == 0 {
		roots = []manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}
	}
	failed := o.Store().FailedResources()
	blocked := make(map[manifest.NamedResource][]manifest.NamedResource)
	for id, info := range failed {
		if info.IsDependencyFailure() {
			blocked[id] = info.BlockedBy()
		}
	}
	raw := &orchestrator.Result{Failed: failed, Blocked: blocked}
	t.Cleanup(raw.Close)
	projectedResults0, projectionErrors0 := orchestrator.Select(
		[]orchestrator.Rendered{{Orchestrator: o, Result: raw}},
		nil,
		roots,
		nil,
	)
	selected, _, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	assert.FatalIf(t, len(selected) != 1, "Select returned %d results, want 1", len(selected))
	return Run(selected[0])
}

func writeTestReport(e output.Encoder, rep *output.TestReport) error {
	r := output.Report{Groups: []output.ReportGroup{{Test: rep}}, Test: true}
	r.Prepare(nil)
	return e.Report(&r, e)
}
