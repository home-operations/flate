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

func TestRun_AllPass(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.AddObject(&manifest.HelmRelease{Name: "demo", Namespace: "apps"})
	s.UpdateStatus(testutil.KustomizationID("flux-system", "apps"), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(testutil.HelmReleaseID("apps", "demo"), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	raw := &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: "foreign.example/v1", Namespace: "apps", Name: "raw"}
	s.AddObject(raw)

	rep := runSelected(t, o)
	if rep.AnyFailed() || rep.Passed != 2 {
		t.Errorf("expected 2 passed, got %+v", rep)
	}
	var b bytes.Buffer
	if err := (output.Encoder{Writer: &b}).Test(rep, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(b.String(), "2 passed") {
		t.Errorf("missing summary: %s", b.String())
	}
}

func TestRun_OneFailed(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.UpdateStatus(testutil.KustomizationID("flux-system", "apps"), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"})

	rep := runSelected(t, o)
	if !rep.AnyFailed() {
		t.Errorf("expected failure: %+v", rep)
	}
	if rep.Cases[0].Reason != "boom" {
		t.Errorf("reason: %q", rep.Cases[0].Reason)
	}
}

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
			s.UpdateStatus(ks.Named(), store.StatusInfo{Status: store.StatusReady, Reason: tc.reason, Message: tc.message})
			rep := runSelected(t, o)
			if rep.AnyFailed() || rep.Matched != 1 || len(rep.Cases) != 1 || (rep.Skipped == 1) != tc.skipped || (rep.Passed == 1) == tc.skipped {
				t.Fatalf("unexpected typed outcome report: %+v", rep)
			}
			assert.Equal(t, rep.Cases[0].Reason, tc.want)
			assert.Diff(t, runSelected(t, o), rep)
		})
	}
}

func TestRun_NoStatus(t *testing.T) {
	o, s := newRunner(t)
	obj := &manifest.Kustomization{Name: "x", Namespace: "ns"}
	s.AddObject(obj)
	rep := runSelected(t, o)
	if !rep.AnyFailed() || rep.Matched != 1 || rep.Failed != 1 || len(rep.Cases) != 1 {
		t.Fatalf("expected one failure for no-status case: %+v", rep)
	}
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
			if rep.Matched != 0 || rep.Failed != 0 || len(rep.Cases) != 0 {
				t.Fatalf("object without prepared reporting status entered the report: %+v", rep)
			}
		})
	}

}

func TestRun_SelectedNamespace(t *testing.T) {
	o, s := newRunner(t)
	alpha := testutil.KustomizationID("alpha", "apps")
	beta := testutil.KustomizationID("beta", "apps")
	s.AddObject(&manifest.Kustomization{Name: alpha.Name, Namespace: alpha.Namespace})
	s.AddObject(&manifest.Kustomization{Name: beta.Name, Namespace: beta.Namespace})
	s.UpdateStatus(alpha, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(beta, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})

	rep := runSelected(t, o, testutil.KustomizationID("alpha", ""))
	if rep.Passed != 1 || len(rep.Cases) != 1 || rep.Cases[0].ID != alpha {
		t.Errorf("selected namespace report = %+v, want only %s", rep, alpha)
	}
}

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

	foreign := manifest.NamedResource{Group: "example.test", Kind: manifest.KindResourceSet, Namespace: "ns", Name: "extra-z"}
	s.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: foreign.Kind, Namespace: foreign.Namespace, Name: foreign.Name})
	s.UpdateStatus(foreign, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "foreign failure"})

	rep := runSelected(t, o, testutil.KustomizationID("", ""))

	if rep.Matched != 3 {
		t.Fatalf("Matched = %d, want 3", rep.Matched)
	}
	got := []manifest.NamedResource{rep.Cases[0].ID, rep.Cases[1].ID, rep.Cases[2].ID}
	want := []manifest.NamedResource{
		testutil.KustomizationID("a", "first"),
		testutil.KustomizationID("m", "middle"),
		testutil.KustomizationID("z", "last"),
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("case order = %v, want %v", got, want)
		}
	}
	if len(rep.Cases) != 6 || rep.Failed != 3 || rep.Cases[3].ID != foreign || rep.Cases[4].ID.Name != "extra-a" || rep.Cases[5].ID.Name != "extra-z" {
		t.Fatalf("diagnostic cases must follow the roster in identity order: %+v", rep.Cases)
	}
}

// TestRun_BlockedByPrimaryFailure pins the cascade: a failure with recorded
// blockers classifies as Blocked (not Failed), is kept out of the roster Cases,
// and lands in Blocked as a "blocked by <root>" case naming the primary failure
// it traces to — the verdict counting it as blocked, the run still non-zero.
func TestRun_BlockedByPrimaryFailure(t *testing.T) {
	o, s := newRunner(t)
	root := testutil.KustomizationID("flux-system", "cluster-apps")
	child := testutil.KustomizationID("media", "plex")
	s.AddObject(&manifest.Kustomization{Name: root.Name, Namespace: root.Namespace})
	s.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
	s.UpdateStatus(root, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "kustomize build boom"}) // primary
	s.UpdateFailure(child, &manifest.DependencyFailedError{Parent: child, Failed: []manifest.NamedResource{root}})

	rep := runSelected(t, o)
	if rep.Failed != 1 || len(rep.Blocked) != 1 {
		t.Fatalf("want 1 failed (root) + 1 blocked (child), got %+v", rep)
	}
	if !rep.AnyFailed() {
		t.Error("a blocked cascade must flip the run to failed")
	}
	for _, c := range rep.Cases {
		if c.ID == child {
			t.Errorf("blocked child must not get a roster Cases row: %+v", c)
		}
	}
	// The root is a primary failure on its own row, so it carries no "(not found)".
	want := []output.Case{{ID: child, Outcome: output.OutcomeBlocked, Reason: "blocked by flux-system/cluster-apps"}}
	if !reflect.DeepEqual(rep.Blocked, want) {
		t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
	}

	var b bytes.Buffer
	_ = (output.Encoder{Writer: &b}).Test(rep, 0)
	out := b.String()
	if !strings.Contains(out, "1 failed") || !strings.Contains(out, "1 blocked") {
		t.Errorf("verdict should separate failed and blocked:\n%s", out)
	}
	// The blocked child gets its own row naming the root cause, so the user can
	// see WHAT is blocked and WHY without it counting as a primary failure.
	if !strings.Contains(out, "media/plex") || !strings.Contains(out, "blocked by flux-system/cluster-apps") {
		t.Errorf("blocked child should render naming its root:\n%s", out)
	}
}

// TestRun_SurfacesNonRosterFailure pins the consolidation contract: a run-time
// failure whose kind isn't in the roster (here a ResourceSet whose template
// won't parse) must still appear as a failed row and count in the verdict.
// Diagnostic failures outside the roster do not count as matched roots.
func TestRun_SurfacesNonRosterFailure(t *testing.T) {
	o, s := newRunner(t)
	ks := testutil.KustomizationID("flux-system", "apps")
	rs := manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "broken-rs"}
	s.AddObject(&manifest.Kustomization{Name: ks.Name, Namespace: ks.Namespace})
	s.AddObject(&manifest.ResourceSet{Name: rs.Name, Namespace: rs.Namespace})
	s.UpdateStatus(ks, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(rs, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: `parse template: function "nope" not defined`})

	chart := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "flux-system", Name: "same"}
	s.AddObject(&manifest.HelmChartSource{Name: chart.Name, Namespace: chart.Namespace})
	s.UpdateStatus(chart, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "synthetic chart failure"})
	foreignChart := chart
	foreignChart.Group = "example.test"
	s.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: foreignChart.Kind, Namespace: foreignChart.Namespace, Name: foreignChart.Name})
	s.UpdateStatus(foreignChart, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "foreign chart failure"})

	rep := runSelected(t, o)
	if rep.Matched != 1 || rep.Passed != 1 || rep.Failed != 2 {
		t.Fatalf("want 1 matched, 1 passed (KS) + 2 diagnostic failures, got %+v", rep)
	}
	foreignFound := false
	var found *output.Case
	for i := range rep.Cases {
		if rep.Cases[i].ID == chart {
			t.Error("synthetic real chart failure entered diagnostic cases")
		}
		if rep.Cases[i].ID == foreignChart {
			foreignFound = rep.Cases[i].Reason == "foreign chart failure"
		}
		if rep.Cases[i].ID == rs {
			found = &rep.Cases[i]
		}
	}
	if !foreignFound {
		t.Errorf("foreign chart failure hidden by kind-only exemption: %+v", rep.Cases)
	}
	if found == nil {
		t.Fatalf("ResourceSet failure not surfaced as a case: %+v", rep.Cases)
	}
	if found.Outcome != output.OutcomeFailed || !strings.Contains(found.Reason, "nope") {
		t.Errorf("ResourceSet case = %+v, want failed with the parse error", found)
	}
	var b bytes.Buffer
	_ = (output.Encoder{Writer: &b}).Test(rep, 0)
	if out := b.String(); !strings.Contains(out, "broken-rs") || !strings.Contains(out, "nope") {
		t.Errorf("roster must show the non-roster failure inline:\n%s", out)
	}

	explicit := runSelected(t, o, manifest.NamedResource{Group: chart.Group, Kind: chart.Kind, Name: chart.Name})
	if explicit.Matched != 1 || len(explicit.Cases) != 2 || explicit.Cases[0].ID != chart || explicit.Cases[0].Reason != "synthetic chart failure" {
		t.Fatalf("explicit chart must retain its selected row before the foreign diagnostic: %+v", explicit)
	}
}

func TestRun_SameNameDiagnosticDoesNotMatchRequestedKind(t *testing.T) {
	o, s := newRunner(t)
	hr := &manifest.HelmRelease{Name: "missing", Namespace: "ns"}
	s.AddObject(hr)
	s.UpdateStatus(hr.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"})
	rep := runSelected(t, o, testutil.KustomizationID("", hr.Name))
	if rep.Matched != 0 || rep.Failed != 1 || len(rep.Cases) != 1 || rep.Cases[0].ID != hr.Named() {
		t.Fatalf("same-name diagnostic must not count as a requested root: %+v", rep)
	}
}

func TestRun_BlockedRootReason(t *testing.T) {
	for _, tc := range []struct {
		name       string
		root       manifest.BaseManifest
		namespace  string
		wantFailed int
		wantReason string
	}{
		{
			name:       "synthetic HelmChart",
			root:       &manifest.HelmChartSource{Name: "chart-16d855d", Namespace: "apps"},
			wantReason: "blocked by apps/chart-16d855d: fetch artifact: 404 Not Found",
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
			s.AddObject(tc.root)
			s.UpdateFailure(hr, &manifest.DependencyFailedError{Parent: hr, Failed: []manifest.NamedResource{root}})
			s.UpdateStatus(root, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "flux error: input error: fetch artifact: 404 Not Found"})

			rep := runSelected(t, o, testutil.HelmReleaseID(tc.namespace, ""))
			if rep.Matched != 1 || rep.Failed != tc.wantFailed || len(rep.Blocked) != 1 || !rep.AnyFailed() {
				t.Fatalf("want %d failed + 1 blocked, got %+v", tc.wantFailed, rep)
			}
			want := []output.Case{{ID: hr, Outcome: output.OutcomeBlocked, Reason: tc.wantReason}}
			if !reflect.DeepEqual(rep.Blocked, want) {
				t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
			}
			var b bytes.Buffer
			if err := (output.Encoder{Writer: &b}).Test(rep, 0); err != nil {
				t.Fatal(err)
			}
			if out := b.String(); !strings.Contains(out, "apps/demo") || strings.Count(out, "fetch artifact: 404 Not Found") != 1 || strings.Contains(out, "flux error:") {
				t.Errorf("report must show the root error exactly once without sentinel prefixes:\n%s", out)
			}
		})
	}
}

// TestRun_BlockedByMissingDep covers a resource blocked on a dependency that was
// never loaded: its reason names the root with a "(not found)" marker and it
// counts as blocked even though no primary failure exists, keeping the run
// non-zero.
func TestRun_BlockedByMissingDep(t *testing.T) {
	o, s := newRunner(t)
	child := testutil.KustomizationID("volsync-system", "kopiur-repository")
	missing := testutil.KustomizationID("kopiur-system", "kopiur")
	s.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
	s.UpdateFailure(child, &manifest.DependencyFailedError{Parent: child, Failed: []manifest.NamedResource{missing}})

	rep := runSelected(t, o)
	if rep.Failed != 0 || len(rep.Blocked) != 1 {
		t.Fatalf("want 0 failed + 1 blocked, got %+v", rep)
	}
	if !rep.AnyFailed() {
		t.Error("a blocked-only run must still be non-zero")
	}
	want := []output.Case{{ID: child, Outcome: output.OutcomeBlocked, Reason: "blocked by kopiur-system/kopiur (not found)"}}
	if !reflect.DeepEqual(rep.Blocked, want) {
		t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
	}
	var b bytes.Buffer
	_ = (output.Encoder{Writer: &b}).Test(rep, 0)
	if !strings.Contains(b.String(), "not found") {
		t.Errorf("missing root should render (not found):\n%s", b.String())
	}
}

// TestRun_BlockedByCycle pins that a cross-kind dependency cycle (which never
// escapes to a primary/missing root) still reports every victim: the count is
// keyed off the blocked set, not off root resolution, so a cycle can't silently
// drop a failure and flip the run green. Each victim falls back to naming its
// immediate blocker.
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
	if rep.Failed != 0 || len(rep.Blocked) != 2 {
		t.Fatalf("want 0 failed + 2 blocked (neither victim dropped), got %+v", rep)
	}
	if !rep.AnyFailed() {
		t.Error("a cyclic block must still flip the run to failed, not pass green")
	}
	// Each cycle victim names its immediate blocker; both
	// blockers carry a status, so neither is tagged "(not found)".
	want := []output.Case{
		{ID: hr, Outcome: output.OutcomeBlocked, Reason: "blocked by flux-system/ks-a"},
		{ID: ks, Outcome: output.OutcomeBlocked, Reason: "blocked by app/hr-b"},
	}
	if !reflect.DeepEqual(rep.Blocked, want) {
		t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
	}
}

func newRunner(t *testing.T) (*orchestrator.Orchestrator, *store.Store) {
	t.Helper()
	o, err := orchestrator.New(t.Context(), orchestrator.Config{Path: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return o, o.Store()
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
	selected, _, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: raw}}, nil, roots, nil)
	if len(selected) != 1 {
		t.Fatalf("Select returned %d results, want 1", len(selected))
	}
	return Run(selected[0])
}
