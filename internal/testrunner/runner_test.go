package testrunner

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/report"
	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestWrite_NotesFooter(t *testing.T) {
	for _, tc := range []struct {
		name  string
		notes []report.Note
		want  string
	}{
		{name: "empty", want: "\n  ✓ 0 passed\n"},
		{
			name:  "single",
			notes: []report.Note{{Text: "resource orphaned id=x"}},
			want:  "\n  notes (1)\n    resource orphaned id=x\n\n  ✓ 0 passed\n",
		},
		{
			name:  "collapsed",
			notes: []report.Note{{Text: "resource orphaned id=x", Count: 3}, {Text: "source fetched", Count: 1}},
			want:  "\n  notes (4)\n    resource orphaned id=x (×3)\n    source fetched\n\n  ✓ 0 passed\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			if err := (Report{}).Write(&b, nil, tc.notes, false, 0); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, b.String(), tc.want)
		})
	}
}

// TestWrite_VerdictGlyphAndElapsed: the summary leads with a green ✓ when all
// pass and a red ✗ when anything fails, and shows the elapsed clock only when
// non-zero.
func TestWrite_VerdictGlyphAndElapsed(t *testing.T) {
	po, pass := newRunner(t)
	ok := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "ok"}
	pass.AddObject(&manifest.Kustomization{Name: ok.Name, Namespace: ok.Namespace})
	pass.UpdateStatus(ok, store.StatusReady, "")
	var pb bytes.Buffer
	if err := runSelected(t, po).Write(&pb, nil, nil, false, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if out := pb.String(); !strings.Contains(out, style.GlyphPass) || strings.Contains(out, style.GlyphFail) {
		t.Errorf("all-pass summary wants ✓ and no ✗:\n%s", out)
	} else if !strings.Contains(out, "1.5s") {
		t.Errorf("summary should show the elapsed clock:\n%s", out)
	}

	fo, fail := newRunner(t)
	bad := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "bad"}
	fail.AddObject(&manifest.Kustomization{Name: bad.Name, Namespace: bad.Namespace})
	fail.UpdateStatus(bad, store.StatusFailed, "boom")
	var fb bytes.Buffer
	_ = runSelected(t, fo).Write(&fb, nil, nil, false, 0)
	if out := fb.String(); !strings.Contains(out, style.GlyphFail) {
		t.Errorf("failing summary wants ✗:\n%s", out)
	} else if strings.Contains(out, "0.0s") {
		t.Errorf("elapsed=0 should be omitted, not rendered:\n%s", out)
	}
}

func TestRun_AllPass(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.AddObject(&manifest.HelmRelease{Name: "demo", Namespace: "apps"})
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}, store.StatusReady, "")
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}, store.StatusReady, "")

	rep := runSelected(t, o)
	if rep.AnyFailed() || rep.Passed != 2 {
		t.Errorf("expected 2 passed, got %+v", rep)
	}
	var b bytes.Buffer
	if err := rep.Write(&b, nil, nil, false, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(b.String(), "2 passed") {
		t.Errorf("missing summary: %s", b.String())
	}
}

func TestRun_OneFailed(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}, store.StatusFailed, "boom")

	rep := runSelected(t, o)
	if !rep.AnyFailed() {
		t.Errorf("expected failure: %+v", rep)
	}
	if rep.Cases[0].Reason != "boom" {
		t.Errorf("reason: %q", rep.Cases[0].Reason)
	}
}

func TestRun_SkippedReasonReportedAsSkipped(t *testing.T) {
	// A KS that soft-skipped (e.g. source was --allow-missing-secrets'd)
	// reports as SKIPPED, not PASSED or FAILED.
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "apps", Namespace: "flux-system"})
	s.UpdateStatus(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"},
		store.StatusReady, "skipped: source GitRepository/flux-system/sealed missing auth")

	rep := runSelected(t, o)
	if rep.AnyFailed() {
		t.Errorf("skipped-reason case must not fail: %+v", rep)
	}
	if rep.Skipped != 1 || rep.Passed != 0 {
		t.Errorf("expected 1 skipped, 0 passed; got %+v", rep)
	}
	if !strings.Contains(rep.Cases[0].Reason, "missing auth") {
		t.Errorf("skip reason should carry the underlying message; got %q", rep.Cases[0].Reason)
	}
}

func TestRun_NoStatus(t *testing.T) {
	o, s := newRunner(t)
	s.AddObject(&manifest.Kustomization{Name: "x", Namespace: "ns"})
	rep := runSelected(t, o)
	if !rep.AnyFailed() {
		t.Errorf("expected failure for no-status case")
	}
}

func TestRun_SelectedNamespace(t *testing.T) {
	o, s := newRunner(t)
	alpha := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "alpha", Name: "apps"}
	beta := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "beta", Name: "apps"}
	s.AddObject(&manifest.Kustomization{Name: alpha.Name, Namespace: alpha.Namespace})
	s.AddObject(&manifest.Kustomization{Name: beta.Name, Namespace: beta.Namespace})
	s.UpdateStatus(alpha, store.StatusReady, "")
	s.UpdateStatus(beta, store.StatusReady, "")

	rep := runSelected(t, o, manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "alpha"})
	if rep.Passed != 1 || len(rep.Cases) != 1 || rep.Cases[0].ID != alpha {
		t.Errorf("selected namespace report = %+v, want only %s", rep, alpha)
	}
}

func TestWrite_ShowsSkippedByDefault(t *testing.T) {
	o, s := newRunner(t)
	passed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "changed"}
	skipped := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "untouched"}
	s.AddObject(&manifest.Kustomization{Name: passed.Name, Namespace: passed.Namespace})
	s.AddObject(&manifest.Kustomization{Name: skipped.Name, Namespace: skipped.Namespace})
	s.UpdateStatus(passed, store.StatusReady, "")
	s.UpdateStatus(skipped, store.StatusReady, store.MsgUnchanged)

	rep := runSelected(t, o)

	var b bytes.Buffer
	if err := rep.Write(&b, nil, nil, false, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	out := b.String()

	if !strings.Contains(out, "changed") {
		t.Errorf("PASSED row must appear: %s", out)
	}
	if !strings.Contains(out, "untouched") {
		t.Errorf("skipped row must appear by default: %s", out)
	}
	if !strings.Contains(out, "‒") || !strings.Contains(out, "unchanged") {
		t.Errorf("skipped row should carry its glyph and reason: %s", out)
	}
	if !strings.Contains(out, "1 skipped") {
		t.Errorf("summary must report the skipped count: %s", out)
	}
}

func TestWrite_FailedNeverHidden(t *testing.T) {
	o, s := newRunner(t)
	failed := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "broken"}
	s.AddObject(&manifest.Kustomization{Name: failed.Name, Namespace: failed.Namespace})
	s.UpdateStatus(failed, store.StatusFailed, "boom")

	rep := runSelected(t, o)
	var b bytes.Buffer
	if err := rep.Write(&b, nil, nil, false, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if out := b.String(); !strings.Contains(out, "✗") || !strings.Contains(out, "broken") {
		t.Errorf("failed row must appear by default: %s", out)
	}
}

// TestWrite_Color: color=true emits ANSI codes (green pass, red fail); color=
// false renders none, so piped / NO_COLOR output stays plain.
func TestWrite_Color(t *testing.T) {
	o, s := newRunner(t)
	pass := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "ok"}
	fail := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "bad"}
	s.AddObject(&manifest.Kustomization{Name: pass.Name, Namespace: pass.Namespace})
	s.AddObject(&manifest.HelmRelease{Name: fail.Name, Namespace: fail.Namespace})
	s.UpdateStatus(pass, store.StatusReady, "")
	s.UpdateStatus(fail, store.StatusFailed, "boom")
	rep := runSelected(t, o)

	var colored, plain bytes.Buffer
	if err := rep.Write(&colored, nil, nil, true, 0); err != nil {
		t.Fatalf("Write(color): %v", err)
	}
	if err := rep.Write(&plain, nil, nil, false, 0); err != nil {
		t.Fatalf("Write(plain): %v", err)
	}
	if !strings.Contains(colored.String(), "\x1b") {
		t.Errorf("colored output emitted no ANSI: %q", colored.String())
	}
	if strings.Contains(plain.String(), "\x1b") {
		t.Errorf("plain output leaked an escape code: %q", plain.String())
	}
}

func TestRun_DeterministicOrderAndMatchCount(t *testing.T) {
	o, s := newRunner(t)
	for _, id := range []manifest.NamedResource{
		{Kind: manifest.KindKustomization, Namespace: "z", Name: "last"},
		{Kind: manifest.KindKustomization, Namespace: "a", Name: "first"},
		{Kind: manifest.KindKustomization, Namespace: "m", Name: "middle"},
	} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		s.UpdateStatus(id, store.StatusReady, "")
	}
	for _, name := range []string{"extra-z", "extra-a"} {
		rs := &manifest.ResourceSet{Name: name, Namespace: "ns"}
		s.AddObject(rs)
		s.UpdateStatus(rs.Named(), store.StatusFailed, "boom")
	}

	rep := runSelected(t, o, manifest.NamedResource{Kind: manifest.KindKustomization})

	if rep.Matched != 3 {
		t.Fatalf("Matched = %d, want 3", rep.Matched)
	}
	got := []manifest.NamedResource{rep.Cases[0].ID, rep.Cases[1].ID, rep.Cases[2].ID}
	want := []manifest.NamedResource{
		{Kind: manifest.KindKustomization, Namespace: "a", Name: "first"},
		{Kind: manifest.KindKustomization, Namespace: "m", Name: "middle"},
		{Kind: manifest.KindKustomization, Namespace: "z", Name: "last"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("case order = %v, want %v", got, want)
		}
	}
	if len(rep.Cases) != 5 || rep.Cases[3].ID.Name != "extra-a" || rep.Cases[4].ID.Name != "extra-z" {
		t.Fatalf("diagnostic cases must follow the roster in identity order: %+v", rep.Cases)
	}
}

func TestWrite_ReturnsWriterError(t *testing.T) {
	want := errors.New("write failed")
	rep := Report{Cases: []Case{{ID: manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "apps"}}}}

	if err := rep.Write(errWriter{err: want}, nil, nil, false, 0); !errors.Is(err, want) {
		t.Fatalf("Write error = %v, want %v", err, want)
	}
}

type errWriter struct {
	err error
}

func (w errWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

// TestRun_BlockedByPrimaryFailure pins the cascade: a failure with recorded
// blockers classifies as Blocked (not Failed), is kept out of the roster Cases,
// and lands in Blocked as a "blocked by <root>" case naming the primary failure
// it traces to — the verdict counting it as blocked, the run still non-zero.
func TestRun_BlockedByPrimaryFailure(t *testing.T) {
	o, s := newRunner(t)
	root := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "media", Name: "plex"}
	s.AddObject(&manifest.Kustomization{Name: root.Name, Namespace: root.Namespace})
	s.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
	s.UpdateStatus(root, store.StatusFailed, "kustomize build boom") // primary
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
	want := []Case{{ID: child, Outcome: OutcomeBlocked, Reason: "blocked by flux-system/cluster-apps"}}
	if !reflect.DeepEqual(rep.Blocked, want) {
		t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
	}

	var b bytes.Buffer
	_ = rep.Write(&b, nil, nil, false, 0)
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
	ks := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	rs := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "flux-system", Name: "broken-rs"}
	s.AddObject(&manifest.Kustomization{Name: ks.Name, Namespace: ks.Namespace})
	s.AddObject(&manifest.ResourceSet{Name: rs.Name, Namespace: rs.Namespace})
	s.UpdateStatus(ks, store.StatusReady, "")
	s.UpdateStatus(rs, store.StatusFailed, `parse template: function "nope" not defined`)

	rep := runSelected(t, o)
	if rep.Matched != 1 || rep.Passed != 1 || rep.Failed != 1 {
		t.Fatalf("want 1 matched, 1 passed (KS) + 1 failed (ResourceSet), got %+v", rep)
	}
	var found *Case
	for i := range rep.Cases {
		if rep.Cases[i].ID == rs {
			found = &rep.Cases[i]
		}
	}
	if found == nil {
		t.Fatalf("ResourceSet failure not surfaced as a case: %+v", rep.Cases)
	}
	if found.Outcome != OutcomeFailed || !strings.Contains(found.Reason, "nope") {
		t.Errorf("ResourceSet case = %+v, want failed with the parse error", found)
	}
	var b bytes.Buffer
	_ = rep.Write(&b, nil, nil, false, 0)
	if out := b.String(); !strings.Contains(out, "broken-rs") || !strings.Contains(out, "nope") {
		t.Errorf("roster must show the non-roster failure inline:\n%s", out)
	}
}

func TestRun_SameNameDiagnosticDoesNotMatchRequestedKind(t *testing.T) {
	o, s := newRunner(t)
	hr := &manifest.HelmRelease{Name: "missing", Namespace: "ns"}
	s.AddObject(hr)
	s.UpdateStatus(hr.Named(), store.StatusFailed, "boom")
	rep := runSelected(t, o, manifest.NamedResource{Kind: manifest.KindKustomization, Name: hr.Name})
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
			hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "demo"}
			root := tc.root.Named()
			s.AddObject(&manifest.HelmRelease{Name: hr.Name, Namespace: hr.Namespace})
			s.AddObject(tc.root)
			s.UpdateFailure(hr, &manifest.DependencyFailedError{Parent: hr, Failed: []manifest.NamedResource{root}})
			s.UpdateStatus(root, store.StatusFailed, "flux error: input error: fetch artifact: 404 Not Found")

			rep := runSelected(t, o, manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: tc.namespace})
			if rep.Matched != 1 || rep.Failed != tc.wantFailed || len(rep.Blocked) != 1 || !rep.AnyFailed() {
				t.Fatalf("want %d failed + 1 blocked, got %+v", tc.wantFailed, rep)
			}
			want := []Case{{ID: hr, Outcome: OutcomeBlocked, Reason: tc.wantReason}}
			if !reflect.DeepEqual(rep.Blocked, want) {
				t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
			}
			var b bytes.Buffer
			if err := rep.Write(&b, nil, nil, false, 0); err != nil {
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
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "volsync-system", Name: "kopiur-repository"}
	missing := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "kopiur-system", Name: "kopiur"}
	s.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
	s.UpdateFailure(child, &manifest.DependencyFailedError{Parent: child, Failed: []manifest.NamedResource{missing}})

	rep := runSelected(t, o)
	if rep.Failed != 0 || len(rep.Blocked) != 1 {
		t.Fatalf("want 0 failed + 1 blocked, got %+v", rep)
	}
	if !rep.AnyFailed() {
		t.Error("a blocked-only run must still be non-zero")
	}
	want := []Case{{ID: child, Outcome: OutcomeBlocked, Reason: "blocked by kopiur-system/kopiur (not found)"}}
	if !reflect.DeepEqual(rep.Blocked, want) {
		t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
	}
	var b bytes.Buffer
	_ = rep.Write(&b, nil, nil, false, 0)
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
	ks := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "ks-a"}
	hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "app", Name: "hr-b"}
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
	want := []Case{
		{ID: hr, Outcome: OutcomeBlocked, Reason: "blocked by flux-system/ks-a"},
		{ID: ks, Outcome: OutcomeBlocked, Reason: "blocked by app/hr-b"},
	}
	if !reflect.DeepEqual(rep.Blocked, want) {
		t.Fatalf("Blocked = %+v, want %+v", rep.Blocked, want)
	}
}

func newRunner(t *testing.T) (*orchestrator.Orchestrator, *store.Store) {
	t.Helper()
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Stop)
	return o, o.Store()
}

func runSelected(t *testing.T, o *orchestrator.Orchestrator, roots ...manifest.NamedResource) Report {
	t.Helper()
	if len(roots) == 0 {
		roots = []manifest.NamedResource{{Kind: manifest.KindKustomization}, {Kind: manifest.KindHelmRelease}}
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
