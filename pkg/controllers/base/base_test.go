package base_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestExecution_Success(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
		func(_ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
			if obj.Name != "app" {
				t.Errorf("re-read got %q, want app", obj.Name)
			}
			return nil
		},
	)
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusReady {
		t.Errorf("status = %v, want Ready", got.Status)
	}
}

func TestExecution_RetiredSuccessfulOutput(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}}}
	c := base.New(s, nil)
	c.Configure(base.Options{Filter: change.NewFilter(change.NewSet([]string{"parent.yaml"}), index, s), Index: index})

	publish := func(value string) *base.Execution {
		execution := controllertest.NewExecution(c, parent.Named())
		execution.Dispatch(t.Context(), manifest.DrainNone, func(_ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
			_, next := execution.Reads.GetEmission(parent.Named())
			defer next.Finish()
			next.Children = []store.Child{{Obj: &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": value}}}}
			next.Complete = true
			execution.Emit(parent.Named(), &next)
			return nil
		})
		return execution
	}
	first := publish("first")
	if !first.Valid() {
		t.Fatal("initial output was invalidated by its publication")
	}
	first.Close()
	for range s.Sweep() {
	}
	second := publish("second")
	defer second.Close()
	if !second.Valid() {
		t.Fatal("successful predecessor output became a semantic input to its replacement")
	}
	if info, _ := s.GetStatus(parent.Named()); info.Status != store.StatusReady {
		t.Fatalf("replacement status=%+v, want Ready", info)
	}
}

func TestExecution_PreflightWithdrawsPublishedOutput(t *testing.T) {
	t.Parallel()
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprintf("source=%t", source), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			c := base.New(st, tasks)

			var obj manifest.BaseManifest = &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			if source {
				obj = &manifest.GitRepository{Name: "producer", Namespace: "apps"}
			}
			id := obj.Named()
			child := &manifest.ConfigMap{Name: "output", Namespace: "apps", Data: map[string]any{"value": "rendered"}}
			st.AddObject(obj)
			var attempts, renders atomic.Int64
			var latest *base.Execution
			d := controllertest.Dispatch{Tasks: tasks, Store: st, ID: id, ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
				attempts.Add(1)
				execution := controllertest.NewExecution(c, id)
				latest = execution
				execution.Dispatch(ctx, level, func(_ context.Context, _ manifest.BaseManifest, _ manifest.DrainLevel) error {
					renders.Add(1)
					if source {
						st.SetArtifact(id, &store.SourceArtifact{Reads: &execution.Reads, Key: "content", LocalPath: "source"})
					} else {
						_, next := execution.Reads.GetEmission(id)
						defer next.Finish()
						next.Children = []store.Child{{Obj: child}}
						next.Complete = true
						execution.Emit(id, &next)
					}
					return nil
				})
				return execution
			}}
			if info := controllertest.DispatchToFixpoint(t.Context(), t, d); info.Status != store.StatusReady || attempts.Load() != 1 || renders.Load() != 1 {
				t.Fatalf("initial status=%+v attempts=%d renders=%d", info, attempts.Load(), renders.Load())
			}
			if st.GetArtifact(id) == nil || !source && st.GetObject(child.Named()) == nil {
				t.Fatal("initial output was not published")
			}
			const cycle = "dependency cycle detected"
			st.SetPreflight(id, cycle)
			attempts.Store(0)
			info := controllertest.DispatchToFixpoint(t.Context(), t, d)
			if info.Status != store.StatusFailed || manifest.TrimSentinelPrefix(info.Message) != cycle || !latest.Valid() || latest.Outcome() != schedule.Terminal || attempts.Load() != 1 || renders.Load() != 1 {
				t.Errorf("cycle status=%+v valid=%t outcome=%v attempts=%d renders=%d, want terminal failure in one attempt without rendering", info, latest.Valid(), latest.Outcome(), attempts.Load(), renders.Load())
			}
			if source {
				if artifact := st.GetArtifact(id); artifact != nil {
					t.Errorf("stale source artifact remained visible: %+v", artifact)
				}
			} else {
				if st.GetObject(child.Named()) != nil {
					t.Error("stale emitted child remained visible")
				}
				output, ok := st.GetArtifact(id).(*store.Emission)
				if !ok || !output.Complete || output.OutputReads != &latest.Reads || !output.OutputReads.Valid() || len(output.Children) != 0 {
					t.Errorf("withdrawal did not publish a complete current inventory: %+v", output)
				}
			}
			st.SetPreflight(id, "")
			attempts.Store(0)
			info = controllertest.DispatchToFixpoint(t.Context(), t, d)
			if info.Status != store.StatusReady || info.Message != "" || !latest.Valid() || attempts.Load() != 1 || renders.Load() != 2 {
				t.Fatalf("recovered status=%+v valid=%t attempts=%d renders=%d", info, latest.Valid(), attempts.Load(), renders.Load())
			}
			if st.GetArtifact(id) == nil || !source && st.GetObject(child.Named()) == nil {
				t.Fatal("recovery did not republish output")
			}
			t.Logf("recovery attempts=%d renders=%d", attempts.Load(), renders.Load())
		})
	}
}

func TestExecution_Failure(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
		func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			return errors.New("render failed")
		},
	)
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if got.Message != "render failed" {
		t.Errorf("message = %q, want %q", got.Message, "render failed")
	}
}

func TestExecution_Panic(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	// Contract: panic is converted into StatusFailed AND re-raised
	// so the enclosing task.Service.Go's recover increments failures
	// (Service.Failures() must count panicked reconciles). Recover
	// the re-raise at the test boundary and assert both pieces.
	var rec any
	func() {
		defer func() { rec = recover() }()
		controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
			func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				panic("kaboom")
			},
		)
	}()
	if rec == nil {
		t.Fatal("expected panic to be re-raised so task.Service.Go's recover counts it; got nil")
	}
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if !strings.Contains(got.Message, "panic:") || !strings.Contains(got.Message, "kaboom") {
		t.Errorf("message = %q, want a 'panic: kaboom' summary", got.Message)
	}
}

func TestExecution_MissingObject(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := testutil.HelmReleaseID("ns", "ghost")
	called := false
	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
		func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			called = true
			return nil
		},
	)
	if called {
		t.Error("fn ran for a missing object; expected silent no-op")
	}
	if _, ok := s.GetStatus(id); ok {
		t.Error("missing object should not get a status entry")
	}
	s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "re-fetching"})
	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
		func(context.Context, *manifest.HelmRelease, manifest.DrainLevel) error {
			t.Error("missing object reconciled")
			return nil
		})
	got, _ := s.GetStatus(id)
	if got.Outcome() != store.OutcomeSkipped || got.Message != "object not in store at reconcile time" {
		t.Fatalf("missing-object status = %+v", got)
	}
}

func TestExecution_PreservesInformativeReadyMessage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		reason  store.StatusReason
		message string
	}{
		{name: "skipped", reason: store.ReasonSkipped, message: "missing secret"},
		{name: "empty skip", reason: store.ReasonSkipped},
		{name: "empty unchanged", reason: store.ReasonUnchanged},
		{name: "empty suspended", reason: store.ReasonSuspended},
		{name: "informative ready", message: "skipped: a literal explanation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			s.AddObject(hr)
			id := hr.Named()
			s.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: tc.reason, Message: tc.message})
			before, _ := s.GetStatus(id)
			controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
				func(context.Context, *manifest.HelmRelease, manifest.DrainLevel) error { return nil })
			got, _ := s.GetStatus(id)
			if got != before {
				t.Fatalf("Ready snapshot changed: %+v, want %+v", got, before)
			}
		})
	}
	for _, tc := range []struct {
		name          string
		reason        store.StatusReason
		message, want string
	}{
		{name: "nested skip", reason: store.ReasonSkipped, message: "skipped: missing auth", want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped: skipped: missing auth"},
		{name: "boundary whitespace", reason: store.ReasonSkipped, message: "  missing auth \n", want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:   missing auth"},
		{name: "empty explanation", reason: store.ReasonSkipped, want: "source OCIRepository.source.toolkit.fluxcd.io/ns/source skipped:"},
		{name: "misleading message", message: "skipped: missing auth"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			s.AddObject(hr)
			c := base.New(s, nil)
			src := &manifest.OCIRepository{Name: "source", Namespace: "ns"}
			s.AddObject(src)
			s.SetArtifact(src.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
			s.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: tc.reason, Message: tc.message})
			execution := controllertest.NewExecution(c, hr.Named())
			execution.Dispatch(t.Context(), manifest.DrainNone,
				func(context.Context, *manifest.HelmRelease, manifest.DrainLevel) error {
					return execution.Require(t.Context(), []manifest.DependencyRef{{NamedResource: src.Named(), Artifact: true}}, manifest.DrainNone)
				})
			got, _ := s.GetStatus(hr.Named())
			want := store.OutcomePassed
			if tc.reason == store.ReasonSkipped {
				want = store.OutcomeSkipped
			}
			if got.Outcome() != want || got.Message != tc.want {
				t.Fatalf("propagated status = %+v, want %s/%q", got, want, tc.want)
			}
		})
	}
	for _, message := range []string{" \t missing auth \n", " skipped: missing auth "} {
		t.Run(fmt.Sprintf("direct/%q", message), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			hr := &manifest.HelmRelease{Name: "app"}
			s.AddObject(hr)
			controllertest.NewExecution(base.New(s, nil), hr.Named()).Dispatch(t.Context(), manifest.DrainNone,
				func(context.Context, *manifest.HelmRelease, manifest.DrainLevel) error {
					return fmt.Errorf("%w: %s", manifest.ErrSourceSkipped, message)
				})
			got, _ := s.GetStatus(hr.Named())
			if got.Outcome() != store.OutcomeSkipped || got.Message != strings.TrimSpace(message) {
				t.Fatalf("direct skip = %+v", got)
			}
		})
	}
}

func TestExecution_PreservesExternalFailure(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	id := hr.Named()

	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone,
		func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			s.UpdateStatus(id, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "dependency cycle detected"})
			return nil
		},
	)
	got, _ := s.GetStatus(id)
	if got.Status != store.StatusFailed {
		t.Errorf("status = %v, want Failed", got.Status)
	}
	if got.Message != "dependency cycle detected" {
		t.Errorf("external failure message was clobbered: %q", got.Message)
	}
}

func TestExecution_Dispatch(t *testing.T) {
	t.Parallel()
	s := store.New()
	ts := task.NewBounded(0)
	c := base.New(s, ts)
	c.Configure(base.Options{PreflightFailure: func(id manifest.NamedResource) (string, bool) {
		return "synthetic cycle", id.Name == "preflightfail"
	}})
	c.StartLifecycle()
	t.Cleanup(func() { ts.BlockTillDone() })

	var ran atomic.Int64
	reconcile := func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error { ran.Add(1); return nil }
	dispatch := func(id manifest.NamedResource) {
		controllertest.NewExecution(c, id).Dispatch(t.Context(), manifest.DrainNone, reconcile)
	}

	// Suspended → preGate bails to Ready/"suspended", no reconcile.
	susHR := &manifest.HelmRelease{Name: "suspended", Namespace: "ns", Suspend: true}
	s.AddObject(susHR)
	dispatch(susHR.Named())
	if info, _ := s.GetStatus(susHR.Named()); info.Outcome() != store.OutcomeSuspended || info.Message != "suspended" {
		t.Errorf("suspended HR status = %+v; want Ready/suspended via preGate", info)
	}

	// Preflight-failed → Failed, no reconcile.
	pfHR := &manifest.HelmRelease{Name: "preflightfail", Namespace: "ns"}
	s.AddObject(pfHR)
	dispatch(pfHR.Named())
	if info, _ := s.GetStatus(pfHR.Named()); info.Status != store.StatusFailed {
		t.Errorf("preflight-failed HR status = %+v; want Failed", info)
	}

	// Clean → reconcile runs → Ready.
	hr := &manifest.HelmRelease{Name: "app", Namespace: "ns"}
	s.AddObject(hr)
	dispatch(hr.Named())
	if info, _ := s.GetStatus(hr.Named()); info.Status != store.StatusReady {
		t.Errorf("clean HR status = %+v; want Ready", info)
	}

	if got := ran.Load(); got != 1 {
		t.Errorf("reconcile ran %d times; want 1 (only the clean, preflight-clean HR)", got)
	}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	chart.Suspend = true
	for _, tc := range []struct {
		name      string
		obj       manifest.BaseManifest
		filtered  bool
		preflight bool
		message   string
		outcome   store.Outcome
		run       bool
	}{
		{"filtered", &manifest.HelmRelease{Name: "active"}, true, false, "unchanged", store.OutcomeUnchanged, false},
		{"suspended before filter", &manifest.HelmRelease{Name: "suspended", Suspend: true}, true, false, "suspended", store.OutcomeSuspended, false},
		{"suspended before preflight", &manifest.HelmRelease{Name: "suspended", Suspend: true}, false, true, "suspended", store.OutcomeSuspended, false},
		{"suspended chart before filter", chart, true, false, "suspended", store.OutcomeSuspended, false},
		{"suspended chart before preflight", chart, false, true, "suspended", store.OutcomeSuspended, false},
		{"active chart", &manifest.HelmChartSource{Name: "active-chart"}, false, false, "", store.OutcomePassed, true},
		{"suspended repository before filter", &manifest.HelmRepository{Name: "repo", Suspend: true}, true, false, "suspended", store.OutcomeSuspended, false},
		{"suspended repository before preflight", &manifest.HelmRepository{Name: "repo", Suspend: true}, false, true, "suspended", store.OutcomeSuspended, false},
		{"active repository", &manifest.HelmRepository{Name: "repo"}, false, false, "", store.OutcomePassed, true},
		{"data", &manifest.ConfigMap{Name: "data"}, false, false, "", store.OutcomePassed, true},
		{"secret", &manifest.Secret{Name: "secret"}, false, false, "", store.OutcomePassed, true},
		{"raw object", &manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "raw", Spec: map[string]any{"suspend": true}}, false, false, "", store.OutcomePassed, true},
		{"input provider", &manifest.ResourceSetInputProvider{Name: "provider"}, false, false, "", store.OutcomePassed, true},
		{"external artifact", &manifest.ExternalArtifact{Name: "source"}, false, false, "", store.OutcomePassed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(tc.obj)
			id := tc.obj.Named()
			c := base.New(st, nil)
			if tc.filtered {
				c.Configure(base.Options{Filter: change.NewFilter(change.NewSet([]string{"other.yaml"}), &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "consumer.yaml"}},
				}, st)})
			}
			if tc.preflight {
				c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle", true }})
			}
			c.StartLifecycle()
			ran := false
			controllertest.NewExecution(c, id).Dispatch(t.Context(), manifest.DrainNone, func(_ context.Context, _ manifest.BaseManifest, _ manifest.DrainLevel) error {
				ran = true
				return nil
			})
			info, _ := st.GetStatus(id)
			if ran != tc.run || info.Outcome() != tc.outcome || info.Message != tc.message {
				t.Fatalf("ran=%t status=%+v, want ran=%t Ready/%s", ran, info, tc.run, tc.message)
			}
		})
	}
}

func TestFingerprintDedup(t *testing.T) {
	s := store.New()
	c := base.New(s, nil)
	ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
	s.AddObject(ks)
	id := ks.Named()
	docs := []map[string]any{{"kind": "ConfigMap"}}
	fp, handled, err := controllertest.NewExecution(c, id).FingerprintDedup(id, []string{"one"})
	if fp == "" || handled || err != nil {
		t.Fatalf("first key = %q, handled=%v, err=%v", fp, handled, err)
	}
	s.SetArtifact(id, &store.Emission{Manifests: docs, Fingerprint: fp, Complete: true})
	if _, handled, err := controllertest.NewExecution(c, id).FingerprintDedup(id, "other"); handled || err != nil {
		t.Errorf("mismatch: handled=%v err=%v; want false,nil", handled, err)
	}
	foreign := id
	foreign.Group = "other.example"
	if _, handled, err := controllertest.NewExecution(c, foreign).FingerprintDedup(foreign, []string{"one"}); handled || err != nil {
		t.Fatalf("foreign group: handled=%v err=%v; want false,nil", handled, err)
	}
	if got, handled, err := controllertest.NewExecution(c, id).FingerprintDedup(id, []string{"one"}); !handled || err != nil || got != fp {
		t.Errorf("match: key=%q handled=%v err=%v; want %q,true,nil", got, handled, err, fp)
	}
	none := testutil.KustomizationID("ns", "none")
	if _, handled, _ := controllertest.NewExecution(c, none).FingerprintDedup(none, []string{"one"}); handled {
		t.Error("no-artifact id: handled=true; want false")
	}
	for _, tc := range []struct {
		name    string
		content any
	}{
		{"nil content", nil}, {"unmarshalable content", []any{make(chan int)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, handled, err := controllertest.NewExecution(c, id).FingerprintDedup(id, tc.content)
			if got != "" || handled || err != nil {
				t.Fatalf("invalid content key=%q, handled=%v, err=%v", got, handled, err)
			}
		})
	}
	s.SetArtifact(id, &store.Emission{Manifests: docs})
	if _, handled, _ := controllertest.NewExecution(c, id).FingerprintDedup(id, []string{"one"}); handled {
		t.Error("empty stored fingerprint replayed")
	}
	if fp, handled, err := controllertest.NewExecution(c, id).FingerprintDedup(id, []any{make(chan int)}); fp != "" || handled || err != nil {
		t.Fatalf("invalid content replayed an empty stored key: key=%q, handled=%v, err=%v", fp, handled, err)
	}
}

func TestFingerprintDedup_PreflightError(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := base.New(s, nil)
	ks := &manifest.Kustomization{Name: "k", Namespace: "ns"}
	s.AddObject(ks)
	id := ks.Named()
	fp, _, _ := controllertest.NewExecution(c, id).FingerprintDedup(id, []string{"one"})
	s.SetArtifact(id, &store.Emission{Fingerprint: fp, Complete: true})
	c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle detected", true }})
	_, handled, err := controllertest.NewExecution(c, id).FingerprintDedup(id, []string{"one"})
	if !handled || err == nil {
		t.Errorf("preflight error: handled=%v err=%v; want true, non-nil", handled, err)
	}
}

func TestExecution_ExcludedOutputValidity(t *testing.T) {
	t.Parallel()
	st := store.New()
	obj := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(obj)
	var prior store.ReadSet
	prior.Init(st, obj.Named(), nil)
	prior.GetObject(obj.Named())
	st.SetArtifact(obj.Named(), &store.SourceArtifact{Reads: &prior})
	st.SetIncluded(obj.Named(), false)
	e := &base.Execution{Controller: base.New(st, nil)}
	e.Reads.Init(st, obj.Named(), nil)
	if !e.Valid() {
		t.Fatal("excluded assessment validated unconsumed output provenance")
	}
	st.SetIncluded(obj.Named(), true)
	if e.Valid() {
		t.Fatal("readmission retained the excluded assessment")
	}
	next := &base.Execution{Controller: e.Controller}
	next.Reads.Init(st, obj.Named(), nil)
	if next.Valid() {
		t.Fatal("admitted assessment ignored stale output provenance")
	}
}

func TestExecution_PendingProgressKeepsConditionStamp(t *testing.T) {
	t.Parallel()
	st := store.New()
	obj := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(obj)
	c := base.New(st, nil)
	var messages []string
	st.AddListener(store.EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		messages = append(messages, payload.(store.StatusInfo).Message)
	}, false)
	c.SetPendingUnlessReady(obj.Named(), "initial progress")
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	reads.GetStatus(obj.Named())
	c.SetPendingUnlessReady(obj.Named(), "later progress")
	if len(messages) != 2 || messages[0] != "initial progress" || messages[1] != "later progress" {
		t.Fatalf("progress notifications=%v", messages)
	}
	if !reads.Valid() {
		t.Fatal("progress text invalidated pending readiness")
	}
	st.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	if reads.Valid() {
		t.Fatal("readiness transition failed to invalidate the observer")
	}
}

func TestExecution_GateReplacement(t *testing.T) {
	t.Parallel()
	st := store.New()
	hr := &manifest.HelmRelease{Name: "reader", Namespace: "apps"}
	hr.Chart.Name = "before"
	st.AddObject(hr)
	c := base.New(st, task.NewBounded(2))
	first := controllertest.NewExecution(c, hr.Named())
	defer first.Close()
	fresh := hr.Clone()
	fresh.Chart.Name = "after"
	unsub := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == hr.Named() && payload.(store.StatusInfo).Status == store.StatusPending {
			st.AddObject(fresh)
		}
	}, false)
	defer unsub()
	first.Dispatch(t.Context(), manifest.DrainNone, func(_ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
		if obj.Chart.Name != "before" {
			t.Fatalf("predecessor changed: %+v", obj)
		}
		c.SetPendingUnlessReady(obj.Named(), "gate replacement")
		_, err := first.RequireRefresh(t.Context(), obj, manifest.GateParents, manifest.DrainNone)
		return err
	})
	if first.Valid() || st.GetArtifact(hr.Named()) != nil {
		t.Fatal("stale gate replacement admitted output")
	}
	second := controllertest.NewExecution(c, hr.Named())
	defer second.Close()
	called := false
	second.Dispatch(t.Context(), manifest.DrainNone, func(_ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
		called = true
		if obj.Chart.Name != "after" {
			t.Fatalf("successor retained stale declaration: %+v", obj)
		}
		return nil
	})
	if !called || !second.Valid() {
		t.Fatal("replacement declaration did not reconcile")
	}
}

func TestExecution_RetainsReadinessOutcome(t *testing.T) {
	t.Parallel()
	dependency := testutil.KustomizationID("apps", "dependency")
	for _, tc := range []struct {
		name string
		err  error
		want schedule.Kind
	}{
		{name: "terminal", want: schedule.Terminal},
		{name: "blocked", err: &depwait.ErrBlocked{Deps: []manifest.NamedResource{dependency}}, want: schedule.Blocked},
		{name: "failed", err: &manifest.DependencyFailedError{Failed: []manifest.NamedResource{dependency}}, want: schedule.DependencyFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			object := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			st.AddObject(object)
			execution := controllertest.NewExecution(base.New(st, nil), object.Named())
			defer execution.Close()
			execution.Dispatch(t.Context(), manifest.DrainNone, func(context.Context, *manifest.Kustomization, manifest.DrainLevel) error { return tc.err })
			if execution.Outcome() != tc.want {
				t.Fatalf("outcome=%v, want %v", execution.Outcome(), tc.want)
			}
			st.UpdateStatus(object.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "external update"})
			if execution.Outcome() != tc.want {
				t.Fatalf("status update replaced retained outcome: %v", execution.Outcome())
			}
		})
	}
}
