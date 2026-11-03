package base_test

import (
	"bytes"
	"context"
	"fmt"
	"log" //nolint:depguard // Restoring slog.SetDefault also requires restoring the standard logger.
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestExecution_Success(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "app",
		Namespace: "ns",
	}
	s.AddObject(hr)
	id := hr.Named()

	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
		func(t *testing.T, _ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
			t.Helper()
			if obj.Name != "app" {
				t.Errorf("re-read got %q, want app", obj.Name)
			}
			return nil
		},
	)
	got, _ := s.GetStatus(id)
	{
		assert.ErrorIf(t, got.Status != store.StatusReady, "status = %v, want Ready", got.Status)
	}
}

func TestExecution_RetiredSuccessfulOutput(t *testing.T) {
	t.Parallel()
	s := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{parent.Named(): {File: "parent.yaml"}},
	}
	c := base.New(s, nil)
	admissionLoader1 := admissiontest.New(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{"parent.yaml"}), Index: index, Inventory: s, Root: ""},
	)
	admissiontest.Commit(t, admissionLoader1, nil)
	c.Configure(base.Options{Filter: admissionLoader1.Options.Filter, Index: index, Loader: admissionLoader1})

	publish := func(value string) *base.Execution {
		execution := controllertest.NewExecution(c, parent.Named())
		execution.Dispatch(
			t.Context(),
			manifest.DrainNone,
			execution,
			func(execution *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
				_, _, next := execution.Reads.GetEmission(parent.Named())
				defer next.Finish()
				next.Children = []store.Child{
					{
						Obj: &manifest.ConfigMap{
							Name:      "settings",
							Namespace: "apps",
							Data:      map[string]any{"VALUE": value},
						},
					},
				}
				next.Complete = true
				execution.Emit(t.Context(), parent.Named(), &next, nil)
				return nil
			},
		)
		return execution
	}
	first := publish("first")
	{
		assert.FatalIf(t, !first.Valid(), "%v", "initial output was invalidated by its publication")
	}
	first.Close()
	for range s.Sweep() {
	}
	second := publish("second")
	defer second.Close()
	{
		assert.FatalIf(
			t,
			!second.Valid(),
			"%v",
			"successful predecessor output became a semantic input to its replacement",
		)
	}
	{
		{
			info, _ := s.GetStatus(parent.Named())
			assert.FatalIf(t, info.Status != store.StatusReady, "replacement status=%+v, want Ready", info)
		}
	}
}

func TestExecution_PreflightWithdrawsPublishedOutput(t *testing.T) {
	t.Parallel()
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprintf("source=%t", source), func(t *testing.T) {
			t.Parallel()
			testExecutionPreflightWithdrawsPublishedOutputCase(
				t,
				&source,
			)
		},
		)
	}
}

type countedDetailError struct{ calls int }

func (e *countedDetailError) Error() string {
	e.calls++
	return "upstream diagnostic"
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestExecution_Failure(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
		t.Run(level.String(), func(t *testing.T) {
			var logs bytes.Buffer
			prior, writer, flags := slog.Default(), log.Writer(), log.Flags()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: level})))
			t.Cleanup(func() {
				slog.SetDefault(prior)
				log.SetOutput(writer)
				log.SetFlags(flags)
			})
			s := store.New()
			hr := &manifest.HelmRelease{
				ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
				Name:      "app",
				Namespace: "ns",
			}
			s.AddObject(hr)
			id := hr.Named()
			detail := new(countedDetailError)
			cause := fmt.Errorf("fetch operation: %w", fmt.Errorf("library operation: %w", detail))
			detail.calls = 0
			controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
				func(_ *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
					return manifest.NewError(manifest.ErrInput, cause, "render failed")
				},
			)
			got, _ := s.GetStatus(id)
			{
				assert.FatalIf(
					t,
					got.Status != store.StatusFailed || got.Message != "render failed",
					"failure status = %+v",
					got,
				)
			}
			if (detail.calls > 0) != (level == slog.LevelDebug) {
				t.Errorf("detail evaluations at %v = %d", level, detail.calls)
			}
			for _, detail := range []string{id.String(), "fetch operation", "library operation", "upstream diagnostic"} {
				if strings.Contains(logs.String(), detail) != (level == slog.LevelDebug) {
					t.Errorf("debug detail %q at %v: %s", detail, level, logs.String())
				}
			}
		})
	}
}

func TestExecution_Panic(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "app",
		Namespace: "ns",
	}
	s.AddObject(hr)
	id := hr.Named()

	// Contract: panic is converted into StatusFailed AND re-raised
	// so the enclosing task.Service.Go's recover increments failures
	// (Service.Failures() must count panicked reconciles). Recover
	// the re-raise at the test boundary and assert both pieces.
	var rec any
	func() {
		defer func() { rec = recover() }()
		controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
			func(_ *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				panic("kaboom")
			},
		)
	}()
	{
		assert.FatalIf(
			t,
			rec == nil,
			"%v",
			"expected panic to be re-raised so task.Service.Go's recover counts it; got nil",
		)
	}
	got, _ := s.GetStatus(id)
	{
		assert.ErrorIf(t, got.Status != store.StatusFailed, "status = %v, want Failed", got.Status)
	}
	{
		assert.ErrorIf(
			t,
			!strings.Contains(got.Message, "panic:") || !strings.Contains(got.Message, "kaboom"),
			"message = %q, want a 'panic: kaboom' summary",
			got.Message,
		)
	}
}

func TestExecution_MissingObject(t *testing.T) {
	t.Parallel()
	s := store.New()
	id := testutil.HelmReleaseID("ns", "ghost")
	called := false
	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
		func(_ *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			called = true
			return nil
		},
	)
	{
		assert.ErrorIf(t, called, "%v", "fn ran for a missing object; expected silent no-op")
	}
	{
		{
			_, ok := s.GetStatus(id)
			assert.ErrorIf(t, ok, "%v", "missing object should not get a status entry")
		}
	}
	s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "re-fetching"})
	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
		func(t *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			t.Helper()
			t.Error("missing object reconciled")
			return nil
		})
	got, _ := s.GetStatus(id)
	{
		assert.FatalIf(
			t,
			got.Outcome() != store.OutcomeSkipped || got.Message != "object not in store at reconcile time",
			"missing-object status = %+v",
			got,
		)
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
			testExecutionPreservesInformativeReadyMessageCase(
				t,
				&tc,
			)
		},
		)
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
			testExecutionPreservesInformativeReadyMessageCase2(
				t,
				&tc,
			)
		},
		)
	}
	for _, message := range []string{" \t missing auth \n", " skipped: missing auth "} {
		t.Run(fmt.Sprintf("direct/%q", message), func(t *testing.T) {
			t.Parallel()
			testExecutionPreservesInformativeReadyMessageCase3(
				t,
				&message,
			)
		},
		)
	}
}

func TestExecution_PreservesExternalFailure(t *testing.T) {
	t.Parallel()
	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "app",
		Namespace: "ns",
	}
	s.AddObject(hr)
	id := hr.Named()

	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
		func(_ *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			s.UpdateStatus(
				id,
				store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "dependency cycle detected"},
			)
			return nil
		},
	)
	got, _ := s.GetStatus(id)
	{
		assert.ErrorIf(t, got.Status != store.StatusFailed, "status = %v, want Failed", got.Status)
	}
	{
		assert.ErrorIf(
			t,
			got.Message != "dependency cycle detected",
			"external failure message was clobbered: %q",
			got.Message,
		)
	}
}

func testExecutionPreflightWithdrawsPublishedOutputCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	st := store.New()
	tasks := task.NewBounded(2)
	c := base.New(st, tasks)

	var obj manifest.BaseManifest = &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	if *testCaseInput {
		obj = &manifest.GitRepository{Name: "producer", Namespace: "apps"}
	}
	id := obj.Named()
	child := &manifest.ConfigMap{Name: "output", Namespace: "apps", Data: map[string]any{"value": "rendered"}}
	st.AddObject(obj)
	var attempts, renders atomic.Int64
	var latest *base.Execution
	d := controllertest.Dispatch{
		Tasks: tasks,
		Store: st,
		ID:    id,
		ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
			attempts.Add(1)
			execution := controllertest.NewExecution(c, id)
			latest = execution
			execution.Dispatch(
				ctx,
				level,
				execution,
				func(execution *base.Execution, _ context.Context, _ manifest.BaseManifest, _ manifest.DrainLevel) error {
					renders.Add(1)
					if *testCaseInput {
						st.SetArtifact(
							id,
							&store.SourceArtifact{Reads: &execution.Reads, Key: "content", LocalPath: "source"},
						)
					} else {
						_, _, next := execution.Reads.GetEmission(id)
						defer next.Finish()
						next.Children = []store.Child{{Obj: child}}
						next.Complete = true
						execution.Emit(t.Context(), id, &next, nil)
					}
					return nil
				},
			)
			return execution
		},
	}
	if info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		d,
	); info.Status != store.StatusReady || attempts.Load() != 1 ||
		renders.Load() != 1 {
		t.Fatalf("initial status=%+v attempts=%d renders=%d", info, attempts.Load(), renders.Load())
	}
	{
		assert.FatalIf(
			t,
			st.GetArtifact(id, store.ArtifactContent) == nil ||
				!(*testCaseInput) && st.GetObject(child.Named()) == nil,
			"%v",
			"initial output was not published",
		)
	}
	const cycle = "dependency cycle detected"
	st.SetPreflight(id, cycle)
	attempts.Store(0)
	info := controllertest.DispatchToFixpoint(t.Context(), t, d)
	if info.Status != store.StatusFailed || info.Message != cycle || !latest.Valid() ||
		latest.Outcome() != schedule.Terminal ||
		attempts.Load() != 1 ||
		renders.Load() != 1 {
		t.Errorf(
			"cycle status=%+v valid=%t outcome=%v attempts=%d renders=%d, want terminal failure in one attempt without rendering",
			info,
			latest.Valid(),
			latest.Outcome(),
			attempts.Load(),
			renders.Load(),
		)
	}
	if *testCaseInput {
		{
			{
				artifact := st.GetArtifact(id, store.ArtifactContent)
				assert.ErrorIf(t, artifact != nil, "stale source artifact remained visible: %+v", artifact)
			}
		}
	} else {
		{
			assert.ErrorIf(t, st.GetObject(child.Named()) != nil, "%v", "stale emitted child remained visible")
		}
		output, ok := st.GetArtifact(id, store.ArtifactContent).(*store.Emission)
		{
			assert.ErrorIf(
				t,
				!ok || !output.Complete || output.OutputReads != &latest.Reads || !output.OutputReads.Valid() ||
					len(
						output.Children,
					) != 0,
				"withdrawal did not publish a complete current inventory: %+v",
				output,
			)
		}
	}
	st.SetPreflight(id, "")
	attempts.Store(0)
	info = controllertest.DispatchToFixpoint(t.Context(), t, d)
	if info.Status != store.StatusReady || info.Message != "" || !latest.Valid() || attempts.Load() != 1 ||
		renders.Load() != 2 {
		t.Fatalf(
			"recovered status=%+v valid=%t attempts=%d renders=%d",
			info,
			latest.Valid(),
			attempts.Load(),
			renders.Load(),
		)
	}
	{
		assert.FatalIf(
			t,
			st.GetArtifact(id, store.ArtifactContent) == nil ||
				!(*testCaseInput) && st.GetObject(child.Named()) == nil,
			"%v",
			"recovery did not republish output",
		)
	}
	t.Logf("recovery attempts=%d renders=%d", attempts.Load(), renders.Load())
}

func testExecutionPreservesInformativeReadyMessageCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		reason  store.StatusReason
		message string
	},
) {
	t.Helper()

	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "app",
		Namespace: "ns",
	}
	s.AddObject(hr)
	id := hr.Named()
	s.UpdateStatus(
		id,
		store.StatusInfo{
			Status:  store.StatusReady,
			Reason:  testCaseInput.reason,
			Message: testCaseInput.message,
		},
	)
	before, _ := s.GetStatus(id)
	controllertest.NewExecution(base.New(s, nil), id).Dispatch(t.Context(), manifest.DrainNone, t,
		func(_ *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			return nil
		})
	got, _ := s.GetStatus(id)
	{
		assert.FatalIf(t, got != before, "Ready snapshot changed: %+v, want %+v", got, before)
	}
}

func testExecutionPreservesInformativeReadyMessageCase2(
	t *testing.T,
	testCaseInput *struct {
		name string

		reason  store.StatusReason
		message string
		want    string
	},
) {
	t.Helper()

	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "app",
		Namespace: "ns",
	}
	s.AddObject(hr)
	c := base.New(s, nil)
	src := &manifest.OCIRepository{Name: "source", Namespace: "ns"}
	s.AddObject(src)
	s.SetArtifact(src.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
	s.UpdateStatus(
		src.Named(),
		store.StatusInfo{
			Status:  store.StatusReady,
			Reason:  testCaseInput.reason,
			Message: testCaseInput.message,
		},
	)
	execution := controllertest.NewExecution(c, hr.Named())
	execution.Dispatch(
		t.Context(),
		manifest.DrainNone,
		execution,
		func(execution *base.Execution, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			return execution.Require(
				t.Context(),
				[]manifest.DependencyRef{{NamedResource: src.Named(), Artifact: true}},
				manifest.DrainNone,
			)
		},
	)
	got, _ := s.GetStatus(hr.Named())
	want := store.OutcomePassed
	if testCaseInput.reason == store.ReasonSkipped {
		want = store.OutcomeSkipped
	}
	{
		assert.FatalIf(
			t,
			got.Outcome() != want || got.Message != testCaseInput.want,
			"propagated status = %+v, want %s/%q",
			got,
			want,
			testCaseInput.want,
		)
	}
}

func testExecutionPreservesInformativeReadyMessageCase3(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:     "app",
	}
	s.AddObject(hr)
	controllertest.NewExecution(base.New(s, nil), hr.Named()).Dispatch(t.Context(), manifest.DrainNone, t,
		func(_ *testing.T, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			return manifest.NewError(manifest.ErrSourceSkipped, nil, (*testCaseInput))
		})
	got, _ := s.GetStatus(hr.Named())
	{
		assert.FatalIf(
			t,
			got.Outcome() != store.OutcomeSkipped || got.Message != strings.TrimSpace((*testCaseInput)),
			"direct skip = %+v",
			got,
		)
	}
}
