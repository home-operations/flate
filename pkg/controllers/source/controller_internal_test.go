package source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	src "github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type fakeFetcher struct {
	during   func()
	calls    int
	retry    sourcecache.Policy
	artifact *store.SourceArtifact
	err      error
	secret   string
	objects  []manifest.BaseManifest
}

func (f *fakeFetcher) Fetch(
	_ context.Context,
	obj manifest.BaseManifest,
	reads *store.ReadSet,
	policy *sourcecache.Policy,
) (src.PreparedFetch, error) {
	if f.secret != "" {
		src.SecretLookup(reads)(obj.Named().Namespace, f.secret)
	}
	return src.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}

			f.calls++
			f.objects = append(f.objects, obj)
			f.retry = *policy

			if f.during != nil {
				f.during()
			}
			if f.err != nil {
				return nil, f.err
			}
			return f.artifact, nil
		},
	}, nil
}

func TestSource_CapturedMaterialRefresh(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{secret: "credentials", artifact: &store.SourceArtifact{Key: "acquired", FS: fstest.MapFS{}}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
	repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/source"}
	secret := &manifest.Secret{
		Name:       f.secret,
		Namespace:  repo.Namespace,
		StringData: map[string]any{"username": "first"},
	}
	parent := &manifest.Kustomization{Name: "parent", Namespace: repo.Namespace}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		repo.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
	}})
	st.AddObjects([]manifest.BaseManifest{parent, repo, secret})
	st.AddDeclaration(
		store.Declaration{
			Obj:       secret,
			Doc:       testutil.ObjectDocument(secret),
			Origin:    manifest.SuppliedDeclaration,
			InputPath: "credentials.yaml",
		},
	)
	controllertest.Emit(
		t.Context(),
		c.Controller,
		parent.Named(),
		&store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true},
	)
	var pending int
	st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); id == repo.Named() && ok && info.Status == store.StatusPending {
			pending++
		}
	}, false)
	var accepted *base.Execution
	for _, phase := range []struct {
		name  string
		calls int
	}{
		{"initial", 1}, {"unrelated", 1}, {"credential", 2}, {"authority", 2}, {"declaration", 3},
	} {
		t.Log(phase.name)
		switch phase.name {
		case "unrelated":
			st.AddObject(&manifest.Secret{Name: "unrelated", Namespace: repo.Namespace})
		case "credential":
			next := *secret
			secret = &next
			secret.StringData = map[string]any{"username": "second"}
			st.AddObject(secret)
		case "authority":
			controllertest.Emit(
				t.Context(),
				c.Controller,
				parent.Named(),
				&store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true},
			)
		case "declaration":
			next := *repo
			repo = &next
			repo.URL = "https://example.test/changed"
			controllertest.Emit(
				t.Context(),
				c.Controller,
				parent.Named(),
				&store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true},
			)
		}
		previous := accepted
		c.Tasks.Go(t.Context(), phase.name, func(ctx context.Context) {
			level := manifest.DrainNone
			if phase.name == "initial" || phase.name == "credential" {
				level = manifest.DrainCascade
			}
			accepted = c.ReconcileNode(ctx, repo.Named(), level, schedule.Result{})
		})
		c.Tasks.BlockTillDone()
		if previous != nil {
			previous.Retire()
		}
		assert.Diff(
			t,
			st.AdvisoryEvidence(),
			store.AdvisoryEvidence{
				Overrides: []store.SuppliedOverride{
					{Resource: secret.Named(), SupplyPath: "credentials.yaml", Keys: []string{"username"}},
				},
			},
		)

		assert.FatalIf(t, pending != 1, "pending transitions=%d, want only initial acquisition", pending)

		assert.FatalIf(
			t,
			accepted.Outcome() == schedule.Blocked || !accepted.Valid() || f.calls != phase.calls,
			"assessment valid=%t unresolved=%t acquisitions=%d, want %d",
			accepted.Valid(),
			accepted.Outcome() == schedule.Blocked,
			f.calls,
			phase.calls,
		)
	}
	accepted.Close()
}

func TestSource_DeclarationContributorsReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ complete, filtered bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		complete := tc.complete
		for _, timing := range []string{"accepted", "in_flight", "changed_in_flight"} {
			t.Run(fmt.Sprintf("complete=%t/filtered=%t/%s", complete, tc.filtered, timing), func(t *testing.T) {
				t.Parallel()
				testSourceDeclarationContributorsReuseCase(
					t,
					testSourceDeclarationContributorsReuseCaseInputs{tc: &tc, complete: &complete, timing: &timing},
				)
			},
			)
		}
	}
}

func TestSource_DeclarationSettlement(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"tag", "layer"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &fakeFetcher{artifact: &store.SourceArtifact{Key: "settled", FS: fstest.MapFS{}}}
			c, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: f})
			repo := &manifest.OCIRepository{
				Name: "chart", Namespace: "apps",
				URL: "oci://example.test/chart", Reference: &sourcev1.OCIRepositoryRef{Tag: "first"},
			}
			settled := *repo
			if name == "tag" {
				settled.Reference = &sourcev1.OCIRepositoryRef{Tag: "settled"}
			} else {
				settled.LayerSelector = &sourcev1.OCILayerSelector{Operation: "copy"}
			}
			parent := &manifest.Kustomization{Name: "producer", Namespace: repo.Namespace}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				repo.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
			}})
			st.AddObjects([]manifest.BaseManifest{repo, parent})
			attempted := make(chan struct{})
			var provisional *base.Execution
			c.Tasks.Go(t.Context(), "provisional", func(ctx context.Context) {
				provisional = c.ReconcileNode(ctx, repo.Named(), manifest.DrainNone, schedule.Result{})
				close(attempted)
			})
			c.Tasks.Go(t.Context(), "producer", func(context.Context) {
				<-attempted
				assert.ErrorIf(
					t,
					provisional.Outcome() != schedule.Blocked || f.calls != 0,
					"provisional unresolved=%t acquisitions=%d, want deferred with no acquisition",
					provisional.Outcome() == schedule.Blocked,
					f.calls,
				)
				controllertest.Emit(
					t.Context(),
					c.Controller,
					parent.Named(),
					&store.Emission{Children: []store.Child{{Obj: &settled}}, Complete: true},
				)
				st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			})
			c.Tasks.BlockTillDone()
			provisional.Retire()
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
			)
			assert.FatalIf(
				t,
				info.Status != store.StatusReady || f.calls != 1 || len(f.objects) != 1 || f.objects[0] != &settled,
				"status=%v acquisitions=%d identities=%v, want exactly the settled declaration",
				info,
				f.calls,
				f.objects,
			)
			t.Logf("acquired settled identity: %#v", f.objects[0])
		})
	}
}

func TestSource_DeclarationProgress(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"absent producer", "failed producer", "content cycle"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testSourceDeclarationProgressCase(t, &name)
		},
		)
	}
}

func newController(t *testing.T, fetchers map[string]src.Fetcher) (*Controller, *store.Store) {
	t.Helper()
	return newConfiguredController(t, fetchers, FetchOptions{})
}

func newConfiguredController(
	t *testing.T,
	fetchers map[string]src.Fetcher,
	opts FetchOptions,
) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
	if opts.Loader != nil {
		st = opts.Loader.Store
	}
	ts := task.NewBounded(2)
	c := New(st, ts)
	maps.Copy(c.Fetchers, fetchers)
	c.Configure(opts, sourcecache.RetryConfig{Attempts: 3})
	c.StartLifecycle()
	t.Cleanup(func() {
		ts.BlockTillDone()
	})
	return c, st
}

//nolint:paralleltest,tparallel // Subtests observe sequential revisions on one source controller.
func TestController_FetchesAndStoresArtifact(t *testing.T) {
	t.Parallel()

	const content = "source artifact bytes"
	f := &fakeFetcher{artifact: &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "u", LocalPath: "/tmp", Key: "pin", Revision: "one", Digest: "aaa",
		FS: fstest.MapFS{"source.txt": {Data: []byte(content)}},
	}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
	repo := &manifest.GitRepository{
		Name:      "r",
		Namespace: "ns",
		URL:       "https://example/r.git",
		Interval:  metav1.Duration{Duration: time.Hour},
	}
	st.AddObject(repo)

	for _, phase := range []string{"initial", "revised", "digested", "reacquired"} {
		t.Run(phase, func(t *testing.T) {
			testControllerFetchesAndStoresArtifactCase(
				t,
				testControllerFetchesAndStoresArtifactCaseInputs{f: &f, c: &c, st: &st, repo: &repo, phase: &phase},
			)
		},
		)
	}
}

func TestController_FetchErrorMarksFailed(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{err: errors.New("boom")}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})

	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		URL: "https://example/r.git",
	}
	st.AddObject(repo)

	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode},
	)

	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)

	assert.ErrorIf(t, info.String() != "boom", "Failed reason = %q, want %q", info.String(), "boom")
}

func TestController_ShortCircuitsToReady(t *testing.T) {
	t.Parallel()
	for _, suspended := range []bool{false, true} {
		t.Run(fmt.Sprintf("suspend=%t", suspended), func(t *testing.T) {
			t.Parallel()
			testControllerShortCircuitsToReadyCase(
				t,
				&suspended,
			)
		},
		)
	}

	t.Run("unchanged", func(t *testing.T) { t.Parallel(); testControllerShortCircuitsToReadyUnchanged(t) })
}

func TestController_UnregisteredKindIgnored(t *testing.T) {
	t.Parallel()
	// Register an OCIRepository fetcher so the controller is alive but has no
	// entry for KindGitRepository. Under the dag scheduler the source controller
	// only exposes ReconcileNode for registered kinds (Owns), so the
	// scheduler never dispatches the GitRepository — it must therefore carry no
	// status once the registered kind has been reconciled.
	registered := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindOCIRepository}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: registered})

	unregistered := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		URL: "https://example/r.git",
	}
	st.AddObject(unregistered)

	// Positive control: a registered kind reaches Ready, proving the controller
	// is alive and the unregistered skip is targeted.
	oci := &manifest.OCIRepository{
		Name: "o", Namespace: "ns",
		URL: "oci://example/img",
	}
	st.AddObject(oci)

	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: oci.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}

	// The unregistered kind was never dispatched, so it must have no status.

	{
		_, ok := st.GetStatus(unregistered.Named())
		assert.ErrorIf(t, ok, "expected no status update for unregistered kind")
	}
}

type testSourceDeclarationContributorsReuseCaseInputs struct {
	tc *struct {
		complete bool
		filtered bool
	}
	complete *bool
	timing   *string
}

func testSourceDeclarationContributorsReuseCase(
	t *testing.T,
	testCaseInput testSourceDeclarationContributorsReuseCaseInputs,
) {
	t.Helper()

	f := &fakeFetcher{
		secret:   "credentials",
		artifact: &store.SourceArtifact{Key: "shared", FS: fstest.MapFS{}},
	}
	chart := &manifest.HelmChartSource{Name: "shared", Namespace: "apps"}
	first := &manifest.HelmRelease{Name: "first", Namespace: "apps"}
	second := &manifest.HelmRelease{Name: "second", Namespace: "apps"}
	var filterLoader *loader.Loader
	var filter *change.Filter
	if testCaseInput.tc.filtered {
		index := &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				chart.Named(): {File: "chart.yaml"},
			},
		}
		filterLoader = admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   change.NewSet([]string{"chart.yaml"}),
				Index:     index,
				Inventory: store.New(),
				Root:      "/repo",
			},
		)
		admissiontest.Commit(t, filterLoader, nil)
		filter = filterLoader.Options.Filter
	}
	c, st := newConfiguredController(
		t,
		map[string]src.Fetcher{manifest.KindHelmChart: f},
		FetchOptions{Filter: filter, Loader: filterLoader},
	)
	st.AddObjects([]manifest.BaseManifest{first, second})
	secret := &manifest.Secret{
		Name:       "credentials",
		Namespace:  "apps",
		StringData: map[string]any{"username": "same"},
	}
	st.AddObject(secret)
	st.AddDeclaration(
		store.Declaration{
			Obj:       secret,
			Doc:       testutil.ObjectDocument(secret),
			Origin:    manifest.SuppliedDeclaration,
			InputPath: "credentials.yaml",
		},
	)
	publish := func(value string) {
		controllertest.Emit(
			t.Context(),
			c.Controller,
			first.Named(),
			&store.Emission{
				Children: []store.Child{
					{Obj: chart},
					{
						Obj: &manifest.ConfigMap{
							Name:      "other",
							Namespace: "apps",
							Data:      map[string]any{"VALUE": value},
						},
					},
				},
				Complete: (*testCaseInput.complete),
			},
		)
	}
	publish("first")
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	if (*testCaseInput.timing) != "accepted" {
		f.during = sync.OnceFunc(func() {
			close(entered)
			select {
			case <-release:
			case <-t.Context().Done():
			}
		})
	}
	var accepted *base.Execution
	c.Tasks.Go(t.Context(), "initial", func(ctx context.Context) {
		accepted = c.ReconcileNode(ctx, chart.Named(), manifest.DrainNone, schedule.Result{})
	})
	if (*testCaseInput.timing) != "accepted" {
		<-entered
		if (*testCaseInput.timing) == "changed_in_flight" {
			next := *chart
			next.Version = "2.0.0"
			chart = &next
		}
		controllertest.Emit(
			t.Context(),
			c.Controller,
			second.Named(),
			&store.Emission{Children: []store.Child{{Obj: chart}}, Complete: (*testCaseInput.complete)},
		)
		publish("changed")
		unblock()
	}
	c.Tasks.BlockTillDone()
	wantCalls := 1
	if (*testCaseInput.timing) == "changed_in_flight" {
		assert.FatalIf(
			t,
			accepted.Valid() || st.GetArtifact(chart.Named(), store.ArtifactContent) != nil,
			"%v",
			"changed content accepted the invalidated acquisition",
		)

		accepted.Retire()
		c.Tasks.Go(t.Context(), "changed content", func(ctx context.Context) {
			accepted = c.ReconcileNode(ctx, chart.Named(), manifest.DrainNone, schedule.Result{})
		})
		c.Tasks.BlockTillDone()
		wantCalls = 2
	}
	defer accepted.Close()
	assert.FatalIf(
		t,
		accepted.Outcome() == schedule.Blocked || !accepted.Valid() || f.calls != wantCalls,
		"initial valid=%t unresolved=%t acquisitions=%d",
		accepted.Valid(),
		accepted.Outcome() == schedule.Blocked,
		f.calls,
	)
	wantEvidence := store.AdvisoryEvidence{
		Overrides: []store.SuppliedOverride{
			{Resource: secret.Named(), SupplyPath: "credentials.yaml", Keys: []string{"username"}},
		},
	}
	assert.Diff(t, st.AdvisoryEvidence(), wantEvidence)
	controllertest.Emit(
		t.Context(),
		c.Controller,
		second.Named(),
		&store.Emission{Children: []store.Child{{Obj: chart}}, Complete: (*testCaseInput.complete)},
	)
	publish("changed again")

	assert.FatalIf(
		t,
		accepted.Valid(),
		"%v",
		"producer publication did not invalidate the captured availability assessment",
	)

	c.Tasks.Go(t.Context(), "successor", func(ctx context.Context) {
		next := c.ReconcileNode(ctx, chart.Named(), manifest.DrainNone, schedule.Result{})
		defer next.Close()
		assert.ErrorIf(
			t,
			next.Outcome() == schedule.Blocked || !next.Valid() || f.calls != wantCalls,
			"successor valid=%t unresolved=%t acquisitions=%d, want unchanged content reused",
			next.Valid(),
			next.Outcome() == schedule.Blocked,
			f.calls,
		)
	})
	c.Tasks.BlockTillDone()
	assert.Diff(t, st.AdvisoryEvidence(), wantEvidence)
}

func testSourceDeclarationProgressCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	f := &fakeFetcher{artifact: &store.SourceArtifact{Key: "bootstrap", FS: fstest.MapFS{}}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
	repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/source"}
	parent := &manifest.Kustomization{Name: "producer", Namespace: repo.Namespace}
	st.AddObject(repo)
	ids := []schedule.NodeID{repo.Named()}
	if (*testCaseInput) != "absent producer" {
		st.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				repo.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
			}},
		)
		st.AddObject(parent)
		ids = append(ids, parent.Named())
	}
	dispatch := controllertest.Dispatch{
		Tasks: c.Tasks,
		Store: st,
		ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, previous schedule.Result) *base.Execution {
			if id == repo.Named() {
				return c.ReconcileNode(ctx, id, level, previous)
			}
			run := controllertest.NewExecution(c.Controller, id)
			run.Dispatch(
				ctx,
				level,
				run,
				func(run *base.Execution, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
					if (*testCaseInput) == "failed producer" {
						return manifest.NewError(manifest.ErrInput, nil, "failed producer")
					}
					if err := run.Require(
						ctx,
						[]manifest.DependencyRef{{NamedResource: repo.Named()}},
						level,
					); err != nil {
						return err
					}

					assert.ErrorIf(
						t,
						run.Reads.GetArtifact(repo.Named(), store.ArtifactContent) == nil,
						"%v",
						"producer did not receive source content",
					)

					run.Emit(
						t.Context(),
						id,
						&store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true},
						nil,
					)
					return nil
				},
			)
			return run
		},
	}
	scheduler := schedule.New(c.Tasks, dispatch)
	scheduler.Seed(ids)

	{
		err := scheduler.Run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}

	info, _ := st.GetStatus(repo.Named())
	if (*testCaseInput) == "absent producer" {
		assert.FatalIf(
			t,
			info.Status != store.StatusReady || f.calls != 1,
			"independent source: status=%+v acquisitions=%d",
			info,
			f.calls,
		)
	} else {
		assert.FatalIf(
			t,
			!info.IsDependencyFailure() || f.calls != 0 ||
				!slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}),
			"covered source acquired raw bytes or lost cause: status=%+v acquisitions=%d",
			info,
			f.calls,
		)
	}
}

type testControllerFetchesAndStoresArtifactCaseInputs struct {
	f     **fakeFetcher
	c     **Controller
	st    **store.Store
	repo  **manifest.GitRepository
	phase *string
}

func testControllerFetchesAndStoresArtifactCase(
	t *testing.T,
	testCaseInput testControllerFetchesAndStoresArtifactCaseInputs,
) {
	t.Helper()
	const content = "source artifact bytes"

	if (*testCaseInput.phase) == "revised" || (*testCaseInput.phase) == "digested" {
		next := *(*testCaseInput.f).artifact
		if (*testCaseInput.phase) == "revised" {
			next.Revision = "two"
		} else {
			next.Digest = "bbb"
		}
		(*testCaseInput.f).artifact = &next
		previous := *(*testCaseInput.st).GetArtifact((*testCaseInput.repo).Named(), store.ArtifactContent).(*store.SourceArtifact)
		previous.FS = nil
		(*testCaseInput.st).SetArtifact((*testCaseInput.repo).Named(), &previous)
	}
	if (*testCaseInput.phase) == "reacquired" {
		pin := *(*testCaseInput.f).artifact
		pin.FS = nil
		(*testCaseInput.st).SetArtifact((*testCaseInput.repo).Named(), &pin)
	}

	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         (*testCaseInput.c).Tasks,
				Store:         (*testCaseInput.st),
				ID:            (*testCaseInput.repo).Named(),
				ReconcileNode: (*testCaseInput.c).ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}

	assert.FatalIf(t, (*testCaseInput.f).retry.MaxAge != time.Hour ||
		(*testCaseInput.f).retry.Retry != (sourcecache.RetryConfig{Attempts: 3}) ||
		(*testCaseInput.f).retry.Timeout != nil ||
		(*testCaseInput.f).retry.Admit != nil, "configured policy=%+v", (*testCaseInput.f).retry)
	art, ok := (*testCaseInput.st).GetArtifact((*testCaseInput.repo).Named(), store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t,
		!ok || art.FS == nil || art.Kind != (*testCaseInput.f).artifact.Kind || art.URL != (*testCaseInput.f).artifact.URL || art.Key != "pin" ||
			art.Revision != (*testCaseInput.f).artifact.Revision ||
			art.Digest != (*testCaseInput.f).artifact.Digest,
		"artifact = %+v, want readable source with original identity",
		art,
	)

	{
		got, err := fs.ReadFile(art.FS, "source.txt")
		assert.FatalIf(
			t,
			err != nil || string(got) != content,
			"artifact bytes = %q, error = %v, want %q",
			got,
			err,
			content,
		)
	}
}

func testControllerShortCircuitsToReadyCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	for _, tc := range []struct {
		name  string
		obj   manifest.BaseManifest
		calls int
	}{
		{"git", &manifest.GitRepository{Name: "repo", Namespace: "ns", URL: "https://invalid.example/repo.git", Suspend: (*testCaseInput)}, 1},
		{"repository", &manifest.HelmRepository{Name: "repo", Namespace: "ns", URL: "https://invalid.example/charts", Suspend: (*testCaseInput)}, 0},
		{"default chart", &manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", Suspend: (*testCaseInput)}, 1},
		{"repository chart", &manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", Suspend: (*testCaseInput), SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository}}, 1},
		{"source chart", &manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", Suspend: (*testCaseInput), SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindGitRepository}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeFetcher{}
			c, st := newController(t, map[string]src.Fetcher{tc.obj.Named().Kind: f})
			st.AddObject(tc.obj)
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{
					Tasks:         c.Tasks,
					Store:         st,
					ID:            tc.obj.Named(),
					ReconcileNode: c.ReconcileNode,
				},
			)
			message, calls := "", tc.calls
			outcome := store.OutcomePassed
			if *testCaseInput {
				message, calls = "suspended", 0
				outcome = store.OutcomeSuspended
			}
			assert.FatalIf(
				t,
				info.Outcome() != outcome || info.String() != message || f.calls != calls,
				"status=%+v fetch calls=%d, want Ready/%q calls=%d",
				info,
				f.calls,
				message,
				calls,
			)

			assert.FatalIf(
				t,
				st.GetArtifact(tc.obj.Named(), store.ArtifactContent) != nil,
				"%v",
				"no-content reconciliation fabricated an artifact",
			)
		})
	}
}

func testControllerShortCircuitsToReadyUnchanged(t *testing.T) {
	t.Helper()

	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks)
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository}}
	c.Fetchers[manifest.KindGitRepository] = f
	filterLoader := admissiontest.New(
		t,
		admissiontest.Config{
			Changes:   change.NewSet(nil),
			Index:     &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			Inventory: st,
			Root:      "",
		},
	)
	admissiontest.Commit(t, filterLoader, nil)
	filter := filterLoader.Options.Filter
	c.Configure(FetchOptions{Filter: filter, Loader: filterLoader}, sourcecache.RetryConfig{})
	t.Cleanup(func() { tasks.BlockTillDone() })
	repo := &manifest.GitRepository{Name: "r", Namespace: "ns", URL: "https://example/r.git"}
	st.AddObject(repo)
	c.StartLifecycle()
	tasks.Go(t.Context(), repo.Named().String(), func(ctx context.Context) {
		{
			deps := c.ReconcileNode(ctx, repo.Named(), manifest.DrainNone, schedule.Result{}).Blocked
			assert.ErrorIf(t, len(deps) != 0, "blocked source %s: %v", repo.Named(), deps)
		}
	})
	tasks.BlockTillDone()
	info, _ := st.GetStatus(repo.Named())
	assert.FatalIf(
		t,
		info.Outcome() != store.OutcomeUnchanged || info.String() != "unchanged" || f.calls != 0,
		"status=%+v fetch calls=%d, want Ready/unchanged calls=0",
		info,
		f.calls,
	)
}
