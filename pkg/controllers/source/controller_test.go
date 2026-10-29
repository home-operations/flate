package source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	src "github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (f *fakeFetcher) Fetch(_ context.Context, obj manifest.BaseManifest, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	f.calls++
	f.objects = append(f.objects, obj)
	f.retry = policy
	if f.secret != "" {
		src.SecretLookup(reads)(obj.Named().Namespace, f.secret)
	}
	if f.during != nil {
		f.during()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.artifact, nil
}

func TestSource_CapturedMaterialRefresh(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{secret: "credentials", artifact: &store.SourceArtifact{Key: "acquired", FS: fstest.MapFS{}}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
	repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/source"}
	secret := &manifest.Secret{Name: f.secret, Namespace: repo.Namespace, StringData: map[string]any{"username": "first"}}
	parent := &manifest.Kustomization{Name: "parent", Namespace: repo.Namespace}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		repo.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
	}})
	st.AddObjects([]manifest.BaseManifest{parent, repo, secret})
	supply, err := store.NewSupplied([]store.SuppliedObject{{Object: secret, Path: "credentials.yaml"}})
	assert.NoError(t, err)
	st.SetSupplied(supply)
	c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true}, nil)
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
			c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true}, nil)
		case "declaration":
			next := *repo
			repo = &next
			repo.URL = "https://example.test/changed"
			c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true}, nil)
		}
		previous := accepted
		c.Tasks.Go(t.Context(), phase.name, func(ctx context.Context) {
			level := manifest.DrainNone
			if phase.name == "initial" || phase.name == "credential" {
				level = manifest.DrainCascade
			}
			accepted = c.ReconcileNode(ctx, repo.Named(), level)
		})
		c.Tasks.BlockTillDone()
		if previous != nil {
			previous.Retire()
		}
		assert.Diff(t, st.AdvisoryEvidence(), store.AdvisoryEvidence{Overrides: []store.SuppliedOverride{{Resource: secret.Named(), SupplyPath: "credentials.yaml", Keys: []string{"username"}}}})
		if pending != 1 {
			t.Fatalf("pending transitions=%d, want only initial acquisition", pending)
		}
		if accepted.Outcome() == schedule.Blocked || !accepted.Valid() || f.calls != phase.calls {
			t.Fatalf("assessment valid=%t unresolved=%t acquisitions=%d, want %d", accepted.Valid(), accepted.Outcome() == schedule.Blocked, f.calls, phase.calls)
		}
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
				f := &fakeFetcher{secret: "credentials", artifact: &store.SourceArtifact{Key: "shared", FS: fstest.MapFS{}}}
				chart := &manifest.HelmChartSource{Name: "shared", Namespace: "apps"}
				first := &manifest.HelmRelease{Name: "first", Namespace: "apps"}
				second := &manifest.HelmRelease{Name: "second", Namespace: "apps"}
				var filter *change.Filter
				if tc.filtered {
					index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{chart.Named(): {File: "chart.yaml"}}}
					filter = change.NewFilter(change.NewSet([]string{"chart.yaml"}), index, filtertest.MapLister{chart.Named(): chart, first.Named(): first, second.Named(): second}, "/repo")
				}
				c, st := newConfiguredController(t, map[string]src.Fetcher{manifest.KindHelmChart: f}, FetchOptions{Filter: filter})
				st.AddObjects([]manifest.BaseManifest{first, second})
				secret := &manifest.Secret{Name: "credentials", Namespace: "apps", StringData: map[string]any{"username": "same"}}
				st.AddObject(secret)
				supply, err := store.NewSupplied([]store.SuppliedObject{{Object: secret, Path: "credentials.yaml"}})
				assert.NoError(t, err)
				st.SetSupplied(supply)
				publish := func(value string) {
					c.Emit(first.Named(), &store.Emission{Children: []store.Child{{Obj: chart}, {Obj: &manifest.ConfigMap{Name: "other", Namespace: "apps", Data: map[string]any{"VALUE": value}}}}, Complete: complete}, nil)
				}
				publish("first")
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				if timing != "accepted" {
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
					accepted = c.ReconcileNode(ctx, chart.Named(), manifest.DrainNone)
				})
				if timing != "accepted" {
					<-entered
					if timing == "changed_in_flight" {
						next := *chart
						next.Version = "2.0.0"
						chart = &next
					}
					c.Emit(second.Named(), &store.Emission{Children: []store.Child{{Obj: chart}}, Complete: complete}, nil)
					publish("changed")
					unblock()
				}
				c.Tasks.BlockTillDone()
				wantCalls := 1
				if timing == "changed_in_flight" {
					if accepted.Valid() || st.GetArtifact(chart.Named(), store.ArtifactContent) != nil {
						t.Fatal("changed content accepted the invalidated acquisition")
					}
					accepted.Retire()
					c.Tasks.Go(t.Context(), "changed content", func(ctx context.Context) {
						accepted = c.ReconcileNode(ctx, chart.Named(), manifest.DrainNone)
					})
					c.Tasks.BlockTillDone()
					wantCalls = 2
				}
				defer accepted.Close()
				if accepted.Outcome() == schedule.Blocked || !accepted.Valid() || f.calls != wantCalls {
					t.Fatalf("initial valid=%t unresolved=%t acquisitions=%d", accepted.Valid(), accepted.Outcome() == schedule.Blocked, f.calls)
				}
				wantEvidence := store.AdvisoryEvidence{Overrides: []store.SuppliedOverride{{Resource: secret.Named(), SupplyPath: "credentials.yaml", Keys: []string{"username"}}}}
				assert.Diff(t, st.AdvisoryEvidence(), wantEvidence)
				c.Emit(second.Named(), &store.Emission{Children: []store.Child{{Obj: chart}}, Complete: complete}, nil)
				publish("changed again")
				if accepted.Valid() {
					t.Fatal("producer publication did not invalidate the captured availability assessment")
				}
				c.Tasks.Go(t.Context(), "successor", func(ctx context.Context) {
					next := c.ReconcileNode(ctx, chart.Named(), manifest.DrainNone)
					defer next.Close()
					if next.Outcome() == schedule.Blocked || !next.Valid() || f.calls != wantCalls {
						t.Errorf("successor valid=%t unresolved=%t acquisitions=%d, want unchanged content reused", next.Valid(), next.Outcome() == schedule.Blocked, f.calls)
					}
				})
				c.Tasks.BlockTillDone()
				assert.Diff(t, st.AdvisoryEvidence(), wantEvidence)
			})
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
			repo := &manifest.OCIRepository{Name: "chart", Namespace: "apps",
				URL: "oci://example.test/chart", Reference: &sourcev1.OCIRepositoryRef{Tag: "first"}}
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
				provisional = c.ReconcileNode(ctx, repo.Named(), manifest.DrainNone)
				close(attempted)
			})
			c.Tasks.Go(t.Context(), "producer", func(context.Context) {
				<-attempted
				if provisional.Outcome() != schedule.Blocked || f.calls != 0 {
					t.Errorf("provisional unresolved=%t acquisitions=%d, want deferred with no acquisition", provisional.Outcome() == schedule.Blocked, f.calls)
				}
				c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: &settled}}, Complete: true}, nil)
				st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			})
			c.Tasks.BlockTillDone()
			provisional.Retire()
			info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode})
			if info.Status != store.StatusReady || f.calls != 1 || len(f.objects) != 1 || f.objects[0] != &settled {
				t.Fatalf("status=%v acquisitions=%d identities=%v, want exactly the settled declaration", info, f.calls, f.objects)
			}
			t.Logf("acquired settled identity: %#v", f.objects[0])
		})
	}
}

func TestSource_DeclarationProgress(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"absent producer", "failed producer", "content cycle"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := &fakeFetcher{artifact: &store.SourceArtifact{Key: "bootstrap", FS: fstest.MapFS{}}}
			c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
			repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/source"}
			parent := &manifest.Kustomization{Name: "producer", Namespace: repo.Namespace}
			st.AddObject(repo)
			ids := []schedule.NodeID{repo.Named()}
			if name != "absent producer" {
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					repo.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
				}})
				st.AddObject(parent)
				ids = append(ids, parent.Named())
			}
			dispatch := controllertest.Dispatch{Tasks: c.Tasks, Store: st, ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
				if id == repo.Named() {
					return c.ReconcileNode(ctx, id, level)
				}
				run := controllertest.NewExecution(c.Controller, id)
				run.Dispatch(ctx, level, run, func(run *base.Execution, ctx context.Context, _ *manifest.Kustomization, level manifest.DrainLevel) error {
					if name == "failed producer" {
						return fmt.Errorf("%w: failed producer", manifest.ErrInput)
					}
					if err := run.Require(ctx, []manifest.DependencyRef{{NamedResource: repo.Named()}}, level); err != nil {
						return err
					}
					if run.Reads.GetArtifact(repo.Named(), store.ArtifactContent) == nil {
						t.Error("producer did not receive source content")
					}
					run.Emit(id, &store.Emission{Children: []store.Child{{Obj: repo}}, Complete: true}, nil)
					return nil
				})
				return run
			}}
			scheduler := schedule.New(c.Tasks, dispatch)
			scheduler.Seed(ids)
			if err := scheduler.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			info, _ := st.GetStatus(repo.Named())
			if name == "absent producer" {
				if info.Status != store.StatusReady || f.calls != 1 {
					t.Fatalf("independent source: status=%+v acquisitions=%d", info, f.calls)
				}
			} else if !info.IsDependencyFailure() || f.calls != 0 || !slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named()}) {
				t.Fatalf("covered source acquired raw bytes or lost cause: status=%+v acquisitions=%d", info, f.calls)
			}
		})
	}
}

func newController(t *testing.T, fetchers map[string]src.Fetcher) (*Controller, *store.Store) {
	t.Helper()
	return newConfiguredController(t, fetchers, FetchOptions{})
}

func newConfiguredController(t *testing.T, fetchers map[string]src.Fetcher, opts FetchOptions) (*Controller, *store.Store) {
	t.Helper()
	st := store.New()
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

func TestController_FetchesAndStoresArtifact(t *testing.T) {
	const content = "source artifact bytes"
	f := &fakeFetcher{artifact: &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "u", LocalPath: "/tmp", Key: "pin", Revision: "one", Digest: "aaa",
		FS: fstest.MapFS{"source.txt": {Data: []byte(content)}},
	}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
	repo := &manifest.GitRepository{Name: "r", Namespace: "ns", URL: "https://example/r.git", Interval: metav1.Duration{Duration: time.Hour}}
	st.AddObject(repo)

	for _, phase := range []string{"initial", "revised", "digested", "reacquired"} {
		t.Run(phase, func(t *testing.T) {
			if phase == "revised" || phase == "digested" {
				next := *f.artifact
				if phase == "revised" {
					next.Revision = "two"
				} else {
					next.Digest = "bbb"
				}
				f.artifact = &next
				previous := *st.GetArtifact(repo.Named(), store.ArtifactContent).(*store.SourceArtifact)
				previous.FS = nil
				st.SetArtifact(repo.Named(), &previous)
			}
			if phase == "reacquired" {
				pin := *f.artifact
				pin.FS = nil
				st.SetArtifact(repo.Named(), &pin)
			}
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
				t.Fatalf("status = %+v, want StatusReady", info)
			}
			if f.retry.MaxAge != time.Hour || f.retry.Retry != (sourcecache.RetryConfig{Attempts: 3}) || f.retry.Timeout != nil || f.retry.Admit != nil {
				t.Fatalf("configured policy=%+v", f.retry)
			}
			art, ok := st.GetArtifact(repo.Named(), store.ArtifactContent).(*store.SourceArtifact)
			if !ok || art.FS == nil || art.Kind != f.artifact.Kind || art.URL != f.artifact.URL || art.Key != "pin" || art.Revision != f.artifact.Revision || art.Digest != f.artifact.Digest {
				t.Fatalf("artifact = %+v, want readable source with original identity", art)
			}
			if got, err := fs.ReadFile(art.FS, "source.txt"); err != nil || string(got) != content {
				t.Fatalf("artifact bytes = %q, error = %v, want %q", got, err, content)
			}
		})
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

	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode})
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want StatusFailed", info)
	}
	if info.String() != "boom" {
		t.Errorf("Failed reason = %q, want %q", info.String(), "boom")
	}
}

func TestController_ShortCircuitsToReady(t *testing.T) {
	t.Parallel()
	for _, suspended := range []bool{false, true} {
		t.Run(fmt.Sprintf("suspend=%t", suspended), func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name  string
				obj   manifest.BaseManifest
				calls int
			}{
				{"git", &manifest.GitRepository{Name: "repo", Namespace: "ns", URL: "https://invalid.example/repo.git", Suspend: suspended}, 1},
				{"repository", &manifest.HelmRepository{Name: "repo", Namespace: "ns", URL: "https://invalid.example/charts", Suspend: suspended}, 0},
				{"default chart", &manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", Suspend: suspended}, 1},
				{"repository chart", &manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", Suspend: suspended, SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository}}, 1},
				{"source chart", &manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", Suspend: suspended, SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindGitRepository}}, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					f := &fakeFetcher{}
					c, st := newController(t, map[string]src.Fetcher{tc.obj.Named().Kind: f})
					st.AddObject(tc.obj)
					info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: tc.obj.Named(), ReconcileNode: c.ReconcileNode})
					message, calls := "", tc.calls
					outcome := store.OutcomePassed
					if suspended {
						message, calls = "suspended", 0
						outcome = store.OutcomeSuspended
					}
					if info.Outcome() != outcome || info.String() != message || f.calls != calls {
						t.Fatalf("status=%+v fetch calls=%d, want Ready/%q calls=%d", info, f.calls, message, calls)
					}
					if st.GetArtifact(tc.obj.Named(), store.ArtifactContent) != nil {
						t.Fatal("no-content reconciliation fabricated an artifact")
					}
				})
			}
		})
	}

	t.Run("unchanged", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		tasks := task.NewBounded(2)
		c := New(st, tasks)
		f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindGitRepository}}
		c.Fetchers[manifest.KindGitRepository] = f
		filter := change.NewFilter(change.NewSet(nil), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, filtertest.MapLister{}, "")
		c.Configure(FetchOptions{Filter: filter}, sourcecache.RetryConfig{})
		t.Cleanup(func() { tasks.BlockTillDone() })
		repo := &manifest.GitRepository{Name: "r", Namespace: "ns", URL: "https://example/r.git"}
		st.AddObject(repo)
		c.StartLifecycle()
		tasks.Go(t.Context(), repo.Named().String(), func(ctx context.Context) {
			if deps := c.ReconcileNode(ctx, repo.Named(), manifest.DrainNone).Blocked; len(deps) != 0 {
				t.Errorf("blocked source %s: %v", repo.Named(), deps)
			}
		})
		tasks.BlockTillDone()
		info, _ := st.GetStatus(repo.Named())
		if info.Outcome() != store.OutcomeUnchanged || info.String() != "unchanged" || f.calls != 0 {
			t.Fatalf("status=%+v fetch calls=%d, want Ready/unchanged calls=0", info, f.calls)
		}
	})
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
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: oci.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	// The unregistered kind was never dispatched, so it must have no status.
	if _, ok := st.GetStatus(unregistered.Named()); ok {
		t.Errorf("expected no status update for unregistered kind")
	}
}

// TestController_OwnsExcludesForeignKind pins issue #872: a CRD from another
// API group that reuses a source kind name (seaweed.seaweedfs.com Bucket)
// parses to a RawObject, so the kind's Fetcher would reject it with
// "unexpected payload". Owns keeps the scheduler from ever dispatching it.
func TestController_OwnsExcludesForeignKind(t *testing.T) {
	t.Parallel()
	f := &fakeFetcher{artifact: &store.SourceArtifact{Kind: manifest.KindBucket}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindBucket: f})

	foreign := &manifest.RawObject{
		Kind:       manifest.KindBucket,
		APIVersion: "seaweed.seaweedfs.com/v1",
		Name:       "greptimedb", Namespace: "observability",
	}
	st.AddObject(foreign)
	if c.Owns(foreign.Named()) {
		t.Fatalf("Owns(%s) = true for a foreign %s CRD", foreign.Named(), foreign.APIVersion)
	}

	flux := &manifest.Bucket{
		Name: foreign.Name, Namespace: foreign.Namespace,
		BucketName: "b", Endpoint: "minio:9000",
	}
	st.AddObject(flux)
	if !c.Owns(flux.Named()) {
		t.Errorf("Owns(%s) = false for the Flux Bucket", flux.Named())
	}
	for _, group := range []string{"", "example.test"} {
		id := flux.Named()
		id.Group = group
		if c.Owns(id) {
			t.Errorf("Owns(%s) = true for an absent lookalike", id)
		}
	}
	if c.Owns(foreign.Named()) {
		t.Fatal("same-name Flux source admitted a foreign object")
	}
	// An id the store never saw stays ours — the vanished-object report is
	// the controller's to write.
	if !c.Owns(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindBucket, Namespace: "ns", Name: "gone"}) {
		t.Errorf("Owns = false for an absent id of a registered kind")
	}
	if c.Owns(manifest.NamedResource{Group: "apps", Kind: "Deployment", Namespace: "ns", Name: "app"}) {
		t.Errorf("Owns = true for a kind with no fetcher")
	}
}

func TestController_MissingSecretWaitsForRender(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		allowMissing bool
	}{
		{name: "strict"},
		{name: "allow missing", allowMissing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeFetcher{err: src.MissingSecretErr(manifest.DiagnosticFor(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "r"}, nil), "ca", "not found")}
			c, st := newConfiguredController(t,
				map[string]src.Fetcher{manifest.KindOCIRepository: f},
				FetchOptions{AllowMissingSecrets: tt.allowMissing})
			repo := &manifest.OCIRepository{Name: "r", Namespace: "ns"}
			st.AddObject(repo)

			if controllertest.Reconcile(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}, 0) {
				t.Fatal("missing Secret must block while renders can still produce it")
			}
			if info, _ := st.GetStatus(repo.Named()); info.Status != store.StatusPending {
				t.Fatalf("status = %+v, want Pending", info)
			}

			st.AddObject(&manifest.Secret{Name: "ca", Namespace: "ns"})
			f.err = nil
			f.artifact = &store.SourceArtifact{Kind: manifest.KindOCIRepository}
			if !controllertest.Reconcile(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}, 0) {
				t.Fatal("source must finish after the Secret arrives")
			}
			if info, _ := st.GetStatus(repo.Named()); info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped {
				t.Fatalf("status = %+v, want Ready without a skip", info)
			}
			if st.GetArtifact(repo.Named(), store.ArtifactContent) == nil {
				t.Fatal("source artifact missing after retry")
			}
		})
	}
}

func TestController_UnavailableSecretAtFixpoint(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		present      bool
		allowMissing bool
		producer     bool
		plain        bool
		malformed    bool
		secret       string
		wantSkip     bool
	}{
		{name: "missing", secret: "auth"},
		{name: "missing allowed", secret: "auth", allowMissing: true, wantSkip: true},
		{name: "missing producer backed", secret: "auth", producer: true, wantSkip: true},
		{name: "missing keys", secret: "auth", present: true},
		{name: "missing keys allowed", secret: "auth", present: true, allowMissing: true, wantSkip: true},
		{name: "missing keys producer backed", secret: "auth", present: true, producer: true, wantSkip: true},
		{name: "plain missing allowed", plain: true, secret: "ghcr-creds", allowMissing: true, wantSkip: true},
		{name: "plain missing strict", plain: true, secret: "ghcr-creds"},
		{name: "missing auth producer backed", secret: "ghcr-creds", producer: true, wantSkip: true},
		{name: "missing auth no producer", secret: "ghcr-creds"},
		{name: "malformed allowed", secret: "auth", present: true, malformed: true, allowMissing: true},
		{name: "malformed producer backed", secret: "auth", present: true, malformed: true, producer: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			secret := &manifest.Secret{Name: tt.secret, Namespace: "ns"}
			detail := "not found"
			if tt.present {
				detail = "missing username/password"
			}
			repo := &manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://example/img", Interval: &metav1.Duration{Duration: time.Hour}}
			fetchErr := src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), secret.Name, detail)
			if tt.plain {
				fetchErr = fmt.Errorf("%w: OCIRepository ns/r: secret ns/%s %s", manifest.ErrMissingSecret, secret.Name, detail)
			}
			if tt.malformed {
				detail = "invalid credential encoding"
				fetchErr = fmt.Errorf("%w: OCIRepository ns/r: secret ns/%s %s", manifest.ErrInput, secret.Name, detail)
			}
			ambientCalls := 0
			if tt.producer && !tt.malformed {
				missing, _ := errors.AsType[*src.MissingSecretError](fetchErr)
				missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
					ambientCalls++
					return &store.SourceArtifact{Kind: repo.Named().Kind}, nil
				}
			}
			f := &fakeFetcher{err: fetchErr, secret: secret.Name}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
			if tt.producer {
				addDeclaration(index, secret.Named(), manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "ns", Name: "auth"})
			}
			c, st := newConfiguredController(t,
				map[string]src.Fetcher{manifest.KindOCIRepository: f},
				FetchOptions{AllowMissingSecrets: tt.allowMissing})
			st.SetDiscoveryIndex(index)
			if tt.present {
				st.AddObject(secret)
			}
			st.AddObject(repo)
			var info store.StatusInfo
			if tt.plain || tt.malformed {
				info = controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode})
			} else {
				if controllertest.Reconcile(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}, manifest.DrainNone) {
					t.Fatal("must wait while renders can still supply Secret contents")
				}
				if pending, _ := st.GetStatus(repo.Named()); pending.Status != store.StatusPending {
					t.Fatalf("status = %+v, want Pending", pending)
				}
				if !controllertest.Reconcile(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}, manifest.DrainCascade) {
					t.Fatal("unavailable Secret must terminalize at the fixpoint")
				}
				info, _ = st.GetStatus(repo.Named())
				if f.calls != 2 {
					t.Errorf("fetch calls = %d, want 2", f.calls)
				}
			}
			wantStatus := store.StatusFailed
			if tt.wantSkip {
				wantStatus = store.StatusReady
			}
			if info.Status != wantStatus || (info.Outcome() == store.OutcomeSkipped) != tt.wantSkip || !strings.Contains(info.String(), detail) || !strings.Contains(info.String(), secret.Name) {
				t.Fatalf("status = %+v, want %v (skip=%t) preserving %q and %q", info, wantStatus, tt.wantSkip, detail, secret.Name)
			}
			if ambientCalls != 0 {
				t.Fatalf("index-only producer invoked ambient continuation %d times", ambientCalls)
			}
			if st.GetArtifact(repo.Named(), store.ArtifactContent) != nil {
				t.Fatal("unavailable or malformed source must not produce an artifact")
			}
		})
	}
}

// fallbackFetcher fails with a missing Secret until rendered is set, and
// carries a registry-fallback retry.
type fallbackFetcher struct {
	rendered      bool
	fallbackCalls int
	retry         sourcecache.Policy
	fallbackRetry sourcecache.Policy
	fallbackErr   error
}

func (f *fallbackFetcher) Fetch(_ context.Context, _ manifest.BaseManifest, _ *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	f.retry = policy
	if f.rendered {
		return &store.SourceArtifact{Kind: manifest.KindOCIRepository}, nil
	}
	missing, _ := errors.AsType[*src.MissingSecretError](src.MissingSecretErr(manifest.DiagnosticFor(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "r"}, nil), "ghcr-creds", "not found"))
	missing.RetryAmbient = func(_ context.Context) (*store.SourceArtifact, error) {
		f.fallbackCalls++
		f.fallbackRetry = policy
		if f.fallbackErr != nil {
			return nil, f.fallbackErr
		}
		return &store.SourceArtifact{Kind: manifest.KindOCIRepository}, nil
	}
	return nil, missing
}

func TestController_MissingSecretRegistryFallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		rendered     bool
		wantFallback int
	}{
		{name: "rendered Secret takes precedence", rendered: true},
		{name: "unrendered Secret falls back at fixpoint", wantFallback: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fallbackFetcher{}
			c, st := newController(t, map[string]src.Fetcher{manifest.KindOCIRepository: f})
			repo := &manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://example/img", Interval: &metav1.Duration{Duration: time.Hour}}
			st.AddObject(repo)

			if controllertest.Reconcile(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}, 0) {
				t.Fatal("missing Secret must block before any registry fallback")
			}
			if f.fallbackCalls != 0 {
				t.Fatalf("fallback calls = %d while renders can still produce the Secret, want 0", f.fallbackCalls)
			}

			if tt.rendered {
				st.AddObject(&manifest.Secret{Name: "ghcr-creds", Namespace: "ns"})
				f.rendered = true
			}
			if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped {
				t.Fatalf("status = %+v, want Ready without a skip", info)
			}
			if st.GetArtifact(repo.Named(), store.ArtifactContent) == nil {
				t.Error("fetch must store its artifact")
			}
			policies := []sourcecache.Policy{f.retry}
			if tt.wantFallback > 0 {
				policies = append(policies, f.fallbackRetry)
			}
			for _, policy := range policies {
				if policy.MaxAge != time.Hour || policy.Retry != (sourcecache.RetryConfig{Attempts: 3}) || policy.Timeout != nil || policy.Admit != nil {
					t.Fatalf("normal/fallback policies=%+v/%+v", f.retry, f.fallbackRetry)
				}
			}
			if f.fallbackCalls != tt.wantFallback {
				t.Errorf("fallback calls = %d, want %d", f.fallbackCalls, tt.wantFallback)
			}
		})
	}
}

func TestController_AmbientPolicy(t *testing.T) {
	t.Parallel()
	for _, repo := range []manifest.BaseManifest{
		&manifest.GitRepository{Name: "r", Namespace: "ns", URL: "ssh://git@fixture/repo"},
		&manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://fixture/image"},
		&manifest.Bucket{Name: "r", Namespace: "ns", Endpoint: "https://bucket.invalid", BucketName: "data"},
		&manifest.HelmChartSource{Name: "chart", Namespace: "ns", Chart: "app", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: "r"}},
	} {
		for _, tt := range []struct {
			name            string
			producer, allow bool
			failure         error
		}{
			{name: "producer", producer: true},
			{name: "producer allowed", producer: true, allow: true},
			{name: "success"},
			{name: "success allowed", allow: true},
			{name: "rejected", failure: &fetcherr.ResponseError{StatusCode: 401}},
			{name: "rejected allowed", allow: true, failure: &fetcherr.ResponseError{StatusCode: 401}},
			{name: "unavailable", failure: src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "no machine credentials")},
			{name: "unavailable allowed", allow: true, failure: src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "no machine credentials")},
			{name: "acquisition error", failure: &fs.PathError{Op: "open", Path: "repositories.yaml", Err: fs.ErrPermission}},
			{name: "acquisition error allowed", allow: true, failure: &fs.PathError{Op: "open", Path: "repositories.yaml", Err: fs.ErrPermission}},
			{name: "timeout", failure: context.DeadlineExceeded},
			{name: "timeout allowed", allow: true, failure: context.DeadlineExceeded},
			{name: "canceled", failure: context.Canceled},
			{name: "canceled allowed", allow: true, failure: context.Canceled},
		} {
			t.Run(repo.Named().Kind+"/"+tt.name, func(t *testing.T) {
				t.Parallel()
				missing, _ := errors.AsType[*src.MissingSecretError](src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "auth", "not found"))
				calls := 0
				failure := tt.failure
				if failure != nil {
					failure = fmt.Errorf("%w: ambient acquisition: %w", manifest.ErrFlux, failure)
				}
				missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
					calls++
					if failure != nil {
						return nil, failure
					}
					return &store.SourceArtifact{Kind: repo.Named().Kind, FS: fstest.MapFS{}}, nil
				}
				f := &fakeFetcher{err: missing, secret: "auth"}
				c, st := newConfiguredController(t, map[string]src.Fetcher{repo.Named().Kind: f}, FetchOptions{AllowMissingSecrets: tt.allow})
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
				if tt.producer {
					addDeclaration(index, missing.Secret, manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "ns", Name: "producer"})
				}
				st.SetDiscoveryIndex(index)
				repository := &manifest.HelmRepository{Name: "r", Namespace: "ns", URL: "https://charts.invalid"}
				st.AddObject(repository)
				st.UpdateStatus(repository.Named(), store.StatusInfo{Status: store.StatusReady})
				st.AddObject(repo)
				info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode})
				wantCalls := 1
				if tt.producer {
					wantCalls = 0
				}
				wantSkip := tt.producer || tt.allow && errors.Is(failure, manifest.ErrMissingSecret) && !errors.Is(failure, context.Canceled) && !errors.Is(failure, context.DeadlineExceeded)
				wantStatus := store.StatusReady
				if failure != nil && !wantSkip {
					wantStatus = store.StatusFailed
				}
				if calls != wantCalls || info.Status != wantStatus || (info.Outcome() == store.OutcomeSkipped) != wantSkip {
					t.Fatalf("status=%+v calls=%d; want status=%v skip=%t calls=%d", info, calls, wantStatus, wantSkip, wantCalls)
				}
				if failure != nil && !wantSkip && !strings.Contains(info.Message, failure.Error()) {
					t.Fatalf("settlement lost acquisition error: %+v", info)
				}
				if failure != nil && !wantSkip && !strings.Contains(info.Message, "secret ns/auth not found") {
					t.Fatalf("settlement lost unavailable-material context: %+v", info)
				}
				if (st.GetArtifact(repo.Named(), store.ArtifactContent) != nil) != (!wantSkip && failure == nil) {
					t.Fatal("ambient settlement published the wrong content outcome")
				}
				if !wantSkip && failure == nil {
					controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode})
					if calls != wantCalls {
						t.Fatal("accepted source artifact was acquired again")
					}
					assert.Equal(t, len(st.Warnings()), 0)
				}
				if wantSkip && !slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
					return w.Resource == repo.Named() && w.Category == manifest.WarnSourceCredentialsUnavailable
				}) {
					t.Fatal("missing-material settlement lost its warning")
				}
			})
		}
	}
}

func TestPendingSource_AmbientContextError(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			repo := &manifest.GitRepository{Name: "r", Namespace: "ns"}
			failure := errors.Join(src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "unavailable"), cause)
			missing, _ := errors.AsType[*src.MissingSecretError](src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "auth", "not found"))
			missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) { return nil, failure }
			controller, st := newConfiguredController(t, nil, FetchOptions{AllowMissingSecrets: true})
			st.AddObject(repo)
			c := &execution{Controller: controller}
			c.run.Init(controller.Controller, repo.Named())
			defer c.run.Close()
			pending := &pendingSource{execution: c, object: repo, missing: missing}
			var err error
			c.Tasks.Go(t.Context(), "settle", func(ctx context.Context) { err = pending.Settle(ctx, manifest.DrainCascade) })
			c.Tasks.BlockTillDone()
			if !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, manifest.ErrSourceSkipped) {
				t.Fatalf("default settlement error = %v; want %v and ErrFlux", err, cause)
			}
		})
	}
}

func TestController_ExistenceOnly_NoTransientPendingOnReemit(t *testing.T) {
	t.Parallel()

	f := &fakeFetcher{}
	c, st := newController(t, map[string]src.Fetcher{
		manifest.KindHelmRepository: f,
	})
	repo := &manifest.HelmRepository{
		Name: "charts", Namespace: "flux-system",
		URL: "https://charts.example", Type: "default",
	}
	st.AddObject(repo)
	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want StatusReady", info)
	}

	var mu sync.Mutex
	var sawPending bool
	st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != repo.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	reemit := &manifest.HelmRepository{
		Name: "charts", Namespace: "flux-system",
		URL: "https://charts.example", Type: "default",
		Interval: metav1.Duration{Duration: time.Hour},
	}
	st.AddObject(reemit)
	controllertest.Reconcile(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}, 0)
	if f.calls != 0 {
		t.Errorf("presence-only acquisitions = %d, want 0", f.calls)
	}

	mu.Lock()
	defer mu.Unlock()
	if sawPending {
		t.Error("presence-only source transiently downgraded Ready to Pending on a no-op re-run (the quiescence-race window)")
	}
	if info, _ := st.GetStatus(repo.Named()); info.Status != store.StatusReady {
		t.Errorf("source not Ready after no-op re-run: %+v", info)
	}
}

func TestController_ArtifactMaterializers(t *testing.T) {
	t.Parallel()
	for _, state := range []store.Status{store.StatusPending, store.StatusFailed, store.StatusReady} {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			fetcher := &fakeFetcher{err: fmt.Errorf("%w: content unavailable", manifest.ErrInput)}
			c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
			generator := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}
			st.AddObject(generator)
			st.UpdateStatus(generator.Named(), store.StatusInfo{Status: state, Reason: "", Message: "materializer state"})
			doc := map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": "ExternalArtifact", "metadata": map[string]any{"name": "generated", "namespace": "apps"}, "spec": map[string]any{"sourceRef": map[string]any{"apiVersion": generator.APIVersion, "kind": generator.Kind, "name": generator.Name}}}
			obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			st.AddObject(obj, authoritative)
			var blocked []manifest.NamedResource
			var outcome schedule.Kind
			c.Tasks.Go(t.Context(), "materializer", func(ctx context.Context) {
				run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
				outcome, blocked = run.Outcome(), run.Blocked
				if outcome == schedule.DependencyFailed {
					status, _ := st.GetStatus(obj.Named())
					blocked = status.BlockedBy()
				}
			})
			c.Tasks.BlockTillDone()
			switch state {
			case store.StatusPending:
				if outcome != schedule.Blocked || len(blocked) != 1 || blocked[0] != generator.Named() || fetcher.calls != 0 {
					t.Fatalf("materializer wait = %v, calls=%d", blocked, fetcher.calls)
				}
			case store.StatusFailed:
				status, _ := st.GetStatus(obj.Named())
				if outcome != schedule.DependencyFailed || len(blocked) != 1 || blocked[0] != generator.Named() || status.Status != store.StatusFailed || fetcher.calls != 0 {
					t.Fatalf("failed materializer result = %+v, %+v, %v, calls=%d", outcome, status, blocked, fetcher.calls)
				}
			default:
				status, _ := st.GetStatus(obj.Named())
				if len(blocked) != 0 || status.Status != store.StatusFailed {
					t.Fatalf("contentless result = %+v, %v", status, blocked)
				}
			}
			payload := &store.SourceArtifact{Kind: manifest.KindExternalArtifact, Key: "generated", FS: fstest.MapFS{"file": {Data: []byte("available")}}}
			st.RecordEmitted(generator.Named(), &store.Emission{Children: []store.Child{{Obj: obj, Doc: authoritative}}}, map[manifest.NamedResource]*store.SourceArtifact{obj.Named(): payload})
			before := fetcher.calls
			st.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: ""})
			c.Tasks.Go(t.Context(), "accepted", func(ctx context.Context) {
				run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
				blocked = run.Blocked
			})
			c.Tasks.BlockTillDone()
			status, _ := st.GetStatus(obj.Named())
			if len(blocked) != 0 || status.Status != store.StatusReady || fetcher.calls != before {
				t.Fatalf("accepted bytes waited for %s producer: %+v, %v", state, status, blocked)
			}
		})
	}
}

type materializerFetcher struct{ calls atomic.Int64 }

func (f *materializerFetcher) Fetch(context.Context, manifest.BaseManifest, *store.ReadSet, sourcecache.Policy) (*store.SourceArtifact, error) {
	f.calls.Add(1)
	return nil, fmt.Errorf("%w: content unavailable", manifest.ErrInput)
}

func TestController_ArtifactMaterializersProduction(t *testing.T) {
	t.Parallel()
	for _, declarationPresent := range []bool{false, true} {
		t.Run(fmt.Sprintf("declaration=%t", declarationPresent), func(t *testing.T) {
			t.Parallel()
			fetcher := &materializerFetcher{}
			c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			generator := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}
			obj := &manifest.ExternalArtifact{Name: "generated", Namespace: "apps"}
			st.AddObject(owner)
			st.AddObject(obj)
			materializer := owner.Named()
			if declarationPresent {
				st.AddObject(generator)
				materializer = generator.Named()
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				obj.Named(): {Productions: []manifest.Production{{Owner: owner.Named(), Declaration: generator.Named()}}},
			}})
			st.UpdateStatus(materializer, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "progressing"})
			var blocked []manifest.NamedResource
			c.Tasks.Go(t.Context(), "predicted", func(ctx context.Context) {
				run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
				blocked = run.Blocked
			})
			c.Tasks.BlockTillDone()
			if len(blocked) != 1 || blocked[0] != materializer || fetcher.calls.Load() != 0 {
				t.Fatalf("progressing materializer = %v, want %s; fetches=%d", blocked, materializer, fetcher.calls.Load())
			}
		})
	}
}

func TestController_ArtifactMaterializersCompleteEmpty(t *testing.T) {
	t.Parallel()
	fetcher := &materializerFetcher{}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
	generator := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}
	st.AddObject(generator)
	doc := map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": manifest.KindExternalArtifact, "metadata": map[string]any{"name": "generated", "namespace": "apps"}, "spec": map[string]any{"sourceRef": map[string]any{"apiVersion": generator.APIVersion, "kind": generator.Kind, "name": generator.Name}}}
	obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st.AddObject(obj, authoritative)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		obj.Named(): {Productions: []manifest.Production{{Owner: generator.Named(), Declaration: generator.Named(), Source: manifest.BootstrapSourceID}}},
	}})
	if len(st.Productions(obj.Named(), nil)) != 1 {
		t.Fatal("fixture has no bound output prediction")
	}
	_, _, empty := st.GetEmission(generator.Named())
	defer empty.Finish()
	empty.Complete = true
	st.RecordEmitted(generator.Named(), &empty, nil)
	st.UpdateStatus(generator.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "complete empty"})
	if len(st.Productions(obj.Named(), nil)) != 0 || st.GetObject(obj.Named()) != nil || st.GetArtifact(obj.Named(), store.ArtifactContent) != nil {
		t.Fatal("complete empty inventory retained predicted output authority")
	}
	st.AddObject(obj, authoritative)
	if len(st.Productions(obj.Named(), nil)) != 0 {
		t.Fatal("independent declaration revived a completed producer prediction")
	}
	var blocked []manifest.NamedResource
	c.Tasks.Go(t.Context(), "complete-empty", func(ctx context.Context) {
		run := c.ReconcileNode(ctx, obj.Named(), manifest.DrainNone)
		blocked = run.Blocked
	})
	c.Tasks.BlockTillDone()
	status, _ := st.GetStatus(obj.Named())
	if len(blocked) != 0 || status.Status != store.StatusFailed || status.IsDependencyFailure() || fetcher.calls.Load() != 1 || st.GetArtifact(obj.Named(), store.ArtifactContent) != nil {
		t.Fatalf("complete empty source = %+v, blocked=%v, fetches=%d", status, blocked, fetcher.calls.Load())
	}
}

type materializerDispatcher struct {
	controller *Controller
	drains     [3]atomic.Int64
}

func (d *materializerDispatcher) Dispatch(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
	d.drains[level].Add(1)
	return schedule.Result{Assessment: &materializerAssessment{Execution: d.controller.ReconcileNode(ctx, id, level), drains: &d.drains}}
}
func (d *materializerDispatcher) Sweep(_ context.Context, _ bool) []schedule.NodeID {
	return slices.Collect(d.controller.Store.Sweep())
}

type materializerAssessment struct {
	*base.Execution
	drains *[3]atomic.Int64
}

func (a *materializerAssessment) Settle(ctx context.Context, level manifest.DrainLevel) {
	a.drains[level].Add(1)
	a.Execution.Settle(ctx, level)
}

func TestController_ArtifactMaterializersCycleDrains(t *testing.T) {
	t.Parallel()
	fetcher := &materializerFetcher{}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindExternalArtifact: fetcher})
	ids := []manifest.NamedResource{{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Name: "a", Namespace: "apps"}, {Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Name: "b", Namespace: "apps"}}
	for i, id := range ids {
		doc := map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": id.Kind, "metadata": map[string]any{"name": id.Name, "namespace": id.Namespace}, "spec": map[string]any{"sourceRef": map[string]any{"apiVersion": "source.toolkit.fluxcd.io/v1", "kind": id.Kind, "name": ids[1-i].Name}}}
		obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		if err != nil {
			t.Fatal(err)
		}
		st.AddObject(obj, authoritative)
	}
	dispatcher := &materializerDispatcher{controller: c}
	scheduler := schedule.New(c.Tasks, dispatcher)
	defer st.AddListener(store.EventStatusUpdated, func(manifest.NamedResource, any) {
		scheduler.OnPublication()
	}, false)()
	scheduler.Seed(ids)
	if err := scheduler.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if dispatcher.drains[manifest.DrainForce].Load() == 0 || fetcher.calls.Load() != 0 || c.Tasks.Failures() != 0 {
		t.Fatalf("cycle force passes=%d, fetches=%d, task failures=%d", dispatcher.drains[manifest.DrainForce].Load(), fetcher.calls.Load(), c.Tasks.Failures())
	}
	for i, id := range ids {
		status, _ := st.GetStatus(id)
		blocked := status.BlockedBy()
		if status.Status != store.StatusFailed || !status.IsDependencyFailure() || len(blocked) != 1 || blocked[0] != ids[1-i] || st.GetArtifact(id, store.ArtifactContent) != nil {
			t.Fatalf("contentless cycle %s = %+v, blocked=%v", id, status, blocked)
		}
	}
}

func TestController_AmbientSettlementExactlyOnce(t *testing.T) {
	t.Parallel()
	for _, secret := range []string{"auth", ""} {
		for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade} {
			t.Run(fmt.Sprintf("secret=%q/level=%d", secret, level), func(t *testing.T) {
				t.Parallel()
				repo := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "ssh://git@fixture/repo"}
				missing, _ := errors.AsType[*src.MissingSecretError](src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), secret, "credentials unavailable"))
				calls := 0
				missing.RetryAmbient = func(context.Context) (*store.SourceArtifact, error) {
					calls++
					return &store.SourceArtifact{Kind: manifest.KindGitRepository}, nil
				}
				f := &fakeFetcher{err: missing}
				c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: f})
				st.AddObject(repo)
				dispatch := controllertest.Dispatch{Tasks: c.Tasks, ID: repo.Named(), ReconcileNode: c.ReconcileNode}
				done := controllertest.Reconcile(t.Context(), t, dispatch, level)
				if secret != "" && level == manifest.DrainNone {
					if done || calls != 0 {
						t.Fatal("ambient tier ran before repository settlement")
					}
					done = controllertest.Reconcile(t.Context(), t, dispatch, manifest.DrainCascade)
				}
				if !done || calls != 1 || st.GetArtifact(repo.Named(), store.ArtifactContent) == nil {
					t.Fatalf("settlement: done=%t calls=%d", done, calls)
				}
			})
		}
	}
}

func TestController_SyntheticCredentialSkip(t *testing.T) {
	t.Parallel()
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
	repo := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts", SecretRef: &manifest.LocalObjectReference{Name: "auth"}})
	failure := src.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "auth", "not found")
	fetcher := &fakeFetcher{err: failure, secret: "auth"}
	controller, st := newConfiguredController(t, map[string]src.Fetcher{manifest.KindOCIRepository: fetcher}, FetchOptions{AllowMissingSecrets: true})
	st.AddObject(repo)
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: controller.Tasks, Store: st, ID: repo.Named(), ReconcileNode: controller.ReconcileNode})
	warnings := st.Warnings()
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "sources", Name: "auth"}
	if info.Outcome() != store.OutcomeSkipped || !strings.Contains(info.String(), chart.Named().Kind+" "+chart.Named().NamespacedName()) || len(warnings) != 1 {
		t.Fatalf("skip lost display or outcome: %+v %+v", info, warnings)
	}
	if warnings[0].Resource != repo.Named() || warnings[0].Subject.Resource != chart.Named() || !slices.Equal(warnings[0].Related, []manifest.NamedResource{secret}) || st.GetArtifact(repo.Named(), store.ArtifactContent) != nil {
		t.Fatalf("skip lost raw provenance: %+v", warnings)
	}
}

func TestSource_BootstrapAliasReplacement(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"url", "ref", "ignore", "late alias"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/same-id-source"
			const url = "https://example.test/self"
			input := fstest.MapFS{"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: https://example.test/self}\n")}, "excluded.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\n")}}
			s := store.New()
			result, err := discovery.Run(t.Context(), discovery.Config{Store: s, Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, SelfURLs: []string{url}})
			assert.NoError(t, err)
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "self"}
			old, ok := s.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			if !ok || !old.InputAlias {
				t.Fatal("bootstrap did not admit an alias")
			}
			tasks := task.NewBounded(2)
			c := New(s, tasks)
			f := &fakeFetcher{artifact: &store.SourceArtifact{Key: "patched-source", FS: fstest.MapFS{}}}
			c.Fetchers[manifest.KindGitRepository] = f
			c.Configure(FetchOptions{Admission: result.Admission}, sourcecache.RetryConfig{})
			if field == "late alias" {
				s.SetArtifact(id, nil)
			}
			c.StartLifecycle()
			if field == "late alias" {
				s.SetArtifact(id, old)
			}
			t.Cleanup(tasks.BlockTillDone)
			parent := &manifest.RawObject{APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "apps", Name: "flux"}
			s.AddObject(parent)
			repo, _ := s.GetObject(id).(*manifest.GitRepository)
			next := *repo
			switch field {
			case "url", "late alias":
				next.URL = "https://example.test/foreign"
			case "ref":
				next.Reference = &sourcev1.GitRepositoryRef{Branch: "other"}
			case "ignore":
				ignored := "excluded.yaml"
				next.Ignore = &ignored
			}
			c.Emit(parent.Named(), &store.Emission{Children: []store.Child{{Obj: &next}}, Complete: true}, nil)
			status := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: tasks, Store: s, ID: id, ReconcileNode: c.ReconcileNode})
			assert.Equal(t, status.Status, store.StatusReady)
			actual, _ := s.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			if actual == nil || actual == old {
				t.Fatal("source replacement reused stale bootstrap material")
			}
			if field == "ignore" {
				assert.Equal(t, f.calls, 0)
				if _, err := fs.ReadFile(actual.FS, "excluded.yaml"); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("patched ignore was not applied: %v", err)
				}
			} else {
				assert.Equal(t, f.calls, 1)
				assert.Equal(t, actual.Key, "patched-source")
				assert.Equal(t, actual.InputAlias, false)
			}
		})
	}
}

func TestSource_BootstrapAuthority(t *testing.T) {
	t.Parallel()
	for _, authority := range []string{"root declaration", "owner inventory"} {
		for _, timing := range []string{"published", "captured"} {
			t.Run(authority+"/"+timing, func(t *testing.T) {
				t.Parallel()
				const path = "/virtual/bootstrap-authority"
				input := &aliasCaptureFS{FS: fstest.MapFS{
					"source.yaml":  {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: 'https://unreachable.invalid/self', ref: {branch: unavailable}}\n")},
					"payload.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\n")},
				}, entered: make(chan struct{}), release: make(chan struct{})}
				unblock := sync.OnceFunc(func() { close(input.release) })
				defer unblock()
				s := store.New()
				result, err := discovery.Run(t.Context(), discovery.Config{Store: s, Input: &store.SourceArtifact{LocalPath: path, FS: input}, Path: path})
				assert.NoError(t, err)
				tasks := task.NewBounded(2)
				t.Cleanup(tasks.BlockTillDone)
				c := New(s, tasks)
				fetcher := &fakeFetcher{artifact: &store.SourceArtifact{Key: "acquired", FS: fstest.MapFS{}}}
				c.Fetchers[manifest.KindGitRepository] = fetcher
				c.Configure(FetchOptions{Admission: result.Admission}, sourcecache.RetryConfig{})
				c.StartLifecycle()
				defer s.AddListener(store.EventObjectAdded, func(_ manifest.NamedResource, payload any) {
					if obj, ok := payload.(manifest.BaseManifest); ok {
						result.Admission.AddApplication(s, obj)
					}
				}, false)()
				source := s.ListObjects(manifest.SourceDomain, manifest.KindGitRepository)[0].(*manifest.GitRepository)
				root := &manifest.Kustomization{Name: "root", Namespace: source.Namespace, Path: ".", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name}}
				owner := &manifest.RawObject{APIVersion: "example.test/v1", Kind: "Producer", Name: "owner", Namespace: source.Namespace}
				if authority == "root declaration" {
					s.AddObject(root)
				} else {
					s.AddObject(owner)
					c.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: source}, {Obj: root}}, Complete: true}, nil)
				}
				var accepted *base.Execution
				reconcile := func() {
					tasks.Go(t.Context(), "source", func(ctx context.Context) { accepted = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone) })
					tasks.BlockTillDone()
				}
				reconcile()
				defer func() { accepted.Close() }()
				artifact := s.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
				assert.Equal(t, artifact.InputAlias, true)
				assert.Equal(t, artifact.Reads.Valid(), true)
				assert.Equal(t, fetcher.calls, 0)
				previous := accepted
				reconcile()
				previous.Retire()
				assert.Equal(t, s.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(artifact))
				assert.Equal(t, fetcher.calls, 0)
				if timing == "captured" {
					next := *source
					next.Ignore = new("excluded")
					source = &next
					if authority == "root declaration" {
						s.AddObject(source)
					} else {
						c.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: source}, {Obj: root}}, Complete: true}, nil)
					}
					input.enabled.Store(true)
					previous = accepted
					attemptDone := make(chan struct{})
					tasks.Go(t.Context(), "capture", func(ctx context.Context) {
						defer close(attemptDone)
						accepted = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone)
					})
					select {
					case <-input.entered:
					case <-attemptDone:
						t.Fatal("alias resolution did not reach the source view")
					case <-t.Context().Done():
						t.Fatal(t.Context().Err())
					}
				}
				if authority == "root declaration" {
					next := root.Clone()
					next.Path = "elsewhere"
					s.AddObject(next)
				} else {
					c.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: source}}, Complete: true}, nil)
				}
				if timing == "captured" {
					unblock()
					tasks.BlockTillDone()
					previous.Retire()
					assert.Equal(t, accepted.Valid(), false)
				}
				assert.Equal(t, artifact.Reads.Valid(), false)
				var consumer store.ReadSet
				consumer.Init(s, manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}, nil)
				defer consumer.Close()
				assert.Equal(t, consumer.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
				assert.Equal(t, consumer.Valid(), false)
				if current, ok := s.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact); ok && current.InputAlias && current.Reads.Valid() {
					t.Fatal("authority replacement retained a valid alias")
				}
				if changed := slices.Collect(s.Sweep()); !slices.Contains(changed, source.Named()) {
					t.Fatalf("source was not rescheduled: %v", changed)
				}
				previous = accepted
				reconcile()
				previous.Retire()
				assert.Equal(t, accepted.Valid(), true)
				assert.Equal(t, fetcher.calls, 1)
				current := s.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
				assert.Equal(t, current.InputAlias, false)
				assert.Equal(t, current.Key, "acquired")
			})
		}
	}
}

type aliasCaptureFS struct {
	fs.FS
	enabled          atomic.Bool
	once             sync.Once
	entered, release chan struct{}
}

func (f *aliasCaptureFS) Open(name string) (fs.File, error) {
	if f.enabled.Load() {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return f.FS.Open(name)
}

func TestSource_AdmissionInvalidatedBeforeFetch(t *testing.T) {
	t.Parallel()
	const path = "/virtual/invalidated-admission"
	s := store.New()
	input := &store.SourceArtifact{LocalPath: path, FS: fstest.MapFS{"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: apps}\nspec: {url: https://unreachable.invalid/source}\n")}}}
	result, err := discovery.Run(t.Context(), discovery.Config{Store: s, Input: input, Path: path})
	assert.NoError(t, err)
	source := s.ListObjects(manifest.SourceDomain, manifest.KindGitRepository)[0].(*manifest.GitRepository)
	s.SetArtifact(source.Named(), &store.SourceArtifact{InputAlias: true, FS: fstest.MapFS{}})
	tasks := task.NewBounded(2)
	c := New(s, tasks)
	fetcher := &fakeFetcher{artifact: &store.SourceArtifact{Key: "acquired", FS: fstest.MapFS{}}}
	c.Fetchers[manifest.KindGitRepository] = fetcher
	c.Configure(FetchOptions{Admission: result.Admission}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	defer s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == source.Named() && s.GetArtifact(id, store.ArtifactContent) == nil {
			next := *source
			next.URL = "https://unreachable.invalid/replacement"
			s.AddObject(&next)
		}
	}, false)()
	var accepted *base.Execution
	reconcile := func() {
		tasks.Go(t.Context(), "source", func(ctx context.Context) { accepted = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone) })
		tasks.BlockTillDone()
	}
	reconcile()
	assert.Equal(t, accepted.Valid(), false)
	assert.Equal(t, fetcher.calls, 0)
	previous := accepted
	reconcile()
	previous.Retire()
	defer accepted.Close()
	assert.Equal(t, accepted.Valid(), true)
	assert.Equal(t, fetcher.calls, 1)
}
