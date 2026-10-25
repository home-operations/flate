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

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
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
	for _, complete := range []bool{false, true} {
		for _, timing := range []string{"accepted", "in_flight", "changed_in_flight"} {
			t.Run(fmt.Sprintf("complete=%t/%s", complete, timing), func(t *testing.T) {
				t.Parallel()
				f := &fakeFetcher{artifact: &store.SourceArtifact{Key: "shared", FS: fstest.MapFS{}}}
				c, st := newController(t, map[string]src.Fetcher{manifest.KindHelmChart: f})
				chart := &manifest.HelmChartSource{Name: "shared", Namespace: "apps"}
				first := &manifest.HelmRelease{Name: "first", Namespace: "apps"}
				second := &manifest.HelmRelease{Name: "second", Namespace: "apps"}
				st.AddObjects([]manifest.BaseManifest{first, second})
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
					if accepted.Valid() || st.GetArtifact(chart.Named()) != nil {
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
					if run.Reads.GetArtifact(repo.Named()) == nil {
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
				previous := *st.GetArtifact(repo.Named()).(*store.SourceArtifact)
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
			if f.retry != (sourcecache.Policy{MaxAge: time.Hour, Retry: sourcecache.RetryConfig{Attempts: 3}}) {
				t.Fatalf("configured policy=%+v", f.retry)
			}
			art, ok := st.GetArtifact(repo.Named()).(*store.SourceArtifact)
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
					if st.GetArtifact(tc.obj.Named()) != nil {
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
		filter := change.NewFilter(change.NewSet(nil), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}, controllertest.MapLister{})
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
			f := &fakeFetcher{err: src.MissingSecretErr(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "r"}, "ca", "not found")}
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
			if st.GetArtifact(repo.Named()) == nil {
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
			repo := &manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://example/img", Interval: metav1.Duration{Duration: time.Hour}}
			fetchErr := src.MissingSecretErr(repo.Named(), secret.Name, detail)
			if tt.plain {
				fetchErr = fmt.Errorf("%w: OCIRepository ns/r: secret ns/%s %s", manifest.ErrMissingSecret, secret.Name, detail)
			}
			if tt.malformed {
				detail = "invalid credential encoding"
				fetchErr = fmt.Errorf("%w: OCIRepository ns/r: secret ns/%s %s", manifest.ErrInput, secret.Name, detail)
			}
			f := &fakeFetcher{err: fetchErr}
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
			if st.GetArtifact(repo.Named()) != nil {
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
	missing, _ := errors.AsType[*src.MissingSecretError](src.MissingSecretErr(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "ns", Name: "r"}, "ghcr-creds", "not found"))
	missing.RegistryConfig = "/registry/config.json"
	missing.RetryWithRegistryConfig = func(_ context.Context, configPath string) (*store.SourceArtifact, error) {
		f.fallbackCalls++
		f.fallbackRetry = policy
		if configPath != missing.RegistryConfig {
			return nil, fmt.Errorf("unexpected registry config %q", configPath)
		}
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
			repo := &manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://example/img", Interval: metav1.Duration{Duration: time.Hour}}
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
			if st.GetArtifact(repo.Named()) == nil {
				t.Error("fetch must store its artifact")
			}
			wantPolicy := sourcecache.Policy{MaxAge: time.Hour, Retry: sourcecache.RetryConfig{Attempts: 3}}
			if f.retry != wantPolicy || tt.wantFallback > 0 && f.fallbackRetry != wantPolicy {
				t.Fatalf("normal/fallback policies=%+v/%+v", f.retry, f.fallbackRetry)
			}
			if f.fallbackCalls != tt.wantFallback {
				t.Errorf("fallback calls = %d, want %d", f.fallbackCalls, tt.wantFallback)
			}
		})
	}
}

func TestController_ProducerBackedSecretSkipsRegistryFallback(t *testing.T) {
	t.Parallel()
	f := &fallbackFetcher{}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	addDeclaration(index, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "ghcr-creds"},
		manifest.NamedResource{Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "ns", Name: "auth"})
	c, st := newConfiguredController(t,
		map[string]src.Fetcher{manifest.KindOCIRepository: f}, FetchOptions{})
	st.SetDiscoveryIndex(index)
	repo := &manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://example/img", Interval: metav1.Duration{Duration: time.Hour}}
	st.AddObject(repo)

	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady || info.Outcome() != store.OutcomeSkipped {
		t.Fatalf("status = %+v, want Ready with a skip", info)
	}
	if f.fallbackCalls != 0 {
		t.Errorf("fallback calls = %d, want 0 for a producer-backed Secret", f.fallbackCalls)
	}
	if st.GetArtifact(repo.Named()) != nil {
		t.Error("skipped source must not store an artifact")
	}
}

func TestController_AllowMissingSecretsSkipsFailedRegistryFallback(t *testing.T) {
	t.Parallel()
	f := &fallbackFetcher{fallbackErr: fmt.Errorf("%w: registry authentication failed", manifest.ErrInput)}
	c, st := newConfiguredController(t,
		map[string]src.Fetcher{manifest.KindOCIRepository: f}, FetchOptions{AllowMissingSecrets: true})
	repo := &manifest.OCIRepository{Name: "r", Namespace: "ns", URL: "oci://example/img", Interval: metav1.Duration{Duration: time.Hour}}
	st.AddObject(repo)

	if info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: repo.Named(), ReconcileNode: c.ReconcileNode}); info.Status != store.StatusReady || info.Outcome() != store.OutcomeSkipped {
		t.Fatalf("status = %+v, want Ready with a skip", info)
	}
	if f.fallbackCalls != 1 {
		t.Errorf("fallback calls = %d, want 1 before skipping", f.fallbackCalls)
	}
	if st.GetArtifact(repo.Named()) != nil {
		t.Error("failed fallback must not store an artifact")
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
	if len(st.Productions(obj.Named(), nil)) != 0 || st.GetObject(obj.Named()) != nil || st.GetArtifact(obj.Named()) != nil {
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
	if len(blocked) != 0 || status.Status != store.StatusFailed || status.IsDependencyFailure() || fetcher.calls.Load() != 1 || st.GetArtifact(obj.Named()) != nil {
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
		if status.Status != store.StatusFailed || !status.IsDependencyFailure() || len(blocked) != 1 || blocked[0] != ids[1-i] || st.GetArtifact(id) != nil {
			t.Fatalf("contentless cycle %s = %+v, blocked=%v", id, status, blocked)
		}
	}
}
