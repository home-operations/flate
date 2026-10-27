package orchestrator

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestBootstrap_AcceptedLiteralChildRoot(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	root := t.TempDir()
	cacheDir := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", `  path: ./producers
  postBuild:
    substitute:
      ROOT: apps/red
`))
	testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(t, root, "producers/child.yaml", testutil.KustomizationYAML("child", "apps", `  path: ./${ROOT}
  sourceRef:
    kind: GitRepository
    name: content
`))
	testutil.WriteFile(t, root, "apps/red/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/red/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n")
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}
	cfg := Config{CacheDir: cacheDir, Concurrency: 2}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	assert.NoError(t, warm.run(t.Context()))
	owner := testutil.KustomizationID("apps", "emitter")
	child := testutil.KustomizationID("apps", "child")
	assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/red")
	testutil.WriteFile(t, root, "apps/red/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n")
	tree.ExternalChanges = change.NewSet([]string{"apps/red/cm.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { fresh.stop() })
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.NoError(t, fresh.bootstrap(t.Context()))
	assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(child, store.ArtifactContent) == nil, true)
	assert.Equal(t, fresh.store.GetObject(child).(*manifest.Kustomization).Path, "./${ROOT}")
	for range fresh.store.Sweep() {
		t.Fatal("Bootstrap left probe registrations waiting for retirement")
	}
	fresh.stop()
	canceled, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { canceled.stop() })
	err = canceled.discover(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ref := canceled.store.GetObject(child).(*manifest.Kustomization).SourceID()
	input := *canceled.store.GetArtifact(ref, store.ArtifactContent).(*store.SourceArtifact)
	input.FS = cancelRootFS{FS: input.FS, cancel: cancel}
	canceled.store.SetArtifact(ref, &input)
	canceled.completeBootstrap()
	canceled.cfg.ExternalChanges = tree.ExternalChanges
	err = canceled.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, canceled.filter == nil, true)
}

func TestCompleteBootstrap_Cancellation(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, Config{Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	err = o.discover(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	o.completeBootstrap()
	o.cfg.ExternalChanges = change.NewSet(nil)
	err = o.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, o.filter == nil, true)
	for range o.store.Sweep() {
		t.Fatal("canceled preparation left probe registrations")
	}
	assert.Equal(t, o.store.GetArtifact(testutil.KustomizationID("apps", "emitter"), store.ArtifactContent) == nil, true)
	assert.Equal(t, o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).FS != nil, true)
}

func TestBootstrap_AcceptedOrphanHelmChildRoot(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
`)
	for _, name := range []string{"charts", "content"} {
		testutil.WriteFile(t, root, "flux/"+name+".yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+name+", namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")
	}
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/demo/templates/child.yaml", testutil.KustomizationYAML("child", "apps", `  path: ./apps/blue
  sourceRef: {kind: GitRepository, name: content}
`))
	testutil.WriteFile(t, root, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n")
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
	cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	assert.NoError(t, warm.run(t.Context()))
	owner := (&manifest.HelmRelease{Namespace: "apps", Name: "emitter"}).Named()
	child := testutil.KustomizationID("apps", "child")
	assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/blue")
	testutil.WriteFile(t, root, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n")
	tree.ExternalChanges = change.NewSet([]string{"apps/blue/cm.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { fresh.stop() })
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.NoError(t, fresh.bootstrap(t.Context()))
	assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(child, store.ArtifactContent) == nil, true)
	declaration := fresh.store.GetObject(owner).(*manifest.HelmRelease)
	declaration = declaration.Clone()
	declaration.Chart.Name = "./missing-chart"
	fresh.store.AddObject(declaration)
	for id := range fresh.store.Sweep() {
		t.Fatalf("Bootstrap left a live probe for %s", id)
	}
}

type cancelProbeFS struct {
	fs.FS
	cancel context.CancelFunc
}

func (f cancelProbeFS) Open(name string) (fs.File, error) {
	f.cancel()
	return f.FS.Open(name)
}

func TestCompleteBootstrap_CanceledLookupMiss(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	err = o.discover(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := *o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	input.FS = cancelProbeFS{FS: input.FS, cancel: cancel}
	o.store.SetArtifact(manifest.BootstrapSourceID, &input)
	o.completeBootstrap()
	owner := testutil.KustomizationID("apps", "emitter")
	// Clean builds exercise lookup cancellation; dirty builds must leave the probe unopened.
	if !o.cache.CanPersistRenders() {
		o.configureControllers()
		reads := &store.ReadSet{}
		reads.Init(o.store, owner, nil)
		defer reads.Retire()
		records, narrow := o.controllers.PreviousRender(ctx, reads)
		assert.Equal(t, records == nil, true)
		assert.Equal(t, narrow, false)
		assert.NoError(t, ctx.Err())
		return
	}
	o.cfg.ExternalChanges = change.NewSet(nil)
	err = o.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, o.filter == nil, true)
	declaration := o.store.GetObject(owner).(*manifest.Kustomization).Clone()
	declaration.Path = "./different"
	o.store.AddObject(declaration)
	for id := range o.store.Sweep() {
		t.Fatalf("canceled probe retained a live input for %s", id)
	}
}

func TestBuildChangeFilter_PreviousRenderNarrowing(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, mode := range []string{"literal", "substitute_from", "helm_values_from"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			postBuild := "  postBuild:\n    substitute: {ROOT: apps/red}\n"
			if mode == "substitute_from" {
				postBuild = "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: supplied}]\n"
			}
			testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./producers\n"+postBuild))
			testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./apps\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: target}]\n"))
			testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "producers/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: target, namespace: apps}\ndata: {value: rendered}\n")
			if mode == "helm_values_from" {
				testutil.WriteFile(t, root, "flux/emitter.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
  valuesFrom: [{kind: ConfigMap, name: supplied}]
`)
				testutil.WriteFile(t, root, "flux/charts.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")
				testutil.WriteFile(t, root, "flux/supplied.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata: {values.yaml: '{}'}\n")
				testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
			}

			tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}

			cfg := Config{CacheDir: t.TempDir(), Concurrency: 2,
				Supply:     []store.SuppliedObject{{Object: &manifest.ConfigMap{Name: "supplied", Namespace: "apps", Data: map[string]any{"ROOT": "apps/red"}}}},
				Substitute: map[string]string{"ROOT": "apps/red"}, StrictSubstitutions: true,
			}
			warm, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			assert.NoError(t, warm.bootstrap(t.Context()))
			warm.configureControllers()
			owner := testutil.KustomizationID("apps", "emitter")
			if mode == "helm_values_from" {
				owner = (&manifest.HelmRelease{Name: "emitter", Namespace: "apps"}).Named()
			}
			reads := &store.ReadSet{}
			reads.Init(warm.store, owner, nil)
			records, narrow := warm.controllers.PreviousRender(t.Context(), reads)
			assert.Equal(t, records == nil, true)
			assert.Equal(t, narrow, false)
			if mode == "helm_values_from" {
				hr := reads.GetDeclaration(owner).(*manifest.HelmRelease)
				prepared, omitted, err := helm.Prepare(hr, helm.NewSources(reads).HelmChart, values.NewStoreProvider(reads, cfg.WipeSecrets), warm.hrc.Helm.ValuesCache())
				assert.NoError(t, err)
				assert.Equal(t, len(omitted), 0)
				_, receipt, err := testutil.RenderMaps(warm.hrc.Helm.Template(t.Context(), prepared, prepared.Values, helm.RenderOptions{Options: warm.hrc.Options, DependencySubstitutions: warm.dependencySubstitutions, Sources: helm.NewSources(reads), WipeSecrets: cfg.WipeSecrets}))
				assert.NoError(t, err)
				assert.NoError(t, warm.hrc.Helm.RecordRender(t.Context(), owner, receipt, func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}))
			} else {
				ks := reads.GetDeclaration(owner).(*manifest.Kustomization)
				prepared, err := kustomize.Prepare(ks, values.NewStoreProvider(reads, cfg.WipeSecrets), cfg.Substitute)
				assert.NoError(t, err)
				source := reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
				_, receipt, err := testutil.RenderMaps(kustomize.Render(t.Context(), warm.ksc.Trees, kustomize.RenderRequest{Source: source, ApplyIgnore: true, SubPath: prepared.Path, Spec: prepared, DependencySubstitutions: warm.dependencySubstitutions, Substitute: cfg.Substitute, StrictSubstitutions: cfg.StrictSubstitutions}, cfg.SourceRetry))
				assert.NoError(t, err)
				assert.NoError(t, warm.ksc.Trees.RecordRender(t.Context(), owner, receipt, func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}))
			}
			records, narrow = warm.controllers.PreviousRender(t.Context(), reads)
			assert.Equal(t, records != nil, true)
			assert.Equal(t, narrow, mode != "substitute_from")
			for range records {
				t.Fatal("accepted empty history gained a child")
			}
			reads.Retire()
			for range warm.store.Sweep() {
			}
			warm.stop()
			tree.ExternalChanges = change.NewSet([]string{"flux/consumer.yaml"})
			fresh, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { fresh.stop() })
			assert.NoError(t, fresh.bootstrap(t.Context()))
			assert.Equal(t, fresh.filter.ShouldReconcile(owner), mode == "substitute_from")
			assert.Equal(t, fresh.filter.ShouldReconcile(testutil.KustomizationID("apps", "consumer")), true)
			assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
		})
	}
}

type cancelRootFS struct {
	fs.FS
	cancel context.CancelFunc
}

func (f cancelRootFS) Open(name string) (fs.File, error) {
	if name == "apps/red" {
		f.cancel()
	}
	return f.FS.Open(name)
}

type pairedHistoryCancelFS struct {
	fs.FS
	enabled *atomic.Bool
	once    sync.Once
	arrived *atomic.Int32
	cancel  context.CancelFunc
	done    <-chan struct{}
}

func (f *pairedHistoryCancelFS) Open(name string) (fs.File, error) {
	if name == "producers/cm.yaml" && f.enabled.Load() {
		var callers [32]uintptr
		frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, "/pkg/kustomize.(*TreeCache).PreviousRender") {
				f.once.Do(func() {
					if f.arrived.Add(1) == 2 {
						f.cancel()
					}
					<-f.done
				})
				break
			}
			if !more {
				break
			}
		}
	}
	return f.FS.Open(name)
}

func TestRenderTrees_HistoryCancellation(t *testing.T) {
	t.Parallel()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	roots := [2]string{t.TempDir(), t.TempDir()}
	cacheDir := t.TempDir()
	inputs := [2]*store.SourceArtifact{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var arrived atomic.Int32
	var enabled atomic.Bool
	for side, root := range roots {
		testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./producers\n"))
		testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "producers/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n")
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
		assert.NoError(t, err)
		input.FS = &pairedHistoryCancelFS{FS: input.FS, enabled: &enabled, arrived: &arrived, cancel: cancel, done: ctx.Done()}
		inputs[side] = input
		warm, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, Input: input}, Config{CacheDir: cacheDir, Concurrency: 2})
		assert.NoError(t, err)
		assert.NoError(t, warm.bootstrap(t.Context()))
		assert.NoError(t, warm.run(t.Context()))
		warm.stop()
	}
	enabled.Store(true)
	results, err := RenderTrees(ctx, []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, RepoRoot: roots[0], Path: filepath.Join(roots[0], "flux"), Input: inputs[0], ExternalChanges: change.NewSet([]string{"flux/emitter.yaml"})}, {ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, RepoRoot: roots[1], Path: filepath.Join(roots[1], "flux"), Input: inputs[1], ExternalChanges: change.NewSet([]string{"flux/emitter.yaml"})}}, Config{CacheDir: cacheDir, Concurrency: 2})
	assert.Equal(t, arrived.Load(), int32(2))
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	for _, side := range results {
		if side.Orchestrator != nil || side.Result == nil || len(side.Result.Manifests) != 0 || !errors.Is(side.Err, context.Canceled) {
			t.Fatalf("canceled history escaped failed-bootstrap result: %+v", side)
		}
	}
}

func TestBuildChangeFilter_ReclaimsProbes(t *testing.T) {
	for _, mode := range []string{"hit", "miss", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "hit" && sourcecache.RenderPersistenceDisabledReason() != "" {
				t.Skip("requires a clean VCS-stamped build for positive persistence")
			}
			root, cacheDir := t.TempDir(), t.TempDir()
			testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n")
			tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}
			cfg := Config{CacheDir: cacheDir, Concurrency: 2}
			if mode == "hit" {
				warm, err := newTestOrchestrator(t, tree, cfg)
				assert.NoError(t, err)
				assert.NoError(t, warm.bootstrap(t.Context()))
				assert.NoError(t, warm.run(t.Context()))
				warm.stop()
			}
			current, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { current.stop() })
			err = current.discover(t.Context())
			assert.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := *current.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			if mode == "error" {
				source.FS = failedProbeFS{source.FS}
			}
			if mode == "cancel" {
				source.FS = cancelProbeFS{FS: source.FS, cancel: cancel}
			}
			current.store.SetArtifact(manifest.BootstrapSourceID, &source)
			owner := testutil.KustomizationID("apps", "emitter")
			released := func() <-chan struct{} {
				declaration := current.store.GetObject(owner).(*manifest.Kustomization)
				done := make(chan struct{})
				runtime.AddCleanup(declaration, func(done chan struct{}) { close(done) }, done)
				current.cfg.ExternalChanges = change.NewSet(nil)
				err := current.buildChangeFilter(ctx)
				if mode == "cancel" && current.cache.CanPersistRenders() {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
					assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
					assert.Equal(t, current.filter == nil, true)
				} else {
					assert.NoError(t, err)
					assert.NoError(t, ctx.Err())
				}
				if !current.cache.CanPersistRenders() {
					reads := &store.ReadSet{}
					reads.Init(current.store, owner, nil)
					defer reads.Retire()
					records, narrow := current.controllers.PreviousRender(ctx, reads)
					assert.Equal(t, records == nil, true)
					assert.Equal(t, narrow, false)
					assert.NoError(t, ctx.Err())
				}
				current.store.DeleteObject(owner)
				return done
			}()
			for id := range current.store.Sweep() {
				t.Fatalf("preselection retained a live input for %s", id)
			}
			runtime.GC()
			wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
			defer stop()
			select {
			case <-released:
			case <-wait.Done():
				t.Fatal("preselection retained a retired probe declaration before Run")
			}
			runtime.KeepAlive(current)
		})
	}
}

type failedProbeFS struct{ fs.FS }

func (f failedProbeFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
}
