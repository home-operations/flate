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
	cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: cacheDir, Concurrency: 2}
	warm, err := New(t.Context(), cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.Bootstrap(t.Context()))
	assert.NoError(t, warm.Run(t.Context()))
	owner := testutil.KustomizationID("apps", "emitter")
	child := testutil.KustomizationID("apps", "child")
	assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/red")
	testutil.WriteFile(t, root, "apps/red/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n")
	cfg.ExternalChanges = change.NewSet([]string{"apps/red/cm.yaml"})
	fresh, err := New(t.Context(), cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, fresh.Stop()) })
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.NoError(t, fresh.Bootstrap(t.Context()))
	assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Equal(t, fresh.store.GetArtifact(owner) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(child) == nil, true)
	assert.Equal(t, fresh.store.GetObject(child).(*manifest.Kustomization).Path, "./${ROOT}")
	for range fresh.store.Sweep() {
		t.Fatal("Bootstrap left probe registrations waiting for retirement")
	}
	assert.NoError(t, fresh.Stop())
	canceled, err := New(t.Context(), cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, canceled.Stop()) })
	result, err := canceled.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ref := canceled.store.GetObject(child).(*manifest.Kustomization).SourceID()
	input := *canceled.store.GetArtifact(ref).(*store.SourceArtifact)
	input.FS = cancelRootFS{FS: input.FS, cancel: cancel}
	canceled.store.SetArtifact(ref, &input)
	err = canceled.completeBootstrap(ctx, result, cfg.ExternalChanges)
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, canceled.bootstrapped, false)
}

func TestCompleteBootstrap_Cancellation(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
	o, err := New(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, o.Stop()) })
	prepared, err := o.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = o.completeBootstrap(ctx, prepared, change.NewSet(nil))
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, o.bootstrapped, false)
	for range o.store.Sweep() {
		t.Fatal("canceled preparation left probe registrations")
	}
	assert.Equal(t, o.store.GetArtifact(testutil.KustomizationID("apps", "emitter")) == nil, true)
	assert.Equal(t, o.store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).FS != nil, true)
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
	cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2, SelfURLs: []string{"https://fixture.invalid/repo"}}
	warm, err := New(t.Context(), cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.Bootstrap(t.Context()))
	assert.NoError(t, warm.Run(t.Context()))
	owner := (&manifest.HelmRelease{Namespace: "apps", Name: "emitter"}).Named()
	child := testutil.KustomizationID("apps", "child")
	assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/blue")
	testutil.WriteFile(t, root, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n")
	cfg.ExternalChanges = change.NewSet([]string{"apps/blue/cm.yaml"})
	fresh, err := New(t.Context(), cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, fresh.Stop()) })
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.NoError(t, fresh.Bootstrap(t.Context()))
	assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(owner) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(child) == nil, true)
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
	o, err := New(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, o.Stop()) })
	prepared, err := o.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := *o.store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	input.FS = cancelProbeFS{FS: input.FS, cancel: cancel}
	o.store.SetArtifact(manifest.BootstrapSourceID, &input)
	err = o.completeBootstrap(ctx, prepared, change.NewSet(nil))
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, o.bootstrapped, false)
	owner := testutil.KustomizationID("apps", "emitter")
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

			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2,
				SelfURLs:       []string{"https://fixture.invalid/repo"},
				SubstituteFrom: []SubstitutionSource{{Object: &manifest.ConfigMap{Name: "supplied", Namespace: "apps", Data: map[string]any{"ROOT": "apps/red"}}}},
				Substitute:     map[string]string{"ROOT": "apps/red"}, StrictSubstitutions: true,
			}
			warm, err := New(t.Context(), cfg)
			assert.NoError(t, err)
			assert.NoError(t, warm.Bootstrap(t.Context()))
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
				prepared, omitted, err := helm.Prepare(hr, helm.NewSources(reads).HelmChart, values.NewStoreProvider(reads, cfg.WipeSecrets, nil), warm.hrc.Helm.ValuesCache())
				assert.NoError(t, err)
				assert.Equal(t, len(omitted), 0)
				_, receipt, err := warm.hrc.Helm.Template(t.Context(), prepared, prepared.Values, helm.RenderOptions{Options: warm.hrc.Options, DependencySubstitutions: warm.dependencySubstitutions, Sources: helm.NewSources(reads), WipeSecrets: cfg.WipeSecrets})
				assert.NoError(t, err)
				assert.NoError(t, warm.hrc.Helm.RecordRender(t.Context(), owner, receipt, func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}))
			} else {
				ks := reads.GetDeclaration(owner).(*manifest.Kustomization)
				prepared, err := kustomize.Prepare(ks, values.NewStoreProvider(reads, cfg.WipeSecrets, warm.substitutionSources), cfg.Substitute)
				assert.NoError(t, err)
				source := reads.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
				_, receipt, err := kustomize.Render(t.Context(), warm.ksc.Trees, kustomize.RenderRequest{Source: source, ApplyIgnore: true, SubPath: prepared.Path, Spec: prepared, DependencySubstitutions: warm.dependencySubstitutions, Substitute: cfg.Substitute, StrictSubstitutions: cfg.StrictSubstitutions}, cfg.SourceRetry)
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
			assert.NoError(t, warm.Stop())
			cfg.ExternalChanges = change.NewSet([]string{"flux/consumer.yaml"})
			fresh, err := New(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, fresh.Stop()) })
			assert.NoError(t, fresh.Bootstrap(t.Context()))
			assert.Equal(t, fresh.filter.ShouldReconcile(owner), mode == "substitute_from")
			assert.Equal(t, fresh.filter.ShouldReconcile(testutil.KustomizationID("apps", "consumer")), true)
			assert.Equal(t, fresh.store.GetArtifact(owner) == nil, true)
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
		warm, err := New(t.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Input: input, CacheDir: cacheDir, Concurrency: 2})
		assert.NoError(t, err)
		assert.NoError(t, warm.Bootstrap(t.Context()))
		assert.NoError(t, warm.Run(t.Context()))
		assert.NoError(t, warm.Stop())
	}
	enabled.Store(true)
	left, right, err := RenderTrees(ctx, Tree{RepoRoot: roots[0], Path: filepath.Join(roots[0], "flux")}, Tree{RepoRoot: roots[1], Path: filepath.Join(roots[1], "flux")}, Config{BaseInput: inputs[0], Input: inputs[1], CacheDir: cacheDir, ExternalChanges: change.NewSet([]string{"flux/emitter.yaml"}), Concurrency: 2})
	assert.Equal(t, arrived.Load(), int32(2))
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	for _, side := range []Rendered{left, right} {
		if side.Orchestrator != nil || side.Result == nil || len(side.Result.Manifests) != 0 || !errors.Is(side.Err, context.Canceled) {
			t.Fatalf("canceled history escaped failed-bootstrap result: %+v", side)
		}
	}
}

func TestBuildChangeFilter_ReclaimsProbes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"hit", "miss", "error", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			if mode == "hit" && sourcecache.RenderPersistenceDisabledReason() != "" {
				t.Skip("requires a clean VCS-stamped build for positive persistence")
			}
			root, cacheDir := t.TempDir(), t.TempDir()
			testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n")
			cfg := Config{Path: filepath.Join(root, "flux"), RepoRoot: root, CacheDir: cacheDir, Concurrency: 2}
			if mode == "hit" {
				warm, err := New(t.Context(), cfg)
				assert.NoError(t, err)
				assert.NoError(t, warm.Bootstrap(t.Context()))
				assert.NoError(t, warm.Run(t.Context()))
				assert.NoError(t, warm.Stop())
			}
			current, err := New(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, current.Stop()) })
			_, err = current.prepareBootstrap(t.Context())
			assert.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			source := *current.store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
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
				err := current.buildChangeFilter(ctx, root, change.NewSet(nil))
				if mode == "cancel" {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
				} else {
					assert.NoError(t, err)
				}
				current.store.DeleteObject(owner)
				return done
			}()
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
