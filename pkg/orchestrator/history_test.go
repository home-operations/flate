package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestRender_EntryOwnerDependsOnSibling(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"direct", "deferred_required", "deferred_optional", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [owner.yaml, sibling.yaml, settings.yaml]\n")
			extra := ""
			if mode == "deferred_required" || mode == "deferred_optional" {
				extra = "  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings, optional: " + fmt.Sprint(mode == "deferred_optional") + "}]}\n"
			}
			testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: sibling}]\n"+extra))
			testutil.WriteFile(t, root, "entry/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {SIBLING: sibling}\n")
			siblingExtra := ""
			if mode == "cycle" {
				siblingExtra = "  dependsOn: [{name: owner}]\n"
			}
			siblingDeclaration := testutil.KustomizationYAML("sibling", "apps", "  path: ./sibling\n"+siblingExtra)
			if mode == "deferred_required" || mode == "deferred_optional" {
				siblingDeclaration = testutil.KustomizationYAML("declare-sibling", "apps", "  path: ./declarations\n"+extra)
				testutil.WriteFile(t, root, "declarations/kustomization.yaml", "resources: [sibling.yaml]\n")
				testutil.WriteFile(t, root, "declarations/sibling.yaml", testutil.KustomizationYAML("${SIBLING}", "apps", "  path: ./sibling\n"))
			}
			testutil.WriteFile(t, root, "entry/sibling.yaml", siblingDeclaration)
			testutil.WriteFile(t, root, "sibling/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "sibling/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n")
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root}, Config{Concurrency: 2})
			assert.NoError(t, err)
			t.Cleanup(o.stop)
			assert.NoError(t, o.bootstrap(t.Context()))
			if !o.entryBinding.Producer.ReportHidden() {
				t.Fatalf("entry prerequisite reused its owner: %+v", o.entryBinding)
			}
			owner := testutil.KustomizationID("apps", "owner")
			if mode == "deferred_required" || mode == "deferred_optional" {
				record, _ := o.index.Lookup(owner)
				if record.Declaration == nil {
					t.Fatal("deferred owner lost its declaration")
				}
				o.configureControllers()
				var reads store.ReadSet
				reads.Init(o.store, owner, nil)
				records, _ := o.controllers.PreviousRender(t.Context(), record.Declaration, &reads)
				reads.Retire()
				if records != nil {
					t.Fatal("deferred owner accepted history before referenced substitution")
				}
				if o.store.GetObject(testutil.KustomizationID("apps", "sibling")) != nil {
					t.Fatal("deferred substitution published the effective sibling during discovery")
				}
			}
			result, err := o.collectResult(o.run(t.Context()))
			if mode == "cycle" {
				if err == nil {
					t.Fatal("authored cycle rendered successfully")
				}
				for _, name := range []string{"owner", "sibling"} {
					if message, ok := o.preflightFailure(testutil.KustomizationID("apps", name)); !ok || !strings.Contains(message, "dependency cycle detected") {
						t.Fatalf("authored cycle lost its ordinary diagnosis: %s", message)
					}
				}
				return
			}
			assert.NoError(t, err)
			sibling := testutil.KustomizationID("apps", "sibling")
			if mode == "deferred_required" || mode == "deferred_optional" {
				assert.Equal(t, o.store.EmissionOwner(sibling), testutil.KustomizationID("apps", "declare-sibling"))
			}
			for _, id := range []manifest.NamedResource{owner, sibling} {
				status, ok := o.store.GetStatus(id)
				if !ok || status.Status != store.StatusReady {
					t.Fatalf("entry prerequisite did not reconcile: %s: %+v", id, status)
				}
			}
			docs := outputByParent(result.Manifests)[sibling]
			if len(docs) != 1 || docs[0]["metadata"].(map[string]any)["name"] != "workload" {
				t.Fatalf("sibling did not render its own path: %v", docs)
			}
		})
	}
}

func TestBootstrap_EntryReceiptDependencySubstitutions(t *testing.T) {
	t.Parallel()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, declaration := range []string{"authored", "patched", "independent"} {
		t.Run(declaration, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			cacheDir := t.TempDir()
			entry := "resources: [child.yaml]\n"
			inline := "  postBuild: {substitute: {VALUE: frozen}}\n"
			if declaration == "patched" {
				entry += "patches:\n- target: {kind: Kustomization, name: child}\n  patch: '[{\"op\":\"add\",\"path\":\"/spec/postBuild\",\"value\":{\"substitute\":{\"VALUE\":\"frozen\"}}}]'\n"
				inline = ""
			}
			if declaration == "independent" {
				entry = "resources: [child.yaml, owner.yaml]\n"
				inline = ""
				testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: '${PREFIX:-dev-}ready', namespace: '${TEAM:-dev}'}]\n"))
				testutil.WriteFile(t, root, "leaf/supplier.yaml", testutil.KustomizationYAML("supplier", "control", "  path: ./empty\n  postBuild: {substitute: {PREFIX: prod-, TEAM: control}}\n"))
				testutil.WriteFile(t, root, "empty/kustomization.yaml", "resources: []\n")
			}
			testutil.WriteFile(t, root, "entry/kustomization.yaml", entry)
			testutil.WriteFile(t, root, "entry/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"+inline))
			leaf := "resources: []\n"
			if declaration == "independent" {
				leaf = "resources: [supplier.yaml]\n"
			}
			testutil.WriteFile(t, root, "leaf/kustomization.yaml", leaf)
			input := &entryBuildFS{FS: os.DirFS(root)}
			for _, lifetime := range []string{"cold", "warm"} {
				t.Run(lifetime, func(t *testing.T) {
					input.builds.Store(0)
					o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, Input: &store.SourceArtifact{LocalPath: root, FS: input}}, Config{CacheDir: cacheDir, Concurrency: 2})
					assert.NoError(t, err)
					defer o.stop()
					if declaration == "independent" {
						o.store.AddObject(&manifest.Kustomization{Name: "prod-ready", Namespace: "control", Path: "./empty"})
					}
					assert.NoError(t, o.bootstrap(t.Context()))
					o.configureControllers()
					if declaration == "independent" {
						assert.Equal(t, o.entryBinding.Producer, testutil.KustomizationID("apps", "owner"))
						owner := o.store.GetObject(o.entryBinding.Producer).(*manifest.Kustomization)
						assert.Equal(t, owner.DependsOn[0].Name, "prod-ready")
						assert.Equal(t, owner.DependsOn[0].Namespace, "control")
						assert.Equal(t, o.dependencySubstitutions.Resolve("${PREFIX}"), "prod-")
					} else {
						assert.Equal(t, o.dependencySubstitutions.Resolve("${VALUE}"), "frozen")
					}
					assert.Equal(t, input.builds.Load() == 0, lifetime == "warm")
					owner := o.entryBinding.Producer
					var reads store.ReadSet
					reads.Init(o.store, owner, nil)
					defer reads.Retire()
					records, narrow := o.controllers.PreviousRender(t.Context(), o.store.GetObject(owner), &reads)
					if records == nil || !narrow {
						t.Fatal("controller rejected the receipt discovery saved")
					}
					found := false
					for id := range records {
						found = found || id == testutil.KustomizationID("apps", "child")
					}
					if !found {
						t.Fatal("entry receipt lost its child inventory")
					}
					o.ksc.Trees.Close()
					reopened, err := sourcecache.New(t.Context(), cacheDir)
					assert.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
					o.ksc.Trees = kustomize.NewTreeCache(reopened, &o.inputs, o.tasks)
					again, complete := o.controllers.PreviousRender(t.Context(), o.store.GetObject(owner), &reads)
					if again == nil || !complete {
						t.Fatal("controller rejected the receipt after reopening persistent cache")
					}
					assert.Diff(t, maps.Collect(again), maps.Collect(records))
					assert.NoError(t, o.run(t.Context()))
				})
			}
		})
	}
}

func TestBootstrap_EntryHistoryMatchesEmission(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, mode := range []string{"authored", "literal", "empty"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			resources := "[child.yaml, bucket.yaml, generator.yaml, consumer.yaml]"
			if mode == "authored" {
				resources = "[owner.yaml, child.yaml, bucket.yaml, generator.yaml, consumer.yaml]"
			}
			if mode == "empty" {
				resources = "[owner.yaml]"
			}
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: "+resources+"\n")
			if mode != "literal" {
				testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n"))
			}
			if mode != "empty" {
				testutil.WriteFile(t, root, "entry/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n  dependsOn: [{name: '${DEP}', namespace: apps}]\n  postBuild: {substitute: {DEP: ready}}\n"))
				testutil.WriteFile(t, root, "entry/bucket.yaml", "apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata: {name: bucket, namespace: apps}\n")
				testutil.WriteFile(t, root, "entry/generator.yaml", "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps}\nspec:\n  pathPattern: '@inputs/{team}'\n  artifacts: [{name: 'artifact-{team}'}]\n")
				testutil.WriteFile(t, root, "entry/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: .\n  sourceRef: {kind: ExternalArtifact, name: artifact-blue, namespace: apps}\n"))
			}
			testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root}, Config{CacheDir: t.TempDir(), Concurrency: 2})
			assert.NoError(t, err)
			assert.NoError(t, o.bootstrap(t.Context()))
			o.configureControllers()
			owner := o.entryBinding.Producer
			readHistory := func() map[manifest.NamedResource]manifest.DiscoveryRecord {
				t.Helper()
				var reads store.ReadSet
				reads.Init(o.store, owner, nil)
				defer reads.Retire()
				records, complete := o.controllers.PreviousRender(t.Context(), o.store.GetObject(owner), &reads)
				if records == nil || !complete {
					t.Fatal("entry lacks complete controller history")
				}
				return maps.Collect(records)
			}
			before := readHistory()
			if _, self := before[owner]; self {
				t.Fatal("accepted entry history includes its own owner")
			}
			if mode == "empty" {
				assert.Equal(t, len(before), 0)
			} else {
				child := testutil.KustomizationID("apps", "child")
				assert.Diff(t, before[child].Refs, []manifest.NamedResource{testutil.KustomizationID("apps", "ready")})
				assert.Diff(t, before[child].ContentRoots, map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"leaf"}})
				consumer := testutil.KustomizationID("apps", "consumer")
				source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Name: "artifact-blue", Namespace: "apps"}
				assert.Diff(t, before[consumer].ContentRoots, map[manifest.NamedResource][]string{source: {"."}})
				generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}
				assert.Diff(t, before[source].Productions, []manifest.Production{{Owner: owner, Declaration: generator, Source: o.entryBinding.Source}})
				secret := manifest.NamedResource{Kind: manifest.KindSecret, Name: "bucket", Namespace: "apps"}
				claim := manifest.NamedResource{Group: "objectbucket.io", Kind: "ObjectBucketClaim", Name: "bucket", Namespace: "apps"}
				assert.Diff(t, before[secret].Productions, []manifest.Production{{Owner: owner, Declaration: claim, Source: o.entryBinding.Source}})
				for _, record := range before {
					if record.Authored != (manifest.NamedResource{}) || record.Parent != (manifest.NamedResource{}) || record.BuilderOwner != (manifest.NamedResource{}) || record.Path != "" || record.File != "" || record.Builder != "" || len(record.Inputs) != 0 || record.Pinned || record.Declaration != nil {
						t.Fatal("accepted history contains static-only evidence")
					}
				}
			}
			paths := []string{"leaf/value.yaml", "entry/child.yaml", "unrelated/value.yaml"}
			selection := func(records map[manifest.NamedResource]manifest.DiscoveryRecord) [][]bool {
				selected := make([][]bool, len(paths))
				for i, path := range paths {
					filter := change.NewFilter(change.NewSet([]string{path}), o.index, o.store, root)
					filter.RefineHistory(map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{owner: maps.All(records)}, map[manifest.NamedResource]bool{owner: true})
					for _, id := range append(slices.SortedFunc(maps.Keys(before), manifest.NamedResource.Compare), owner, testutil.KustomizationID("apps", "ready")) {
						selected[i] = append(selected[i], filter.ShouldReconcile(id))
					}
				}
				return selected
			}
			selected := selection(before)
			var execution *base.Execution
			o.tasks.Go(t.Context(), "entry history", func(ctx context.Context) {
				execution = o.controllers.ReconcileNode(ctx, owner, manifest.DrainNone)
			})
			o.tasks.BlockTillDone()
			assert.Equal(t, o.tasks.Failures(), int64(0))
			if execution == nil {
				t.Fatal("entry reconciliation did not run")
			}
			defer execution.Close()
			status, ok := o.store.GetStatus(owner)
			if !ok || status.Status != store.StatusReady {
				t.Fatalf("entry emission failed: %+v", status)
			}
			_, _, emitted := o.store.GetEmission(owner)
			defer emitted.Finish()
			assert.Equal(t, o.store.GetArtifact(owner, store.ArtifactContent).(*store.Emission).Complete, true)
			after := readHistory()
			assert.Diff(t, after, before)
			assert.Diff(t, selection(after), selected)
		})
	}
}

type entryBuildFS struct {
	fs.FS
	builds atomic.Int64
}

func (f *entryBuildFS) Open(name string) (fs.File, error) {
	if name == "entry/child.yaml" {
		var callers [64]uintptr
		frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
		for {
			frame, more := frames.Next()
			if frame.Function == "github.com/home-operations/flate/internal/kustomize.Build" {
				f.builds.Add(1)
				break
			}
			if !more {
				break
			}
		}
	}
	return f.FS.Open(name)
}

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
	assert.Equal(t, fresh.index.Records[child].Declaration.(*manifest.Kustomization).Path, "./${ROOT}")
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
	ref := canceled.index.Records[child].Declaration.(*manifest.Kustomization).SourceID()
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
	declaration := fresh.index.Records[owner].Declaration.(*manifest.HelmRelease)
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
		records, narrow := o.controllers.PreviousRender(ctx, o.index.Records[owner].Declaration, reads)
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
	declaration := o.index.Records[owner].Declaration.(*manifest.Kustomization).Clone()
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
				Supply:              []store.SuppliedObject{{Object: &manifest.ConfigMap{Name: "supplied", Namespace: "apps", Data: map[string]any{"ROOT": "apps/red"}}}},
				StrictSubstitutions: true,
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
			declaration := warm.index.Records[owner].Declaration
			records, narrow := warm.controllers.PreviousRender(t.Context(), declaration, reads)
			assert.Equal(t, records == nil, true)
			assert.Equal(t, narrow, false)
			if mode == "helm_values_from" {
				supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
				assert.Equal(t, warm.store.GetObject(supplied) == nil, true)
				reads.Retire()
				reconcileEntryForTest(t, warm)
				assert.Equal(t, warm.store.EmissionOwner(supplied), warm.entryBinding.Producer)
				assert.Equal(t, warm.store.GetDeclaration(supplied) != nil, true)
				reads.Init(warm.store, owner, nil)
				hr := declaration.(*manifest.HelmRelease)
				prepared, omitted, err := helm.Prepare(hr, helm.NewSources(reads).Declaration, values.NewStoreProvider(reads, cfg.WipeSecrets), warm.hrc.Helm.ValuesCache())
				assert.NoError(t, err)
				assert.Equal(t, len(omitted), 0)
				_, receipt, err := testutil.RenderMaps(warm.hrc.Helm.Template(t.Context(), prepared, prepared.Values, helm.RenderOptions{Options: warm.hrc.Options, DependencySubstitutions: warm.dependencySubstitutions, Sources: helm.NewSources(reads), WipeSecrets: cfg.WipeSecrets}))
				assert.NoError(t, err)
				assert.NoError(t, warm.hrc.Helm.RecordRender(t.Context(), owner, receipt, func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}))
			} else {
				ks := declaration.(*manifest.Kustomization)
				prepared, err := kustomize.Prepare(ks, values.NewStoreProvider(reads, cfg.WipeSecrets))
				assert.NoError(t, err)
				source := reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
				_, receipt, err := testutil.RenderMaps(kustomize.Render(t.Context(), warm.ksc.Trees, kustomize.RenderRequest{Source: source, ApplyIgnore: true, SubPath: prepared.Path, Spec: prepared, DependencySubstitutions: warm.dependencySubstitutions, StrictSubstitutions: cfg.StrictSubstitutions}, cfg.SourceRetry))
				assert.NoError(t, err)
				assert.NoError(t, warm.ksc.Trees.RecordRender(t.Context(), kustomize.RenderAcceptance{Owner: owner, Receipt: receipt, Records: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}, DependencySubstitutions: warm.dependencySubstitutions}))
			}
			records, narrow = warm.controllers.PreviousRender(t.Context(), declaration, reads)
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
			if mode == "helm_values_from" {
				assert.NoError(t, fresh.discover(t.Context()))
				fresh.completeBootstrap()
				reconcileEntryForTest(t, fresh)
				supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
				assert.Equal(t, fresh.store.EmissionOwner(supplied), fresh.entryBinding.Producer)
				assert.NoError(t, fresh.buildChangeFilter(t.Context()))
			} else {
				assert.NoError(t, fresh.bootstrap(t.Context()))
			}
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
			current.store.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace, Path: "./apps"})
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
					records, narrow := current.controllers.PreviousRender(ctx, declaration, reads)
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
