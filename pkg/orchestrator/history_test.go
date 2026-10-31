package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
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

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
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
					bootstrap := o.cfg.Input
					owners, err := reopened.ProducersForPaths(t.Context(), []manifest.SourceEvidence{{Source: bootstrap.HistoryIdentity(manifest.BootstrapSourceID), Path: "entry/kustomization.yaml"}})
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(owners, owner), true)
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
			reopened := cachetest.New(t, o.cfg.CacheDir, sourcecache.New)
			bootstrap := o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			query := []manifest.SourceEvidence{{Source: bootstrap.HistoryIdentity(manifest.BootstrapSourceID), Path: "entry/kustomization.yaml"}}
			owners, err := reopened.ProducersForPaths(t.Context(), query)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(owners, owner), true)
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
					fixture := admissiontest.New(t, admissiontest.Config{Changes: change.NewSet([]string{path}), Index: o.index, Inventory: o.store, Root: root})
					proposal, failure := fixture.Prepare(t.Context(), store.Child{}, nil, map[manifest.NamedResource]struct {
						Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
						Complete bool
					}{owner: {Records: maps.All(records), Complete: true}})
					assert.NoError(t, failure)
					admissiontest.Commit(t, fixture, proposal)
					filter := fixture.Options.Filter
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
	assert.Equal(t, fresh.loader.IndexedObject(child, fresh.index.Records[child]).Obj.(*manifest.Kustomization).Path, "./${ROOT}")
	for range fresh.store.Sweep() {
		t.Fatal("Bootstrap left probe registrations waiting for retirement")
	}
	fresh.stop()
	canceled, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { canceled.stop() })
	err = canceled.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ref := canceled.loader.IndexedObject(child, canceled.index.Records[child]).Obj.(*manifest.Kustomization).SourceID()
	input := *canceled.store.GetArtifact(ref, store.ArtifactContent).(*store.SourceArtifact)
	input.FS = cancelRootFS{FS: input.FS, cancel: cancel}
	canceled.store.SetArtifact(ref, &input)
	canceled.completeBootstrap()
	canceled.cfg.ExternalChanges = tree.ExternalChanges
	err = canceled.buildChangeFilter(ctx)
	if err == nil {
		err = canceled.installAdmission(ctx)
	}
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, len(canceled.filter.KeepNames()) == 0, true)
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
	o.cfg.ExternalChanges = change.NewSet([]string{"flux/emitter.yaml"})
	err = o.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, len(o.filter.KeepNames()) == 0, true)
	for range o.store.Sweep() {
		t.Fatal("canceled preparation left probe registrations")
	}
	assert.Equal(t, o.store.GetArtifact(testutil.KustomizationID("apps", "emitter"), store.ArtifactContent) == nil, true)
	assert.Equal(t, o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).FS != nil, true)
}

func TestHistory_ProspectiveReader(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: templates, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: child, namespace: apps}
    spec:
      interval: 1m
      path: ./apps/child
      sourceRef: {kind: GitRepository, name: content}
      postBuild:
        substituteFrom: [{kind: ConfigMap, name: settings}]
`)
	testutil.WriteFile(t, root, "apps/child/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "apps/child/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: '${VALUE}'}\n")
	input := func(value string) {
		t.Helper()
		testutil.WriteFile(t, root, "flux/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: '"+value+"'}\n")
	}
	run := func(t *testing.T, tree Tree, cacheDir string) *Result {
		t.Helper()
		o, err := newTestOrchestrator(t, tree, Config{CacheDir: cacheDir, Concurrency: 2})
		assert.NoError(t, err)
		defer func() {
			o.stop()
			assert.NoError(t, o.cache.Close())
		}()
		assert.NoError(t, o.bootstrap(t.Context()))
		result, err := o.collectResult(o.run(t.Context()))
		assert.NoError(t, err)
		return result
	}
	payload := func(t *testing.T, result *Result) map[string]any {
		t.Helper()
		for _, batch := range result.Manifests {
			for _, doc := range batch.Documents {
				if manifest.DocIdentity(doc) == (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "payload", Namespace: "apps"}) {
					return doc
				}
			}
		}
		return nil
	}
	testutil.WriteFile(t, root, "flux/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: content, namespace: apps}\nspec: {url: https://fixture.invalid/repo, interval: 1m}\n")
	tree := Tree{RepoRoot: root, Path: filepath.Join(root, "flux"), SelfURLs: []string{"https://fixture.invalid/repo"}}
	input("before")
	cacheDir := t.TempDir()
	before := payload(t, run(t, tree, cacheDir))
	assert.Equal(t, before != nil, true)
	input("after")
	oracle := payload(t, run(t, tree, t.TempDir()))
	assert.Equal(t, oracle != nil, true)
	assert.Equal(t, manifest.ValueEqual(before, oracle), false)
	tree.ExternalChanges = change.NewSet([]string{"flux/settings.yaml"})
	for _, history := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_history", true: "accepted_history"}[history], func(t *testing.T) {
			if history {
				if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
					t.Skip(reason)
				}
				reopened := cachetest.New(t, cacheDir, sourcecache.New)
				producer := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "templates"}
				owners, err := reopened.ProducersForPaths(t.Context(), []manifest.SourceEvidence{{Source: manifest.SourceIdentity{Root: root}, Path: "apps/child/output.yaml"}})
				assert.NoError(t, err)
				assert.NoError(t, reopened.Close())
				if !slices.Contains(owners, producer) {
					t.Fatalf("reopened history owners = %v, want ResourceSet %s", owners, producer)
				}
			}
			var previous map[string]any
			for range 2 {
				selectedCache := cacheDir
				if !history {
					selectedCache = t.TempDir()
				}
				result := run(t, tree, selectedCache)
				got := payload(t, result)
				assert.Diff(t, got, oracle)
				if previous != nil {
					assert.Diff(t, got, previous)
				}
				previous = got
			}
		})
	}
}

type historyLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *historyLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *historyLogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer.Reset()
}

func (b *historyLogBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buffer.Bytes())
}

func TestBootstrap_AcceptedOrphanHelmChildRoot(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	var logs historyLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, mode := range []string{"inline", "values_from", "substitute_from"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  interval: 10m
  values: {root: ./apps/blue, marker: keep}
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
`)
			testutil.WriteFile(t, root, "flux/unrelated.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unrelated, namespace: apps}
spec:
  chart:
    spec:
      chart: ./charts/unrelated
      sourceRef: {kind: GitRepository, name: charts}
`)
			testutil.WriteFile(t, root, "charts/unrelated/Chart.yaml", "apiVersion: v2\nname: unrelated\nversion: 0.1.0\n")
			testutil.WriteFile(t, root, "charts/unrelated/templates/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output, namespace: apps}\n")
			testutil.WriteFile(t, root, "flux/unrelated-kustomize.yaml", testutil.KustomizationYAML("unrelated", "apps", "  path: ./apps/unrelated\n  sourceRef: {kind: GitRepository, name: content}\n"))
			testutil.WriteFile(t, root, "apps/unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-kustomize, namespace: apps}\n")
			for _, name := range []string{"charts", "content"} {
				testutil.WriteFile(t, root, "flux/"+name+".yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: "+name+", namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")
			}
			testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
			testutil.WriteFile(t, root, "charts/demo/templates/child.yaml", strings.Replace(testutil.KustomizationYAML("child", "apps", `  path: {{ .Values.root }}
  sourceRef: {kind: GitRepository, name: content}
`), "metadata: {name: child, namespace: apps}", `metadata: {name: child, namespace: apps, labels: {witness: {{ .Values.marker }}}}`, 1))
			if mode != "inline" {
				testutil.WriteFile(t, root, "flux/supplied.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata:\n  values.yaml: 'root: ./apps/blue'\n  ROOT: ./apps/blue\n")
			}
			if mode == "values_from" {
				release, err := os.ReadFile(filepath.Join(root, "flux/release.yaml"))
				assert.NoError(t, err)
				testutil.WriteFile(t, root, "flux/release.yaml", strings.Replace(string(release), "  values: {root: ./apps/blue, marker: keep}", "  values: {marker: keep}\n  valuesFrom: [{kind: ConfigMap, name: supplied}]", 1))
			}
			if mode == "substitute_from" {
				testutil.WriteFile(t, root, "flux/supplied.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: supplied, namespace: apps}\ndata: {ROOT: ./apps/blue}\n")
				testutil.WriteFile(t, root, "flux/release.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./producers\n  sourceRef: {kind: GitRepository, name: content}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: supplied}]}\n  patches:\n  - target: {kind: Kustomization, name: child}\n    patch: '[{\"op\":\"add\",\"path\":\"/spec/interval\",\"value\":\"11m\"}]'\n"))
				testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [child.yaml]\n")
				testutil.WriteFile(t, root, "producers/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: '${ROOT}'\n  sourceRef: {kind: GitRepository, name: content}\n"))
			}
			testutil.WriteFile(t, root, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n")
			tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
			cfg := Config{CacheDir: t.TempDir(), Concurrency: 2}
			warm, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			assert.NoError(t, warm.bootstrap(t.Context()))
			assert.NoError(t, warm.run(t.Context()))
			owner := testutil.HelmReleaseID("apps", "emitter")
			if mode == "substitute_from" {
				owner = testutil.KustomizationID("apps", "emitter")
			}
			child := testutil.KustomizationID("apps", "child")
			assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/blue")
			if mode == "substitute_from" {
				assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Interval.Duration, 11*time.Minute)
			}
			testutil.WriteFile(t, root, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n")
			logs.Reset()
			tree.ExternalChanges = change.NewSet([]string{"apps/blue/cm.yaml"})
			fresh, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { fresh.stop() })
			assert.NoError(t, fresh.discover(t.Context()))
			if mode != "substitute_from" {
				assert.Equal(t, fresh.loader.IndexedObject(owner, fresh.index.Records[owner]).Obj == nil, true)
			}
			assert.Equal(t, fresh.store.GetDeclaration(owner) == nil, true)
			if mode != "inline" {
				supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
				assert.Equal(t, fresh.store.GetObject(supplied) == nil, true)
			}
			assert.Equal(t, fresh.store.GetObject(child) == nil, true)
			assert.NoError(t, fresh.lookupHistory(t.Context()))
			assert.Equal(t, slices.Contains(fresh.loader.Options.HistoryOwners, owner), true)
			assert.NoError(t, fresh.prepareBootstrap(t.Context()))
			if fresh.store.GetDeclaration(owner) == nil {
				t.Fatalf("normal admission omitted the declaration of %s", owner)
			}
			if mode == "substitute_from" {
				declaration := fresh.store.GetDeclaration(owner).(*manifest.Kustomization)
				assert.Equal(t, len(declaration.Patches), 1)
				assert.Equal(t, len(declaration.PostBuild.SubstituteFrom), 1)
			} else {
				declaration := fresh.store.GetDeclaration(owner).(*manifest.HelmRelease)
				assert.Equal(t, declaration.Values["marker"], any("keep"))
				if mode == "values_from" {
					assert.Equal(t, len(declaration.ValuesFrom), 1)
				}
			}
			if mode != "inline" {
				supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
				assert.Equal(t, fresh.store.GetObject(supplied) != nil, true)
			}
			fresh.completeBootstrap()
			assert.NoError(t, fresh.buildChangeFilter(t.Context()))
			assert.NoError(t, fresh.installAdmission(t.Context()))
			assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
			assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
			assert.Equal(t, fresh.store.GetObject(child) == nil, true)
			assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
			assert.Equal(t, fresh.store.GetArtifact(child, store.ArtifactContent) == nil, true)
			unrelated := testutil.HelmReleaseID("apps", "unrelated")
			assert.Equal(t, fresh.filter.ShouldReconcile(unrelated), false)
			result, err := fresh.collectResult(fresh.run(t.Context()))
			assert.NoError(t, err)
			docs := outputByParent(result.Manifests)[child]
			if len(docs) != 1 {
				t.Fatalf("changed generated child output: %v", docs)
			}
			assert.Equal(t, docs[0]["data"].(map[string]any)["value"], "after")
			var renders [3]int
			for line := range bytes.Lines(logs.Bytes()) {
				var event struct {
					Message string `json:"msg"`
					Release string `json:"release"`
					Path    string `json:"path"`
				}
				assert.NoError(t, json.Unmarshal(line, &event))
				if event.Message == "helm: render" && event.Release == unrelated.String() {
					renders[0]++
				}
				if event.Message == "kustomize: build" && strings.HasSuffix(event.Path, "apps/blue") {
					renders[1]++
				}
				if event.Message == "kustomize: build" && strings.HasSuffix(event.Path, "apps/unrelated") {
					renders[2]++
				}
			}
			assert.Equal(t, renders, [3]int{0, 1, 0})
			if mode == "substitute_from" {
				declaration := fresh.store.GetObject(owner).(*manifest.Kustomization).Clone()
				declaration.Path = "./missing-root"
				fresh.store.AddObject(declaration)
			} else {
				declaration := fresh.store.GetObject(owner).(*manifest.HelmRelease).Clone()
				declaration.Chart.Name = "./missing-chart"
				fresh.store.AddObject(declaration)
			}
			for id := range fresh.store.Sweep() {
				t.Fatalf("Bootstrap left a live probe for %s", id)
			}

		})
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
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, ExternalChanges: change.NewSet([]string{"flux/emitter.yaml"})}, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	err = o.prepareBootstrap(t.Context())
	assert.NoError(t, err)
	owner := testutil.KustomizationID("apps", "emitter")
	o.store.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace, Path: "./apps"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := *o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	input.FS = cancelProbeFS{FS: input.FS, cancel: cancel}
	o.store.SetArtifact(manifest.BootstrapSourceID, &input)
	o.completeBootstrap()
	// Clean builds exercise lookup cancellation; dirty builds must leave the probe unopened.
	if !o.cache.CanPersistRenders() {
		o.configureControllers()
		reads := &store.ReadSet{}
		reads.Init(o.store, owner, nil)
		defer reads.Retire()
		records, narrow := o.controllers.PreviousRender(ctx, o.store.GetObject(owner), reads)
		assert.Equal(t, records == nil, true)
		assert.Equal(t, narrow, false)
		assert.NoError(t, ctx.Err())
		return
	}
	o.cfg.ExternalChanges = change.NewSet(nil)
	err = o.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, len(o.filter.KeepNames()), 0)
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
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [emitter.yaml, consumer.yaml]\n")
			postBuild := "  postBuild:\n    substitute: {ROOT: apps/red}\n"
			if mode == "substitute_from" {
				postBuild = "  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: supplied}]\n"
			}
			testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./producers\n"+postBuild))
			testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./apps\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: target}]\n"))
			testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "producers/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: target, namespace: apps}\ndata: {value: rendered}\n")
			if mode == "helm_values_from" {
				testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [emitter.yaml, consumer.yaml, charts.yaml, supplied.yaml]\n")
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
				assert.NoError(t, warm.hrc.Helm.RecordRender(t.Context(), helm.RenderAcceptance{Owner: owner, Receipt: receipt, Records: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}}))
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
				assert.NoError(t, fresh.prepareBootstrap(t.Context()))
				fresh.completeBootstrap()
				supplied := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "supplied"}
				var entryReads store.ReadSet
				entryReads.Init(fresh.store, fresh.entryBinding.Producer, nil)
				_, _, entry := entryReads.GetEmission(fresh.entryBinding.Producer)
				entry.Children = []store.Child{{Obj: declaration}, {Obj: warm.store.GetObject(supplied)}}
				entryReads.Bind(&entry)
				fresh.store.RecordEmitted(fresh.entryBinding.Producer, &entry, nil)
				entry.Finish()
				entryReads.Retire()
				assert.Equal(t, fresh.store.EmissionOwner(supplied), fresh.entryBinding.Producer)
				assert.NoError(t, fresh.buildChangeFilter(t.Context()))
				assert.NoError(t, fresh.installAdmission(t.Context()))
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
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [emitter.yaml]\n")
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
			tree.ExternalChanges = change.NewSet([]string{"flux/emitter.yaml"})
			current, err := newTestOrchestrator(t, tree, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { current.stop() })
			err = current.prepareBootstrap(t.Context())
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
				err := current.buildChangeFilter(ctx)
				if mode == "cancel" && current.cache.CanPersistRenders() {
					assert.Equal(t, errors.Is(err, context.Canceled), true)
					assert.Equal(t, errors.Is(ctx.Err(), context.Canceled), true)
					assert.Equal(t, len(current.filter.KeepNames()) == 0, true)
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

func TestRender_ChangedPathUsesStaticReaders(t *testing.T) {
	var logs historyLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: apps}
spec: {url: https://fixture.invalid/repo}
`)
	testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unrelated, namespace: apps}
spec:
  chart:
    spec:
      chart: ./charts/unrelated
      sourceRef: {kind: GitRepository, name: cluster}
`)
	testutil.WriteFile(t, root, "charts/unrelated/Chart.yaml", "apiVersion: v2\nname: unrelated\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/unrelated/templates/output.yaml", testutil.KustomizationYAML("emitted", "apps", "  path: ./apps/unrelated\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
	testutil.WriteFile(t, root, "apps/unrelated/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "apps/unrelated/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: kustomize-output, namespace: apps}\n")
	testutil.WriteFile(t, root, "unclaimed/notes.txt", "changed\n")
	testutil.WriteFile(t, root, "unclaimed/input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unclaimed, namespace: apps}\ndata: {value: changed}\n")
	counts := func() [2]int {
		var renders [2]int
		for line := range bytes.Lines(logs.Bytes()) {
			var event struct {
				Message string `json:"msg"`
				Path    string `json:"path"`
			}
			assert.NoError(t, json.Unmarshal(line, &event))
			if event.Message == "helm: render" {
				renders[0]++
			}
			if event.Message == "kustomize: build" && strings.HasSuffix(event.Path, "apps/unrelated") {
				renders[1]++
			}
		}
		return renders
	}
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
	full, err := newTestOrchestrator(t, tree, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	assert.NoError(t, full.bootstrap(t.Context()))
	assert.NoError(t, full.run(t.Context()))
	full.stop()
	assert.Equal(t, counts(), [2]int{1, 1})
	for _, file := range []string{"unclaimed/notes.txt", "unclaimed/input.yaml", "apps/unrelated/output.yaml", "charts/unrelated/templates/output.yaml"} {
		t.Run(filepath.Base(file), func(t *testing.T) {
			tree.ExternalChanges = change.NewSet([]string{file})
			if strings.HasSuffix(file, ".yaml") {
				assert.NoError(t, tree.ExternalChanges.CollectInputs(t.Context(), os.DirFS(root), file, true))
			}
			for range 2 {
				logs.Reset()
				o, err := newTestOrchestrator(t, tree, Config{CacheDir: t.TempDir(), Concurrency: 2})
				assert.NoError(t, err)
				assert.NoError(t, o.bootstrap(t.Context()))
				result, err := o.collectResult(o.run(t.Context()))
				assert.NoError(t, err)
				o.stop()
				if strings.HasPrefix(file, "apps/") || strings.HasPrefix(file, "charts/") {
					assert.Equal(t, counts(), [2]int{1, 1})
					assert.Equal(t, len(result.Manifests) > 0, true)
				} else {
					assert.Equal(t, counts(), [2]int{})
					assert.Equal(t, len(result.Manifests), 0)
					assert.Equal(t, len(o.index.Readers), 0)
				}
			}
		})
	}
}

func TestHistory_RemovedPathSourceQualification(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	var logs historyLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, source := range []string{"bootstrap", "foreign"} {
		t.Run(source, func(t *testing.T) {
			roots := [2]string{t.TempDir(), t.TempDir()}
			contentURL := "https://fixture.invalid/repo"
			if source == "foreign" {
				foreign := t.TempDir()
				repo := testutil.GitInit(t, foreign)
				testutil.WriteFile(t, foreign, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, foreign, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: foreign}\n")
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
				contentURL = "file://" + filepath.ToSlash(foreign)
			}
			cacheDir := t.TempDir()
			for _, root := range roots {
				testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: apps}
spec:
  chart:
    spec:
      chart: ./charts/demo
      sourceRef: {kind: GitRepository, name: charts}
`)
				testutil.WriteFile(t, root, "flux/unrelated.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: unrelated, namespace: apps}\nspec: {chart: {spec: {chart: ./charts/unrelated, sourceRef: {kind: GitRepository, name: charts}}}}\n")
				testutil.WriteFile(t, root, "charts/unrelated/Chart.yaml", "apiVersion: v2\nname: unrelated\nversion: 0.1.0\n")
				testutil.WriteFile(t, root, "charts/unrelated/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated, namespace: apps}\n")
				testutil.WriteFile(t, root, "flux/charts.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")
				testutil.WriteFile(t, root, "flux/content.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: content, namespace: apps}\nspec: {url: "+contentURL+"}\n")
				testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
				testutil.WriteFile(t, root, "charts/demo/templates/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./apps/blue\n  sourceRef: {kind: GitRepository, name: content}\n"))
				testutil.WriteFile(t, root, "apps/blue/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "apps/blue/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n")
			}
			var trees [2]Tree
			var configs [2]Config
			for side, root := range roots {
				trees[side] = Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}, Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
				configs[side] = Config{CacheDir: cacheDir, Concurrency: 2}
				warm, err := newTestOrchestrator(t, trees[side], configs[side])
				assert.NoError(t, err)
				_, err = warm.render(t.Context())
				assert.NoError(t, err)
			}
			assert.NoError(t, os.Remove(filepath.Join(roots[1], "apps/blue/cm.yaml")))
			testutil.WriteFile(t, roots[1], "apps/blue/kustomization.yaml", "resources: []\n")
			var current [2]*Orchestrator
			for side := range trees {
				trees[side].ExternalChanges = change.NewSet([]string{"apps/blue/cm.yaml"})
				var err error
				current[side], err = newTestOrchestrator(t, trees[side], configs[side])
				assert.NoError(t, err)
				assert.NoError(t, current[side].discover(t.Context()))
			}
			changes, err := change.Qualify(t.Context(), change.NewSet([]string{"apps/blue/cm.yaml"}), current[0].store, current[1].store)
			assert.NoError(t, err)
			changes = changes.WithReaders(current[0].index, current[1].index)
			owner := testutil.HelmReleaseID("apps", "emitter")
			child := testutil.KustomizationID("apps", "child")
			for _, o := range current {
				o.cfg.ExternalChanges = changes
				assert.NoError(t, o.bootstrap(t.Context()))
				assert.Equal(t, o.filter.ShouldReconcile(owner), source == "bootstrap")
				assert.Equal(t, o.filter.ShouldReconcile(child), source == "bootstrap")
			}
			var previousOutput [2][]byte
			for run := range 2 {
				logs.Reset()
				results, err := RenderTrees(t.Context(), trees[:], Config{CacheDir: cacheDir, Concurrency: 2})
				assert.NoError(t, err)
				for side, result := range results {
					assert.NoError(t, result.Err)
					output, err := json.Marshal(result.Result.Manifests)
					assert.NoError(t, err)
					if run != 0 {
						assert.Equal(t, string(output), string(previousOutput[side]))
					}
					previousOutput[side] = output
				}
				for line := range bytes.Lines(logs.Bytes()) {
					var event struct {
						Message string `json:"msg"`
						Release string `json:"release"`
					}
					assert.NoError(t, json.Unmarshal(line, &event))
					if event.Message == "helm: render" && event.Release == testutil.HelmReleaseID("apps", "unrelated").String() {
						t.Fatal("paired removal rendered an unrelated producer")
					}
				}
			}
		})
	}
}

func TestRender_ChangedPathThroughGeneratedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: GitRepository
    metadata: {name: generated, namespace: apps}
    spec: {url: https://fixture.invalid/repo}
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: generated, namespace: apps}
    spec:
      path: ./apps/generated
      sourceRef: {kind: GitRepository, name: generated}
`)
	testutil.WriteFile(t, root, "apps/generated/kustomization.yaml", "resources: [payload.yaml]\n")
	testutil.WriteFile(t, root, "apps/generated/payload.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: changed}\n")
	tree := Tree{ID: manifest.TreeID{Cluster: "."}, Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
	var expected map[string]any
	for _, changed := range []bool{false, true} {
		if changed {
			tree.ExternalChanges = change.NewSet([]string{"apps/generated/payload.yaml"})
		}
		results, err := RenderTrees(t.Context(), []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(results), 1)
		assert.NoError(t, results[0].Err)
		var payload map[string]any
		for _, batch := range results[0].Result.Manifests {
			for _, doc := range batch.Documents {
				name, _ := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) == manifest.KindConfigMap && name == "payload" {
					payload = doc
				}
			}
		}
		assert.Equal(t, payload != nil, true)
		if changed {
			assert.Diff(t, payload, expected)
		} else {
			expected = payload
		}
	}
}

func TestRender_ChangedPathInputFromProjectedComponent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: https://fixture.invalid/repo}\n")
	testutil.WriteFile(t, root, "flux/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: flux-system}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: consumer, namespace: flux-system}
    spec:
      path: ./apps/workload
      targetNamespace: workload
      components: [../../components/settings]
      sourceRef: {kind: GitRepository, name: cluster}
      postBuild:
        substituteFrom: [{kind: Secret, name: settings}]
    ---
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: provider, namespace: flux-system}
    spec:
      path: ./apps/configuration
      targetNamespace: flux-system
      components: [../../components/settings]
      sourceRef: {kind: GitRepository, name: cluster}
`)
	testutil.WriteFile(t, root, "apps/workload/kustomization.yaml", "resources: [payload.yaml]\n")
	testutil.WriteFile(t, root, "apps/workload/payload.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: '${SETTING}'}\n")
	testutil.WriteFile(t, root, "apps/configuration/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "components/settings/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [settings.yaml]\n")
	testutil.WriteFile(t, root, "components/settings/settings.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: settings}\ndata: {SETTING: dmFsdWU=}\n")
	tree := Tree{ID: manifest.TreeID{Cluster: "."}, Path: filepath.Join(root, "flux"), RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"}}
	var expected map[string]any
	for _, changed := range []bool{false, true} {
		if changed {
			tree.ExternalChanges = change.NewSet([]string{"apps/workload/payload.yaml"})
		}
		results, err := RenderTrees(t.Context(), []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(results), 1)
		if results[0].Err != nil {
			t.Fatalf("changed=%t: %v", changed, results[0].Err)
		}
		var payload map[string]any
		for _, batch := range results[0].Result.Manifests {
			for _, doc := range batch.Documents {
				name, _ := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) == manifest.KindConfigMap && name == "payload" {
					payload = doc
				}
			}
		}
		assert.Equal(t, payload != nil, true)
		if changed {
			assert.Diff(t, payload, expected)
		} else {
			expected = payload
		}
	}
}
