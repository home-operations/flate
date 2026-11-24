package orchestrator

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/gittree"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	kustomizationctrl "github.com/home-operations/flate/internal/engine/controllers/kustomization"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_EntryApplicationBindingLifecycle(t *testing.T) {
	t.Parallel()
	f := newBindingLifecycleFixture(t)
	assert.NoError(t, f.orchestrator.bootstrap(t.Context()))
	assert.Equal(t, f.orchestrator.entryBinding.Producer, testutil.KustomizationID("apps", "z"))
	assert.Equal(t, len(f.orchestrator.admission.EntryProducers()), 0)
	initial := make(chan struct{})
	f.orchestrator.controllers.Kustomization = bindingLifecycleEntryController{
		reconcileController: f.orchestrator.ksc,
		state:               f,
		initial:             initial,
	}
	assert.NoError(t, f.orchestrator.run(t.Context()))
	f.assertBinding(t, "a", 1, "a", "z")
	assert.Equal(t, f.orchestrator.store.EmissionOwner(testutil.KustomizationID("apps", "a")), f.parent)
	f.assertQuiescent(t)
	f.replaceParent(t, false)
	assert.NoError(t, f.orchestrator.runDAG(t.Context()))
	f.assertBinding(t, "z", 0, "z")
	f.assertQuiescent(t)
	seed := bindingLifecycleObject(t, testutil.KustomizationYAML("seed-owner", "apps", "  path: ./empty\n"))
	f.orchestrator.store.AddObject(seed.Object, seed.Document)
	assert.NoError(t, f.orchestrator.runDAG(t.Context()))
	assert.ErrorIf(
		t,
		f.orchestrator.entryBinding.Producer == testutil.KustomizationID("apps", "a") ||
			f.orchestrator.entryBinding.Producer == testutil.KustomizationID("apps", "z"),
		"withdrawn applier remained bound",
	)
	warnings := f.orchestrator.store.Warnings()
	for i := range warnings {
		assert.ErrorIf(
			t,
			warnings[i].Category == manifest.WarnConflictingProducers,
			"withdrawal retained a stale binding advisory",
		)
	}
}

func TestRender_NamedEntryApplicationWithdrawal(t *testing.T) {
	t.Parallel()
	for _, namespace := range []string{"explicit", "omitted"} {
		t.Run(namespace, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"cluster/kustomization.yaml",
				"resources: [source.yaml, bootstrap.yaml, workload.yaml]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"cluster/bootstrap/factory/kustomization.yaml",
				"namespace: apps\nresources: [factory.yaml]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"cluster/bootstrap.yaml",
				testutil.KustomizationYAML(
					"bootstrap",
					"apps",
					"  path: ./cluster/bootstrap/factory\n  sourceRef: {kind: GitRepository, name: self}\n",
				),
			)
			testutil.WriteFile(t, root, "cluster/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: self, namespace: apps}
spec: {url: https://fixture.invalid/repo, ref: {branch: main}}
`)
			testutil.WriteFile(
				t,
				root,
				"cluster/workload.yaml",
				testutil.KustomizationYAML(
					"workload",
					"apps",
					"  path: ./payload\n  sourceRef: {kind: GitRepository, name: self}\n",
				),
			)
			testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [config.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"payload/config.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\n",
			)
			metadata := "name: factory"
			if namespace == "explicit" {
				metadata += ", namespace: apps"
			}
			testutil.WriteFile(t, root, "cluster/bootstrap/factory/factory.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {`+metadata+`}
spec:
  resourcesTemplate: |
    apiVersion: fluxcd.controlplane.io/v1
    kind: FluxInstance
    metadata: {name: flux, namespace: apps}
    spec:
      distribution: {version: 2.6.x}
      sync: {name: sync, kind: GitRepository, url: https://fixture.invalid/repo, ref: refs/heads/main, path: ./cluster}
      kustomize:
        patches:
          - target: {kind: Kustomization, name: sync}
            patch: |-
              - op: add
                path: /spec/patches
                value:
                  - target: {kind: Kustomization, name: workload}
                    patch: |-
                      - op: add
                        path: /spec/targetNamespace
                        value: patched
`)
			workload := testutil.KustomizationID("apps", "workload")
			syncRoot := testutil.KustomizationID("apps", "sync")
			o, err := newTestOrchestrator(t, Tree{
				Path: root, RepoRoot: root, SelfURLs: []string{"https://fixture.invalid/repo"},
			}, Config{
				Concurrency: 2,
				CacheDir:    t.TempDir(),
				Selection:   selection.Request{Roots: []manifest.NamedResource{workload}},
			})
			assert.NoError(t, err)
			t.Cleanup(o.stop)
			assert.NoError(t, o.bootstrap(t.Context()))
			assert.NoError(t, o.run(t.Context()))

			factory := (&manifest.ResourceSet{Name: "factory", Namespace: "apps"}).Named()
			instance := (&manifest.RawObject{Kind: "FluxInstance", APIVersion: "fluxcd.controlplane.io/v1", Namespace: "apps", Name: "flux"}).Named()
			assert.Equal(t, o.store.EmissionOwner(instance), factory)
			assert.Equal(t, o.store.EmissionOwner(workload), syncRoot)
			patched, ok := o.store.GetDeclaration(workload).(*manifest.Kustomization)
			assert.FatalIf(t, !ok, "generated root did not emit the workload")
			assert.Equal(t, patched.TargetNamespace, "patched")
			assert.Equal(t, o.selection.Snapshot().OrdinaryDemand(syncRoot), true)
			assert.Equal(t, o.selection.Snapshot().Output(workload), true)
			current, ok := o.store.GetDeclaration(factory).(*manifest.ResourceSet)
			assert.FatalIf(t, !ok, "normalized ResourceSet was not admitted")
			empty := *current
			empty.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: replacement, namespace: apps}\n"
			o.store.AddObject(&empty, testutil.ObjectDocument(&empty))
			assert.NoError(t, o.runDAG(t.Context()))

			assert.Equal(t, o.store.GetDeclaration(syncRoot) == nil, true)
			assert.Equal(t, o.store.EmissionOwner(workload), manifest.NamedResource{})
			proposal, err := o.selection.Prepare(t.Context(), selection.PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, slices.Contains(proposal.Selected, workload), true)
			assert.Equal(t, slices.Contains(proposal.Ordinary, syncRoot), false)
		})
	}
}

func TestAcceptEntry_UsesSupportOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		retain bool
	}{
		{"current support descends from previous entry", true},
		{"only the receipt owner descends from previous entry", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := newBindingLifecycleFixture(t).orchestrator
			assert.NoError(t, o.bootstrap(t.Context()))
			producer := o.entryBinding.Producer
			previous := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "./former", manifest.NamedResource{})
			o.store.AddObject(previous)
			support := &manifest.ConfigMap{Name: "support", Namespace: "apps"}
			receipt := &manifest.ConfigMap{Name: "receipt", Namespace: "apps"}
			child := receipt
			if tc.retain {
				child = support
			}
			o.store.RecordEmitted(previous.Named(), &store.Emission{
				Children: []store.Child{{Obj: child}}, Complete: true,
			}, nil)
			parent, found := o.store.Parent(child.Named(), o.index)
			assert.FatalIf(
				t,
				!found || parent != previous.Named(),
				"missing previous producer relation for %s",
				child.Named(),
			)

			o.admission.AddApplication(o.store, o.store.GetDeclaration(producer))
			_, binding, err := o.admission.BindEntry(t.Context(), o.store)
			assert.NoError(t, err)
			assert.FatalIf(t, binding.Reads == nil || binding.Producer != producer, "entry proposal = %v", binding)
			defer binding.Reads.ReleaseCapture()
			defer o.store.Retire(binding.Reads)
			binding.Entry = &kustomize.EntryEvidence{Owner: receipt.Named()}
			binding.SupportOwner = support.Named()
			o.entryBinding.Producer = previous.Named()

			accepted, err := o.acceptEntry(t.Context(), &binding)
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			assert.Equal(t, o.store.GetDeclaration(previous.Named()) != nil, tc.retain)
		})
	}
}

type bindingLifecycleFixture struct {
	orchestrator *Orchestrator
	parent       manifest.NamedResource
	a, z         atomic.Int32
	released     sync.Once
}

func newBindingLifecycleFixture(t *testing.T) *bindingLifecycleFixture {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [payload.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"entry/payload.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: unchanged}\n",
	)
	testutil.WriteFile(t, root, "outside/kustomization.yaml", "resources: [applier.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"outside/applier.yaml",
		testutil.KustomizationYAML(
			"a",
			"apps",
			"  path: ./entry\n  targetNamespace: from-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "empty/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "seed/kustomization.yaml", "resources: [applier.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"seed/applier.yaml",
		testutil.KustomizationYAML("z", "apps", "  path: ./entry\n  targetNamespace: from-z\n"),
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join(root, "entry"), RepoRoot: root},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.NoError(t, err)
	stageBindingLifecycleRoot(t, o, root)
	f := &bindingLifecycleFixture{
		orchestrator: o,
		parent: manifest.NamedResource{
			Group:     manifest.FluxKustomizeDomain,
			Kind:      manifest.KindKustomization,
			Namespace: "apps",
			Name:      "zz-late-owner",
		},
	}
	f.replaceParent(t, true)
	return f
}

func bindingLifecycleObject(t *testing.T, text string) manifest.ParsedDoc {
	t.Helper()
	docs, err := manifest.DecodeDocs(strings.NewReader(text), manifest.RequiredDocuments)
	assert.NoError(t, err)
	assert.FatalIf(t, len(docs) != 1, "lifecycle fixture expected one declaration")
	parsed, err := manifest.ParseDoc(docs[0].Document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	return parsed
}

func (f *bindingLifecycleFixture) replaceParent(t *testing.T, include bool) {
	t.Helper()
	path := "./empty"
	if include {
		path = "./outside"
	}
	parent := bindingLifecycleObject(
		t,
		testutil.KustomizationYAML(
			"zz-late-owner",
			"apps",
			"  path: "+path+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	f.orchestrator.store.AddObject(parent.Object, parent.Document)
}

func (f *bindingLifecycleFixture) assertBinding(t *testing.T, name string, conflicts int, ordinary ...string) {
	t.Helper()
	o := f.orchestrator
	id := testutil.KustomizationID("apps", name)
	assert.Equal(t, o.entryBinding.Producer, id)
	assert.Equal(t, len(o.admission.EntryProducers()), 0)
	owner := o.store.EmissionOwner(id)
	if owner == (manifest.NamedResource{}) {
		owner = id
	}
	output, accepted := o.store.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
	assert.ErrorIf(
		t,
		!accepted || !output.Complete || !output.OutputReads.Valid() || output.Render == nil,
		"binding publishing owner lost its accepted rendering receipt",
	)
	var conflictCount, ownCount int
	warnings := o.store.Warnings()
	for i := range warnings {
		warning := &warnings[i]
		switch warning.Category {
		case manifest.WarnConflictingProducers:
			conflictCount++
			assert.Equal(t, warning.Count, 1)
		case "EntryExecution":
			ownCount++
		}
	}
	assert.Equal(t, conflictCount, conflicts)
	assert.Equal(t, ownCount, 1)
	// Round 9 ruling 2 keeps each ordinary applier output after the binding moves.
	want := make(map[manifest.NamedResource]int, len(ordinary))
	for _, producer := range ordinary {
		want[testutil.KustomizationID("apps", producer)] = 1
	}
	payloads := make(map[manifest.NamedResource]int)
	for _, batch := range o.collectManifests() {
		for _, document := range batch.Documents {
			if manifest.DocKind(document) == manifest.KindConfigMap &&
				manifest.DocMetadata(document).Name == "payload" {
				parent := batch.Parent.Named()
				payloads[parent]++
				assert.Equal(t, manifest.DocMetadata(document).Namespace, "from-"+parent.Name)
			}
		}
	}
	assert.Diff(t, payloads, want)
}

func (f *bindingLifecycleFixture) assertQuiescent(t *testing.T) {
	t.Helper()
	beforeA, beforeZ := f.a.Load(), f.z.Load()
	binding, warnings := f.orchestrator.entryBinding, f.orchestrator.store.Warnings()
	dispatcher := dagDispatcher{f.orchestrator}
	for range 2 {
		progressed, err := f.orchestrator.admitEntry(t.Context())
		assert.NoError(t, err)
		assert.Equal(t, progressed, false)
		assert.Equal(t, len(dispatcher.Sweep(t.Context(), true)), 0)
	}
	assert.Equal(t, f.a.Load(), beforeA)
	assert.Equal(t, f.z.Load(), beforeZ)
	assert.Equal(t, f.orchestrator.entryBinding, binding)
	assert.Diff(t, f.orchestrator.store.Warnings(), warnings)
}

type bindingLifecycleEntryController struct {
	reconcileController

	state   *bindingLifecycleFixture
	initial chan struct{}
}

func (c bindingLifecycleEntryController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	if id == c.state.parent {
		c.state.orchestrator.tasks.YieldSlot(func() {
			select {
			case <-c.initial:
			case <-ctx.Done():
			}
		})
	}
	execution := c.reconcileController.ReconcileNode(ctx, id, level, previous)
	output, ok := c.state.orchestrator.store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	if execution != nil && ok && output.Complete && output.OutputReads.Valid() {
		if id.Name == "a" || id.Name == "z" {
			execution.Reads.AddWarning(
				manifest.Warning{Category: "EntryExecution", Message: "entry controller warning"},
			)
		}
		switch id.Name {
		case "a":
			c.state.a.Add(1)
		case "z":
			c.state.z.Add(1)
			c.state.released.Do(func() { close(c.initial) })
		}
	}
	return execution
}

func stageBindingLifecycleRoot(t *testing.T, o *Orchestrator, root string) {
	t.Helper()
	view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: o.cfg.Input.FS})
	assert.NoError(t, err)
	o.cfg.Input = o.cfg.Input.WithSourceView(view)
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	o.store.AddObject(bootstrap)
	o.store.SetArtifact(manifest.BootstrapSourceID, o.cfg.Input)
	o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusInfo{Status: store.StatusReady})
	seed := bindingLifecycleObject(t, testutil.KustomizationYAML("seed-owner", "apps", "  path: ./seed\n"))
	o.store.AddObject(seed.Object, seed.Document)
	controller := kustomizationctrl.New(o.store, o.tasks, o.ksc.Trees)
	controller.Configure(kustomizationctrl.Options{Cache: o.cache}, cache.RetryConfig{})
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{
			Tasks:         o.tasks,
			Store:         o.store,
			ID:            seed.Object.Named(),
			ReconcileNode: controller.ReconcileNode,
		},
	)
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, o.store.EmissionOwner(testutil.KustomizationID("apps", "z")), seed.Object.Named())
}
