package orchestrator

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	kustomizationctrl "github.com/home-operations/flate/pkg/controllers/kustomization"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // Orchestrator setup changes process-global source policy.
func TestRender_EntryApplicationBindingLifecycle(t *testing.T) {
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
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	o.store.AddObject(bootstrap)
	o.store.SetArtifact(manifest.BootstrapSourceID, o.cfg.Input)
	o.store.UpdateStatus(manifest.BootstrapSourceID, store.StatusInfo{Status: store.StatusReady})
	seed := bindingLifecycleObject(t, testutil.KustomizationYAML("seed-owner", "apps", "  path: ./seed\n"))
	o.store.AddObject(seed.Object, seed.Document)
	controller := kustomizationctrl.New(o.store, o.tasks, o.ksc.Trees)
	controller.Configure(kustomizationctrl.Options{}, cache.RetryConfig{})
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
