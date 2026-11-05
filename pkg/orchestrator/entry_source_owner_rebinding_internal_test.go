package orchestrator

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gitlib "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

type entryRebindingDispatcher struct {
	dagDispatcher

	t                             *testing.T
	source, owner                 manifest.NamedResource
	replaced, attempted           chan struct{}
	initialOutput                 *store.Emission
	declaration, ownerDeclaration manifest.BaseManifest
	replacement                   atomic.Bool
	rebound                       atomic.Bool
}

func (d *entryRebindingDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
) schedule.Result {
	if id == d.owner && d.replacement.Load() {
		d.o.tasks.YieldSlot(func() { <-d.attempted })
	}
	result := d.dagDispatcher.Dispatch(ctx, id, level)
	if id != d.owner {
		return result
	}
	output, _ := d.o.store.GetArtifact(d.owner, store.ArtifactContent).(*store.Emission)
	if output == nil || !output.Complete || !output.OutputReads.Valid() {
		return result
	}
	if d.replacement.CompareAndSwap(false, true) {
		d.initialOutput = output
		d.declaration = d.o.store.GetDeclaration(d.source)
		owner := d.o.store.GetDeclaration(d.owner).(*manifest.Kustomization).Clone()
		owner.Force = !owner.Force
		d.o.store.AddObject(owner)
		d.ownerDeclaration = d.o.store.GetDeclaration(d.owner)
		assert.Equal(d.t, output.OutputReads.Valid(), false)
		close(d.replaced)
	} else {
		assert.Equal(d.t, output != d.initialOutput, true)
		assert.Equal(d.t, manifest.Equal(d.declaration, d.o.store.GetDeclaration(d.source)), true)
		assert.Equal(d.t, manifest.Equal(d.ownerDeclaration, d.o.store.GetDeclaration(d.owner)), true)
		d.rebound.Store(true)
	}
	return result
}

func startEntrySourceRebinding(ctx context.Context, t *testing.T, o *Orchestrator, root string) *advisorySourceFetcher {
	t.Helper()
	fetcher := &advisorySourceFetcher{root: root}
	o.src.Fetchers[manifest.KindGitRepository] = fetcher
	assert.NoError(t, o.bootstrap(ctx))
	o.configureControllers()
	o.src.StartLifecycle()
	o.ksc.StartLifecycle()
	o.hrc.StartLifecycle()
	o.rsc.StartLifecycle()
	o.agc.StartLifecycle()
	return fetcher
}

func writeEntrySourceOwnerRebinding(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	assert.NoError(
		t,
		repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))),
	)
	testutil.WriteFile(t, root, "entry/instance.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: control}
spec:
  distribution: {version: 2.x}
  sync: {kind: GitRepository, url: https://example.test/self, ref: refs/heads/unavailable, path: ./entry}
  kustomize:
    patches:
    - target: {kind: ConfigMap}
      patch: '[{"op":"add","path":"/data","value":{"patched":"yes"}}]'
`)
	testutil.WriteFile(
		t,
		root,
		"entry/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n",
	)
	testutil.WriteFile(t, root, "entry/unrelated.yaml", "apiVersion: fluxcd.controlplane.io/v1\n"+
		"kind: FluxInstance\n"+
		"metadata: {name: installation, namespace: other}\n"+
		"spec: {distribution: {version: 2.x}}\n")
	testutil.GitCommitAll(t,
		repo,
		"fixture",
		&gitlib.CommitOptions{Author: &object.Signature{
			Name:  "test",
			Email: "test@example.test",
			When: time.Unix(0,
				0),
		}})
	return root
}

func TestDAG_EntrySourceDuringOwnerRebinding(t *testing.T) {
	t.Parallel()
	root := writeEntrySourceOwnerRebinding(t)
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "entry"), RepoRoot: root}, Config{Concurrency: 2})
	assert.NoError(t, err)
	defer o.stop()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	fetcher := startEntrySourceRebinding(ctx, t, o, root)
	source := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "control", Name: "control",
	}
	application := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "control", Name: "flux",
	}
	owner := manifest.NewEntryKustomization(source, "./entry", application).Named()
	dispatcher := &entryRebindingDispatcher{
		dagDispatcher: dagDispatcher{o}, t: t, source: source, owner: owner,
		replaced: make(chan struct{}), attempted: make(chan struct{}),
	}
	var during atomic.Pointer[base.Execution]
	var ordered atomic.Bool
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		<-dispatcher.replaced
		o.tasks.Go(ctx, "source during owner replacement", func(ctx context.Context) {
			defer close(dispatcher.attempted)
			assert.Equal(t, dispatcher.initialOutput.OutputReads.Valid(), false)
			assert.Equal(t, o.store.GetArtifact(owner, store.ArtifactContent), nil)
			result := dispatcher.dagDispatcher.Dispatch(ctx, source, manifest.DrainNone)
			execution := result.Assessment.(*base.Execution)
			during.Store(execution)
			// Pass 17 projected-source ruling: invocation authority excludes an application-owner retry gate.
			assert.Equal(t, execution.Outcome(), schedule.Terminal)
			assert.Equal(t, len(execution.Blocked), 0)
			assert.Equal(t, execution.Valid(), true)
			for gate := range execution.Reads.Gates() {
				assert.Equal(t, gate.NamedResource == owner, false)
			}
			assert.Equal(t, fetcher.calls.Load(), int64(0))
			ordered.Store(true)
		})
	}()
	defer o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.KindInfo().Render {
			o.updateDependencyGraphFor(id)
		}
	}, false)()
	scheduler := schedule.New(dispatcher, dispatcher)
	defer o.watchDAG(scheduler)()
	seeds, err := o.seedNodes()
	assert.NoError(t, err)
	scheduler.Seed(seeds)
	assert.NoError(t, scheduler.Run(ctx))
	<-probeDone
	assert.Equal(t, owner.ReportHidden(), true)
	assert.Equal(t, o.entryBinding.Producer, owner)
	assert.Equal(t, ordered.Load(), true)
	assert.Equal(t, dispatcher.replacement.Load(), true)
	assert.Equal(t, dispatcher.rebound.Load(), true)
	assert.Equal(t, during.Load().Valid(), true)
	artifact, ok := o.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	if !ok || !artifact.InputAlias || !artifact.Reads.Valid() {
		t.Fatalf("replacement did not publish an alias with accepted authority: %+v", artifact)
	}
	assert.Equal(t, fetcher.calls.Load(), int64(0))
}
