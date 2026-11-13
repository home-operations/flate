package source

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAdmission_SelfSourceConstructionAndRuntime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/file", "dirty local material")
	document := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindGitRepository,
		"metadata": map[string]any{"name": "self", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://fixture.invalid/self.git", "ref": map[string]any{"branch": "main"}},
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	st := store.New()
	st.AddObject(parsed.Object)
	result, err := runDiscovery(t.Context(), discovery.Config{
		Path: root, Store: st, SelfURLs: []string{"ssh://git@fixture.invalid/self"},
	})
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	tasks := task.NewBounded(2)
	controller := New(st, tasks, nil)
	controller.Configure(FetchOptions{Admission: result.Admission}, cache.RetryConfig{})
	fetcher := &fakeFetcher{err: errors.New("self source attempted remote acquisition")}
	controller.Fetchers = map[string]src.Fetcher{manifest.KindGitRepository: fetcher}
	id := parsed.Object.Named()
	before := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	document["metadata"] = map[string]any{
		"name":      "self",
		"namespace": "apps",
		"labels":    map[string]any{"rendered": "true"},
	}
	updated, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	st.AddObject(updated.Object)
	var failure error
	tasks.Go(
		t.Context(),
		"construct self source",
		func(ctx context.Context) { failure = controller.Construct(ctx, id) },
	)
	tasks.BlockTillDone()
	assert.NoError(t, failure)
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact).SameMaterial(before), true)
	tasks.Go(t.Context(), "reconcile self source", func(ctx context.Context) {
		execution := controller.ReconcileNode(ctx, id, manifest.DrainNone, schedule.Result{})
		execution.Close()
	})
	tasks.BlockTillDone()
	assert.Equal(t, fetcher.calls, 0)
	material := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, material.Reads.Valid(), true)
	data, err := fs.ReadFile(material.FS, "apps/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "dirty local material")
	status, _ := st.GetStatus(id)
	assert.Equal(t, status.Status, store.StatusReady)
}

func TestAdmission_PublishCompletesAcceptedRecord(t *testing.T) {
	t.Parallel()
	st := store.New()
	result, err := runDiscovery(t.Context(), discovery.Config{Path: t.TempDir(), Store: st})
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "shared",
	}
	first, second := aliasRequestReads(t, st, id), aliasRequestReads(t, st, id)
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	unsubscribe := st.AddListener(store.EventObjectAdded, func(added manifest.NamedResource, _ any) {
		if added == id && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}, false)
	t.Cleanup(unsubscribe)
	var published bool
	var failure error
	var workers sync.WaitGroup
	workers.Go(func() {
		published, failure = result.Admission.Publish(
			t.Context(),
			id,
			first,
			func(store.Child) (bool, error) { return true, nil },
		)
	})
	<-entered
	accepted, material, err := result.Admission.Resolve(t.Context(), id, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, material != nil, true)
	tasks := task.NewBounded(2)
	controller := New(st, tasks, nil)
	controller.Configure(FetchOptions{Admission: result.Admission}, cache.RetryConfig{})
	var constructionErr error
	tasks.Go(
		t.Context(),
		"construct source",
		func(ctx context.Context) { constructionErr = controller.Construct(ctx, id) },
	)
	tasks.BlockTillDone()
	assert.NoError(t, constructionErr)
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) != nil, true)
	status, _ := st.GetStatus(id)
	assert.Equal(t, status.Status, store.StatusReady)
	completed, err := result.Admission.Publish(t.Context(), id, second, func(child store.Child) (bool, error) {
		assert.Equal(t, child.Obj == accepted.Obj, true)
		return true, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, completed, true)
	completedArtifact := st.GetArtifact(id, store.ArtifactContent)
	assert.Equal(t, completedArtifact != nil, true)
	close(release)
	workers.Wait()
	assert.NoError(t, failure)
	assert.Equal(t, published, true)
	assert.Equal(t, st.GetObject(id) == accepted.Obj, true)
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == completedArtifact, true)
}

func aliasRequestReads(t *testing.T, st *store.Store, id manifest.NamedResource) *store.ReadSet {
	t.Helper()
	ks := &manifest.Kustomization{
		Name:      "request-" + id.Name,
		Namespace: id.Namespace,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: id.Kind, Name: id.Name, Namespace: id.Namespace},
	}
	st.AddObject(ks)
	reads := &store.ReadSet{}
	reads.Init(st, ks.Named(), nil)
	t.Cleanup(reads.Retire)
	return reads
}
