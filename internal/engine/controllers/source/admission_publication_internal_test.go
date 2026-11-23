package source

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/discovery"
	"github.com/home-operations/flate/internal/engine/schedule"
	src "github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSource_PublishCheckoutReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		extent  store.SourceExtent
		current bool
		reused  bool
	}{
		{name: "fleet", current: true},
		{name: "flux-system", current: true},
		{name: "reused-entry", current: true, reused: true},
		{name: "composed", extent: store.ExtentComposed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			const remote = "https://fixture.invalid/source"
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [gotk-sync.yaml]\n")
			testutil.WriteFile(t, root, "gotk-sync.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: %[1]s, namespace: flux-system}
spec: {url: %[2]s, interval: 1m}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sync, namespace: flux-system}
spec:
  path: ./
  prune: false
  interval: 1m
  sourceRef: {kind: GitRepository, name: %[1]s}
`, tc.name, remote))
			discovered := store.New()
			source := &manifest.GitRepository{Name: tc.name, Namespace: "flux-system", URL: remote}
			discovered.AddObject(source)
			result, err := runDiscovery(
				t.Context(), discovery.Config{Path: root, Store: discovered, SelfURLs: []string{remote}},
			)
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			id := source.Named()
			_, _, checkoutEntry, err := result.Admission.Resolve(t.Context(), id, source, nil)
			assert.NoError(t, err)
			assert.Equal(t, checkoutEntry, true)
			st := store.New()
			st.AddObject(source)
			entry := manifest.NewEntryKustomization(id, "./", manifest.NamedResource{})
			st.AddObject(entry)
			c := execution{Controller: &Controller{
				Controller: &base.Controller{Store: st}, admission: result.Admission,
			}, checkoutEntry: checkoutEntry}
			c.run.Reads.Init(st, id, nil)
			defer c.run.Reads.Retire()
			assert.Equal(t, c.run.Reads.GetDeclaration(id), manifest.BaseManifest(source))
			assert.Equal(t, c.run.Reads.GetObject(entry.Named()), manifest.BaseManifest(entry))
			const payload = "checkout material"
			artifact := &store.SourceArtifact{
				Provider: store.ProviderAlias,
				Extent:   tc.extent,
				FS:       fstest.MapFS{"payload": {Data: []byte(payload)}},
			}
			if tc.reused {
				prior := new(store.ReadSet)
				prior.Init(st, id, nil)
				defer prior.Retire()
				assert.Equal(t, prior.GetDeclaration(id), manifest.BaseManifest(source))
				artifact = artifact.WithReads(prior)
				st.SetArtifact(id, artifact)
				assert.Equal(t, artifact.Available(), true)
			}
			c.publish(artifact, tc.reused)
			view := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, view.Reads.Valid(), true)
			assert.Equal(t, view.Reads == nil, tc.current)
			c.publish(view, true)
			assert.FatalIf(
				t,
				st.GetArtifact(id, store.ArtifactContent) != view,
				"reused current artifact was republished",
			)

			applied := *source
			applied.Labels = map[string]string{"applied": "true"}
			st.RecordEmitted(
				entry.Named(),
				&store.Emission{Children: []store.Child{{Obj: &applied}}, Complete: true},
				nil,
			)
			assert.Equal(t, st.GetObject(id), manifest.BaseManifest(&applied))
			assert.Equal(t, c.run.Reads.Valid(), false)
			assert.Equal(t, view.Reads.Valid(), tc.current)
			assert.Equal(t, view.Available(), tc.current)
			if tc.current {
				current, _ := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
				assert.FatalIf(t, !current.Available(), "checkout became unreadable after declaration replacement")
				data, err := fs.ReadFile(current.FS, "payload")
				assert.NoError(t, err)
				assert.Equal(t, string(data), payload)
			}
		})
	}
}

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
	assert.Equal(
		t,
		st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact).SameMaterial(
			before,
			store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
		),
		true,
	)
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
	accepted, material, _, err := result.Admission.Resolve(t.Context(), id, nil, nil)
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
