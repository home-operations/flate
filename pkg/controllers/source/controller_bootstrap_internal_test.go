package source

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type bootstrapAuthorityFixture struct {
	store      *store.Store
	tasks      *task.Service
	controller *Controller
	fetcher    *fakeFetcher
	source     *manifest.GitRepository
	root       *manifest.Kustomization
	owner      *manifest.RawObject
}

func TestSource_AdmissionInvalidatedBeforeFetch(t *testing.T) {
	t.Parallel()
	const path = "/virtual/invalidated-admission"
	s := store.New()
	input := &store.SourceArtifact{
		LocalPath: path,
		FS: fstest.MapFS{"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" +
			"kind: GitRepository\n" +
			"metadata: {name: remote, namespace: apps}\n" +
			"spec: {url: https://unreachable.invalid/source}\n")}},
	}
	result, err := discovery.Run(t.Context(), discovery.Config{Store: s, Input: input, Path: path})
	assert.NoError(t, err)
	source := s.ListObjects(manifest.SourceDomain, manifest.KindGitRepository)[0].(*manifest.GitRepository)
	s.SetArtifact(source.Named(), &store.SourceArtifact{InputAlias: true, FS: fstest.MapFS{}})
	tasks := task.NewBounded(2)
	c := New(s, tasks)
	fetcher := &fakeFetcher{artifact: &store.SourceArtifact{Key: "acquired", FS: fstest.MapFS{}}}
	c.Fetchers[manifest.KindGitRepository] = fetcher
	c.Configure(FetchOptions{Admission: result.Admission}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	defer s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == source.Named() && s.GetArtifact(id, store.ArtifactContent) == nil {
			next := *source
			next.URL = "https://unreachable.invalid/replacement"
			s.AddObject(&next)
		}
	}, false)()
	var accepted *base.Execution
	reconcile := func() {
		tasks.Go(
			t.Context(),
			"source",
			func(ctx context.Context) { accepted = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone) },
		)
		tasks.BlockTillDone()
	}
	reconcile()
	assert.Equal(t, accepted.Valid(), false)
	assert.Equal(t, fetcher.calls, 0)
	previous := accepted
	reconcile()
	previous.Retire()
	defer accepted.Close()
	assert.Equal(t, accepted.Valid(), true)
	assert.Equal(t, fetcher.calls, 1)
}

func (f *bootstrapAuthorityFixture) replaceSource(t *testing.T, authority string) *manifest.GitRepository {
	t.Helper()
	s, c := f.store, f.controller
	source, root, owner := f.source, f.root, f.owner
	next := *source
	next.Ignore = new("excluded")
	source = &next
	if authority == "root declaration" {
		s.AddObject(source)
	} else {
		assert.NoError(t,
			controllertest.Emit(t.Context(),
				c.Controller,
				owner.Named(),
				&store.Emission{
					Children: []store.Child{
						{Obj: source},
						{Obj: root},
					},
					Complete: true,
				}))
	}
	return source
}

type aliasCaptureFS struct {
	fs.FS

	enabled          atomic.Bool
	once             sync.Once
	entered, release chan struct{}
}

func TestSource_BootstrapAliasReplacement(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"url", "ref", "ignore", "late alias"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/same-id-source"
			const url = "https://example.test/self"
			input := fstest.MapFS{
				"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" +
					"kind: GitRepository\n" +
					"metadata: {name: self, namespace: apps}\n" +
					"spec: {url: https://example.test/self}\n")},
				"excluded.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\n")},
			}
			s := store.New()
			result, err := discovery.Run(t.Context(),
				discovery.Config{
					Store: s,
					Input: &store.SourceArtifact{
						LocalPath: root,
						FS:        input,
					},
					Path:     root,
					SelfURLs: []string{url},
				})
			assert.NoError(t, err)
			id := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "self",
			}
			old, ok := s.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok || !old.InputAlias, "bootstrap did not admit an alias")
			tasks := task.NewBounded(2)
			c := New(s, tasks)
			f := &fakeFetcher{artifact: &store.SourceArtifact{Key: "patched-source", FS: fstest.MapFS{}}}
			c.Fetchers[manifest.KindGitRepository] = f
			c.Configure(FetchOptions{Admission: result.Admission}, sourcecache.RetryConfig{})
			if field == "late alias" {
				s.SetArtifact(id, nil)
			}
			c.StartLifecycle()
			if field == "late alias" {
				s.SetArtifact(id, old)
			}
			t.Cleanup(tasks.BlockTillDone)
			parent := &manifest.RawObject{
				APIVersion: manifest.FluxOperatorDomain + "/v1",
				Kind:       "FluxInstance",
				Namespace:  "apps",
				Name:       "flux",
			}
			s.AddObject(parent)
			repo, _ := s.GetObject(id).(*manifest.GitRepository)
			next := *repo
			switch field {
			case "url", "late alias":
				next.URL = "https://example.test/foreign"
			case "ref":
				next.Reference = &sourcev1.GitRepositoryRef{Branch: "other"}
			case "ignore":
				ignored := "excluded.yaml"
				next.Ignore = &ignored
			}
			assert.NoError(t,
				controllertest.Emit(t.Context(),
					c.Controller,
					parent.Named(),
					&store.Emission{
						Children: []store.Child{{Obj: &next}},
						Complete: true,
					}))
			status := controllertest.DispatchToFixpoint(t.Context(),
				t,
				controllertest.Dispatch{
					Tasks:         tasks,
					Store:         s,
					ID:            id,
					ReconcileNode: c.ReconcileNode,
				})
			assert.Equal(t, status.Status, store.StatusReady)
			actual, _ := s.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, actual == nil || actual == old, "source replacement reused stale bootstrap material")
			if field == "ignore" {
				assert.Equal(t, f.calls, 0)
				_, err := fs.ReadFile(actual.FS, "excluded.yaml")
				assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "patched ignore was not applied: %v", err)
			} else {
				assert.Equal(t, f.calls, 1)
				assert.Equal(t, actual.Key, "patched-source")
				assert.Equal(t, actual.InputAlias, false)
			}
		})
	}
}

func (f *aliasCaptureFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.enabled.Load() {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return fs.ReadDir(f.FS, name)
}

func TestSource_BootstrapAuthority(t *testing.T) {
	t.Parallel()
	for _, authority := range []string{"root declaration", "owner inventory"} {
		for _, timing := range []string{"published", "captured"} {
			t.Run(authority+"/"+timing, func(t *testing.T) {
				t.Parallel()
				testBootstrapAuthority(t, authority, timing)
			})
		}
	}
}

func testBootstrapAuthority(t *testing.T, authority, timing string) {
	t.Helper()
	input := &aliasCaptureFS{FS: fstest.MapFS{
		"source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" +
			"kind: GitRepository\n" +
			"metadata: {name: self, namespace: apps}\n" +
			"spec: {url: 'https://unreachable.invalid/self', ref: {branch: unavailable}}" +
			"\n")},
		"payload.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\n")},
	}, entered: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(input.release) })
	defer unblock()
	f := newBootstrapAuthorityFixture(t, input, authority)
	s, tasks, c, fetcher := f.store, f.tasks, f.controller, f.fetcher
	source, root, owner := f.source, f.root, f.owner
	var accepted *base.Execution
	reconcile := func() {
		tasks.Go(t.Context(),
			"source",
			func(ctx context.Context) {
				accepted = c.ReconcileNode(ctx,
					source.Named(),
					manifest.DrainNone)
			})
		tasks.BlockTillDone()
	}
	reconcile()
	defer func() { accepted.Close() }()
	artifact := s.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, artifact.InputAlias, true)
	assert.Equal(t, artifact.Reads.Valid(), true)
	assert.Equal(t, fetcher.calls, 0)
	previous := accepted
	reconcile()
	previous.Retire()
	assert.Equal(t, s.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(artifact))
	assert.Equal(t, fetcher.calls, 0)
	var consumer store.ReadSet
	consumer.Init(s,
		manifest.NamedResource{
			Group:     manifest.FluxKustomizeDomain,
			Kind:      manifest.KindKustomization,
			Namespace: "apps",
			Name:      "consumer",
		},
		nil)
	defer consumer.Close()
	assert.Equal(t, consumer.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(artifact))
	if timing == "captured" {
		source = f.replaceSource(t, authority)
		input.enabled.Store(true)
		previous = accepted
		attemptDone := make(chan struct{})
		tasks.Go(t.Context(), "capture", func(ctx context.Context) {
			defer close(attemptDone)
			accepted = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone)
		})
		select {
		case <-input.entered:
		case <-attemptDone:
			t.Fatal("alias resolution did not reach the source view")
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	if authority == "root declaration" {
		next := root.Clone()
		next.Path = "elsewhere"
		s.AddObject(next)
	} else {
		assert.NoError(t,
			controllertest.Emit(t.Context(),
				c.Controller,
				owner.Named(),
				&store.Emission{
					Children: []store.Child{{Obj: source}},
					Complete: true,
				}))
	}
	if timing == "captured" {
		unblock()
		tasks.BlockTillDone()
		previous.Retire()
		assert.Equal(t, accepted.Valid(), false)
	}
	assert.Equal(t, artifact.Reads.Valid(), false)
	assert.Equal(t, consumer.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	assert.Equal(t, consumer.Valid(), false)
	current,
		ok := s.GetArtifact(source.Named(),
		store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, ok && current.InputAlias && current.Reads.Valid(), "authority replacement retained a valid alias")
	changed := slices.Collect(s.Sweep())
	assert.FatalIf(t, !slices.Contains(changed, source.Named()), "source was not rescheduled: %v", changed)
	previous = accepted
	reconcile()
	previous.Retire()
	f.assertFetched(t, accepted)
}

func newBootstrapAuthorityFixture(t *testing.T, input *aliasCaptureFS, authority string) *bootstrapAuthorityFixture {
	t.Helper()
	const path = "/virtual/bootstrap-authority"
	s := store.New()
	result, err := discovery.Run(t.Context(),
		discovery.Config{
			Store: s,
			Input: &store.SourceArtifact{
				LocalPath: path,
				FS:        input,
			},
			Path: path,
		})
	assert.NoError(t, err)
	tasks := task.NewBounded(2)
	t.Cleanup(tasks.BlockTillDone)
	c := New(s, tasks)
	fetcher := &fakeFetcher{artifact: &store.SourceArtifact{Key: "acquired", FS: fstest.MapFS{}}}
	c.Fetchers[manifest.KindGitRepository] = fetcher
	c.Configure(FetchOptions{Admission: result.Admission}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	t.Cleanup(s.AddListener(store.EventObjectAdded, func(_ manifest.NamedResource, payload any) {
		if obj, ok := payload.(manifest.BaseManifest); ok {
			result.Admission.AddApplication(s, obj)
		}
	}, false))
	source := s.ListObjects(manifest.SourceDomain, manifest.KindGitRepository)[0].(*manifest.GitRepository)
	root := &manifest.Kustomization{
		Name:      "root",
		Namespace: source.Namespace,
		Path:      ".",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository,
			Name: source.Name,
		},
	}
	owner := &manifest.RawObject{
		APIVersion: "example.test/v1",
		Kind:       "Producer",
		Name:       "owner",
		Namespace:  source.Namespace,
	}
	if authority == "root declaration" {
		s.AddObject(root)
	} else {
		s.AddObject(owner)
		assert.NoError(t,
			controllertest.Emit(t.Context(),
				c.Controller,
				owner.Named(),
				&store.Emission{
					Children: []store.Child{
						{Obj: source},
						{Obj: root},
					},
					Complete: true,
				}))
	}
	return &bootstrapAuthorityFixture{
		store:      s,
		tasks:      tasks,
		controller: c,
		fetcher:    fetcher,
		source:     source,
		root:       root,
		owner:      owner,
	}
}

func (f *bootstrapAuthorityFixture) assertFetched(t *testing.T, accepted *base.Execution) {
	t.Helper()
	s, fetcher, source := f.store, f.fetcher, f.source
	assert.Equal(t, accepted.Valid(), true)
	assert.Equal(t, fetcher.calls, 1)
	current := s.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, current.InputAlias, false)
	assert.Equal(t, current.Key, "acquired")
}
