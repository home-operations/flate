package source

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type composedSource struct {
	manifest.ExternalArtifact

	includes []manifest.ArtifactInclude
}

func (s *composedSource) Named() manifest.NamedResource {
	return manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      "TestSource",
		Namespace: s.Namespace,
		Name:      s.Name,
	}
}

func (s *composedSource) Composition() manifest.ArtifactComposition {
	return manifest.ArtifactComposition{Includes: s.includes, Policy: "source"}
}

func (s *composedSource) ReferenceCapacity() manifest.ReferenceLimits {
	return manifest.ReferenceLimits{References: len(s.includes)}
}

func compositionFixture(t *testing.T) (*Controller, *store.Store, *composedSource, *manifest.GitRepository) {
	t.Helper()
	st := store.New()
	c := New(st, task.NewBounded(2), cachetest.New(t, "", cache.New))
	input := &manifest.GitRepository{Name: "input", Namespace: "ns"}
	source := &composedSource{Name: "source", Namespace: "ns"}
	projection := (&manifest.GitRepository{Namespace: "ns", Include: []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: input.Name}, FromPath: "file", ToPath: "included"}}}).Composition()
	source.includes = projection.Includes
	c.Fetchers[source.Named().Kind] = &fakeFetcher{
		artifact: &store.SourceArtifact{
			Kind:     "TestSource",
			URL:      "original",
			Revision: "revision",
			Content:  "base",
			Key:      "family",
			FS:       fstest.MapFS{"base": {Data: []byte("base")}},
		},
	}
	st.AddObject(source)
	st.AddObject(input)
	return c, st, source, input
}

func sourcePass(t *testing.T, c *Controller, id manifest.NamedResource, previous schedule.Result) *base.Execution {
	t.Helper()
	var run *base.Execution
	c.Tasks.Go(
		t.Context(),
		"composition",
		func(ctx context.Context) { run = c.ReconcileNode(ctx, id, manifest.DrainNone, previous) },
	)
	c.Tasks.BlockTillDone()
	assert.Equal(t, c.Tasks.Failures(), 0)
	return run
}

func TestSource_IncludeWaitsForArtifact(t *testing.T) {
	t.Parallel()
	c, st, source, input := compositionFixture(t)
	refs := slices.Collect(manifest.References(source, manifest.ReferenceRelations))
	assert.Equal(t, len(refs), 1)
	assert.Equal(t, refs[0].BindsSource(), true)
	assert.Equal(t, refs[0].RequiresDeclaration, true)
	run := sourcePass(t, c, source.Named(), schedule.Result{})
	assert.Equal(t, run.Outcome(), schedule.Blocked)
	assert.Equal(t, c.Fetchers[source.Named().Kind].(*fakeFetcher).calls, 0)
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "input1", FS: fstest.MapFS{"file": {Data: []byte("included")}}},
	)
	next := sourcePass(t, c, source.Named(), schedule.Result{Assessment: run})
	run.Retire()
	assert.Equal(t, next.Outcome(), schedule.Terminal)
	artifact, _ := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no completed source")
	data, err := fs.ReadFile(artifact.FS, "included")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "included")
	assert.Equal(t, artifact.Kind, "TestSource")
	assert.Equal(t, artifact.URL, "original")
	assert.Equal(t, artifact.Revision, "revision")
	assert.Equal(t, artifact.InputAlias, false)
	assert.Equal(t, artifact.Reads.Valid(), true)
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "input2", FS: fstest.MapFS{"file": {Data: []byte("new")}}},
	)
	assert.Equal(t, artifact.Reads.Valid(), false)
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	latest := sourcePass(t, c, source.Named(), schedule.Result{Assessment: next})
	next.Retire()
	defer latest.Close()
	artifact, _ = st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no refreshed source")
	data, err = fs.ReadFile(artifact.FS, "included")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "new")
}

type deferredDeclarationDispatcher struct {
	controller *Controller
	publish    func()
}

func (d *deferredDeclarationDispatcher) Dispatch(
	ctx context.Context,
	id schedule.NodeID,
	level manifest.DrainLevel,
	previous schedule.Result,
) schedule.Result {
	return schedule.Result{Assessment: d.controller.ReconcileNode(ctx, id, level, previous)}
}

func (d *deferredDeclarationDispatcher) Sweep(ctx context.Context, quiescent bool) []schedule.NodeID {
	if quiescent && ctx.Err() == nil && d.publish != nil {
		publish := d.publish
		d.publish = nil
		d.controller.Tasks.Go(ctx, "publish source declaration", func(context.Context) { publish() })
		d.controller.Tasks.BlockTillDone()
	}
	return slices.Collect(d.controller.Store.Sweep())
}

func TestSource_IncludeDeclarationConvergesAfterWithdrawal(t *testing.T) {
	t.Parallel()
	c, st, including, input := compositionFixture(t)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input.Named(): {Authored: input.Named(), File: "input.yaml", Productions: []manifest.Production{
			{Authored: input.Named(), Source: manifest.BootstrapSourceID, File: "input.yaml"},
		}},
	}})
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Content: "before", FS: fstest.MapFS{}})
	var admission store.ReadSet
	admission.Init(st, manifest.NamedResource{}, nil)
	defer admission.Retire()
	child := store.Child{Obj: input, SourceSupport: manifest.BootstrapSourceID}
	admission.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
	fetcher := &fakeFetcher{artifact: &store.SourceArtifact{
		Content: "input", FS: fstest.MapFS{"file": {Data: []byte("included")}},
	}}
	c.Fetchers[input.Named().Kind] = fetcher
	first := sourcePass(t, c, input.Named(), schedule.Result{})
	defer first.Close()
	combined := sourcePass(t, c, including.Named(), schedule.Result{})
	defer combined.Close()
	before := st.GetArtifact(input.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, before.Reads.Valid(), true)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Content: "after", FS: fstest.MapFS{}})
	assert.Equal(t, before.Reads.Valid(), false)
	assert.Equal(t, st.GetDeclaration(input.Named()), manifest.BaseManifest(nil))
	dispatcher := &deferredDeclarationDispatcher{controller: c, publish: func() {
		assert.Equal(t, st.GetArtifact(input.Named(), store.ArtifactContent), store.Artifact(nil))
		var current store.ReadSet
		current.Init(st, manifest.NamedResource{}, nil)
		defer current.Retire()
		current.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
	}}
	scheduler := schedule.New(c.Tasks, dispatcher)
	scheduler.Seed([]schedule.NodeID{input.Named(), including.Named()})
	assert.NoError(t, scheduler.Run(t.Context()))
	assert.Equal(t, dispatcher.publish == nil, true)
	assert.Equal(t, fetcher.calls, 2)
	artifact, _ := st.GetArtifact(including.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "missing completed composition")
	assert.Equal(t, artifact.Reads.Valid(), true)
	data, err := fs.ReadFile(artifact.FS, "included")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "included")
}

func TestSource_ComposedMaterializerPublication(t *testing.T) {
	t.Parallel()
	c, st, source, input := compositionFixture(t)
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "input", FS: fstest.MapFS{"file": {Data: []byte("included")}}},
	)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "ns"}
	st.AddObject(producer)
	emit := func(value string) {
		st.RecordEmitted(
			producer.Named(),
			&store.Emission{Complete: true, Children: []store.Child{{Obj: source}}},
			map[manifest.NamedResource]*store.SourceArtifact{
				source.Named(): {Kind: "TestSource", Content: value, FS: fstest.MapFS{"raw": {Data: []byte(value)}}},
			},
		)
	}
	emit("first")
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	run := sourcePass(t, c, source.Named(), schedule.Result{})
	artifact, _ := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no materializer composition")
	assert.Equal(t, run.Reads.Valid(), true)
	data, err := fs.ReadFile(artifact.FS, "raw")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "first")
	data, err = fs.ReadFile(artifact.FS, "included")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "included")
	emit("replacement")
	assert.Equal(t, run.Reads.Valid(), false)
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	next := sourcePass(t, c, source.Named(), schedule.Result{Assessment: run})
	run.Retire()
	defer next.Close()
	artifact, _ = st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no replacement composition")
	data, err = fs.ReadFile(artifact.FS, "raw")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "replacement")
}

type compositionFetcher struct{ artifact *store.SourceArtifact }

func (f compositionFetcher) Fetch(
	_ context.Context,
	_ manifest.BaseManifest,
	_ *store.ReadSet,
	_ *cache.Policy,
) (src.PreparedFetch, error) {
	return src.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}
			return f.artifact, ctx.Err()
		},
	}, nil
}

type blockedCompositionFS struct {
	fs.FS

	once             sync.Once
	entered, release chan struct{}
}

func (f *blockedCompositionFS) Open(name string) (fs.File, error) {
	if name == "base" {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return f.FS.Open(name)
}

func TestSource_ComposeYieldsAcquisitionWait(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c, st, first, input := compositionFixture(t)
		st.SetArtifact(
			input.Named(),
			&store.SourceArtifact{Content: "input", FS: fstest.MapFS{"file": {Data: []byte("included")}}},
		)
		backing := &blockedCompositionFS{
			FS:      fstest.MapFS{"base": {Data: []byte("base")}},
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		c.Fetchers[first.Named().Kind] = compositionFetcher{&store.SourceArtifact{Content: "same-base", FS: backing}}
		second := *first
		second.Name = "second"
		st.AddObject(&second)
		unrelated := &manifest.ExternalArtifact{Name: "unrelated", Namespace: "ns"}
		st.AddObject(unrelated)
		c.Fetchers[unrelated.Named().Kind] = compositionFetcher{
			&store.SourceArtifact{Content: "unrelated", FS: fstest.MapFS{"other": {Data: []byte("other")}}},
		}
		c.Tasks.Go(
			t.Context(),
			"first",
			func(ctx context.Context) { c.ReconcileNode(ctx, first.Named(), manifest.DrainNone, schedule.Result{}) },
		)
		<-backing.entered
		c.Tasks.Go(
			t.Context(),
			"same key",
			func(ctx context.Context) { c.ReconcileNode(ctx, second.Named(), manifest.DrainNone, schedule.Result{}) },
		)
		synctest.Wait()
		var progressed atomic.Bool
		c.Tasks.Go(t.Context(), "unrelated", func(ctx context.Context) {
			c.ReconcileNode(ctx, unrelated.Named(), manifest.DrainNone, schedule.Result{})
			progressed.Store(true)
		})
		synctest.Wait()
		beforeRelease := progressed.Load()
		close(backing.release)
		c.Tasks.BlockTillDone()
		assert.Equal(t, beforeRelease, true)
		assert.Equal(t, c.Tasks.Failures(), 0)
		for _, id := range []manifest.NamedResource{first.Named(), second.Named(), unrelated.Named()} {
			artifact, _ := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, artifact == nil, "source did not complete: %s", id)
			assert.Equal(t, artifact.Reads.Valid(), true)
		}
	})
}

func TestSource_CompositionRetainsAcquisition(t *testing.T) {
	t.Parallel()
	c, st, source, input := compositionFixture(t)
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "old", FS: fstest.MapFS{"file": {Data: []byte("old")}}},
	)
	backing := &blockedCompositionFS{
		FS:      fstest.MapFS{"base": {Data: []byte("base")}},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	fetcher := &fakeFetcher{
		artifact: &store.SourceArtifact{
			Content: "base",
			FS:      backing,
			Warnings: []manifest.Warning{
				{Resource: source.Named(), Category: manifest.WarnSourceRefUnavailable, Message: "raw base warning"},
			},
		},
	}
	c.Fetchers[source.Named().Kind] = fetcher
	var run *base.Execution
	c.Tasks.Go(t.Context(), "source", func(ctx context.Context) {
		run = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone, schedule.Result{})
	})
	<-backing.entered
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "new", FS: fstest.MapFS{"file": {Data: []byte("new")}}},
	)
	close(backing.release)
	c.Tasks.BlockTillDone()
	defer run.Close()
	assert.Equal(t, fetcher.calls, 1)
	artifact, _ := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "no completed source after input invalidation")
	data, err := fs.ReadFile(artifact.FS, "included")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "new")
	assert.Equal(t, artifact.Reads.Valid(), true)
	warnings := st.Warnings()
	assert.Equal(t, len(warnings), 1)
	assert.Equal(t, warnings[0].Count, 1)
}

func TestSource_ComposedMaterializerReplacement(t *testing.T) {
	t.Parallel()
	c, st, source, input := compositionFixture(t)
	c.Fetchers[source.Named().Kind] = &materializerFetcher{}
	st.SetArtifact(
		input.Named(),
		&store.SourceArtifact{Content: "input", FS: fstest.MapFS{"file": {Data: []byte("included")}}},
	)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "ns"}
	st.AddObject(producer)
	backing := &blockedCompositionFS{
		FS:      fstest.MapFS{"base": {Data: []byte("first")}},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	emit := func(artifact *store.SourceArtifact) {
		st.RecordEmitted(
			producer.Named(),
			&store.Emission{Complete: true, Children: []store.Child{{Obj: source}}},
			map[manifest.NamedResource]*store.SourceArtifact{source.Named(): artifact},
		)
	}
	emit(&store.SourceArtifact{Content: "first", FS: backing})
	var run *base.Execution
	c.Tasks.Go(t.Context(), "source", func(ctx context.Context) {
		run = c.ReconcileNode(ctx, source.Named(), manifest.DrainNone, schedule.Result{})
	})
	<-backing.entered
	emit(&store.SourceArtifact{Content: "replacement", FS: fstest.MapFS{"base": {Data: []byte("replacement")}}})
	close(backing.release)
	c.Tasks.BlockTillDone()
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	next := sourcePass(t, c, source.Named(), schedule.Result{Assessment: run})
	run.Retire()
	artifact, _ := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, artifact == nil, "replacement did not complete")
	data, err := fs.ReadFile(artifact.FS, "base")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "replacement")
	assert.Equal(t, next.Reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
	assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
	next.Retire()
}

func TestSource_CompositionInputKinds(t *testing.T) {
	t.Parallel()
	inputs := []manifest.BaseManifest{
		&manifest.GitRepository{Name: "kind-input", Namespace: "ns"},
		&manifest.OCIRepository{Name: "kind-input", Namespace: "ns"},
		&manifest.Bucket{Name: "kind-input", Namespace: "ns"},
		&manifest.ExternalArtifact{Name: "kind-input", Namespace: "ns"},
	}
	for _, input := range inputs {
		t.Run(input.Named().Kind, func(t *testing.T) {
			t.Parallel()
			c, st, source, _ := compositionFixture(t)
			clone := *source
			include := source.includes[0]
			include.Reference.NamedResource = input.Named()
			clone.includes = []manifest.ArtifactInclude{include}
			st.AddObject(&clone)
			st.AddObject(input)
			st.SetArtifact(
				input.Named(),
				&store.SourceArtifact{Content: "equal-input", FS: fstest.MapFS{"file": {Data: []byte("included")}}},
			)
			run := sourcePass(t, c, source.Named(), schedule.Result{})
			defer run.Close()
			artifact, _ := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, artifact == nil, "no composed source for %s", input.Named().Kind)
			names, err := fs.Glob(artifact.FS, "*")
			assert.NoError(t, err)
			assert.Diff(t, names, []string{"base", "included"})
			for _, name := range names {
				data, readErr := fs.ReadFile(artifact.FS, name)
				assert.NoError(t, readErr)
				assert.Equal(t, string(data), name)
			}
		})
	}
}

func TestSource_CompositionCredentialPolicy(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"required", "allowed", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			c, st, source, _ := compositionFixture(t)
			c.Configure(FetchOptions{AllowMissingSecrets: mode == "allowed"}, cache.RetryConfig{})
			cause := src.MissingSecretErr(manifest.DiagnosticFor(source.Named(), source), "auth", "not found")
			if mode == "malformed" {
				cause = manifest.NewError(manifest.ErrInput, errors.New("malformed credentials"), "")
			}
			c.Fetchers[source.Named().Kind] = compositionCredentials{cause}
			status := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: source.Named(), ReconcileNode: c.ReconcileNode},
			)
			assert.Equal(t, st.GetArtifact(source.Named(), store.ArtifactContent), store.Artifact(nil))
			if mode == "allowed" {
				assert.Equal(t, status.Outcome(), store.OutcomeSkipped)
				warnings := st.Warnings()
				assert.Equal(t, len(warnings), 1)
				assert.Equal(t, warnings[0].Resource, source.Named())
				assert.Equal(t, warnings[0].Category, manifest.WarnSourceCredentialsUnavailable)
			} else {
				assert.Equal(t, status.Status, store.StatusFailed)
				assert.Equal(t, len(st.Warnings()), 0)
				if mode == "malformed" {
					assert.Equal(t, status.String(), "malformed credentials")
				}
			}
		})
	}
}

type compositionCredentials struct{ cause error }

func (f compositionCredentials) Fetch(
	context.Context,
	manifest.BaseManifest,
	*store.ReadSet,
	*cache.Policy,
) (src.PreparedFetch, error) {
	return src.PreparedFetch{}, f.cause
}

func TestSource_ComposedFamiliesShareBase(t *testing.T) {
	t.Parallel()
	c, st, first, input := compositionFixture(t)
	root := t.TempDir()
	c.Fetchers[first.Named().Kind] = compositionFetcher{
		&store.SourceArtifact{
			Kind:      "TestSource",
			URL:       "same-base",
			LocalPath: root,
			LocalRoot: root,
			Content:   "base",
			FS:        fstest.MapFS{"base": {Data: []byte("base")}},
		},
	}
	second := *first
	second.Name = "second"
	second.includes = slices.Clone(first.includes)
	second.includes[0].ToPath = "other"
	st.AddObject(&second)
	producer := &manifest.Kustomization{Name: "producer", Namespace: "ns"}
	st.AddObject(producer)
	child := &manifest.Kustomization{
		Name:      "child",
		Namespace: "ns",
		Path:      "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			APIVersion: manifest.SourceDomain + "/v1",
			Kind:       first.Named().Kind,
			Name:       first.Name,
		},
	}
	var initial *store.SourceArtifact
	var evidence []manifest.SourceEvidence
	var accepted store.Emission
	var publication store.ReadSet
	publication.Init(st, producer.Named(), nil)
	t.Cleanup(publication.Retire)
	for _, generation := range []string{"before", "outside-only"} {
		st.SetArtifact(
			input.Named(),
			&store.SourceArtifact{
				Content: generation,
				FS:      fstest.MapFS{"file": {Data: []byte("included")}, "outside": {Data: []byte(generation)}},
			},
		)
		run := sourcePass(t, c, first.Named(), schedule.Result{})
		t.Cleanup(run.Close)
		otherRun := sourcePass(t, c, second.Named(), schedule.Result{})
		t.Cleanup(otherRun.Close)
		a := st.GetArtifact(first.Named(), store.ArtifactContent).(*store.SourceArtifact)
		b := st.GetArtifact(second.Named(), store.ArtifactContent).(*store.SourceArtifact)
		assert.Equal(t, a.LocalPath == b.LocalPath, false)
		assert.Equal(t, a.LocalRoot, a.LocalPath)
		assert.Equal(t, b.LocalRoot, b.LocalPath)
		assert.Equal(t, a.HistoryIdentity(first.Named()) == b.HistoryIdentity(second.Named()), false)
		for _, read := range []struct {
			artifact *store.SourceArtifact
			file     string
		}{{a, "included"}, {b, "other"}} {
			data, err := fs.ReadFile(read.artifact.FS, read.file)
			assert.NoError(t, err)
			assert.Equal(t, string(data), "included")
			path := filepath.Join(read.artifact.LocalPath, read.file)
			assert.Equal(t, read.artifact.OriginPath(path), path)
		}
		_, err := fs.Stat(a.FS, "other")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = fs.Stat(b.FS, "included")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		if initial == nil {
			initial = a
			evidence = st.ChildSourcePaths(producer.Named(), []store.Child{{Obj: child}}, nil)
			_, _, accepted = publication.GetEmission(producer.Named())
			t.Cleanup(accepted.Finish)
			accepted.Children, accepted.Complete = []store.Child{{Obj: child}}, true
			st.RecordEmitted(producer.Named(), &accepted, nil)
			assert.FatalIf(t, len(evidence) != 1, "missing accepted source evidence: %+v", evidence)
			assert.Diff(t, accepted.SourcePaths(), evidence)
		} else {
			assert.Equal(t, initial.Content, a.Content)
			assert.Equal(t, initial.Key == a.Key, false)
			assert.Equal(t, initial.LocalPath, a.LocalPath)
			assert.Equal(t, initial.Reads.Valid(), false)
			assert.Equal(t, evidence[0].Source, a.HistoryIdentity(first.Named()))
			assert.Diff(t, accepted.SourcePaths(), evidence)
			assert.Diff(t, st.ChildSourcePaths(producer.Named(), []store.Child{{Obj: child}}, nil), evidence)
		}
	}
}
