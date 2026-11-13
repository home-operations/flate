package artifactgenerator

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestArtifactGenerator_AuthoritativeDocument(t *testing.T) {
	t.Parallel()
	t.Run("malformed_copy_metadata", func(t *testing.T) {
		t.Parallel()
		fixture := generatorForTest(
			t,
			[]any{
				map[string]any{
					"name": "literal",
					"copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file", "exclude": 1}},
				},
			},
		)
		obj, doc := fixture.object, fixture.document
		before := manifest.Fingerprint(obj)
		refs := slices.Collect(manifest.References(obj, manifest.ReferenceRelations))
		assert.Equal(t, len(refs), 1)
		assert.Equal(t, refs[0].Name, "repo")
		targets := manifest.ProducerTargets(obj)
		assert.Equal(t, len(targets), 1)
		assert.Equal(t, targets[0].Name, "literal")
		st := store.New()
		st.AddObject(obj, doc)
		c := New(st, task.NewBounded(2), nil)
		var got error
		c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
			run := &execution{Controller: c}
			run.run.Init(c.Controller, obj.Named())
			defer run.run.Close()
			got = run.reconcile(ctx, obj, manifest.DrainNone)
		})
		c.Tasks.BlockTillDone()
		assert.Equal(t, c.Tasks.Failures(), 0)
		assert.Equal(t, errors.Is(got, manifest.ErrInput), true)
		assert.Equal(t, errors.Is(got, manifest.ErrFlux), true)
		assert.Equal(t, strings.HasPrefix(got.Error(), "generator spec: decode spec: "), true)
		assert.Equal(t, st.GetArtifact(obj.Named(), store.ArtifactContent) == nil, true)
		assert.Equal(t, manifest.Fingerprint(obj), before)
	})
	for _, tc := range []struct {
		name string
		edit func(map[string]any) map[string]any
	}{
		{"missing", func(map[string]any) map[string]any { return nil }},
		{"different_group", func(doc map[string]any) map[string]any { doc["apiVersion"] = "other.example/v1"; return doc }},
		{"different_kind", func(doc map[string]any) map[string]any { doc["kind"] = "Other"; return doc }},
		{"different_name", func(doc map[string]any) map[string]any {
			doc["metadata"].(map[string]any)["name"] = "other"
			return doc
		}},
		{"different_spec", func(doc map[string]any) map[string]any { doc["spec"] = map[string]any{}; return doc }},
		{"spec_marshal", func(doc map[string]any) map[string]any { return doc }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			c := New(st, task.NewBounded(2), nil)

			generatorForTestResult := generatorForTest(t, nil)
			obj, doc := generatorForTestResult.object, generatorForTestResult.document
			if tc.name == "spec_marshal" {
				obj.Spec["invalid"] = make(chan int)
			}
			doc["metadata"].(map[string]any)["annotations"] = map[string]any{
				"source.extensions.fluxcd.io/reconcile": "disabled",
			}
			doc = tc.edit(doc)
			st.AddObject(obj, doc)
			var got error
			c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
				run := &execution{Controller: c}
				run.run.Init(c.Controller, obj.Named())
				got = run.reconcile(ctx, any(obj).(*manifest.RawObject), manifest.DrainNone)
			})
			c.Tasks.BlockTillDone()
			assert.FatalIf(t, !errors.Is(
				got,
				manifest.ErrInput,
			) || !errors.Is(
				got,
				manifest.ErrFlux,
			) || st.GetArtifact(
				obj.Named(),
				store.ArtifactContent,
			) != nil, "document rejection = %v", got)
			assert.FatalIf(t, strings.Contains(
				got.Error(),
				obj.Named().String(),
			) || strings.Contains(
				got.Error(),
				"flux error",
			) || strings.Contains(
				got.Error(),
				"input error",
			), "document diagnostic repeated classification or owner: %v", got)
			assert.FatalIf(
				t,
				tc.name == "missing" && got.Error() != "missing authoritative document",
				"missing document diagnostic = %v",
				got,
			)
		})
	}
}

func TestArtifactGenerator_InputIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*store.SourceArtifact)
	}{
		{"key", func(input *store.SourceArtifact) { input.Key = "next-key" }},
		{"digest", func(input *store.SourceArtifact) { input.Digest = "next-digest" }},
		{"content", func(input *store.SourceArtifact) { input.Content = "next-content" }},
		{"revision", func(input *store.SourceArtifact) { input.Revision = "next-revision" }},
		{"root", func(input *store.SourceArtifact) { input.LocalRoot = "next-root" }},
		{"path", func(input *store.SourceArtifact) { input.LocalPath = "next-path" }},
		{"origin_presence", func(input *store.SourceArtifact) {
			input.Metadata = map[string]string{"org.opencontainers.image.revision": ""}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() {
				{
					closeErr := owner.Close()
					assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
				}
			})
			c := New(st, task.NewBounded(2), owner)

			generatorForTestResult2 := generatorForTest(
				t,
				[]any{
					map[string]any{
						"name":           "output",
						"revision":       "@repo",
						"originRevision": "@repo",
						"copy":           []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}},
					},
				},
			)
			obj, doc := generatorForTestResult2.object, generatorForTestResult2.document
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "repo",
			}
			input := store.SourceArtifact{
				Key:       "key",
				Content:   "key",
				Digest:    "digest",
				Revision:  "revision",
				LocalRoot: "root",
				LocalPath: "path",
				FS:        fstest.MapFS{"file": {Data: []byte("same bytes")}},
			}
			st.SetArtifact(repo, &input)
			runForTest(t, c, obj.Named())
			target := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindExternalArtifact,
				Namespace: "apps",
				Name:      "output",
			}
			before := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
			next := input
			tc.edit(&next)
			st.SetArtifact(repo, &next)
			runForTest(t, c, obj.Named())
			after := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(
				t,
				before.Key == after.Key || before.Digest != after.Digest,
				"input %s identity before=%+v after=%+v",
				tc.name,
				before,
				after,
			)
		})
	}
}

func TestArtifactGenerator_EmptyAndCanceledInventory(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetWorkTree(manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, (&manifest.WorkRecorder{}).Scope())
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			closeErr := owner.Close()
			assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	generatorForTestResult3 := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name": "{App}",
				"copy": []any{map[string]any{"from": "@repo/apps/{App}/file", "to": "@artifact/file"}},
			},
		},
	)
	doc := generatorForTestResult3.document
	doc["spec"].(map[string]any)["pathPattern"] = "@repo/apps/{App}"
	parseDocResult, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, doc := parseDocResult.Object, parseDocResult.Document
	assert.FatalIf(t, err != nil, "%v", err)
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:     "first",
			Content: "first",
			Digest:  "first",
			FS:      fstest.MapFS{"apps/prod/file": {Data: []byte("bytes")}},
		},
	)
	runForTest(t, c, obj.Named())
	phaseCounts1 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts1[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 2, Completed: 2}
	phaseCounts1[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 2}
	phaseCounts1[manifest.WorkAcquisition] = manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1}
	phaseCounts1[manifest.WorkEvaluation] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	phaseCounts1[manifest.WorkCache] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	assertGeneratorWork(t, st, obj.Named(), phaseCounts1)
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "prod",
	}
	accepted := st.GetArtifact(target, store.ArtifactContent)
	assert.FatalIf(t, accepted == nil, "%v", "initial output missing")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:     "empty",
			Content: "empty",
			Digest:  "empty",
			FS:      cancelOnReadDirFS{FS: fstest.MapFS{}, cancel: cancel},
		},
	)
	var got error
	c.Tasks.Go(t.Context(), "generator", func(context.Context) {
		run := &execution{Controller: c}
		run.run.Init(c.Controller, obj.Named())
		t.Cleanup(run.run.Close)
		got = run.reconcile(ctx, any(obj).(*manifest.RawObject), manifest.DrainNone)
	})
	c.Tasks.BlockTillDone()
	phaseCounts2 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts2[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 2, Completed: 2}
	phaseCounts2[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 1}
	assertGeneratorWork(t, st, obj.Named(), phaseCounts2)
	assert.FatalIf(
		t,
		!errors.Is(got, context.Canceled) || st.GetArtifact(target, store.ArtifactContent) != accepted,
		"canceled evaluation = %v; output=%v",
		got,
		st.GetArtifact(target, store.ArtifactContent),
	)
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:      "empty",
			Content:  "empty",
			Digest:   "empty",
			Metadata: map[string]string{"fresh": "readable"},
			FS:       fstest.MapFS{},
		},
	)
	runForTest(t, c, obj.Named())
	phaseCounts3 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts3[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 2, Completed: 2}
	phaseCounts3[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 1}
	assertGeneratorWork(t, st, obj.Named(), phaseCounts3)
	emission := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !emission.Complete || len(
		emission.Children,
	) != 0 || emission.Fingerprint == "" || st.GetArtifact(
		target,
		store.ArtifactContent,
	) != nil, "zero expansion inventory = %+v", emission)
}

type cancelOnReadDirFS struct {
	fs.FS

	cancel context.CancelFunc
}

func (f cancelOnReadDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(f.FS, name)
	f.cancel()
	return entries, err
}

func TestArtifactGenerator_MetadataCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			closeErr := owner.Close()
			assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	generatorForTestResult4 := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name":           "output",
				"revision":       "@repo",
				"originRevision": "@repo",
				"copy":           []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}},
			},
		},
	)
	obj, doc := generatorForTestResult4.object, generatorForTestResult4.document
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "output",
	}
	stable := &store.SourceArtifact{
		Key:      "stable",
		Content:  "stable",
		Digest:   "stable",
		Revision: "stable-revision",
		Metadata: map[string]string{"org.opencontainers.image.revision": "stable-origin"},
		FS:       fstest.MapFS{"file": {Data: []byte("stable")}},
	}
	st.SetArtifact(repo, stable)
	runForTest(t, c, obj.Named())
	checkCapturedMetadata(t, st, target, "stable")
	var replaced atomic.Bool
	first := &observedFS{FS: fstest.MapFS{"file": {Data: []byte("first")}}}
	first.onOpen = func(name string) {
		if name == "file" && replaced.CompareAndSwap(false, true) {
			st.SetArtifact(
				repo,
				&store.SourceArtifact{
					Key:      "next",
					Content:  "next",
					Digest:   "next",
					Revision: "next-revision",
					Metadata: map[string]string{"org.opencontainers.image.revision": "next-origin"},
					FS:       fstest.MapFS{"file": {Data: []byte("next")}},
				},
			)
		}
	}
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:      "first",
			Content:  "first",
			Digest:   "first",
			Revision: "first-revision",
			Metadata: map[string]string{"org.opencontainers.image.revision": "first-origin"},
			FS:       first,
		},
	)
	var work generatorWork
	tasks := &convergenceTasks{Service: c.Tasks, generator: obj.Named(), work: &work, completed: func(string) {}}
	dispatcher := &convergenceDispatcher{generator: c, id: obj.Named(), work: &work}
	scheduler := schedule.New(tasks, dispatcher)
	defer st.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, value any) {
		if id == target {
			{
				artifact, ok := value.(*store.SourceArtifact)
				assert.ErrorIf(
					t,
					ok && artifact.Revision == "first-revision",
					"%v",
					"stale evaluation replaced the accepted bundle",
				)
			}
		}
		scheduler.OnPublication()
	}, false)()
	defer st.AddListener(
		store.EventStatusUpdated,
		func(manifest.NamedResource, any) { scheduler.OnPublication() },
		false,
	)()
	scheduler.Seed([]schedule.NodeID{obj.Named()})
	{
		err := scheduler.Run(t.Context())
		assert.FatalIf(t, err != nil, "%v", err)
	}
	c.Tasks.BlockTillDone()
	assert.FatalIf(
		t,
		!replaced.Load() || work.dispatched.Load() != 2 || c.Tasks.Failures() != 0,
		"metadata successor work = %+v",
		work.snapshot(),
	)
	checkCapturedMetadata(t, st, target, "next")
}

func TestArtifactGenerator_MutableInput(t *testing.T) {
	t.Parallel()
	for _, identity := range []struct{ name, key, digest, revision string }{
		{"missing_key", "", "digest", ""},
		{"missing_generation", "family", "", ""},
		{"metadata_without_content", "family", "digest", "revision"},
	} {
		t.Run(identity.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			owner, err := cache.New(t.Context(), "")
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() {
				{
					closeErr := owner.Close()
					assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
				}
			})
			c := New(st, tasks, owner)

			generatorForTestResult5 := generatorForTest(
				t,
				[]any{
					map[string]any{
						"name": "output",
						"copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}},
					},
				},
			)
			obj, doc := generatorForTestResult5.object, generatorForTestResult5.document
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "repo",
			}
			target := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindExternalArtifact,
				Namespace: "apps",
				Name:      "output",
			}
			for _, value := range []string{"first", "second"} {
				st.SetArtifact(
					repo,
					&store.SourceArtifact{
						Key:      identity.key,
						Digest:   identity.digest,
						Revision: identity.revision,
						Metadata: map[string]string{"snapshot": value},
						FS:       fstest.MapFS{"file": {Data: []byte(value)}},
					},
				)
				runForTest(t, c, obj.Named())
				artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
				assert.FatalIf(t, !ok, "missing %s output", value)
				data, err := fs.ReadFile(artifact.FS, "file")
				assert.FatalIf(
					t,
					err != nil || string(data) != value,
					"mutable source = %q, %v; want %s",
					data,
					err,
					value,
				)
			}
		})
	}
}
