package kustomize

import (
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestPrepareRender_OwnerScaleWork(t *testing.T) {
	t.Parallel()
	for _, count := range []int{10, 100, 1000, 5000} {
		t.Run(
			strconv.Itoa(count),
			func(t *testing.T) {
				t.Parallel()
				root := t.TempDir()
				input := &store.SourceArtifact{FS: os.DirFS(root), LocalPath: root}
				cache := &TreeCache{}
				keys := make(map[string]bool, count)
				for i := range count {
					name := fmt.Sprintf("owner-%04d", i)
					owner := &manifest.Kustomization{Name: name, Namespace: "apps", Path: "./" + name}
					work := &manifest.WorkRecorder{
						Key: manifest.WorkKey{Owner: owner.Named(), Source: manifest.BootstrapSourceID},
					}
					prepared, err := cache.prepareRender(
						t.Context(), &RenderRequest{
							Work:    work.Scope().RenderWork(),
							Source:  input,
							Spec:    owner,
							SubPath: name,
						},
					)
					assert.NoError(t, err)
					assert.FatalIf(
						t,
						keys[prepared.key] || prepared.key == "",
						"%v",
						"distinct owner preparation shared a raw key",
					)
					keys[prepared.key] = true
					var counts manifest.WorkCounts
					for _, record := range work.Scope().Snapshot(0).Normalize() {
						assert.Equal(t, record.Key.Owner, owner.Named())
						counts.Add(&record.Counts)
					}
					assert.Equal(
						t,
						counts,
						manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1, RawKeyPrepares: 1},
					)
				}
				assert.Equal(t, len(keys), count)
			},
		)
	}
}

func TestPrepareRender_InputGeneration(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"entry", "history", "render"} {
		t.Run(first, func(t *testing.T) {
			t.Parallel()
			testPrepareRenderInputGeneration(t, first)
		})
	}
}

func testPrepareRenderInputGeneration(t *testing.T, first string) {
	t.Helper()
	root := testutil.WriteTree(
		t,
		map[string]string{
			"kustomization.yaml": "resources: [cm.yaml]\n",
			"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n",
		},
	)
	cache := newTestTreeCache(t, t.TempDir())
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	var recorder manifest.WorkRecorder
	recorder.Init(&manifest.WorkKey{Owner: owner.Named()}, manifest.WorkScope{})
	var preparation Preparation
	defer func() { preparation = Preparation{} }()
	req := RenderRequest{
		Preparation: &preparation,
		Source: localSource(
			t,
			root,
		),
		SubPath: ".",
		SkipKinds: []string{
			"Pod",
		},
		Work: recorder.Scope().RenderWork(),
	}
	if first == "entry" {
		cache.PreviousEntry(t.Context(), req)
	}
	if first == "history" {
		cache.PreviousRender(t.Context(), req)
	}
	initial, _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	cache.PreviousRender(t.Context(), req)
	testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: second}\n")
	changed, _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.FatalIf(t, manifest.ValueEqual(initial, changed), "%v", "mutable render reused stale bytes")
	req.SkipKinds[0] = "Service"
	req.Work = recorder.Scope().RenderWork()
	_, _, err = Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	var counts manifest.WorkCounts
	records := recorder.Scope().Snapshot(0)
	for i := range records {
		record := &records[i]
		if record.Key.Phase == manifest.WorkPreparation && record.Key.Input == "" {
			counts.Add(&record.Counts)
		}
	}
	prepares := uint64(2)
	assert.Equal(
		t,
		counts,
		manifest.WorkCounts{Attempts: prepares, Completed: prepares, OwnerPrepares: prepares},
	)
	preparation = Preparation{}
	assert.Equal(
		t,
		preparation.complete || preparation.source != nil || preparation.spec != nil || preparation.lower != nil,
		false,
	)
}

//nolint:paralleltest,tparallel // Admissions share one retained preparation and mutable filesystem.
func TestPrepareRender_FilteredAdmission(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{".sourceignore": "b.yaml\n"})
	original := &store.SourceArtifact{FS: os.DirFS(root), LocalPath: root}
	cache := newTestTreeCache(t, t.TempDir())
	var recorder manifest.WorkRecorder
	recorder.Init(&manifest.WorkKey{}, manifest.WorkScope{})
	var preparation Preparation
	t.Cleanup(func() { preparation = Preparation{} })
	req := RenderRequest{
		Preparation: &preparation, Source: original.WithReads(&store.ReadSet{}),
		ApplyIgnore: true, Label: root, SkipKinds: []string{"Pod"}, Work: recorder.Scope().RenderWork(),
	}
	first, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	decoded, shaping := &decodedRender{}, &preparedShaping{}
	preparation.decoded, preparation.shaping = decoded, shaping
	for _, tc := range []struct {
		name, policy  string
		renew, retain bool
		prepares      uint64
	}{
		{"unchanged_wrapper", "b.yaml\n", true, true, 0},
		{"repeated_admission", "b.yaml\n", false, true, 0},
		{"changed_policy", "a.yaml\n", true, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder.Init(&manifest.WorkKey{}, manifest.WorkScope{})
			req.Work = recorder.Scope().RenderWork()
			previousLower := preparation.lower
			if tc.renew {
				testutil.WriteFile(t, root, ".sourceignore", tc.policy)
				req.Source = original.WithReads(&store.ReadSet{})
			}
			input, err := cache.sourceRootFor(t.Context(), req.Source, root)
			assert.NoError(t, err)
			next, err := cache.prepareRender(t.Context(), &req)
			assert.NoError(t, err)
			for _, prepared := range []*Preparation{&next.Preparation, &preparation} {
				assert.Equal(t, prepared.lower, input.fs)
				assert.Equal(t, prepared.lower != previousLower, tc.renew)
				assert.Equal(t, prepared.source, req.Source)
				assert.Equal(t, prepared.key, renderKey(&req, root, input.ignore))
				assert.Equal(t, prepared.key == first.key, tc.retain)
				assert.Equal(t, &prepared.binding.Policy.Skip[0] == &first.binding.Policy.Skip[0], tc.retain)
				if tc.retain {
					assert.Equal(t, prepared.decoded, decoded)
					assert.Equal(t, prepared.shaping, shaping)
				} else {
					assert.Equal(t, prepared.decoded == nil, true)
					assert.Equal(t, prepared.shaping == nil, true)
				}
			}
			var counts manifest.WorkCounts
			records := recorder.Scope().Snapshot(0)
			for i := range records {
				if record := &records[i]; record.Key.Phase == manifest.WorkPreparation {
					counts.Add(&record.Counts)
				}
			}
			assert.Equal(t, counts, manifest.WorkCounts{
				Attempts: tc.prepares, Completed: tc.prepares, OwnerPrepares: tc.prepares,
				RawKeyPrepares: 1,
			})
		})
	}
}

func TestPrepareRender_UncomparableSourceFS(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{"content": &fstest.MapFile{Data: []byte("current")}}
	original := &store.SourceArtifact{FS: files}
	cache := newTestTreeCache(t, t.TempDir())
	var recorder manifest.WorkRecorder
	recorder.Init(&manifest.WorkKey{}, manifest.WorkScope{})
	var preparation Preparation
	t.Cleanup(func() { preparation = Preparation{} })
	req := RenderRequest{
		Preparation: &preparation, Source: original.WithReads(&store.ReadSet{}),
		ApplyIgnore: false, SkipKinds: []string{"Pod"}, Work: recorder.Scope().RenderWork(),
	}
	first, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	req.Source = original.WithReads(&store.ReadSet{})
	next, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	repeated, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	for _, prepared := range []*Preparation{&next.Preparation, &repeated.Preparation, &preparation} {
		assert.Equal(t, prepared.source, req.Source)
		assert.Equal(t, prepared.key, first.key)
		assert.Equal(t, &prepared.binding.Policy.Skip[0], &first.binding.Policy.Skip[0])
		data, readErr := fs.ReadFile(prepared.lower, "content")
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), "current")
	}
	var counts manifest.WorkCounts
	records := recorder.Scope().Snapshot(0)
	for i := range records {
		if record := &records[i]; record.Key.Phase == manifest.WorkPreparation {
			counts.Add(&record.Counts)
		}
	}
	assert.Equal(t, counts, manifest.WorkCounts{
		Attempts: 1, Completed: 1, OwnerPrepares: 1, RawKeyPrepares: 3,
	})
}

func TestPrepareRender_SourceBinding(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"revision", "url", "mutable_wrapper", "independent_mutable", "origin_presence"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testPrepareRenderSourceBinding(t, name)
		})
	}
}

func testPrepareRenderSourceBinding(t *testing.T, name string) {
	t.Helper()
	root := t.TempDir()
	original := store.SourceArtifact{
		Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root,
		Content: "immutable-source", Revision: "first", URL: "https://example.com/first",
	}
	if name == "mutable_wrapper" || name == "independent_mutable" {
		original.Content = ""
	}
	cache := newTestTreeCache(t, t.TempDir())
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	var recorder manifest.WorkRecorder
	recorder.Init(&manifest.WorkKey{Owner: owner.Named()}, manifest.WorkScope{})
	var preparation Preparation
	defer func() { preparation = Preparation{} }()
	req := RenderRequest{
		Preparation: &preparation, Source: &original, Spec: owner,
		SkipKinds: []string{"Pod"}, Work: recorder.Scope().RenderWork(),
	}
	first, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	assert.Equal(t, &first.binding.Policy.Skip[0] != &req.SkipKinds[0], true)
	replacement := original
	current := &replacement
	prepares := uint64(1)
	switch name {
	case "revision":
		replacement.Revision = "second"
		prepares++
	case "origin_presence":
		replacement.Metadata = map[string]string{"org.opencontainers.image.revision": ""}
		prepares++
	case "independent_mutable":
		prepares++
	case "mutable_wrapper":
		current = original.WithReads(&store.ReadSet{}).WithReads(&store.ReadSet{})
	default:
		replacement.URL = "https://example.com/second"
	}
	req.Source = current
	next, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	repeated, err := cache.prepareRender(t.Context(), &req)
	assert.NoError(t, err)
	for _, prepared := range []*Preparation{&next.Preparation, &repeated.Preparation, &preparation} {
		assert.Equal(t, prepared.binding.Artifact.Revision, replacement.Revision)
		assert.Equal(t, prepared.binding.Artifact.URL, replacement.URL)
		assert.Equal(t, prepared.source, current)
		assert.Equal(t, prepared.key, first.key)
		assert.Equal(t, prepared.lower, first.lower)
	}
	assert.Equal(t, &next.binding.Policy.Skip[0], &repeated.binding.Policy.Skip[0])
	if name == "url" || name == "mutable_wrapper" {
		assert.Equal(t, &next.binding.Policy.Skip[0], &first.binding.Policy.Skip[0])
	}
	assert.Equal(t, first.binding.Artifact.Revision, original.Revision)
	assert.Equal(t, first.binding.Artifact.URL, original.URL)
	assert.Equal(t, first.source, &original)
	var counts manifest.WorkCounts
	records := recorder.Scope().Snapshot(0)
	for i := range records {
		record := &records[i]
		if record.Key.Phase == manifest.WorkPreparation {
			counts.Add(&record.Counts)
		}
	}
	assert.Equal(t, counts, manifest.WorkCounts{
		Attempts: prepares, Completed: prepares, OwnerPrepares: prepares, RawKeyPrepares: 3,
	})
	t.Logf("preparation work: %+v", counts)
	if name == "mutable_wrapper" {
		var renderRecorder manifest.WorkRecorder
		renderRecorder.Init(&manifest.WorkKey{Owner: owner.Named()}, manifest.WorkScope{})
		req.Work = renderRecorder.Scope().RenderWork()
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: first}\n")
		initial, _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
		assert.NoError(t, err)
		initialBytes, err := manifest.EncodeRender(initial)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\n")
		changed, _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
		assert.NoError(t, err)
		changedBytes, err := manifest.EncodeRender(changed)
		assert.NoError(t, err)
		repeated, _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
		assert.NoError(t, err)
		repeatedBytes, err := manifest.EncodeRender(repeated)
		assert.NoError(t, err)
		assert.Equal(t, string(changedBytes) != string(initialBytes), true)
		assert.Equal(t, string(repeatedBytes), string(changedBytes))
		var prepares, engines uint64
		records := renderRecorder.Scope().Snapshot(0)
		for i := range records {
			record := &records[i]
			prepares += record.Counts.OwnerPrepares
			if record.Key.Phase == manifest.WorkKustomize {
				engines += record.Counts.Attempts
			}
		}
		assert.Equal(t, prepares, uint64(0))
		assert.Equal(t, engines, uint64(2))
	}
}
