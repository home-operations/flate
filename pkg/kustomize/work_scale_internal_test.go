package kustomize

import (
	"fmt"
	"os"
	"strconv"
	"testing"

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
	defer preparation.Retire()
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
	preparation.Retire()
	assert.Equal(
		t,
		preparation.complete || preparation.source != nil || preparation.spec != nil || preparation.lower != nil,
		false,
	)
}

func TestPrepareRender_SourceBinding(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"revision", "url"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			original := store.SourceArtifact{
				Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root,
				Content: "immutable-source", Revision: "first", URL: "https://example.com/first",
			}
			cache := &TreeCache{}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			var recorder manifest.WorkRecorder
			recorder.Init(&manifest.WorkKey{Owner: owner.Named()}, manifest.WorkScope{})
			var preparation Preparation
			defer preparation.Retire()
			req := RenderRequest{
				Preparation: &preparation, Source: &original, Spec: owner,
				SkipKinds: []string{"Pod"}, Work: recorder.Scope().RenderWork(),
			}
			first, err := cache.prepareRender(t.Context(), &req)
			assert.NoError(t, err)
			assert.Equal(t, &first.binding.Skip[0] != &req.SkipKinds[0], true)
			replacement := original
			prepares := uint64(1)
			if name == "revision" {
				replacement.Revision = "second"
				prepares++
			} else {
				replacement.URL = "https://example.com/second"
			}
			req.Source = &replacement
			next, err := cache.prepareRender(t.Context(), &req)
			assert.NoError(t, err)
			repeated, err := cache.prepareRender(t.Context(), &req)
			assert.NoError(t, err)
			for _, prepared := range []*Preparation{&next.Preparation, &repeated.Preparation, &preparation} {
				assert.Equal(t, prepared.binding.Revision, replacement.Revision)
				assert.Equal(t, prepared.binding.URL, replacement.URL)
				assert.Equal(t, prepared.source, &replacement)
				assert.Equal(t, prepared.key, first.key)
				assert.Equal(t, prepared.lower, first.lower)
			}
			assert.Equal(t, &next.binding.Skip[0], &repeated.binding.Skip[0])
			if name == "url" {
				assert.Equal(t, &next.binding.Skip[0], &first.binding.Skip[0])
			}
			assert.Equal(t, first.binding.Revision, original.Revision)
			assert.Equal(t, first.binding.URL, original.URL)
			assert.Equal(t, first.source, &original)
			var counts manifest.WorkCounts
			for _, record := range recorder.Scope().Snapshot(0) {
				if record.Key.Phase == manifest.WorkPreparation {
					counts.Add(&record.Counts)
				}
			}
			assert.Equal(t, counts, manifest.WorkCounts{
				Attempts: prepares, Completed: prepares, OwnerPrepares: prepares, RawKeyPrepares: 3,
			})
			t.Logf("preparation work: %+v", counts)
		})
	}
}
