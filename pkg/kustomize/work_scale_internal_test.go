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
	for _, first := range []string{"history", "render"} {
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
	req := RenderRequest{
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
	prepares := uint64(3)
	if renderPersistenceEligible(t) {
		prepares++
		if first == "history" {
			prepares++
		}
	}
	assert.Equal(
		t,
		counts,
		manifest.WorkCounts{Attempts: prepares, Completed: prepares, OwnerPrepares: prepares},
	)
}
