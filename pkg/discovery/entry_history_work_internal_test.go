package discovery

import (
	"os"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryReceipt_RecordsHistory(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, t.TempDir(), cache.New)
	if !owner.CanPersistRenders() {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": "resources: []\n"})
	recorder := &manifest.WorkRecorder{Key: manifest.WorkKey{Source: manifest.BootstrapSourceID}}
	scope := recorder.Scope()
	d := Session{loader: &loader.Loader{}, cfg: Config{
		Path: root, Input: &store.SourceArtifact{FS: os.DirFS(root), LocalPath: root, LocalRoot: root},
		Trees: kustomize.NewTreeCache(owner, nil, nil), Work: scope,
	}}
	d.entryReceipt(t.Context(), &Admission{qualification: &entryQualification{}})
	var preparation, service manifest.WorkCounts
	for _, record := range scope.Snapshot(0) {
		assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
		if record.Key.Phase == manifest.WorkPreparation {
			preparation.Add(&record.Counts)
		}
		if record.Key.Phase == manifest.WorkCache {
			service.Add(&record.Counts)
		}
	}
	assert.Equal(t, preparation.OwnerPrepares, uint64(1))
	assert.Equal(t, service, manifest.WorkCounts{Attempts: 1, Failures: 1})
}
