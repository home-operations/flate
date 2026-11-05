package loader

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testFinalizeIndexOwnersOfFileDanglingResourceCase(
	t *testing.T,
	testCaseInput *struct {
		name      string
		statError error
		present   bool
		preload   bool
		wantOpens int
	},
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/homelab/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [../base/postgres]\n",
	)
	testutil.WriteFile(
		t,
		dir,
		"apps/base/postgres/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [cluster.yaml]\n",
	)
	s := store.New()
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps/homelab"}
	other := &manifest.Kustomization{
		Name:            "other",
		Namespace:       "flux-system",
		Path:            "./apps/homelab",
		TargetNamespace: "second",
	}
	s.AddObject(ks)
	s.AddObject(other)
	input := &missingResourceFS{
		FS:        os.DirFS(dir),
		missing:   "apps/base/postgres/cluster.yaml",
		statError: testCaseInput.statError,
	}
	s.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: dir, LocalRoot: dir, FS: input},
	)
	l := loaderFixture(s, nil)
	l.SourceRoot = dir
	if testCaseInput.present {
		testutil.WriteFile(
			t,
			dir,
			input.missing,
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: postgres, namespace: authored}\n",
		)
	}
	if testCaseInput.preload {
		l.documents.mountSources(s)
		_, err := l.loadFile(t.Context(), filepath.Join(dir, input.missing), &l.documents)
		assert.NoError(t, err)
		input.opens = 0
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), dir, true))
	if testCaseInput.statError != nil {
		failed := l.documents.cache[filepath.Join(dir, input.missing)].objects
		assert.FatalIf(
			t,
			len(failed) != 1 || !errors.Is(failed[0].outcome.Err, fs.ErrNotExist),
			"unreadable resource = %+v",
			failed,
		)
	}
	{
		err := l.FinalizeIndex(t.Context(), dir, true)
		assert.NoError(t, err)
	}
	for _, owner := range []manifest.NamedResource{ks.Named(), other.Named()} {
		{
			got := ownersForTest(l.Index, input.missing)
			assert.ErrorIf(
				t,
				!slices.Contains(got, owner),
				"OwnersOfFile(cluster.yaml) = %v, want %v",
				got,
				owner,
			)
		}
	}
	if input.opens != testCaseInput.wantOpens {
		t.Fatalf("resource decode opens=%d, want %d", input.opens, testCaseInput.wantOpens)
	}
	wantStats := 1
	if testCaseInput.preload {
		wantStats = 0
	}
	if input.stats != wantStats {
		t.Fatalf("resource stats across owners and index freezes=%d, want %d", input.stats, wantStats)
	}
	wantProbes := 0
	if testCaseInput.statError != nil {
		wantProbes = len(manifest.KustomizeBuilderFilenames)
	}
	if input.probes != wantProbes {
		t.Fatalf("kustomization probes beneath a resource=%d, want %d", input.probes, wantProbes)
	}
	s.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: dir, LocalRoot: dir, FS: input},
	)
	{
		err := l.FinalizeIndex(t.Context(), dir, true)
		assert.NoError(t, err)
	}
	if input.stats != wantStats+1 {
		t.Fatalf("resource stats after source replacement=%d, want %d", input.stats, wantStats+1)
	}
}
