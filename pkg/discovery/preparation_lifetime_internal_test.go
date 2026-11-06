package discovery

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRun_RejectedPreparationRetiresSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [owner.yaml, ready.yaml]\n")
	testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML(
		"owner", "apps", "  path: ./entry\n  dependsOn: [{name: ready}]\n",
	))
	testutil.WriteFile(t, root, "entry/ready.yaml", testutil.KustomizationYAML("ready", "apps", "  path: ./ready\n"))
	testutil.WriteFile(t, root, "ready/kustomization.yaml", "resources: []\n")
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	result, err := Run(t.Context(), Config{
		Input: source, Path: filepath.Join(root, "entry"), Store: st, Trees: testTrees(t),
	})
	assert.NoError(t, err)
	assert.Equal(t, result.Binding.Producer.ReportHidden(), true)
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root + "-replacement", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), testutil.KustomizationID("apps", "owner")), false)
}

func TestPrepareDocuments_ReplacementRetiresSupport(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: []\n")
	st := store.New()
	source := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
	owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "first"}
	first := owner.SourceID()
	st.SetArtifact(first, source)
	d := Session{cfg: Config{Store: st, Trees: testTrees(t)}, loader: loader.New(st)}
	_, err := d.prepareDocuments(t.Context(), owner, source, nil)
	assert.NoError(t, err)
	replacement := *owner
	replacement.SourceRef.Name = "second"
	second := replacement.SourceID()
	st.SetArtifact(second, source)
	_, err = d.prepareDocuments(t.Context(), &replacement, source, nil)
	assert.NoError(t, err)
	st.SetArtifact(first, &store.SourceArtifact{LocalPath: root + "-first", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), false)
	st.SetArtifact(second, &store.SourceArtifact{LocalPath: root + "-second", FS: source.FS})
	assert.Equal(t, slices.Contains(slices.Collect(st.Sweep()), owner.Named()), true)
}
