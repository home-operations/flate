package loader

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func externalFileHelmReleaseFiles(t *testing.T) fstest.MapFS {
	t.Helper()
	dir := os.DirFS(filepath.Join("..", "..", "testdata", "external-file-helmrelease", "repo"))
	files := fstest.MapFS{}
	assert.NoError(t, fs.WalkDir(dir, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := fs.ReadFile(dir, path)
		if err != nil {
			return err
		}
		files[path] = &fstest.MapFile{Data: data}
		return nil
	}))
	return files
}

func TestFinalizeIndex_ExternalFileStructuralClosure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: root, FS: externalFileHelmReleaseFiles(t)},
	)
	l := New(st)
	defer l.Close()
	l.SourceRoot = root
	hr := testutil.HelmReleaseID("default", "demo")
	child := testutil.KustomizationID("flux-system", "child")
	parent := testutil.KustomizationID("flux-system", "parent")
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{hr}, Changed: true})
	l.Options.Changes = change.NewSet([]string{"payload/one/hr.yaml"})
	_, err := l.Load(t.Context(), filepath.Join(root, "site-a"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	assert.Equal(t, l.frozen, false)
	occurrence, known := l.Occurrence(child)
	assert.FatalIf(t, !known, "external child has no discovery occurrence")
	assert.Equal(t, occurrence.Identity, child)
	assert.Equal(t, occurrence.Authored, child)
	assert.Equal(t, occurrence.Path, "./payload/one")
	assert.Equal(t, occurrence.Declaration.File, "layer/common/child.yaml")
	record, known := l.Index.Lookup(child)
	assert.Equal(t, known, true)
	assert.Equal(t, record.Parent, parent)
	assert.Equal(t, slices.Contains(testutil.Owners(record.Productions), parent), true)
	assert.Equal(t, st.GetObject(child) == nil, true)
	assert.Equal(t, slices.Contains(l.Index.Claims["payload/one/"], child), true)
	assert.Equal(t, slices.ContainsFunc(l.scopes.contributions, func(c scopeContribution) bool {
		return c.owner == child && c.context.dir == "payload/one"
	}), true)
	payload, known := l.Index.Lookup(hr)
	assert.Equal(t, known, true)
	assert.Equal(t, slices.Contains(testutil.Owners(payload.Productions), child), true)
	construction := l.construction
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	assert.Equal(t, l.construction, construction)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.Equal(t, l.frozen, true)
	assert.Equal(t, l.construction, construction)
	assert.Equal(t, st.GetObject(child) == nil, true)
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, slices.Contains(proposal.Selected, hr), true)
	assert.Equal(t, slices.Contains(proposal.Needed, child), true)
}
