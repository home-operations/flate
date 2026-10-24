package loader

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_SourceValuesReaders(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"working", "alias-subtree", "committed", "external", "unavailable", "missing", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "config/prod.yaml", "invalid: [")
			files, err := json.Marshal([]string{"./config/prod.yaml", "config/prod.yaml", "config/missing.yaml", "../escape.yaml", "/absolute.yaml", "config/../../escape.yaml", "", ".", "bad\x00.yaml"})
			assert.NoError(t, err)
			group := manifest.SourceDomain
			if mode == "foreign" {
				group = "foreign.example"
			}
			testutil.WriteFile(t, root, "manifests/kustomization.yaml", "namespace: apps\nresources: [hr.yaml]\n")
			testutil.WriteFile(t, root, "manifests/hr.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {apiVersion: %s/v1, kind: GitRepository, name: cluster, namespace: flux-system}
      valuesFiles: %s
      ignoreMissingValuesFiles: true
`, group, files))
			newLoader := func() (*Loader, *missingResourceFS, []string) {
				t.Helper()
				st := store.New()
				l := New(st)
				l.SourceRoot = root
				admitForTest(st, root)
				source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
				input := &missingResourceFS{FS: fstest.MapFS{"config/prod.yaml": &fstest.MapFile{Data: []byte("invalid: [")}}, missing: "config/prod.yaml"}
				art := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input}
				want := []string{"config/prod.yaml", "config/missing.yaml"}
				switch mode {
				case "alias-subtree":
					art.LocalPath = filepath.Join(root, ".artifacts", "alias")
					art.LocalRoot = filepath.Join(root, "subtree")
					art.InputAlias, art.Key = true, "derived"
					want = []string{".artifacts/alias/config/prod.yaml", ".artifacts/alias/config/missing.yaml", "subtree/config/prod.yaml", "subtree/config/missing.yaml"}
				case "committed":
					art.LocalPath, art.Key = filepath.Join(root, ".artifacts", "commit"), "committed"
					want = []string{".artifacts/commit/config/prod.yaml", ".artifacts/commit/config/missing.yaml"}
				case "external":
					art.LocalPath, art.LocalRoot = filepath.Join(root, ".artifacts", "remote"), t.TempDir()
					want = nil
				case "unavailable":
					art.FS = nil
					want = nil
				case "missing", "foreign":
					want = nil
				}
				if mode != "missing" {
					st.AddObject(source)
					st.SetArtifact(source.Named(), art)
				}
				if mode == "working" {
					input.FS = os.DirFS(root)
					st.SetArtifact(manifest.BootstrapSourceID, art)
				}
				_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "manifests"))
				assert.NoError(t, err)
				return l, input, want
			}
			l, input, want := newLoader()
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			id := testutil.HelmReleaseID("apps", "demo")
			var got []string
			for file, readers := range l.Index.Readers {
				if slices.Contains(readers, id) {
					got = append(got, file)
					assert.Diff(t, readers, []manifest.NamedResource{id})
				}
			}
			slices.Sort(got)
			slices.Sort(want)
			assert.Diff(t, got, want)
			assert.Equal(t, input.opens, 0)
			assert.Equal(t, len(l.Index.Claims), 0)
			assert.Equal(t, len(l.Index.Records), 1)
			record, _ := l.Index.Lookup(id)
			assert.Equal(t, len(record.Productions), 1)
			filter := change.NewFilter(change.NewSet([]string{"config/prod.yaml"}), l.Index, l.Store)
			assert.Equal(t, filter.ShouldReconcile(id), mode == "working")
			snapshot := snapshotIndexForTest(l.Index)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *l.Index, snapshot)
			assert.Equal(t, input.opens, 0)
			fresh, freshInput, _ := newLoader()
			if err := fresh.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *fresh.Index, snapshot)
			assert.Equal(t, freshInput.opens, 0)
		})
	}
}
