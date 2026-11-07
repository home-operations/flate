package orchestrator

import (
	"io/fs"
	"maps"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type discoveryWalkFS struct {
	fs.FS

	walks atomic.Int32
}

func (f *discoveryWalkFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if name == "." {
		f.walks.Add(1)
	}
	return fs.ReadDir(f.FS, name)
}

func TestRenderTrees_FullDiscoveryOnce(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	input := &discoveryWalkFS{FS: fstest.MapFS{
		"object.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\ndata: {value: kept}\n"),
		},
	}}
	results, err := renderTreesForTest(t.Context(), t, []Tree{{
		ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root,
		Input: &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input},
	}}, Config{Concurrency: 2})
	assert.NoError(t, err)
	assert.Equal(t, len(results), 1)
	assert.Equal(t, len(results[0].Result.Manifests), 0)
	assert.Equal(t, input.walks.Load(), int32(1))
}

func TestProjectEntryReaders_PreservesSideEvidence(t *testing.T) {
	t.Parallel()
	authored := testutil.KustomizationID("apps", "owner")
	internal := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{}).Named()
	other := testutil.KustomizationID("apps", "other")
	for _, pair := range [][2]manifest.NamedResource{{authored, internal}, {internal, authored}} {
		t.Run(pair[0].NamespacedName(), func(t *testing.T) {
			t.Parallel()
			from, to := pair[0], pair[1]
			readers := []manifest.FileReader{
				{
					Reader: from,
					Source: manifest.BootstrapSourceID,
					Declaration: manifest.NamedResource{
						Kind:      manifest.KindConfigMap,
						Namespace: "apps",
						Name:      "settings",
					},
					Exact: true,
				},
				{
					Reader: other,
					Source: manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindGitRepository,
						Namespace: "apps",
						Name:      "other",
					},
				},
			}
			physical := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "removed"}
			visible := manifest.InputVisibility{
				Source:   manifest.BootstrapSourceID,
				Root:     ".",
				Complete: true,
				Inputs:   map[manifest.NamedResource][]string{physical: {"removed.yaml"}},
			}
			empty := visible
			empty.Inputs = nil
			index := &manifest.DiscoveryIndex{
				Readers:    map[string][]manifest.FileReader{"removed.yaml": readers},
				Visibility: []manifest.InputVisibility{visible},
			}
			projected := projectEntryReaders(index, from, to)
			assert.Diff(t, index.Readers["removed.yaml"], readers)
			want := append([]manifest.FileReader{}, readers...)
			want[0].Reader = to
			assert.Diff(t, projected.Readers["removed.yaml"], want)

			changes := change.NewSet([]string{"removed.yaml"})
			assert.NoError(
				t,
				changes.CollectInputs(
					t.Context(),
					fstest.MapFS{
						"removed.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")},
					},
					"removed.yaml",
					true,
				),
			)
			opposite := &manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{empty}}
			paired, err := changes.WithReaders(
				t.Context(),
				[2]*manifest.DiscoveryIndex{projected, opposite},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			replaced := projectEntryReaders(
				index,
				from,
				other,
			)
			current, err := paired.WithReaders(
				t.Context(),
				[2]*manifest.DiscoveryIndex{replaced, opposite},
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Diff(
				t,
				maps.Collect(current.Inputs(manifest.NamedResource{})),
				map[manifest.NamedResource]change.InputChange{
					physical: {File: "removed.yaml"},
					other:    {File: "removed.yaml"},
				},
			)
			assert.Diff(
				t,
				maps.Collect(paired.Inputs(manifest.NamedResource{})),
				map[manifest.NamedResource]change.InputChange{
					physical: {File: "removed.yaml"},
					to:       {File: "removed.yaml"},
				},
			)
			assert.Diff(t, projected.Visibility, index.Visibility)
			assert.Diff(t, index.Visibility[0].Inputs[physical], []string{"removed.yaml"})
			st := store.New()
			local := &manifest.Kustomization{Name: to.Name, Namespace: to.Namespace}
			st.AddObject(local)
			root := t.TempDir()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			filter := admissiontest.Decision(
				t,
				admissiontest.Config{Changes: paired, Index: &manifest.DiscoveryIndex{}, Inventory: st, Root: root},
			)
			assert.Equal(t, filter.Reconcile(to), true)
			assert.Equal(t, filter.Reconcile(from), false)
			assert.Diff(t, changes.Paths(), []string{"removed.yaml"})
			assert.Diff(t, index.Readers["removed.yaml"], readers)
			projected.Readers["removed.yaml"][0].Exact = false
			assert.Equal(t, index.Readers["removed.yaml"][0].Exact, true)
		})
	}
}

// writeCluster lays out a two-app Flux cluster under dir: app-a renders
// ConfigMap cfg-a (data k=aVal), app-b renders ConfigMap cfg-b (data
// k=bVal). The two per-app Kustomizations are independent, so a change to
// one's rendered output must not pull the other into changed-only scope.
func writeCluster(t *testing.T, dir, aVal, bVal string) {
	t.Helper()
	ks := func(name string) string {
		return testutil.KustomizationYAML(name, "flux-system", "  path: ./apps/"+name+"\n"+
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
	}
	cm := func(name, val string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "}\ndata: {k: " + val + "}\n"
	}
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml]\n")
	testutil.WriteFile(t, dir, "ks-a.yaml", ks("app-a"))
	testutil.WriteFile(t, dir, "ks-b.yaml", ks("app-b"))
	testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", cm("cfg-a", aVal))
	testutil.WriteFile(t, dir, "apps/app-b/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/app-b/cm.yaml", cm("cfg-b", bVal))
}

// configMapNames returns the set of ConfigMap names across every parent's
// rendered manifests in a Result — what each side of the comparison
// actually rendered.
func configMapNames(res *Result) map[string]bool {
	out := map[string]bool{}
	for _, docs := range outputByParent(res.Manifests) {
		for _, m := range docs {
			if m["kind"] == "ConfigMap" {
				if meta, ok := m["metadata"].(map[string]any); ok {
					if name, ok := meta["name"].(string); ok {
						out[name] = true
					}
				}
			}
		}
	}
	return out
}
