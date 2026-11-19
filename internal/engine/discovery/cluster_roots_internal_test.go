package discovery

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestClusterRoots_PinnedOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		claims [][2]string
		want   []string
	}{
		{name: "unclaimed", want: []string{"a", "b", "c"}},
		{name: "retired claims", claims: [][2]string{{"a", "b"}, {"b", "c"}}, want: []string{"a", "c"}},
		{name: "owner sorts later", claims: [][2]string{{"c", "a"}}, want: []string{"b", "c"}},
		{name: "mutual owner with downstream", claims: [][2]string{{"b", "c"}, {"c", "b"}, {"b", "a"}}, want: []string{"b"}},
		{name: "mutual ownership", claims: [][2]string{{"a", "b"}, {"b", "a"}}, want: []string{"a", "c"}},
		{name: "self ownership", claims: [][2]string{{"a", "a"}}, want: []string{"a", "b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roots := []clusterRoot{{Path: "a"}, {Path: "b"}, {Path: "c"}}
			for i := range roots {
				roots[i].pinned.Claims = map[string][]manifest.NamedResource{}
				for _, claim := range tc.claims {
					if roots[i].Path != claim[0] {
						continue
					}
					prefix := claim[1] + "/"
					roots[i].pinned.Claims[prefix] = append(
						roots[i].pinned.Claims[prefix],
						manifest.NamedResource{Kind: manifest.KindKustomization, Name: claim[0]},
					)
				}
			}
			d := &Session{cfg: Config{Input: &store.SourceArtifact{LocalPath: t.TempDir()}}}
			paths := func(roots []clusterRoot) []string {
				result := make([]string, len(roots))
				for i := range roots {
					result[i] = roots[i].Path
				}
				slices.Sort(result)
				return result
			}
			assert.Diff(t, paths(d.unpinnedRoots(slices.Clone(roots))), tc.want)
			slices.Reverse(roots)
			assert.Diff(t, paths(d.unpinnedRoots(roots)), tc.want)
		})
	}
}

func TestPlan_ClusterRootsWithAppBases(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"main", "test", "utility"} {
		testutil.WriteFile(t, root, "kubernetes/clusters/"+name+"/apps.yaml",
			testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./kubernetes/apps/"+name+"\n"))
		testutil.WriteFile(t, root, "kubernetes/apps/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n",
		)
	}
	for _, name := range []string{"create", "vibecraft"} {
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/base/"+name+"/kustomization.yaml",
			"resources: [source.yaml, cm.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/base/"+name+"/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: "+name+"}\nspec: {url: oci://fixture.invalid/"+name+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/base/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n",
		)
	}
	for range 2 {
		entries, err := Plan(t.Context(), Config{Path: root})
		assert.NoError(t, err)
		paths := make([]string, 0, len(entries))
		for _, entry := range entries {
			paths = append(paths, entry.Path)
		}
		assert.Diff(
			t,
			paths,
			[]string{
				filepath.Join(root, "kubernetes", "clusters", "main"),
				filepath.Join(root, "kubernetes", "clusters", "test"),
				filepath.Join(root, "kubernetes", "clusters", "utility"),
			},
		)
	}
}

func TestPlan_InterleavedCompatiblePackages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"x", "y", "z"} {
		testutil.WriteFile(t, root, "a/"+name+"/kustomization.yaml", "resources: [objects.yaml]\n")
		var contents strings.Builder
		contents.WriteString(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: " + name + "}\nspec: {url: oci://fixture.invalid/" + name + "}\n",
		)
		for _, identity := range []string{"X", "Z"} {
			if name == "x" && identity == "Z" || name == "z" && identity == "X" {
				continue
			}
			contents.WriteString(
				"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + strings.ToLower(identity) + "}\n",
			)
		}
		testutil.WriteFile(t, root, "a/"+name+"/objects.yaml", contents.String())
	}
	entries, err := Plan(t.Context(), Config{Path: root})
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Path, root)
	assert.Equal(t, entries[0].aggregate, true)
}

func TestPlan_DependentNamespacePackages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, namespace := range []string{"storage", "workloads"} {
		spec := "  path: ./apps/" + namespace + "/app\n"
		if namespace == "workloads" {
			spec += "  dependsOn: [{name: storage, namespace: storage}]\n"
		}
		testutil.WriteFile(t, root, "apps/"+namespace+"/kustomization.yaml", "resources: [ks.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/"+namespace+"/ks.yaml",
			testutil.KustomizationYAML(namespace, namespace, spec),
		)
		testutil.WriteFile(t, root, "apps/"+namespace+"/app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/"+namespace+"/app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+namespace+"}\n",
		)
	}
	for range 2 {
		entries, err := Plan(t.Context(), Config{Path: filepath.Join(root, "apps"), RepoRoot: root})
		assert.NoError(t, err)
		ids := make([]manifest.ClusterID, len(entries))
		for i := range entries {
			ids[i] = entries[i].ID
		}
		assert.Diff(t, ids, []manifest.ClusterID{"apps"})
		assert.FatalIf(t, len(entries) != 1, "namespace packages produced %d entries", len(entries))
		assert.Equal(t, entries[0].Path, filepath.Join(root, "apps"))
		assert.Equal(t, entries[0].aggregate, true)
	}
}

func TestPlan_AncestorBootstrapSiblingApplication(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{}
	write := func(path, body string) { files[path] = &fstest.MapFile{Data: []byte(body)} }
	write(
		"entry/flux-system/kustomization.yaml",
		"resources: [sync.yaml]\npatches:\n- target: {kind: Kustomization, name: flux-system}\n  patch: |-\n    - op: add\n      path: /spec/prune\n      value: true\n",
	)
	write(
		"entry/flux-system/sync.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: 'https://fixture.invalid/repo'}\n---\n"+
			testutil.KustomizationYAML(
				"flux-system",
				"flux-system",
				"  path: ./entry\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
	)
	write(
		"entry/apps/ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./workloads\n  sourceRef: {kind: GitRepository, name: cluster}\n  dependsOn: [{name: unavailable}]\n",
		),
	)
	write("workloads/kustomization.yaml", "resources: [cm.yaml]\n")
	write("workloads/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n")
	cfg := testConfig(
		t,
		Config{
			Input:    &store.SourceArtifact{LocalPath: root, FS: files},
			Path:     filepath.Join(root, "entry"),
			RepoRoot: root,
			SelfURLs: []string{"https://fixture.invalid/repo"},
		},
	)
	entries, err := Plan(t.Context(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].ID, manifest.ClusterID("entry"))
	assert.Equal(t, entries[0].aggregate, false)
	cfg.Input, cfg.Entry, cfg.Path, cfg.Store = entries[0].Input, &entries[0], entries[0].Path, store.New()
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	defer result.Close()
	apps := testutil.KustomizationID("flux-system", "apps")
	record, found := result.Index.Lookup(apps)
	assert.Equal(t, found, true)
	assert.Equal(t, record.Authored, apps)
	assert.Equal(t, record.Parent, testutil.KustomizationID("flux-system", "flux-system"))
	assert.Equal(t, result.Binding.Producer, testutil.KustomizationID("flux-system", "flux-system"))
}
