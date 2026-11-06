package discovery

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPartitionEntries_Constraints(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		paths   []string
		scan    string
		size    int
		edges   [][2]int
		anchors map[int][]string
		want    []manifest.ClusterID
		failure string
	}{
		{name: "singleton without collisions", size: 3, want: []manifest.ClusterID{"."}},
		{name: "pair", size: 2, edges: [][2]int{{0, 1}}, want: []manifest.ClusterID{"a", "b"}},
		{name: "K3", size: 3, edges: [][2]int{{0, 1}, {0, 2}, {1, 2}}, want: []manifest.ClusterID{"a", "b", "c"}},
		{name: "complete bipartite", size: 4, paths: []string{"a/x", "a/y", "b/x", "b/y"}, edges: [][2]int{{0, 2}, {0, 3}, {1, 2}, {1, 3}}, want: []manifest.ClusterID{"a", "b"}},
		{name: "overlapping entry coordinates", size: 4, paths: []string{"a/x", "a/y", "a/z", "a/w"}, edges: [][2]int{{0, 2}, {0, 3}, {1, 2}, {1, 3}}, failure: "conflicting entry coordinates"},
		{name: "interleaved compatible group", size: 3, paths: []string{"a/x", "a/y", "a/z"}, edges: [][2]int{{0, 1}, {1, 2}}, failure: "conflicting entry coordinates"},
		{name: "dominant collision component", size: 5, edges: [][2]int{{0, 1}, {2, 3}, {2, 4}, {3, 4}}, want: []manifest.ClusterID{"c", "d", "e"}},
		{name: "scan boundary", size: 2, scan: "a", edges: [][2]int{{0, 1}}, failure: "entry escapes requested scan"},
		{name: "chordless C5", size: 5, edges: [][2]int{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 0}}, failure: "ambiguous collision constraints"},
		{name: "disconnected pairs", size: 4, edges: [][2]int{{0, 1}, {2, 3}}, failure: "ambiguous collision constraints"},
		{name: "outside root omitted", size: 3, edges: [][2]int{{0, 1}}, want: []manifest.ClusterID{"a", "b"}},
		{name: "independent third anchor", size: 3, edges: [][2]int{{0, 1}}, anchors: map[int][]string{2: {"c"}}, want: []manifest.ClusterID{"a", "b", "c"}},
		{name: "conflicting anchors", size: 2, edges: [][2]int{{0, 1}}, anchors: map[int][]string{0: {"a", "a/nested"}}, failure: "conflicting bootstrap targets"},
		{name: "equal coordinates", size: 2, edges: [][2]int{{0, 1}}, anchors: map[int][]string{0: {"a"}, 1: {"a"}}, failure: "incompatible mandatory entry group"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roots := make([]entryRoot, tc.size)
			for i := range roots {
				roots[i].Path = string(rune('a' + i))
				roots[i].Anchors = tc.anchors[i]
			}
			for i, path := range tc.paths {
				roots[i].Path = path
			}
			for i, edge := range tc.edges {
				for _, node := range edge {
					id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: fmt.Sprintf("edge-%d", i)}
					roots[node].Occurrences = append(
						roots[node].Occurrences,
						loader.Occurrence{
							Identity: id,
							Declaration: loader.OccurrenceID{
								Source:   "view",
								File:     roots[node].Path + "/value.yaml",
								Document: i,
							},
						},
					)
				}
			}
			root := t.TempDir()
			scan := filepath.Join(root, tc.scan)
			gotInput := &store.SourceArtifact{LocalPath: root}
			got, err := partitionEntries(roots, gotInput, scan)
			reversed := slices.Clone(roots)
			slices.Reverse(reversed)
			again, repeatedErr := partitionEntries(reversed, gotInput, scan)
			assert.Equal(t, fmt.Sprint(err), fmt.Sprint(repeatedErr))
			assert.Equal(t, slices.Equal(got, again), true)
			if tc.failure != "" {
				assert.FatalIf(t, err == nil, "missing rejection: %s", tc.failure)
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, strings.Contains(err.Error(), tc.failure), true)
				assert.Equal(t, strings.Contains(err.Error(), "--path"), true)
				return
			}
			assert.NoError(t, err)
			var ids []manifest.ClusterID
			for _, entry := range got {
				ids = append(ids, entry.ID)
			}
			assert.Diff(t, ids, tc.want)
		})
	}
}

func TestPartitionEntries_SharedOccurrence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		source   string
		document int
		want     int
	}{
		{name: "same coordinate", source: "view", want: 1},
		{name: "different source", source: "another view", want: 2},
		{name: "different document", source: "view", document: 1, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shared := loader.Occurrence{
				Identity:    manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "shared"},
				Declaration: loader.OccurrenceID{Source: "view", File: "shared.yaml"},
			}
			other := shared
			other.Declaration.Source, other.Declaration.Document = tc.source, tc.document
			roots := []entryRoot{
				{Path: "a", Occurrences: []loader.Occurrence{shared}},
				{Path: "b", Occurrences: []loader.Occurrence{other}},
			}
			root := t.TempDir()
			entries, err := partitionEntries(roots, &store.SourceArtifact{LocalPath: root}, root)
			assert.NoError(t, err)
			assert.Equal(t, len(entries), tc.want)
		})
	}
}

func TestPartitionEntries_ListItems(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: same}\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: same}\n",
			),
		},
	}})
	l := loader.New(st)
	l.SourceRoot = root
	l.Options.Selection.Changed = true
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	occurrences, err := l.Occurrences(t.Context(), root, "")
	assert.NoError(t, err)
	assert.FatalIf(t, len(occurrences) != 2, "occurrences: %+v", occurrences)
	assert.Equal(t, occurrences[0].Declaration == occurrences[1].Declaration, false)
	entries, err := partitionEntries([]entryRoot{
		{Path: "a", Occurrences: occurrences[:1]},
		{Path: "b", Occurrences: occurrences[1:]},
	}, &store.SourceArtifact{LocalPath: root}, root)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 2)
}

func TestPartitionRoots_PinnedOwnership(t *testing.T) {
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
			roots := []entryRoot{{Path: "a"}, {Path: "b"}, {Path: "c"}}
			for i := range roots {
				for _, claim := range tc.claims {
					if roots[i].Path == claim[0] {
						roots[i].pinned = append(roots[i].pinned, manifest.KSClaim{
							ID:     manifest.NamedResource{Kind: manifest.KindKustomization, Name: claim[0]},
							Prefix: claim[1] + "/",
						})
					}
				}
			}
			d := &Session{cfg: Config{Input: &store.SourceArtifact{LocalPath: t.TempDir()}}}
			paths := func(roots []entryRoot) []string {
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
	assert.FatalIf(t, err == nil, "overlapping entry scans: %+v", entries)
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, strings.Contains(err.Error(), "conflicting entry coordinates"), true)
}
