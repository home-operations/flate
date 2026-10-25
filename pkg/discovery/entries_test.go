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

func TestPlanEntries_Constraints(t *testing.T) {
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
				if tc.paths != nil {
					roots[i].Path = tc.paths[i]
				}
				roots[i].Anchors = tc.anchors[i]
			}
			for i, edge := range tc.edges {
				for _, node := range edge {
					id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: fmt.Sprintf("edge-%d", i)}
					roots[node].Occurrences = append(roots[node].Occurrences, loader.Occurrence{Identity: id, Declaration: loader.OccurrenceID{Source: "view", File: roots[node].Path + "/value.yaml", Document: i}})
				}
			}
			root := t.TempDir()
			scan := root
			if tc.scan != "" {
				scan = filepath.Join(root, tc.scan)
			}
			got, err := planEntries(roots, &store.SourceArtifact{LocalPath: root}, scan)
			if tc.failure != "" {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), tc.failure) || !strings.Contains(err.Error(), "--path") {
					t.Fatalf("classification/witness: %v", err)
				}
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

func TestPlan_OccurrenceBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, left, right, outer string
		plain                    bool
		want                     []manifest.ClusterID
	}{
		{name: "plain resource collisions", plain: true, want: []manifest.ClusterID{"."}},
		{name: "equal bytes", want: []manifest.ClusterID{"alpha", "omega"}},
		{name: "namespace projection", left: "namespace: left\n", right: "namespace: right\n", want: []manifest.ClusterID{"."}},
		{name: "name projection", left: "namePrefix: left-\n", right: "nameSuffix: -right\n", want: []manifest.ClusterID{"."}},
		{name: "explicit composition", outer: "resources: [alpha, omega]\n", want: []manifest.ClusterID{"."}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, side := range []struct{ dir, projection string }{{"alpha", tc.left}, {"omega", tc.right}} {
				testutil.WriteFile(t, root, side.dir+"/kustomization.yaml", side.projection+"resources: [cm.yaml]\n")
				objects := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {key: equal}\n"
				if !tc.plain {
					objects += "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: " + side.dir + "}\nspec: {url: oci://fixture.invalid/chart}\n"
				}
				testutil.WriteFile(t, root, side.dir+"/cm.yaml", objects)
			}
			if tc.outer != "" {
				testutil.WriteFile(t, root, "kustomization.yaml", tc.outer)
			}
			for range 2 {
				entries, err := Plan(t.Context(), Config{Path: root, WipeSecrets: true})
				assert.NoError(t, err)
				var ids []manifest.ClusterID
				for _, entry := range entries {
					ids = append(ids, entry.ID)
				}
				assert.Diff(t, ids, tc.want)
			}
			entries, err := Plan(t.Context(), Config{Path: filepath.Join(root, "alpha"), RepoRoot: root, WipeSecrets: true})
			assert.NoError(t, err)
			if len(entries) != 1 || entries[0].ID != "." || entries[0].Path != filepath.Join(root, "alpha") {
				t.Fatalf("explicit entry widened: %+v", entries)
			}
		})
	}
}

func TestPlanEntries_SharedOccurrence(t *testing.T) {
	t.Parallel()
	roots := []entryRoot{{Path: "a"}, {Path: "b"}}
	shared := loader.Occurrence{Identity: manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "shared"}, Declaration: loader.OccurrenceID{Source: "view", File: "shared.yaml", Document: 3}}
	for i := range roots {
		roots[i].Occurrences = append(roots[i].Occurrences, shared)
	}
	root := t.TempDir()
	entries, err := planEntries(roots, &store.SourceArtifact{LocalPath: root}, root)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].ID, manifest.ClusterID("."))
	roots[1].Occurrences = slices.Clone(roots[1].Occurrences)
	roots[1].Occurrences[0].Declaration.Source = "independent view"
	entries, err = planEntries(roots, &store.SourceArtifact{LocalPath: root}, root)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 2)
}

func TestPlan_AdmittedSourceViews(t *testing.T) {
	t.Parallel()
	const root = "/virtual/plan-source-views"
	for _, tc := range []struct {
		name, excluded string
		multi          bool
	}{
		{name: "ordinary", multi: true},
		{name: "group versions", multi: true},
		{name: "distinct groups"},
		{name: "ignored", excluded: "right/**\n"},
		{name: "hidden"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := fstest.MapFS{}
			dirs := []string{"left", "right"}
			if tc.name == "hidden" {
				dirs[1] = ".hidden"
			}
			for _, dir := range dirs {
				input[dir+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [object.yaml]\n")}
				version := "example.test/v1"
				if dir == "right" && tc.name == "group versions" {
					version = "example.test/v2"
				}
				if dir == "right" && tc.name == "distinct groups" {
					version = "other.example/v1"
				}
				input[dir+"/object.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: " + version + "\nkind: Widget\nmetadata: {name: same, namespace: apps}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: " + dir + ", namespace: apps}\nspec: {url: oci://fixture.invalid/chart}\n")}
			}
			if tc.excluded != "" {
				input[".krmignore"] = &fstest.MapFile{Data: []byte(tc.excluded)}
			}
			input["decoy.zip"] = &fstest.MapFile{Data: []byte("archive bytes are not manifest input")}
			entries, err := Plan(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root})
			assert.NoError(t, err)
			want := 1
			if tc.multi {
				want = 2
			}
			assert.Equal(t, len(entries), want)
			if !tc.multi {
				assert.Equal(t, entries[0].ID, manifest.ClusterID("."))
			}
			if tc.name == "ordinary" {
				renamed := fstest.MapFS{}
				for name, file := range input {
					name = strings.Replace(name, "left/", "unrelated/first/cfg/", 1)
					name = strings.Replace(name, "right/", "new-second/control/", 1)
					renamed[name] = file
				}
				changed, err := Plan(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: renamed}, Path: root})
				assert.NoError(t, err)
				var ids []manifest.ClusterID
				for _, entry := range changed {
					ids = append(ids, entry.ID)
				}
				assert.Diff(t, ids, []manifest.ClusterID{"new-second/control", "unrelated/first/cfg"})
			}
			other := fstest.MapFS{"left/kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")}, "left/object.yaml": {Data: []byte("apiVersion: example.test/v2\nkind: Widget\nmetadata: {name: changed, namespace: apps}\n")}}
			independent, err := Plan(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: other}, Path: root})
			assert.NoError(t, err)
			if len(independent) != 1 || independent[0].ID != "." || independent[0].Input.FS == nil {
				t.Fatalf("equal coordinate borrowed another admitted view: %+v", independent)
			}
		})
	}
}
