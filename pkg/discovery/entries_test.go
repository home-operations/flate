package discovery

import (
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/store"
)

func TestPlan_OccurrenceBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, left, right, outer string
		plain                    bool
	}{
		{name: "plain resource collisions", plain: true},
		{name: "equal bytes"},
		{name: "namespace projection", left: "namespace: left\n", right: "namespace: right\n"},
		{name: "name projection", left: "namePrefix: left-\n", right: "nameSuffix: -right\n"},
		{name: "explicit composition", outer: "resources: [alpha, omega]\n"},
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
				var paths []string
				for _, entry := range entries {
					paths = append(paths, entry.Path)
				}
				assert.Diff(t, paths, []string{root})
			}
			entries, err := Plan(t.Context(), Config{Path: filepath.Join(root, "alpha"), RepoRoot: root, WipeSecrets: true})
			assert.NoError(t, err)
			if len(entries) != 1 || entries[0].Path != filepath.Join(root, "alpha") {
				t.Fatalf("explicit entry widened: %+v", entries)
			}
		})
	}
}

func TestPlan_AdmittedSourceViews(t *testing.T) {
	t.Parallel()
	const root = "/virtual/plan-source-views"
	for _, tc := range []struct {
		name, excluded string
	}{
		{name: "ordinary"},
		{name: "group versions"},
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
			assert.Equal(t, len(entries), want)
			assert.Equal(t, entries[0].Path, root)
			assert.Equal(t, entries[0].prepared != nil, true)
			if tc.name == "ordinary" {
				renamed := fstest.MapFS{}
				for name, file := range input {
					name = strings.Replace(name, "left/", "unrelated/first/cfg/", 1)
					name = strings.Replace(name, "right/", "new-second/control/", 1)
					renamed[name] = file
				}
				changed, err := Plan(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: renamed}, Path: root})
				assert.NoError(t, err)
				var paths []string
				for _, entry := range changed {
					paths = append(paths, entry.Path)
				}
				assert.Diff(t, paths, []string{root})
			}
			other := fstest.MapFS{"left/kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")}, "left/object.yaml": {Data: []byte("apiVersion: example.test/v2\nkind: Widget\nmetadata: {name: changed, namespace: apps}\n")}}
			independent, err := Plan(t.Context(), Config{Input: &store.SourceArtifact{LocalPath: root, FS: other}, Path: root})
			assert.NoError(t, err)
			if len(independent) != 1 || independent[0].Path != root || independent[0].Input.FS == nil {
				t.Fatalf("equal coordinate borrowed another admitted view: %+v", independent)
			}
		})
	}
}
