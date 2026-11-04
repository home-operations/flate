package discovery

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
			testPlanOccurrenceBoundariesCase(t, &tc)
		},
		)
	}
}

func TestPlan_AdmittedSourceViews(t *testing.T) {
	t.Parallel()
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
			testPlanAdmittedSourceViewsCase(t, &tc)
		},
		)
	}
}

func TestPlan_SharedProducerProjection(t *testing.T) {
	t.Parallel()
	projections := manifest.NewProducerProjections()
	const document = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: set, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: child, namespace: apps}
    spec:
      path: ./workload
      sourceRef: {kind: GitRepository, name: content}
`
	var entries [2][]Entry
	var failures [2]error
	var callers sync.WaitGroup
	for i := range entries {
		callers.Go(func() {
			root := filepath.Join(t.TempDir(), "plan")
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"set.yaml": {Data: []byte(document)}}}
			entries[i], failures[i] = Plan(
				t.Context(),
				Config{
					Input:       input,
					Path:        root,
					Selection:   manifest.Selection{Changed: true},
					Projections: projections,
				},
			)
		})
	}
	callers.Wait()
	projections.Clear()
	var objects [2]manifest.BaseManifest
	child := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Name:      "child",
		Namespace: "apps",
	}
	for i := range entries {
		assert.NoError(t, failures[i])
		assert.Equal(t, len(entries[i]), 1)
		entry := entries[i][0]
		session, err := Begin(
			t.Context(),
			Config{
				Entry:     &entry,
				Input:     entry.Input,
				Path:      entry.Path,
				Store:     store.New(),
				Selection: manifest.Selection{Changed: true},
			},
		)
		assert.NoError(t, err)
		record, ok := session.loader.Index.Lookup(child)
		assert.Equal(t, ok, true)
		objects[i] = session.loader.IndexedObject(child, record).Obj
		assert.FatalIf(
			t,
			record.File != "set.yaml" || entry.Input.LocalPath != entry.Path,
			"shared projection lost its source coordinate: %+v",
			record,
		)
	}
	assert.FatalIf(
		t,
		objects[0] == nil || objects[0] != objects[1],
		"%v",
		"identical-content plans computed separate projected declarations",
	)
	assert.FatalIf(
		t,
		entries[0][0].Path == entries[1][0].Path,
		"%v",
		"independent plans shared an entry coordinate",
	)
}

func testPlanOccurrenceBoundariesCase(t *testing.T, testCaseInput *struct {
	name string

	left  string
	right string
	outer string
	plain bool
},
) {
	t.Helper()

	root := t.TempDir()
	for _, side := range []struct{ dir, projection string }{{"alpha", testCaseInput.left}, {"omega", testCaseInput.right}} {
		testutil.WriteFile(t, root, side.dir+"/kustomization.yaml", side.projection+"resources: [cm.yaml]\n")
		objects := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {key: equal}\n"
		if !testCaseInput.plain {
			objects += "---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: " + side.dir + "}\nspec: {url: oci://fixture.invalid/chart}\n"
		}
		testutil.WriteFile(t, root, side.dir+"/cm.yaml", objects)
	}
	if testCaseInput.outer != "" {
		testutil.WriteFile(t, root, "kustomization.yaml", testCaseInput.outer)
	}
	for range 2 {
		entries, err := Plan(t.Context(), Config{Path: root})
		assert.NoError(t, err)
		var paths []string
		if len(entries) != 0 {
			paths = make([]string, 0, len(entries))
		}
		for _, entry := range entries {
			paths = append(paths, entry.Path)
		}
		assert.Diff(t, paths, []string{root})
	}
	entries, err := Plan(
		t.Context(),
		Config{Path: filepath.Join(root, "alpha"), RepoRoot: root},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(entries) != 1 || entries[0].Path != filepath.Join(root, "alpha"),
		"explicit entry widened: %+v",
		entries,
	)
}

func testPlanAdmittedSourceViewsCase(t *testing.T, testCaseInput *struct {
	name string

	excluded string
},
) {
	t.Helper()
	const root = "/virtual/plan-source-views"

	input := fstest.MapFS{}
	dirs := []string{"left", "right"}
	if testCaseInput.name == "hidden" {
		dirs[1] = ".hidden"
	}
	for _, dir := range dirs {
		input[dir+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [object.yaml]\n")}
		version := "example.test/v1"
		if dir == "right" && testCaseInput.name == "group versions" {
			version = "example.test/v2"
		}
		if dir == "right" && testCaseInput.name == "distinct groups" {
			version = "other.example/v1"
		}
		input[dir+"/object.yaml"] = &fstest.MapFile{
			Data: []byte(
				"apiVersion: " + version + "\nkind: Widget\nmetadata: {name: same, namespace: apps}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: " + dir + ", namespace: apps}\nspec: {url: oci://fixture.invalid/chart}\n",
			),
		}
	}
	if testCaseInput.excluded != "" {
		input[".krmignore"] = &fstest.MapFile{Data: []byte(testCaseInput.excluded)}
	}
	input["decoy.zip"] = &fstest.MapFile{Data: []byte("archive bytes are not manifest input")}
	entries, err := Plan(
		t.Context(),
		Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root},
	)
	assert.NoError(t, err)
	want := 1
	assert.Equal(t, len(entries), want)
	assert.Equal(t, entries[0].Path, root)
	assert.Equal(t, entries[0].prepared != nil, true)
	if testCaseInput.name == "ordinary" {
		renamed := fstest.MapFS{}
		for name, file := range input {
			name = strings.Replace(name, "left/", "unrelated/first/cfg/", 1)
			name = strings.Replace(name, "right/", "new-second/control/", 1)
			renamed[name] = file
		}
		changed, localErr := Plan(
			t.Context(),
			Config{Input: &store.SourceArtifact{LocalPath: root, FS: renamed}, Path: root},
		)
		assert.NoError(t, localErr)
		var paths []string
		if len(changed) != 0 {
			paths = make([]string, 0, len(changed))
		}
		for _, entry := range changed {
			paths = append(paths, entry.Path)
		}
		assert.Diff(t, paths, []string{root})
	}
	other := fstest.MapFS{
		"left/kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
		"left/object.yaml": {
			Data: []byte(
				"apiVersion: example.test/v2\nkind: Widget\nmetadata: {name: changed, namespace: apps}\n",
			),
		},
	}
	independent, err := Plan(
		t.Context(),
		Config{Input: &store.SourceArtifact{LocalPath: root, FS: other}, Path: root},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(independent) != 1 || independent[0].Path != root || independent[0].Input.FS == nil,
		"equal coordinate borrowed another admitted view: %+v",
		independent,
	)
}
