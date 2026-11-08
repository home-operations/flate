package discovery

import (
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/selection"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
					Selection:   selection.Request{Changed: true},
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
				Selection: selection.Request{Changed: true},
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
		// The owner ruling restores #1379's multi-entry planning expectations.
		want := []string{root}
		if testCaseInput.name == "equal bytes" {
			want = []string{filepath.Join(root, "alpha"), filepath.Join(root, "omega")}
		}
		assert.Diff(t, paths, want)
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
	// The owner ruling restores #1379's multi-entry planning expectations.
	want := 1
	if testCaseInput.name == "ordinary" || testCaseInput.name == "group versions" {
		want = 2
	}
	assert.Equal(t, len(entries), want)
	if want == 1 {
		assert.Equal(t, entries[0].Path, root)
	} else {
		assert.Equal(t, entries[0].Path, filepath.Join(root, "left"))
		assert.Equal(t, entries[1].Path, filepath.Join(root, "right"))
	}
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
		paths := make([]string, 0, len(changed))
		for _, entry := range changed {
			paths = append(paths, entry.Path)
		}
		// The owner ruling restores #1379's renamed multi-entry expectation.
		assert.Diff(t, paths, []string{filepath.Join(root, "new-second"), filepath.Join(root, "unrelated")})
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

func TestPlan_OrdinarySupplierCensusReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"flux/consumer.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"consumer",
					"apps",
					"  path: ./consumer\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: hello}]}\n",
				),
			),
		},
		"flux/supplier.yaml": {
			Data: []byte(testutil.KustomizationYAML("supplier", "apps", "  path: ./supplier\n")),
		},
		"supplier/kustomization.yaml": {Data: []byte("resources: [cm.yaml, spare.yaml]\n")},
		"supplier/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: supplied}\n",
			),
		},
		"consumer/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"consumer/cm.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result, namespace: apps}\ndata: {value: '${value}'}\n",
			),
		},
	}
	files["supplier/spare.yaml"] = &fstest.MapFile{
		Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: spare, namespace: apps}\n"),
	}
	var reads, requiredReads atomic.Int64
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{
			Mode: gittree.Direct,
			Current: entryReadFS{
				FS:    files,
				reads: map[string]*atomic.Int64{"supplier/spare.yaml": &reads, "supplier/cm.yaml": &requiredReads},
			},
		},
	)
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: view}
	request := selection.Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "consumer")}}
	cfg := Config{Input: input, Path: filepath.Join(root, "flux"), Selection: request}
	entries, err := Plan(t.Context(), cfg)
	assert.NoError(t, err)
	before := reads.Load()
	assert.Equal(t, before, int64(1))
	assert.Equal(t, requiredReads.Load(), int64(1))
	cfg.Store = store.New()
	cfg.Loader = loader.New(cfg.Store)
	configureSelection(t, cfg.Loader, cfg.Selection)
	cfg.Entry = &entries[0]
	session, err := Begin(t.Context(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, reads.Load(), before)
	assert.NoError(t, session.refreshDeclarations(t.Context(), root))
	assert.Equal(t, reads.Load(), before)
	assert.Equal(t, requiredReads.Load(), int64(2))
	data := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "hello"}
	record, ok := cfg.Loader.Index.Lookup(data)
	assert.Equal(t, ok, true)
	assert.Equal(t, record.File, "supplier/cm.yaml")
	assert.Equal(t, len(selectedNames(cfg.Loader)), 0)
	assert.Equal(t, cfg.Store.GetObject(data), nil)
}
