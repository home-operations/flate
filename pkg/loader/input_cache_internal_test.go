package loader

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestInputCache_RestoresIndependentWalks(t *testing.T) {
	t.Parallel()
	for _, edit := range []string{"unchanged", "file", "builder", "precedence", "membership", "missing", "transformer"} {
		t.Run(edit, func(t *testing.T) {
			t.Parallel()
			reason := cache.RenderPersistenceDisabledReason()
			t.Logf("render persistence reason=%q backing=disk", reason)
			persistent := reason == ""
			root := t.TempDir()
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			files := fstest.MapFS{
				"left/kustomization.yml": {
					Data: []byte("resources: [objects, absent.yaml]\ntransformers: [../transformer]\n"),
				},
				"left/objects/object.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: left}\n"),
				},
				"right/kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
				"right/object.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: right}\n"),
				},
				"transformer/kustomization.yaml": {Data: []byte("namespace: old\nresources: [namespace.yaml]\n")},
				"transformer/namespace.yaml": {
					Data: []byte(
						"apiVersion: builtin\nkind: NamespaceTransformer\nmetadata: {name: namespace}\nfieldSpecs:\n- group: kustomize.toolkit.fluxcd.io\n  kind: Kustomization\n  path: spec/targetNamespace\n  create: true\n",
					),
				},
			}
			key := func(side string) physicalContext {
				return physicalContext{root: root, dir: side, role: kustomizeinput.Resource, sourceRoot: root}
			}
			first := cachedFixtureLoader(t, root, files, owner)
			first.ensureScopes()
			for _, side := range []string{"left", "right"} {
				first.scopeWalk(&graphVisit{graphContext: key(side).graphContext})
			}
			assert.NoError(t, first.PersistInputs(t.Context()))
			first.Close()
			switch edit {
			case "file":
				files["left/objects/object.yaml"].Data = []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\n",
				)
			case "builder":
				files["left/kustomization.yml"].Data = []byte("namespace: changed\nresources: [objects]\n")
			case "precedence":
				files["left/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
			case "membership":
				files["left/objects/added.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added}\n"),
				}
			case "missing":
				files["left/absent.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: present}\n"),
				}
			case "transformer":
				files["transformer/namespace.yaml"].Data = []byte(
					"apiVersion: builtin\nkind: NamespaceTransformer\nmetadata: {name: namespace}\nfieldSpecs: []\n",
				)
			}
			restored := cachedFixtureLoader(t, root, files, owner)
			defer restored.Close()
			facts := restored.sourceFacts(root)
			_, left := facts.walk(new(key("left")))
			_, right := facts.walk(new(key("right")))
			assert.Equal(t, left, persistent && edit == "unchanged")
			assert.Equal(t, right, persistent)
			cold := cachedFixtureLoader(t, root, files, nil)
			defer cold.Close()
			restored.ensureScopes()
			cold.ensureScopes()
			for _, side := range []string{"left", "right"} {
				visit := graphVisit{graphContext: key(side).graphContext}
				got, want := restored.scopeWalk(&visit), cold.scopeWalk(&visit)
				if diff := cmp.Diff(want, got, cmp.AllowUnexported(graphVisit{}, graphContext{})); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}

func TestInputCache_ParsesOnlyEditedFile(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			persistent := cache.RenderPersistenceDisabledReason() == ""
			root, backing := t.TempDir(), t.TempDir()
			files := fstest.MapFS{}
			reads := map[string]*atomic.Int64{}
			for i := range count {
				name := fmt.Sprintf("cm-%03d.yaml", i)
				files[name] = &fstest.MapFile{
					Data: []byte(
						fmt.Sprintf(
							"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm-%03d}\ndata: {value: red}\n",
							i,
						),
					),
				}
				reads[name] = new(atomic.Int64)
			}
			fixture := inputCacheFixture{root: root, backing: backing, files: files, reads: reads}

			fixture.run(t, count, persistent)
			fixture.run(t, 0, persistent)
			files["cm-000.yaml"] = &fstest.MapFile{
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm-000}\ndata: {value: blue}\n"),
			}
			fixture.run(t, 1, persistent)
		})
	}
}

type inputCacheFixture struct {
	root, backing string
	files         fstest.MapFS
	reads         map[string]*atomic.Int64
}

//nolint:gocognit // The owner lifetime MUST cover restored inputs, source reads, projections, and publication in both modes.
func (f *inputCacheFixture) run(t *testing.T, want int, persistent bool) {
	t.Helper()
	root, backing, files, reads := f.root, f.backing, f.files, f.reads
	count := len(files)
	warm := want != count
	wantReads := want
	if !persistent {
		wantReads = count
	}
	owner, err := cache.New(t.Context(), backing)
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: parsedReadFS{FS: files, reads: reads}},
	)
	assert.NoError(t, err)
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
	l := New(st)
	defer l.Close()
	l.SourceRoot, l.Options.Cache = root, owner
	l.Options.Changes = change.NewSet([]string{"cm-000.yaml"})
	configureSelection(t, l, selection.Request{Changed: true})
	l.documents.compact = true
	l.mountSources()
	assert.NoError(t, l.restoreCachedInputs(t.Context()))
	if want == 0 && persistent {
		for name := range files {
			doc := l.documents.cache[filepath.Join(root, name)]
			assert.Equal(t, doc.deferred != nil, true)
			assert.Equal(t, len(doc.objects), 0)
		}
		source := l.sourceFacts(root)
		key := loadContext{path: root}
		pending, known, _ := source.loads.get(key)
		assert.Equal(t, known, true)
		assert.Equal(t, pending.cached != nil, true)
		assert.Equal(t, len(pending.files), 0)
		load, _ := source.load(key)
		assert.Equal(t, load.complete, true)
		assert.Equal(t, len(load.files), count)
		again, _ := source.load(key)
		assert.Equal(t, &load.files[0] == &again.files[0], true)
	}
	if want == 0 && !persistent {
		for name := range files {
			_, known := l.documents.cache[filepath.Join(root, name)]
			assert.Equal(t, known, false)
		}
		source := l.sourceFacts(root)
		_, known, _ := source.loads.get(loadContext{path: root})
		assert.Equal(t, known, false)
	}
	for _, read := range reads {
		read.Store(0)
	}
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	parsed := 0
	for name, read := range reads {
		if read.Load() != 0 {
			parsed++
			if warm && persistent {
				assert.Equal(t, name, "cm-000.yaml")
			}
		}
	}
	assert.Equal(t, parsed, wantReads)
	assert.Equal(t, len(l.Index.Records), count)
	if warm {
		occurrence, known := l.Occurrence(manifest.NamedResource{Kind: "ConfigMap", Name: "cm-000"})
		assert.Equal(t, known, true)
		child, err := l.OccurrenceObject(t.Context(), &occurrence, nil)
		assert.NoError(t, err)
		value := "blue"
		if want == 0 {
			value = "red"
		}
		assert.Equal(t, child.Doc["data"].(map[string]any)["value"], any(value))
	}
	for _, read := range reads {
		read.Store(0)
	}
	input := l.documents.artifacts[root]
	for name := range files {
		projection, err := l.projectionFile(t.Context(), input, name, true)
		assert.NoError(t, err)
		assert.Equal(t, projection.known, true)
		assert.Equal(t, projection.stop, false)
		assert.Diff(t, projection.inputs.Names, map[string]struct{}{})
	}
	projected := 0
	for _, read := range reads {
		if read.Load() != 0 {
			projected++
		}
	}
	assert.Equal(t, projected, wantReads)
	assert.NoError(t, l.PersistInputs(t.Context()))
	if warm {
		restored := l.documents.cache[filepath.Join(root, "cm-001.yaml")]
		assert.Equal(t, len(restored.objects), 1)
	}
}

func TestInputCache_RejectsChangedPublication(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	files := fstest.MapFS{"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: before}\n")}}
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: files},
	)
	assert.NoError(t, err)
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
	l := New(st)
	defer l.Close()
	l.SourceRoot, l.Options.Cache = root, owner
	configureSelection(t, l, selection.Request{Changed: true})
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	files["object.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: after}\n")}
	err = l.PersistInputs(t.Context())
	assert.Equal(t, errors.Is(err, manifest.ErrInput), persistent)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), persistent)
	if !persistent {
		assert.NoError(t, err)
	}
}

func cachedFixtureLoader(tb testing.TB, root string, files fstest.MapFS, owner *cache.Cache) *Loader {
	tb.Helper()
	view, _, err := gittree.New(
		tb.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: files},
	)
	assert.NoError(tb, err)
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
	l := New(st)
	l.SourceRoot, l.Options.Cache = root, owner
	configureSelection(tb, l, selection.Request{Changed: true})
	_, err = l.Load(tb.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(tb, err)
	return l
}

func TestInputCache_SubstitutionNamesFollowContent(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	files := fstest.MapFS{}
	for _, variable := range []string{"RED", "RED", "BLUE"} {
		files["object.yaml"] = &fstest.MapFile{
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\ndata: {value: '${" + variable + "}'}\n",
			),
		}
		l := cachedFixtureLoader(t, root, files, owner)
		input := l.documents.artifacts[root]
		projection, err := l.projectionFile(t.Context(), input, "object.yaml", true)
		assert.NoError(t, err)
		assert.Equal(t, projection.known, true)
		assert.Diff(t, projection.inputs.Names, map[string]struct{}{variable: {}})
		assert.NoError(t, l.PersistInputs(t.Context()))
		l.Close()
	}
}

func TestInputCache_SubstitutionExclusions(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"enabled", "annotations", "labels"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			persistent := cache.RenderPersistenceDisabledReason() == ""
			root := t.TempDir()
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			assert.Equal(t, owner.CanPersistRenders(), persistent)
			metadata := "metadata:\n  name: '${BUCKET}'\n"
			if field != "enabled" {
				metadata += "  " + field + ": {kustomize.toolkit.fluxcd.io/substitute: disabled}\n"
			}
			files := fstest.MapFS{"object.yaml": {Data: []byte(
				"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\n" + metadata +
					"spec:\n  target: {name: '${BUCKET}'}\n",
			)}}
			first := cachedFixtureLoader(t, root, files, owner)
			assert.NoError(t, first.PersistInputs(t.Context()))
			first.Close()
			restored := cachedFixtureLoader(t, root, files, owner)
			defer restored.Close()
			objects, err := restored.documents.censusFile(t.Context(), filepath.Join(root, "object.yaml"),
				manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assert.FatalIf(t, len(objects) != 1, "restored objects: %d", len(objects))
			assert.Equal(t, objects[0].fact.facts.substituteDisabled, field != "enabled")
		})
	}
}

func TestInputCache_ParsedHandoffPublishesNewFacts(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	files := fstest.MapFS{
		"object.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\ndata: {value: '${COLOR}'}\n"),
		},
	}
	planning := cachedFixtureLoader(t, root, files, owner)
	defer planning.Close()
	assert.NoError(t, planning.PersistInputs(t.Context()))
	prepared := planning.ParsedInputs()
	input := planning.documents.artifacts[root]
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := New(st)
	defer l.Close()
	l.SourceRoot, l.Options.Cache, l.Options.ParsedInputs = root, owner, prepared
	configureSelection(t, l, selection.Request{Changed: true})
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, err = l.projectionFile(t.Context(), input, "object.yaml", true)
	assert.NoError(t, err)
	assert.NoError(t, l.PersistInputs(t.Context()))
	if persistent {
		assert.Equal(
			t,
			prepared.sources[root].cache.files[inputCoordinate{name: "object.yaml"}].projection == nil,
			true,
		)
	} else {
		assert.Equal(t, prepared.sources[root].cache == nil, true)
	}
	restored := cachedFixtureLoader(t, root, files, owner)
	defer restored.Close()
	facts := restored.documents.cache[filepath.Join(root, "object.yaml")].projection
	assert.Equal(t, facts != nil, persistent)
	if persistent {
		assert.Equal(t, facts.Names == nil, true)
	}
	projection, err := restored.projectionFile(
		t.Context(),
		restored.documents.artifacts[root],
		"object.yaml",
		true,
	)
	assert.NoError(t, err)
	facts = projection.inputs
	assert.Diff(t, facts.Names, map[string]struct{}{"COLOR": {}})
	captured := restored.ParsedInputs()
	file := filepath.Join(root, "object.yaml")
	assert.Equal(t, restored.documents.cache[file].parsed != nil, true)
	assert.NoError(t, restored.FinalizeIndex(t.Context(), root, true))
	assert.Equal(t, restored.documents.cache[file].parsed == nil, true)
	assert.Equal(t, restored.documents.cache[file].projection == nil, true)
	prior := captured.sources[root]
	assert.Diff(t, prior.lookup(file).projection.Names, map[string]struct{}{"COLOR": {}})
}

func TestInputCache_LoadEvidenceKeepsPathBoundaries(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	state := &inputCache{
		input: &store.SourceArtifact{LocalPath: root},
		types: map[string]fs.FileMode{
			".":                        fs.ModeDir,
			"app":                      fs.ModeDir,
			"app-before":               fs.ModeDir,
			"app-before/other.yaml":    0,
			"app/.hidden":              fs.ModeDir,
			"app/.hidden/ignored.yaml": 0,
			"app/object.yaml":          0,
			"app/part":                 fs.ModeDir,
			"app/part/child.yaml":      0,
			"app2":                     fs.ModeDir,
			"app2/other.yaml":          0,
		},
		digests: map[string]string{
			"app-before/other.yaml":    "sibling-before",
			"app/.hidden/ignored.yaml": "hidden",
			"app/object.yaml":          "object",
			"app/part/child.yaml":      "child",
			"app2/other.yaml":          "sibling-after",
		},
	}
	for _, name := range []string{"app", "."} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			facts := &loadFacts{directories: map[string]loadDirectory{filepath.Join(root, name): {}}}
			load, valid := cacheLoad(
				state,
				loadContext{path: filepath.Join(root, name)},
				facts,
				slices.Sorted(maps.Keys(state.types)),
			)
			assert.Equal(t, valid, true)
			inputs := []string{
				filepath.ToSlash(filepath.Join(name, ".krmignore")), "app/object.yaml", "app/part/child.yaml",
			}
			types := []string{"app", "app/.hidden", "app/object.yaml", "app/part", "app/part/child.yaml"}
			if name == "." {
				inputs = append(inputs, "app-before/other.yaml", "app2/other.yaml")
				types = append(types, ".", "app-before", "app-before/other.yaml", "app2", "app2/other.yaml")
			}
			slices.Sort(inputs)
			slices.Sort(types)
			assert.Diff(t, load.Inputs, inputs)
			assert.Diff(t, load.Types, types)
		})
	}
}

func TestInputCache_NestedMountOverridesParentFrame(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	parentFiles := fstest.MapFS{
		"nested/object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: parent}\n")},
	}
	first := cachedFixtureLoader(t, root, parentFiles, owner)
	assert.NoError(t, first.PersistInputs(t.Context()))
	first.Close()
	load := func(cached bool) *Loader {
		parent, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: parentFiles},
		)
		assert.NoError(t, err)
		child, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
				"object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n")},
			}},
		)
		assert.NoError(t, err)
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: parent})
		source := &manifest.GitRepository{Name: "nested", Namespace: "apps"}
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: nested, FS: child})
		l := New(st)
		l.SourceRoot = root
		if cached {
			l.Options.Cache = owner
		}
		configureSelection(t, l, selection.Request{Changed: true})
		_, err = l.Load(t.Context(), nested, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		return l
	}
	cold := load(false)
	defer cold.Close()
	warm := load(true)
	defer warm.Close()
	assert.Diff(t, warm.Index, cold.Index)
}

func TestInputCache_HelmCatalogMatchesCold(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	files := fstest.MapFS{
		"release.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec:\n  chart:\n    spec:\n      chart: app\n      version: 1.2.3\n      sourceRef: {kind: HelmRepository, name: charts}\n",
			),
		},
	}
	cold := cachedFixtureLoader(t, root, files, owner)
	defer cold.Close()
	assert.NoError(t, cold.PersistInputs(t.Context()))
	warm := cachedFixtureLoader(t, root, files, owner)
	defer warm.Close()
	id := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Name:      "app",
		Namespace: "apps",
	}
	before, known := cold.Occurrence(id)
	assert.Equal(t, known, true)
	after, known := warm.Occurrence(id)
	assert.Equal(t, known, true)
	beforeSource, beforePath, beforeHasPath := before.Catalog.SourcePath()
	afterSource, afterPath, afterHasPath := after.Catalog.SourcePath()
	t.Logf(
		"cold catalog source=%s path=%q hasPath=%t; warm catalog source=%s path=%q hasPath=%t",
		beforeSource,
		beforePath,
		beforeHasPath,
		afterSource,
		afterPath,
		afterHasPath,
	)
	assert.Equal(t, manifest.Equal(before.Catalog, after.Catalog), true)
}

func TestInputCache_NestedMountInvalidatesTraversals(t *testing.T) {
	t.Parallel()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	files := fstest.MapFS{
		"nested/kustomization.yaml": {Data: []byte("resources: [object.yaml]\nnamespace: parent\n")},
		"nested/object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: parent}\n")},
		"right/kustomization.yaml":  {Data: []byte("resources: [object.yaml]\n")},
		"right/object.yaml":         {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: right}\n")},
	}
	first := cachedFixtureLoader(t, root, files, owner)
	first.ensureScopes()
	key := func(side string) physicalContext {
		return physicalContext{root: root, dir: side, role: kustomizeinput.Resource, sourceRoot: root}
	}
	for _, side := range []string{"nested", "right"} {
		first.scopeWalk(&graphVisit{graphContext: key(side).graphContext})
	}
	assert.NoError(t, first.PersistInputs(t.Context()))
	first.Close()
	load := func(cached bool) *Loader {
		parent, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: files},
		)
		assert.NoError(t, err)
		child, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
				"kustomization.yaml": {Data: []byte("resources: [object.yaml]\nnamespace: child\n")},
				"object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n")},
			}},
		)
		assert.NoError(t, err)
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: parent})
		source := &manifest.GitRepository{Name: "nested", Namespace: "apps"}
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: nested, FS: child})
		l := New(st)
		l.SourceRoot = root
		if cached {
			l.Options.Cache = owner
		}
		configureSelection(t, l, selection.Request{Changed: true})
		l.documents.compact = true
		l.mountSources()
		assert.NoError(t, l.restoreCachedInputs(t.Context()))
		if cached {
			facts := l.sourceFacts(root)
			_, restored := facts.walk(new(key("nested")))
			assert.Equal(t, restored, false)
			_, restored = facts.walk(new(key("right")))
			assert.Equal(t, restored, persistent)
			_, restored = facts.load(loadContext{path: root})
			assert.Equal(t, restored, false)
			assert.Equal(t, l.documents.cache[nested].sourceRoot, "")
		}
		_, err = l.Load(t.Context(), nested, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		return l
	}
	cold := load(false)
	defer cold.Close()
	warm := load(true)
	defer warm.Close()
	assert.Diff(t, warm.Index, cold.Index)
}

type mountedInputCase struct {
	name, resource, childPath string
	builder                   bool
}

func TestInputCache_NewMountMatchesCold(t *testing.T) {
	t.Parallel()
	for _, testCase := range []mountedInputCase{
		{name: "directory", resource: "nested", childPath: "object.yaml"},
		{name: "document", resource: "nested/object.yaml", childPath: "object.yaml"},
		{name: "ancestor", resource: "nested/absent", childPath: "absent/object.yaml"},
		{name: "builder", resource: "nested", childPath: "object.yaml", builder: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			testInputCacheNewMountMatchesCold(t, testCase)
		})
	}
}

func testInputCacheNewMountMatchesCold(t *testing.T, testCase mountedInputCase) {
	t.Helper()
	persistent := cache.RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	files := fstest.MapFS{
		"kustomization.yaml":       {Data: []byte("resources: [" + testCase.resource + "]\n")},
		"right/kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
		"right/object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: right}\n")},
	}
	key := func(dir string) physicalContext {
		return physicalContext{root: root, dir: dir, role: kustomizeinput.Resource, sourceRoot: root}
	}
	first := cachedFixtureLoader(t, root, files, owner)
	first.ensureScopes()
	for _, dir := range []string{".", "right"} {
		first.scopeWalk(&graphVisit{graphContext: key(dir).graphContext})
	}
	assert.NoError(t, first.PersistInputs(t.Context()))
	first.Close()
	load := func(cached bool) *Loader {
		parent, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: files},
		)
		assert.NoError(t, err)
		childFiles := fstest.MapFS{
			testCase.childPath: {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n")},
		}
		if testCase.builder {
			childFiles["kustomization.yaml"] = &fstest.MapFile{
				Data: []byte("resources: [object.yaml]\nnamespace: child\n"),
			}
		}
		child, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: childFiles},
		)
		assert.NoError(t, err)
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: parent})
		source := &manifest.GitRepository{Name: "nested", Namespace: "apps"}
		st.AddObject(source)
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: nested, FS: child})
		l := New(st)
		l.SourceRoot = root
		if cached {
			l.Options.Cache = owner
		}
		configureSelection(t, l, selection.Request{Changed: true})
		l.documents.compact = true
		l.mountSources()
		assert.NoError(t, l.restoreCachedInputs(t.Context()))
		if cached {
			facts := l.sourceFacts(root)
			_, restored := facts.walk(new(key(".")))
			assert.Equal(t, restored, false)
			_, restored = facts.walk(new(key("right")))
			assert.Equal(t, restored, persistent)
			_, restored = facts.load(loadContext{path: root})
			assert.Equal(t, restored, false)
			assert.Equal(t, l.documents.cache[nested].sourceRoot, "")
		}
		_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		return l
	}
	cold := load(false)
	defer cold.Close()
	warm := load(true)
	defer warm.Close()
	assert.Diff(t, warm.Index, cold.Index)
	for _, l := range []*Loader{cold, warm} {
		l.ensureScopes()
	}
	visit := graphVisit{graphContext: key(".").graphContext}
	got, want := warm.scopeWalk(&visit), cold.scopeWalk(&visit)
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(graphVisit{}, graphContext{})); diff != "" {
		t.Fatal(diff)
	}
	file := filepath.Join(nested, filepath.FromSlash(testCase.childPath))
	assert.Equal(t, warm.documents.cache[file].sourceRoot, nested)
	if testCase.resource == "nested" {
		assert.Equal(t, warm.documents.cache[nested].pathState, pathDirectory)
		assert.Equal(t, warm.documents.cache[nested].sourceRoot, nested)
	}
}

func TestInputCache_DeferredPublicationPreservesOffsets(t *testing.T) {
	t.Parallel()
	body, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: map[string]any{"kind": "ConfigMap"}}})
	assert.NoError(t, err)
	original := &cachedInput{
		Name: "object.yaml", Digest: "observed",
		Objects:    []cachedOccurrence{{Offset: 23, Size: len(body), payload: &inputPayload{data: body}}},
		Projection: &cachedProjection{Offset: 17, Size: 2, payload: &inputPayload{data: []byte("{}")}},
	}
	saved, known, err := cacheInput(original.Name, original.Digest, &document{deferred: original})
	assert.NoError(t, err)
	assert.Equal(t, known, true)
	frame := inputFrame{Files: []cachedInput{saved}}
	_, err = frame.prepare(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, frame.Files[0].Projection.Offset, int64(0))
	assert.Equal(t, frame.Files[0].Objects[0].Offset, int64(2))
	assert.Equal(t, original.Projection.Offset, int64(17))
	assert.Equal(t, original.Objects[0].Offset, int64(23))
}
