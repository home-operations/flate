package loader

import (
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestInputCache_RestoresIndependentWalks(t *testing.T) {
	t.Parallel()
	for _, edit := range []string{"unchanged", "file", "builder", "precedence", "membership", "missing", "transformer"} {
		t.Run(edit, func(t *testing.T) {
			t.Parallel()
			testInputCacheRestoresIndependentWalks(t, edit)
		})
	}
}

func testInputCacheRestoresIndependentWalks(t *testing.T, edit string) {
	t.Helper()
	reason := cache.RenderPersistenceDisabledReason()
	t.Logf("render persistence reason=%q backing=disk", reason)
	persistent := reason == ""
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	files := fstest.MapFS{
		"left/kustomization.yml": {
			Data: []byte(`resources: [objects, absent.yaml]
components: [component, empty-component]
transformers: [../transformer]
`),
		},
		"left/objects/object.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: left}\n"),
		},
		"left/component/kustomization.yaml": {
			Data: []byte("kind: Component\nresources: [object.yaml]\n"),
		},
		"left/component/object.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: component}\n"),
		},
		"left/empty-component/kustomization.yaml": {Data: []byte("kind: Component\n")},
		"right/kustomization.yaml":                {Data: []byte("resources: [object.yaml]\n")},
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
		wantUnknownRecipe := side == "left" && edit != "missing" && edit != "builder" && edit != "precedence"
		assert.Equal(t, cold.scopes.walks[visit.graphContext].unknownRecipe, wantUnknownRecipe)
		assert.Equal(t, restored.scopes.walks[visit.graphContext].unknownRecipe, wantUnknownRecipe)
		if diff := cmp.Diff(want, got, cmp.AllowUnexported(graphVisit{}, graphContext{})); diff != "" {
			t.Fatal(diff)
		}
		if side == "left" && edit == "unchanged" {
			input := &store.SourceArtifact{LocalPath: root}
			if persistent {
				assert.Equal(t, facts.walks.values[key(side)].imported, true)
			} else {
				got = roundTripScopeWalk(t, restored, new(key(side)), want)
			}
			for _, visits := range [][]graphVisit{want, got} {
				assert.Equal(t, packageReached(input, visits, filepath.Join(root, "left", "component")), true)
				assert.Equal(t, packageReached(input, visits, filepath.Join(root, "left", "empty-component")), false)
			}
		}
	}
}

func roundTripScopeWalk(t *testing.T, l *Loader, key *physicalContext, visits []graphVisit) []graphVisit {
	t.Helper()
	saved, ok := cacheWalk(&inputCache{input: &store.SourceArtifact{LocalPath: key.root}}, key, physicalWalk{
		visits: visits,
		inputs: []walkInput{{path: filepath.Join(key.root, key.dir)}},
	})
	assert.FatalIf(t, !ok, "scope walk did not fit the input frame")
	frame := &inputFrame{Walks: []cachedWalk{saved}}
	generation := fstest.MapFS{"content": {Data: encodeInputFrame(t, frame)}}
	decoded, err := readInputFrame(t.Context(), generation)
	assert.NoError(t, err)
	restored, ok := l.restoreWalk(key.root, &decoded.Walks[0])
	assert.FatalIf(t, !ok, "scope walk did not restore from the input frame")
	if diff := cmp.Diff(visits, restored.visits, cmp.AllowUnexported(graphVisit{}, graphContext{})); diff != "" {
		t.Fatal(diff)
	}
	return restored.visits
}

func TestInputCache_RejectsFrameWithoutComponentEdges(t *testing.T) {
	t.Parallel()
	if reason := cache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skipf("render persistence disabled: %s", reason)
	}
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, owner.Close()) })
	files := fstest.MapFS{
		"apps/kustomization.yaml":       {Data: []byte("components: [empty]\n")},
		"apps/empty/kustomization.yaml": {Data: []byte("kind: Component\n")},
	}
	key := physicalContext{root: root, dir: "apps", sourceRoot: root}
	entry := filepath.Join(root, "apps", "empty")
	first := cachedFixtureLoader(t, root, files, owner)
	t.Cleanup(first.Close)
	first.ensureScopes()
	visits := first.scopeWalk(&graphVisit{graphContext: key.graphContext})
	state := first.inputCaches[root]
	assert.Equal(t, packageReached(state.input, visits, entry), false)
	for i := range visits {
		visits[i].edge = kustomizeinput.Resource
	}
	assert.Equal(t, packageReached(state.input, visits, entry), true)

	origin, url := state.input.OriginPath(root), state.input.URL
	frame, err := first.cacheSourceInputs(state)
	assert.NoError(t, err)
	assert.FatalIf(t, frame == nil, "expected an input frame for the earlier lookup")
	publishVersionedInputFrame(t, first, frame, 1)
	policy := &cache.Policy{MaxAge: cache.Forever}

	current := cache.InputFrameLookup(origin, url)
	_, _, _, err = owner.Blob(t.Context(), current, policy, nil)
	assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
	restored := cachedFixtureLoader(t, root, files, owner)
	t.Cleanup(restored.Close)
	facts := restored.sourceFacts(root)
	_, hit := facts.walk(&key)
	assert.Equal(t, hit, false)
	restored.ensureScopes()
	got := restored.scopeWalk(&graphVisit{graphContext: key.graphContext})
	assert.Equal(t, packageReached(state.input, got, entry), false)
}
