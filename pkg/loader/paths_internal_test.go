package loader

import (
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ProducedChildPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, postbuild string
		known           bool
	}{
		{name: "inline", postbuild: "  postBuild: {substitute: {ROOT: apps/red}}\n", known: true},
		{name: "reference", postbuild: "  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n", known: true},
		{name: "unknown", postbuild: "  postBuild: {substituteFrom: [{kind: ConfigMap, name: missing}]}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"flux/kustomization.yaml": {Data: []byte("resources: [owner.yaml,settings.yaml]\n")},
				"flux/owner.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
					"metadata: {name: owner, namespace: apps}\nspec:\n  path: ./producers\n  targetNamespace: apps\n" + tc.postbuild)},
				"flux/settings.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
					"metadata: {name: settings, namespace: apps}\ndata: {ROOT: apps/red}\n")},
				"producers/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
				"producers/child.yaml": {
					Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
						"metadata: {name: child, namespace: apps}\nspec: {path: './${ROOT}'}\n"),
				},
				"apps/red/kustomization.yaml": {Data: []byte("resources: []\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Changes = change.NewSet([]string{"apps/red/cm.yaml"})

			_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			child := manifest.NamedResource{
				Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "child",
			}
			owner := child
			owner.Name = "owner"
			record, _ := l.Index.Lookup(child)
			want := []string{"apps/red"}
			if !tc.known {
				want = []string{""}
			}
			assert.Diff(t, record.ContentRoots[manifest.BootstrapSourceID], want)
			assert.Equal(t, l.CatalogObject(child).(*manifest.Kustomization).Path, "./${ROOT}")
			parent, _ := l.Index.Lookup(owner)
			assert.Equal(t, parent.StaticOutput, tc.known)
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, child), true)
			assert.Equal(t, slices.Contains(proposal.Needed, owner), true)
		})
	}
}

func TestFinalizeIndex_EffectiveInputContent(t *testing.T) {
	t.Parallel()
	for _, tc := range effectiveInputCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var previous any
			for side := range 2 {
				root := t.TempDir()
				files := new(tc).files(side)
				st := store.New()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
				l := New(st)
				l.SourceRoot = root
				configureSelection(t, l, selection.Request{Changed: true})
				l.Options.Changes = change.NewSet([]string{"flux/settings.yaml"})

				_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
				assert.NoError(t, err)
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
				id := manifest.NamedResource{
					Group:     manifest.FluxKustomizeDomain,
					Kind:      manifest.KindKustomization,
					Namespace: "apps",
					Name:      "reader",
				}
				got, known := l.InputContent[id]
				assert.Equal(t, known, tc.known)
				if side == 1 && tc.known {
					assert.Equal(t, manifest.ValueEqual(previous, got.Content), tc.same)
				}
				if known {
					previous = got.Content
				}
			}
		})
	}
}

type effectiveInputCase struct {
	name, expression, postbuild string
	packageBody, operand, patch string
	known, same                 bool
}

func effectiveInputCases() []effectiveInputCase {
	return []effectiveInputCase{
		{name: "unused", expression: "${OTHER}", known: true, same: true},
		{name: "used", expression: "${CHANGED}", known: true},
		{
			name: "Flux patch", expression: "${OTHER}", known: true,
			patch: `[{"op":"replace","path":"/data/value","value":"${CHANGED}"}]`,
		},
		{
			name: "escaped Flux patch", expression: "${OTHER}", known: true,
			patch: `[{"op":"replace","path":"/data/value","value":"\u0024{CHANGED}"}]`,
		},
		{
			name: "malformed inline patch", expression: "${OTHER}",
			packageBody: "patches: [{patch: '[invalid'}]\n",
		},
		{
			name: "malformed patch file", expression: "${OTHER}",
			packageBody: "patches: [{path: x.txt}]\n", operand: "[invalid",
		},
		{
			name: "escaped inline JSON patch", expression: "${OTHER}", known: true,
			packageBody: `patches:
- target: {kind: ConfigMap, name: payload}
  patch: '[{"op":"replace","path":"/data/value","value":"\u0024{CHANGED}"}]'
`,
		},
		{
			name: "escaped JSON patch file", expression: "${OTHER}", known: true,
			packageBody: "patches:\n- target: {kind: ConfigMap, name: payload}\n  path: x.txt\n",
			operand:     `[{"op":"replace","path":"/data/value","value":"\u0024{CHANGED}"}]`,
		},
		{name: "unproved Flux transform", expression: "${OTHER}"},
		{
			name:       "inline precedence",
			expression: "${CHANGED}",
			postbuild:  ", substitute: {CHANGED: fixed}",
			known:      true,
			same:       true,
		},
		{name: "activation", expression: "${MISSING:=fallback}", known: true},
		{name: "invalid syntax", expression: "${ARR[@]}"},
		{name: "invalid names", expression: "${OTHER}"},
		{name: "missing provider", expression: "${OTHER}"},
		{name: "unproved transform", expression: "${OTHER}"},
		{
			name: "ConfigMap files", expression: "${OTHER}", operand: "${CHANGED}",
			packageBody: "configMapGenerator: [{name: generated, files: [x.txt]}]\n",
		},
		{
			name: "ConfigMap envs", expression: "${OTHER}", operand: "VALUE=${CHANGED}\n",
			packageBody: "configMapGenerator: [{name: generated, envs: [x.txt]}]\n",
		},
		{
			name: "Secret files", expression: "${OTHER}", operand: "${CHANGED}",
			packageBody: "secretGenerator: [{name: generated, files: [x.txt]}]\n",
		},
		{
			name: "Secret envs", expression: "${OTHER}", operand: "VALUE=${CHANGED}\n",
			packageBody: "secretGenerator: [{name: generated, envs: [x.txt]}]\n",
		},
		{
			name: "generator literal", expression: "${OTHER}", known: true,
			packageBody: "configMapGenerator: [{name: generated, literals: ['value=${CHANGED}']}]\n",
		},
		{
			name: "replacements", expression: "prefix{CHANGED}",
			packageBody: "replacements:\n- source: {kind: ConfigMap, name: payload, fieldPath: data.prefix}\n" +
				"  targets:\n  - select: {kind: ConfigMap, name: payload}\n" +
				"    fieldPaths: [metadata.annotations.value]\n    options: {delimiter: '{', index: 0}\n",
		},
		{
			name: "vars", expression: "$(DOLLAR){CHANGED}",
			packageBody: "vars:\n- name: DOLLAR\n" +
				"  objref: {apiVersion: v1, kind: ConfigMap, name: payload}\n  fieldref: {fieldpath: data.prefix}\n",
		},
	}
}

func (c *effectiveInputCase) files(side int) fstest.MapFS {
	tc := *c
	data := "{CHANGED: old, OTHER: same}"
	if side == 1 {
		data = "{CHANGED: new, OTHER: same}"
	}
	if tc.name == "activation" && side == 0 {
		data = "{}"
	}
	if tc.name == "invalid names" {
		data = "{bad-name: invalid, OTHER: same}"
	}
	packageBody := "resources: [payload.yaml]\n" + tc.packageBody
	if tc.name == "unproved transform" {
		packageBody += "namePrefix: projected-\n"
	}
	declaration := "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
		"metadata: {name: reader, namespace: apps}\nspec:\n path: ./app\n" +
		" postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]" + tc.postbuild + "}\n"
	if tc.patch != "" {
		declaration += " patches:\n - target: {kind: ConfigMap, name: payload}\n   patch: '" + tc.patch + "'\n"
	}
	if tc.name == "unproved Flux transform" {
		declaration += " namePrefix: projected-\n"
	}
	files := fstest.MapFS{
		"flux/reader.yaml": {Data: []byte(declaration)},
		"flux/settings.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
			"metadata: {name: settings, namespace: apps}\ndata: " + data + "\n")},
		"app/kustomization.yaml": {Data: []byte(packageBody)},
		"app/payload.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
			"metadata: {name: payload, namespace: apps, annotations: {value: '" + tc.expression + "'}}\n" +
			"data: {value: '" + tc.expression + "', prefix: '$'}\n")},
		"app/x.txt": {Data: []byte(tc.operand)},
	}
	if tc.name == "missing provider" {
		delete(files, "flux/settings.yaml")
	}
	return files
}

func TestFinalizeIndex_InputProjectionLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	reader := manifest.NamedResource{
		Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "reader",
	}
	provider := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	files := fstest.MapFS{
		"flux/reader.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
			"metadata: {name: reader, namespace: apps}\n" +
			"spec: {path: ./app, postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}}\n")},
		"flux/settings.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
			"metadata: {name: settings, namespace: apps}\ndata: {VALUE: old}\n")},
		"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"app/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${VALUE}'}\n"),
		},
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	borrowed := l.InputContent
	first, known := borrowed[reader]
	assert.Equal(t, known, true)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	assert.Equal(t, l.InputContent[reader].Inputs[provider], first.Inputs[provider])
	files = maps.Clone(files)
	files["flux/settings.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\n" +
		"metadata: {name: settings, namespace: apps}\ndata: {VALUE: new}\n")}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	_, err = l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	second, known := l.InputContent[reader]
	assert.Equal(t, known, true)
	assert.Equal(t, manifest.ValueEqual(first.Content, second.Content), false)
	valueResult, err := first.Inputs[provider].Value("VALUE")
	value, present := valueResult.Value, valueResult.Found
	assert.NoError(t, err)
	assert.Equal(t, present, true)
	assert.Equal(t, value, "old")
	assert.Equal(t, borrowed[reader].Inputs[provider], first.Inputs[provider])
	files = maps.Clone(files)
	files["flux/reader.yaml"] = &fstest.MapFile{
		Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\n" +
			"metadata: {name: reader, namespace: apps}\nspec: {path: ./app}\n"),
	}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	_, err = l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	_, known = l.InputContent[reader]
	assert.Equal(t, known, false)
	assert.Equal(t, len(borrowed), 1)
	l.Retire()
	assert.Equal(t, len(l.InputContent), 0)
	assert.Equal(t, len(borrowed), 1)
}
