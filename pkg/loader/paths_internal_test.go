package loader

import (
	"context"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

//nolint:gocognit // Context identity, convergence, and uncertainty MUST share the same duplicate-visit assertions.
func TestFinalizeIndex_RepeatedContext(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, expression, value string
		published, uncertain    bool
	}{
		{name: "changed", expression: "${VALUE}", value: "work", published: true},
		{name: "unchanged", expression: "${VALUE}", value: "${VALUE}"},
		{name: "uncertain", expression: "${UNKNOWN}", value: "work", published: true, uncertain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			l.scopes = &scopeConstruction{}
			l.documents.compact = true
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": tc.value}}
			if tc.uncertain {
				owner.PostBuild.SubstituteFrom = []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "unread", Optional: true},
				}
			}
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
			child.Path = tc.expression
			if tc.uncertain {
				child.Name = tc.expression
			}
			fact := compactOccurrence(child, manifest.TemplateProjection{})
			fact.Declaration = occurrenceID{Source: l.SourceRoot, File: "child.yaml"}
			file := filepath.Join(l.SourceRoot, "child.yaml")
			visit := graphVisit{file: file, owner: owner.Named(), source: manifest.BootstrapSourceID}
			visits := []graphVisit{visit}
			if tc.published {
				qualified := visit
				qualified.source.Name = "another-source"
				transformed := visit
				transformed.namePrefix = "pre-"
				anotherFile := visit
				anotherFile.file = filepath.Join(l.SourceRoot, "copy.yaml")
				visits = append(visits, qualified, transformed, anotherFile)
			}
			anotherDeclaration := fact
			anotherDeclaration.Declaration.Document++
			for i := range visits {
				objects := []parsedObject{{fact: &fact, version: manifest.FluxKustomizeDomain + "/v1"}}
				if i == len(visits)-1 {
					objects = append(objects, parsedObject{
						fact: &anotherDeclaration, version: manifest.FluxKustomizeDomain + "/v1",
					})
				}
				l.documents.cache[visits[i].file] = document{objects: objects}
			}
			for pass := range 2 {
				resolution := substitutionResolution{loader: l}
				for i := range visits {
					assert.NoError(t, resolution.resolveVisit(t.Context(), &visits[i]))
					assert.NoError(t, resolution.resolveVisit(t.Context(), &visits[i]))
				}
				assert.Equal(t, len(resolution.visited), len(visits))
				wantPublished := 0
				if tc.published {
					wantPublished = len(visits) + 1
				}
				assert.Equal(t, len(resolution.contextual), wantPublished)
				assert.Equal(t, resolution.contextualChanged, tc.published && pass == 0)
				for key, projected := range resolution.contextual {
					assert.Equal(t, projected.facts.uncertain, tc.uncertain)
					assert.Equal(t, projected.Authored, fact.Authored)
					assert.Equal(t, projected.Declaration, key.declaration)
					assert.Equal(t, key.declaration.Source, fact.Declaration.Source)
					assert.Equal(t, key.declaration.File, fact.Declaration.File)
					if !tc.uncertain {
						assert.Equal(t, projected.Identity.Name, key.context.namePrefix+"child")
						assert.Equal(t, projected.facts.path, "work")
					}
				}
				l.scopes.contextual = resolution.contextual
			}
			assert.Equal(t, fact.facts.path, tc.expression)
		})
	}
}

func TestSubstitutionResolution_InputDiscovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		encrypted bool
	}{
		{name: "absent"},
		{name: "encrypted", encrypted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			owner.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindSecret, Name: "vars"},
			}}
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
			reader := &substitutionDataReader{
				inputs: map[manifest.NamedResource]store.ReferenceFacts{}, lookups: map[manifest.NamedResource]int{},
			}
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "vars"}
			if tc.encrypted {
				data := manifest.NewSecret(id, nil, nil)
				reader.inputs[id] = store.ReferenceFacts{Data: data, Original: data, Warnings: []manifest.Warning{
					{Resource: id, Category: manifest.WarnSOPSUnreadable, Detail: []string{"VALUE"}},
				}}
			}
			resolution := substitutionResolution{loader: l, provider: values.NewStoreProvider(reader)}
			_, known := resolution.resolveValue(t.Context(), owner.Named(), "${VALUE}")
			assert.Equal(t, known, false)
			assert.Equal(t, resolution.needsInputs, !tc.encrypted)
			assert.Equal(t, reader.lookups[id], 1)
		})
	}
}

func TestFinalizeIndex_ApplicationInputProducer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"root/kustomization.yaml": {Data: []byte("resources: [owner.yaml]\n")},
		"root/owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: apps}
spec:
  path: ./children
  postBuild: {substituteFrom: [{kind: Secret, name: vars}]}
`)},
		"children/kustomization.yaml": {Data: []byte("resources: [objects.yaml]\n")},
		"children/objects.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: './${VALUE}'}
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: vars, namespace: apps}
spec: {target: {name: vars}, dataFrom: [{extract: {key: vars}}]}
`)},
	}})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	_, err = l.Load(t.Context(), filepath.Join(root, "children"), testutil.KustomizationID("apps", "owner"), nil)
	assert.NoError(t, err)
	l.Admit(root)
	file := filepath.Join(root, "children", "objects.yaml")
	assert.Equal(t, l.documents.cache[file].coverage, manifest.ApplicationDocuments)
	producer := manifest.NamedResource{
		Group: "external-secrets.io", Kind: "ExternalSecret", Namespace: "apps", Name: "vars",
	}
	_, found := l.Index.Lookup(producer)
	assert.Equal(t, found, false)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	_, found = l.Index.Lookup(producer)
	assert.Equal(t, found, true)
	assert.Equal(t, l.documents.cache[file].coverage, manifest.IncidentalDocuments)
}

func TestSubstitutionResolution_ApplicationBindings(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: apps}
spec: {path: ./work, postBuild: {substitute: {VALUE: known}}}
`)},
	}})
	l := New(st)
	l.SourceRoot = root
	l.documents.compact = true
	l.mountSources()
	reader := l.documents
	reader.intent = manifest.ApplicationDocuments
	file := filepath.Join(root, "owner.yaml")
	objects, err := reader.censusFile(t.Context(), file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(objects), 1)
	occurrence := objects[0].fact
	owner := occurrence.Identity
	l.locations = map[manifest.NamedResource]authoredOccurrence{owner: *occurrence}
	l.Index.Records[owner] = manifest.DiscoveryRecord{Authored: owner, Path: file, File: "owner.yaml"}
	assert.Equal(t, occurrence.facts.structural.PostBuild == nil, true)
	resolution := substitutionResolution{loader: l}
	got, known := resolution.resolveValue(t.Context(), owner, "${VALUE}")
	assert.Equal(t, got, "known")
	assert.Equal(t, known, true)
	assert.Equal(t, resolution.needsInputs, false)
	assert.Equal(t, occurrence.facts.structural.PostBuild == nil, true)
}

func TestSubstitutionResolution_InheritedBindings(t *testing.T) {
	t.Parallel()
	ancestor := &manifest.Kustomization{Name: "ancestor", Namespace: "apps"}
	ancestor.PostBuild = &kustomizev1.PostBuild{
		Substitute: map[string]string{"FIRST_VALUE": "one", "SECOND_VALUE": "two"},
		SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "unread", Optional: true},
		},
	}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{
		"FIRST": "${FIRST_VALUE}", "SECOND": "${SECOND_VALUE}", "UNUSED": "${UNREAD}",
	}}
	l := New(store.New())
	l.Index.Records[ancestor.Named()] = manifest.DiscoveryRecord{Declaration: ancestor}
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner, Parent: ancestor.Named()}
	resolution := substitutionResolution{loader: l}
	for _, tc := range []struct {
		expression, want string
		known            bool
	}{
		{"${FIRST}", "one", true},
		{"${SECOND}", "two", true},
		{"${UNUSED}", "${UNUSED}", false},
		{"${FIRST}/${SECOND}", "one/two", true},
		{"${FIRST}", "one", true},
	} {
		got, known := resolution.resolveValue(t.Context(), owner.Named(), tc.expression)
		assert.Equal(t, got, tc.want)
		assert.Equal(t, known, tc.known)
	}
	assert.Diff(t, owner.PostBuild.Substitute, map[string]string{
		"FIRST": "${FIRST_VALUE}", "SECOND": "${SECOND_VALUE}", "UNUSED": "${UNREAD}",
	})
}

func TestSubstitutionResolution_RecursiveOwner(t *testing.T) {
	t.Parallel()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{
		"FIXED": "known", "LINK": "${UPSTREAM}",
	}}
	ancestor := &manifest.Kustomization{Name: "ancestor", Namespace: "apps"}
	ancestor.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"UPSTREAM": "${LINK}"}}
	l := New(store.New())
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner, Parent: ancestor.Named()}
	l.Index.Records[ancestor.Named()] = manifest.DiscoveryRecord{Declaration: ancestor, Parent: owner.Named()}
	resolution := substitutionResolution{loader: l}
	got, known := resolution.resolveValue(t.Context(), owner.Named(), "${FIXED}")
	assert.Equal(t, got, "known")
	assert.Equal(t, known, true)
	resolution.resolving[owner.Named()] = true
	got, known = resolution.resolveValue(t.Context(), owner.Named(), "${FIXED}")
	assert.Equal(t, got, "${FIXED}")
	assert.Equal(t, known, false)
	delete(resolution.resolving, owner.Named())
	got, known = resolution.resolveValue(t.Context(), owner.Named(), "${LINK}")
	assert.Equal(t, got, "${LINK}")
	assert.Equal(t, known, false)
	assert.Equal(t, len(resolution.resolving), 0)
	got, known = resolution.resolveValue(t.Context(), owner.Named(), "${FIXED}")
	assert.Equal(t, got, "known")
	assert.Equal(t, known, true)
}

func TestSubstitutionResolution_ReusedCancellation(t *testing.T) {
	t.Parallel()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "work"}}
	l := New(store.New())
	l.scopes = &scopeConstruction{}
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	child.Path = "./${VALUE}"
	fact := compactOccurrence(child, manifest.TemplateProjection{})
	parsed := parsedObject{fact: &fact}
	visit := &graphVisit{owner: owner.Named()}
	resolution := substitutionResolution{loader: l}
	assert.NoError(t, resolution.resolveContextualObject(t.Context(), &parsed, visit))
	got, known := resolution.resolveValue(t.Context(), owner.Named(), "./${VALUE}")
	assert.Equal(t, got, "./work")
	assert.Equal(t, known, true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, known = resolution.resolveValue(ctx, owner.Named(), "./${VALUE}")
	assert.Equal(t, got, "./${VALUE}")
	assert.Equal(t, known, false)
	assert.Equal(t, resolution.resolveContextualObject(ctx, &parsed, visit), ctx.Err())
}

func TestSubstitutionResolution_PassLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"root/kustomization.yaml": {Data: []byte("resources: [red.yaml, blue.yaml]\n")},
		"root/red.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ancestor-red, namespace: apps}
spec: {path: ./children, postBuild: {substitute: {UPSTREAM: red}}}
`)},
		"root/blue.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ancestor-blue, namespace: apps}
spec: {path: ./empty, postBuild: {substitute: {UPSTREAM: blue}}}
`)},
		"empty/kustomization.yaml": {Data: []byte("resources: []\n")},
		"children/owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: apps}
spec:
  path: ./work
  postBuild:
    substitute: {COLOR: '${UPSTREAM}'}
    substituteFrom: [{kind: ConfigMap, name: vars, optional: true}]
`)},
		"work/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out}\n")},
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"work/cm.yaml"})
	_, err := l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	owner := testutil.KustomizationID("apps", "owner")
	first := substitutionResolution{loader: l}
	got, known := first.resolveValue(t.Context(), owner, "${COLOR}")
	assert.Equal(t, got, "red")
	assert.Equal(t, known, true)
	_, known = first.resolveValue(t.Context(), owner, "./${DIR}")
	assert.Equal(t, known, false)
	borrowed := first.catalogs[owner]
	assert.Equal(t, borrowed.PostBuild.Substitute["COLOR"], "red")
	files = maps.Clone(files)
	files["root/red.yaml"] = &fstest.MapFile{
		Data: []byte(strings.ReplaceAll(string(files["root/red.yaml"].Data), "./children", "./empty")),
	}
	files["root/blue.yaml"] = &fstest.MapFile{
		Data: []byte(strings.ReplaceAll(string(files["root/blue.yaml"].Data), "./empty", "./children")),
	}
	files["root/vars.yaml"] = &fstest.MapFile{
		Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {DIR: work}\n"),
	}
	files["root/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [red.yaml, blue.yaml, vars.yaml]\n")}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	_, err = l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	record, found := l.Index.Lookup(owner)
	assert.Equal(t, found, true)
	assert.Equal(t, record.Parent, testutil.KustomizationID("apps", "ancestor-blue"))
	current := substitutionResolution{loader: l}
	got, known = current.resolveValue(t.Context(), owner, "${COLOR}")
	assert.Equal(t, got, "blue")
	assert.Equal(t, known, true)
	got, known = current.resolveValue(t.Context(), owner, "./${DIR}")
	assert.Equal(t, got, "./work")
	assert.Equal(t, known, true)
	assert.Equal(t, borrowed.PostBuild.Substitute["COLOR"], "red")
	assert.Equal(t, l.CatalogObject(owner).(*manifest.Kustomization).PostBuild.Substitute["COLOR"], "${UPSTREAM}")
}

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
				"apps/red/kustomization.yaml": {Data: []byte("resources: [vars.yaml]\n")},
				"apps/red/vars.yaml": {
					Data: []byte(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {VALUE: selected}\n",
					),
				},
			}})
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			l.Options.Changes = change.NewSet([]string{"apps/red/cm.yaml"})

			_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
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
			if tc.known {
				assert.Equal(t, slices.ContainsFunc(l.scopes.contributions, func(c scopeContribution) bool {
					return c.owner == child && c.context.dir == "apps/red" &&
						slices.ContainsFunc(c.visits, func(v graphVisit) bool {
							return v.file == filepath.Join(root, "apps", "red", "vars.yaml")
						})
				}), true)
				vars := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "vars"}
				produced, found := l.Index.Lookup(vars)
				assert.Equal(t, found, true)
				assert.Equal(t, slices.ContainsFunc(produced.Productions, func(p manifest.Production) bool {
					return p.Owner == child && p.File == "apps/red/vars.yaml"
				}), true)
			}
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
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

func TestSubstitutionResolution_UnavailableProjectionInputs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &declarationReadFS{FS: fstest.MapFS{
		"work/cm.yaml": {Data: []byte(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {VALUE: '${VALUE}'}\n",
		)},
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
	l := New(st)
	l.SourceRoot = root
	owner := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
	owner.Path = "./work"
	owner.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
		{Kind: manifest.KindConfigMap, Name: "missing"},
	}}
	record := manifest.DiscoveryRecord{ContentRoots: map[manifest.NamedResource][]string{
		manifest.BootstrapSourceID: {"work/cm.yaml"},
	}}
	resolution := substitutionResolution{loader: l}
	assert.NoError(t, resolution.projectSubstitutions(t.Context(), owner.Named(), owner, &record))
	assert.Equal(t, len(resolution.projections), 0)
	assert.Equal(t, input.opens.Load(), 0)
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
		{
			name: "Always activation", expression: "${MISSING:=fallback}",
			postbuild: ", substituteStrategy: Always", known: true, same: true,
		},
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
	if (tc.name == "activation" || tc.name == "Always activation") && side == 0 {
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
