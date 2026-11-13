package loader

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

type substitutionData struct {
	manifest.Data

	reads int
}

func (d *substitutionData) Value(key string) (manifest.DataValue, error) {
	d.reads++
	return d.Data.Value(key)
}

type substitutionDataReader struct {
	inputs  map[manifest.NamedResource]store.ReferenceFacts
	lookups map[manifest.NamedResource]int
}

func (r *substitutionDataReader) ReferenceFacts(
	id manifest.NamedResource,
	_ *manifest.DiscoveryIndex,
	_ manifest.NamedResource,
) store.ReferenceFacts {
	r.lookups[id]++
	return r.inputs[id]
}

type substitutionConfigMap struct {
	*manifest.ConfigMap

	reads int
}

func (m *substitutionConfigMap) Value(key string) (manifest.DataValue, error) {
	m.reads++
	return m.ConfigMap.Value(key)
}

func TestSubstitutionResolution_RepeatedScalar(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	l.scopes = &scopeConstruction{}
	l.documents.compact = true
	file := filepath.Join(l.SourceRoot, "shared", "child.yaml")
	doc := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata": map[string]any{
			"name": "${VALUE}", "namespace": "${VALUE}",
			"labels": map[string]any{"role": "${VALUE}"},
		},
		"spec": map[string]any{"path": "${VALUE}", "targetNamespace": "${VALUE}"},
	}
	child := new(documentReader).prepareStreamObject(manifest.ParseDocOptions{}, &manifest.DocOccurrence{Document: doc})
	assert.NoError(t, child.outcome.Err)
	fact := compactOccurrence(child.Obj, manifest.TemplateProjection{})
	fact.Declaration = occurrenceID{Source: l.SourceRoot, File: "shared/child.yaml"}
	child.fact = &fact
	l.documents.cache[file] = document{objects: []parsedObject{child}, coverage: l.documents.intent}
	reader := &substitutionDataReader{
		inputs:  map[manifest.NamedResource]store.ReferenceFacts{},
		lookups: map[manifest.NamedResource]int{},
	}
	for _, color := range []string{"red", "blue"} {
		owner := &manifest.Kustomization{Name: color, Namespace: "apps"}
		input := &manifest.ConfigMap{Name: "vars-" + color, Namespace: "apps", Data: map[string]any{"VALUE": color}}
		owner.PostBuild = &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: input.Name}},
		}
		l.Store.AddObject(owner)
		l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
		reader.inputs[input.Named()] = store.ReferenceFacts{Data: &substitutionData{Data: input}}
		visit := graphVisit{role: kustomizeinput.Resource, file: file, readable: true}
		l.scopes.contributions = append(l.scopes.contributions, scopeContribution{
			owner: owner.Named(), source: manifest.BootstrapSourceID, active: true, visits: []graphVisit{visit, visit},
		})
	}
	l.scopes.contributions = append(l.scopes.contributions, scopeContribution{
		visits: []graphVisit{{
			role:     kustomizeinput.Resource,
			file:     filepath.Join(l.SourceRoot, "unrelated", "child.yaml"),
			readable: true,
		}},
	})
	resolution := substitutionResolution{loader: l, provider: values.NewStoreProvider(reader)}
	assert.NoError(t, resolution.resolveContextual(t.Context()))
	assert.Equal(t, len(resolution.contextual), 2)
	for _, color := range []string{"red", "blue"} {
		owner := testutil.KustomizationID("apps", color)
		key := occurrenceContext{
			fact.Declaration, owner, manifest.BootstrapSourceID,
			graphContext{role: kustomizeinput.Resource},
			file,
		}
		projected, found := resolution.contextual[key]
		assert.Equal(t, found, true)
		assert.Equal(t, projected.Identity, testutil.KustomizationID(color, color))
		assert.Equal(t, projected.facts.path, color)
		assert.Equal(t, projected.facts.policy.Namespace, color)
		assert.Equal(t, projected.facts.labels["role"], color)
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "vars-" + color}
		data := reader.inputs[id].Data.(*substitutionData)
		t.Logf("owner=%s provider lookups=%d data reads=%d", color, reader.lookups[id], data.reads)
		assert.Equal(t, reader.lookups[id], 1)
		assert.Equal(t, data.reads, 1)
	}
	assert.Equal(t, len(reader.lookups), 2)
	assert.Equal(t, fact.Authored, testutil.KustomizationID("${VALUE}", "${VALUE}"))
	assert.Equal(t, fact.facts.path, "${VALUE}")
	l.scopes.contextual = resolution.contextual
	want := []authoredRecord{
		{id: testutil.KustomizationID("blue", "blue"), authored: fact.Authored, path: file, file: "shared/child.yaml"},
		{id: testutil.KustomizationID("red", "red"), authored: fact.Authored, path: file, file: "shared/child.yaml"},
	}
	assert.Equal(t, slices.Equal(l.indexAuthoredRecords(), want), true)
	assert.Equal(t, slices.Equal(l.indexAuthoredRecords(), want), true)
}

func TestFinalizeIndex_ContextualEligibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, fixture, prefix          string
		inactive, membership, unread   bool
		configuration, plain, template bool
		eligible, substitutes          bool
		coverage                       manifest.DocumentRole
	}{
		{name: "inactive", inactive: true},
		{name: "membership only", membership: true},
		{name: "unread", unread: true},
		{name: "configuration input", configuration: true},
		{name: "plain structural", plain: true, eligible: true},
		{name: "application coverage", coverage: manifest.ApplicationDocuments, eligible: true, substitutes: true},
		{name: "transformed name", plain: true, prefix: "${VALUE}-", eligible: true, substitutes: true},
		{name: "provider labels", fixture: "providers/provider.yaml", eligible: true, substitutes: true},
		{name: "selector operands", fixture: "readers/set.yaml", eligible: true, substitutes: true},
		{name: "template reader", template: true, eligible: true, substitutes: true},
		{name: "uncertain identity", eligible: true, substitutes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			l.documents.compact = true
			l.scopes = &scopeConstruction{}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			owner.PostBuild = &kustomizev1.PostBuild{
				Substitute: map[string]string{"VALUE": "known", "KEY": "role", "OP": "NotIn"},
				SubstituteFrom: []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "unread", Optional: true},
				},
			}
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
			structural := &manifest.Kustomization{Name: "${UNKNOWN}", Namespace: "apps"}
			structural.Path = "./work"
			var object manifest.BaseManifest = structural
			var templates manifest.TemplateProjection
			if tc.plain {
				structural.Name = "plain"
			}
			if tc.template {
				object = &manifest.ResourceSet{Name: "set", Namespace: "apps"}
				templates.Inputs = []manifest.TemplateInput{{
					Reader:    testutil.KustomizationID("apps", "${VALUE}"),
					Reference: manifest.Reference{Kind: manifest.KindConfigMap, Name: "vars", Namespace: "apps"},
				}}
			}
			if tc.fixture != "" {
				var doc map[string]any
				assert.NoError(
					t,
					yaml.NewDecoder(strings.NewReader(string(contextualSelectorFiles()[tc.fixture].Data))).Decode(&doc),
				)
				parsed := new(
					documentReader,
				).prepareStreamObject(manifest.ParseDocOptions{}, &manifest.DocOccurrence{Document: doc})
				assert.NoError(t, parsed.outcome.Err)
				object = parsed.Obj
			}
			fact := compactOccurrence(object, templates)
			fact.Declaration = occurrenceID{Source: l.SourceRoot, File: "child.yaml"}
			file := filepath.Join(l.SourceRoot, "child.yaml")
			l.documents.cache[file] = document{objects: []parsedObject{{fact: &fact}}, coverage: tc.coverage}
			visit := graphVisit{
				file: file, readable: !tc.unread,
				role: kustomizeinput.Resource, namePrefix: tc.prefix, namespace: "projected",
			}
			if tc.configuration {
				visit.role = kustomizeinput.Configuration
			}
			l.scopes.contributions = []scopeContribution{{
				owner: owner.Named(), source: manifest.BootstrapSourceID,
				active: !tc.inactive, membershipOnly: tc.membership, visits: []graphVisit{visit, visit},
			}}
			resolution := substitutionResolution{loader: l}
			assert.NoError(t, resolution.resolveContextual(t.Context()))
			assert.Equal(t, len(resolution.visited) == 1, tc.eligible)
			assert.Equal(t, len(resolution.contextual) == 1, tc.eligible)
			assert.Equal(t, resolution.contextualChanged, tc.substitutes)
			assert.Equal(t, l.Store.GetObject(object.Named()) == nil, true)
			assert.Equal(t, resolution.provider != nil, tc.substitutes)
			assert.Equal(t, len(resolution.hydrated) == 1, tc.substitutes)
			for _, projected := range resolution.contextual {
				assert.Equal(t, projected.Identity.Namespace, "projected")
				switch {
				case tc.plain:
					assert.Equal(t, projected.facts.structural.Namespace, "projected")
					assert.Equal(t, projected.Identity.Name, strings.ReplaceAll(tc.prefix, "${VALUE}", "known")+"plain")
				case tc.template:
					assert.Equal(t, projected.facts.templateInputs[0].Reader, testutil.KustomizationID("apps", "known"))
				case tc.fixture == "providers/provider.yaml":
					assert.Diff(t, projected.facts.labels, map[string]string{"role": "known"})
				case tc.fixture == "readers/set.yaml":
					assert.Equal(t, projected.facts.refs[0].Selector.MatchExpressions[0].Key, "role")
				default:
					assert.Equal(t, projected.facts.uncertain, true)
					assert.Equal(t, projected.facts.path, "./work")
				}
			}
		})
	}
}

func TestFinalizeIndex_ContextualChildren(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"root/kustomization.yaml":     {Data: []byte("resources: [red.yaml, blue.yaml]\n")},
		"children/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
		"children/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: '${NAME}', namespace: '${NS}'}
spec: {path: './${DIR}'}
`)},
		"work/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"work/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out}\n")},
	}
	for _, color := range []string{"red", "blue"} {
		files["root/"+color+".yaml"] = &fstest.MapFile{
			Data: []byte(fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s, namespace: flux-system}
spec:
  path: ./children
  postBuild: {substitute: {NAME: child-%s, NS: %s, DIR: work}}
`, color, color, color)),
		}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"work/cm.yaml"})
	_, err := l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	for _, color := range []string{"red", "blue"} {
		_, indexed := l.Index.Lookup(testutil.KustomizationID(color, "child-"+color))
		assert.Equal(t, indexed, false)
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	for _, color := range []string{"red", "blue"} {
		id := testutil.KustomizationID(color, "child-"+color)
		owner := testutil.KustomizationID("flux-system", color)
		o, known := l.Occurrence(id)
		assert.Equal(t, known, true)
		assert.Equal(t, o.Identity, id)
		assert.Equal(t, o.Authored, testutil.KustomizationID("${NS}", "${NAME}"))
		assert.Equal(t, o.Declaration.Source, root)
		assert.Equal(t, o.Declaration.File, "children/child.yaml")
		assert.Equal(t, o.Declaration.Document, 0)
		assert.Equal(t, o.Path, "./work")
		assert.Equal(t, o.Structural.Named(), id)
		assert.Equal(t, o.Structural.Path, "./work")
		assert.Equal(t, l.CatalogObject(id).(*manifest.Kustomization).Path, "./${DIR}")
		record, found := l.Index.Lookup(id)
		assert.Equal(t, found, true)
		assert.Equal(t, record.Parent, owner)
		assert.Equal(t, slices.Contains(testutil.Owners(record.Productions), owner), true)
		assert.Equal(t, slices.Contains(l.Index.Claims["work/"], id), true)
		assert.Equal(t, slices.ContainsFunc(l.scopes.contributions, func(c scopeContribution) bool {
			return c.owner == id && c.context.dir == "work"
		}), true)
	}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	for _, color := range []string{"red", "blue"} {
		assert.Equal(t, slices.Contains(proposal.Needed, testutil.KustomizationID(color, "child-"+color)), true)
		assert.Equal(t, slices.Contains(proposal.Needed, testutil.KustomizationID("flux-system", color)), true)
	}

	borrowed := maps.Clone(l.locations)
	files = maps.Clone(files)
	for _, color := range []string{"red", "blue"} {
		files["root/"+color+".yaml"] = &fstest.MapFile{
			Data: []byte(fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s, namespace: flux-system}
spec:
  path: ./children
  postBuild: {substitute: {NAME: next-%s, NS: %s, DIR: next}}
`, color, color, color)),
		}
	}
	files["next/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
	files["next/cm.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\n")}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	_, err = l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	catalog := l.DetachCatalog()
	defer catalog.Close()
	l.Close()
	for _, color := range []string{"red", "blue"} {
		old := testutil.KustomizationID(color, "child-"+color)
		id := testutil.KustomizationID(color, "next-"+color)
		assert.Equal(t, borrowed[old].facts.path, "./work")
		assert.Equal(t, borrowed[old].Authored, testutil.KustomizationID("${NS}", "${NAME}"))
		current, found := catalogOccurrence(catalog.locations, id)
		assert.Equal(t, found, true)
		assert.Equal(t, current.facts.path, "./next")
		assert.Equal(t, current.Authored, borrowed[old].Authored)
		assert.Equal(t, current.Declaration.File, "children/child.yaml")
		assert.Equal(t, catalog.CatalogObject(id).(*manifest.Kustomization).Path, "./${DIR}")
		_, found = catalog.index.Lookup(old)
		assert.Equal(t, found, false)
	}
}

func TestFinalizeIndex_UnknownChildOwnerReach(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"root/owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec:
  path: ./children
  postBuild: {substitute: {ACTIVE: "true"}, substituteFrom: [{kind: ConfigMap, name: unavailable, optional: true}]}
`)},
		"children/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: '${NAME:-a}', namespace: flux-system}
spec: {path: ./work}
`)},
		"work/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out}\n")},
	}})
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"work/cm.yaml"})
	_, err := l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	owner := testutil.KustomizationID("flux-system", "owner")
	resolution := substitutionResolution{loader: l}
	_, known := resolution.resolveValue(t.Context(), owner, "${NAME:-a}")
	assert.Equal(t, known, false)
	record, found := l.Index.Lookup(owner)
	assert.Equal(t, found, true)
	assert.Equal(t, record.StaticOutput, false)
	assert.Equal(t, slices.Contains(record.ContentRoots[manifest.BootstrapSourceID], "work"), true)
	for id := range l.Index.Records {
		assert.Equal(t, manifest.HasEnvsubstReference(id.Name), false)
		assert.Equal(t, manifest.HasEnvsubstReference(id.Namespace), false)
	}
	proposal, err := prepareSelection(
		t.Context(),
		t,
		l,
		selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner), true)
}

func TestFinalizeIndex_MetadataKeepsScopes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	files := contextualSelectorFiles()
	files["root/provider-owner.yaml"] = &fstest.MapFile{Data: []byte(strings.ReplaceAll(
		string(files["root/provider-owner.yaml"].Data),
		"postBuild: {substitute:",
		"postBuild: {substituteFrom: [{kind: ConfigMap, name: vars}], substitute:",
	))}
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	data := &substitutionConfigMap{ConfigMap: &manifest.ConfigMap{
		Name: "vars", Namespace: "apps", Data: map[string]any{"UNUSED": "constant"},
	}}
	st.AddObject(data)
	l := New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	l.Options.Changes = change.NewSet([]string{"providers/provider.yaml"})
	_, err := l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.prepareIndexScopes(root, false)
	before := l.construction
	assert.NoError(t, l.resolveAuthored(t.Context(), root))
	assert.Equal(t, l.construction, before)
	assert.Equal(t, data.reads, 2)
	provider := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider,
		Namespace: "apps", Name: "provider",
	}
	occurrence, found := l.occurrence(provider)
	assert.Equal(t, found, true)
	assert.Diff(t, occurrence.facts.labels, map[string]string{"role": "y"})
}

func TestFinalizeIndex_ContextualSelectors(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := contextualSelectorFiles()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	l := New(st)
	l.SourceRoot = root
	reader := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "apps",
		Name:      "reader",
	}
	provider := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "apps",
		Name:      "provider",
	}
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{reader}})
	_, err := l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	occurrence, known := l.Occurrence(reader)
	assert.Equal(t, known, true)
	var input manifest.Reference
	for _, ref := range occurrence.Refs {
		if ref.Use == "inputsFrom" {
			input = ref
		}
	}
	assert.Equal(t, input.NamespaceDefault, true)
	assert.Equal(t, input.Position, 0)
	assert.Equal(t, input.Selector.MatchExpressions[0].Key, "role")
	assert.Equal(t, string(input.Selector.MatchExpressions[0].Operator), "NotIn")
	assert.Diff(t, input.Selector.MatchExpressions[0].Values, []string{"x"})
	candidate, known := l.Occurrence(provider)
	assert.Equal(t, known, true)
	assert.Diff(t, candidate.Labels, map[string]string{"role": "y"})
	assert.Diff(
		t,
		candidate.Catalog.(*manifest.ResourceSetInputProvider).Labels,
		map[string]string{"${KEY}": "${VALUE}"},
	)
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, provider), true)
	assert.Equal(t, slices.Contains(proposal.Needed, testutil.KustomizationID("apps", "provider-owner")), true)
}

//nolint:gocognit // Known, unread, and excluded contexts MUST share the same scalar and immutability assertions.
func TestSubstitutionResolution_ContextualScalars(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, disabled string
		unknown        bool
	}{
		{name: "known"},
		{name: "unread input with defaults", unknown: true},
		{name: "disabled annotation", disabled: "annotations"},
		{name: "disabled label", disabled: "labels"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"ACTIVE": "true"}}
			if tc.unknown {
				owner.PostBuild.SubstituteFrom = []manifest.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "unread"},
				}
			}
			st := store.New()
			st.AddObject(owner)
			l := New(st)
			l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
			doc := map[string]any{
				"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
				"kind":       "Kustomization",
				"metadata": map[string]any{
					"name":      "child",
					"namespace": "apps",
					"labels":    map[string]any{"${LABEL:-role}": "${VALUE:-y}"},
				},
				"spec": map[string]any{
					"path":            "./${DIR:-a}",
					"targetNamespace": "${TARGET:-work}",
					"sourceRef": map[string]any{
						"kind":      "GitRepository",
						"name":      "${SOURCE:-repo}",
						"namespace": "${NS:-apps}",
					},
					"components": []any{"${COMP:-../../shared}"},
					"dependsOn":  []any{map[string]any{"name": "${DEP:-prior}", "namespace": "${NS:-apps}"}},
					"postBuild": map[string]any{
						"substituteFrom": []any{
							map[string]any{"kind": "ConfigMap", "name": "${INPUT:-vars}", "optional": true},
						},
					},
				},
			}
			if tc.disabled != "" {
				metadata := doc["metadata"].(map[string]any)
				fields, _ := metadata[tc.disabled].(map[string]any)
				if fields == nil {
					fields = map[string]any{}
					metadata[tc.disabled] = fields
				}
				fields[manifest.SubstituteAnnotationKey] = "disabled"
			}
			child := new(
				documentReader,
			).prepareStreamObject(manifest.ParseDocOptions{}, &manifest.DocOccurrence{Document: doc})
			assert.NoError(t, child.outcome.Err)
			template := &manifest.Kustomization{Name: "${READER:-reader}", Namespace: "${NS:-apps}"}
			templates := manifest.TemplateProjection{
				Inputs: []manifest.TemplateInput{
					{
						Reader:    template.Named(),
						Reference: manifest.Reference{Kind: manifest.KindConfigMap, Name: "vars", Namespace: "apps"},
					},
					{
						Reader: testutil.KustomizationID("apps", "literal"),
						Reference: manifest.Reference{
							Kind: manifest.KindConfigMap, Name: "${INPUT:-vars}", Namespace: "${NS:-apps}",
							Key: "${KEY:-token}", Files: []string{"${FILE:-values.yaml}"},
						},
					},
				},
				Objects: []manifest.BaseManifest{template},
			}
			for _, projection := range []manifest.TemplateProjection{
				{Inputs: templates.Inputs[:1]},
				{Inputs: templates.Inputs[1:]},
				{Objects: templates.Objects},
			} {
				literal := compactOccurrence(&manifest.ResourceSet{Name: "literal", Namespace: "apps"}, projection)
				assert.Equal(t, literal.facts.substituted, true)
			}
			fact := compactOccurrence(child.Obj, templates)
			l.SourceRoot = t.TempDir()
			fact.Declaration.Source = l.SourceRoot
			fact.facts.substituteDisabled = child.substituteDisabled
			resolution := substitutionResolution{loader: l}
			projected, err := resolution.resolveOccurrence(
				t.Context(),
				&fact,
				&graphVisit{owner: owner.Named(), namePrefix: "${PREFIX:-pre}-"},
				child.version,
			)
			assert.NoError(t, err)
			known := !tc.unknown && tc.disabled == ""
			want := func(raw, resolved string) string {
				if known {
					return resolved
				}
				return raw
			}
			reader := testutil.KustomizationID(want("${NS:-apps}", "apps"), want("${READER:-reader}", "reader"))
			assert.Equal(t, projected.facts.templateInputs[0].Reader, reader)
			assert.Equal(t, projected.facts.templateInputs[0].Reference.Name, "vars")
			input := projected.facts.templateInputs[1].Reference
			assert.Equal(t, input.Name, want("${INPUT:-vars}", "vars"))
			assert.Equal(t, input.Namespace, want("${NS:-apps}", "apps"))
			assert.Equal(t, input.Key, want("${KEY:-token}", "token"))
			assert.Diff(t, input.Files, []string{want("${FILE:-values.yaml}", "values.yaml")})
			assert.Equal(t, projected.facts.templateObjects[0].Named(), reader)
			assert.Equal(
				t,
				fact.facts.templateInputs[0].Reader,
				testutil.KustomizationID("${NS:-apps}", "${READER:-reader}"),
			)
			assert.Equal(t, fact.facts.templateInputs[1].Reference.Name, "${INPUT:-vars}")
			assert.Equal(t, fact.facts.templateInputs[1].Reference.Namespace, "${NS:-apps}")
			assert.Equal(t, fact.facts.templateInputs[1].Reference.Key, "${KEY:-token}")
			assert.Diff(t, fact.facts.templateInputs[1].Reference.Files, []string{"${FILE:-values.yaml}"})
			assert.Equal(t, template.Named(), testutil.KustomizationID("${NS:-apps}", "${READER:-reader}"))
			assert.Equal(t, fact.facts.templateObjects[0], manifest.BaseManifest(template))
			l.locations = map[manifest.NamedResource]authoredOccurrence{projected.Identity: projected}
			l.Index.Records[projected.Identity] = manifest.DiscoveryRecord{File: "child.yaml"}
			l.projectTemplates()
			record, found := l.Index.Lookup(reader)
			assert.Equal(t, found, true)
			assert.Equal(t, record.Parent, projected.Identity)
			assert.Equal(t, slices.Contains(testutil.Owners(record.Productions), projected.Identity), true)
			assert.Equal(t, l.candidates[reader].Obj.Named(), reader)
			assert.Equal(t, projected.Identity.Name, want("${PREFIX:-pre}-child", "pre-child"))
			assert.Equal(t, projected.facts.uncertain, !known)
			assert.Equal(t, fact.facts.catalog.Named().Name, "child")
			assert.Equal(t, projected.facts.labels[want("${LABEL:-role}", "role")], want("${VALUE:-y}", "y"))
			assert.Equal(t, fact.facts.labels["${LABEL:-role}"], "${VALUE:-y}")
			assert.Equal(t, projected.facts.path, want("./${DIR:-a}", "./a"))
			assert.Equal(t, projected.facts.source.Name, want("${SOURCE:-repo}", "repo"))
			assert.Equal(t, projected.facts.source.Namespace, want("${NS:-apps}", "apps"))
			assert.Equal(t, projected.facts.policy.Namespace, want("${TARGET:-work}", "work"))
			assert.Equal(t, projected.facts.structural.Path, projected.facts.path)
			assert.Equal(t, projected.facts.structural.Components[0], want("${COMP:-../../shared}", "../../shared"))
			assert.Equal(t, fact.facts.path, "./${DIR:-a}")
			assert.Equal(t, fact.facts.structural.SourceRef.Name, "${SOURCE:-repo}")
			for ref := range projected.references() {
				switch ref.Use {
				case "source":
					assert.Equal(t, ref.Name, want("${SOURCE:-repo}", "repo"))
				case "dependsOn":
					assert.Equal(t, ref.Name, want("${DEP:-prior}", "prior"))
				case "substituteFrom":
					assert.Equal(t, ref.Name, want("${INPUT:-vars}", "vars"))
					assert.Equal(t, ref.Namespace, "apps")
					assert.Equal(t, ref.NamespaceDefault, true)
					assert.Equal(t, ref.Optional, true)
					assert.Equal(t, ref.Position, 0)
				}
			}
			selectorDoc := map[string]any{
				"apiVersion": "fluxcd.controlplane.io/v1", "kind": "ResourceSet", "metadata": doc["metadata"],
				"spec": map[string]any{"inputsFrom": []any{map[string]any{"selector": map[string]any{
					"matchLabels": map[string]any{"${LABEL:-role}": "${VALUE:-y}"},
					"matchExpressions": []any{
						map[string]any{
							"key":      "${KEY:-peer}",
							"operator": "${OP:-NotIn}",
							"values":   []any{"${SELF:-excluded}"},
						},
					},
				}}}},
			}
			set := new(
				documentReader,
			).prepareStreamObject(manifest.ParseDocOptions{}, &manifest.DocOccurrence{Document: selectorDoc})
			assert.NoError(t, set.outcome.Err)
			setFact := compactOccurrence(set.Obj, manifest.TemplateProjection{})
			setFact.facts.substituteDisabled = set.substituteDisabled
			setProjection, err := resolution.resolveOccurrence(
				t.Context(),
				&setFact,
				&graphVisit{owner: owner.Named()},
				set.version,
			)
			assert.NoError(t, err)
			references := 0
			for ref := range setProjection.references() {
				if ref.Use != "inputsFrom" {
					continue
				}
				references++
				assert.Equal(t, ref.Selector.MatchLabels[want("${LABEL:-role}", "role")], want("${VALUE:-y}", "y"))
				expression := ref.Selector.MatchExpressions[0]
				assert.Equal(t, expression.Key, want("${KEY:-peer}", "peer"))
				assert.Equal(t, string(expression.Operator), want("${OP:-NotIn}", "NotIn"))
				assert.Diff(t, expression.Values, []string{want("${SELF:-excluded}", "excluded")})
				assert.Equal(t, ref.NamespaceDefault, true)
				assert.Equal(t, ref.Position, 0)
			}
			assert.Equal(t, references, 1)
			for ref := range setFact.references() {
				if ref.Use == "inputsFrom" {
					assert.Equal(t, ref.Selector.MatchExpressions[0].Key, "${KEY:-peer}")
					assert.Equal(t, ref.Selector.MatchLabels["${LABEL:-role}"], "${VALUE:-y}")
				}
			}
		})
	}
}

func TestSubstitutionResolution_ReferenceOperands(t *testing.T) {
	t.Parallel()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"ACTIVE": "true"}}
	st := store.New()
	st.AddObject(owner)
	l := New(st)
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
	raw := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: apps}
spec:
  chart:
    spec:
      chart: "./${CHART:-charts/app}"
      sourceRef: {kind: GitRepository, name: "${SOURCE:-repo}"}
      valuesFiles: ["${FILE:-values.yaml}"]
  valuesFrom: [{kind: ConfigMap, name: "${INPUT:-vars}", valuesKey: "${KEY:-values.yaml}", optional: true}]
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: external, namespace: apps}
spec:
  target: {name: "${SECRET:-credentials}"}
  data: [{secretKey: "${KEY:-token}", remoteRef: {key: remote}}]
`
	resolution := substitutionResolution{loader: l}
	for raw := range strings.SplitSeq(raw, "\n---\n") {
		var doc map[string]any
		assert.NoError(t, yaml.NewDecoder(strings.NewReader(raw)).Decode(&doc))
		child := new(
			documentReader,
		).prepareStreamObject(manifest.ParseDocOptions{}, &manifest.DocOccurrence{Document: doc})
		assert.NoError(t, child.outcome.Err)
		fact := compactOccurrence(child.Obj, manifest.TemplateProjection{})
		projected, err := resolution.resolveOccurrence(
			t.Context(),
			&fact,
			&graphVisit{owner: owner.Named()},
			child.version,
		)
		assert.NoError(t, err)
		if child.Obj.Named().Name == "release" {
			uses := []string{}
			for ref := range projected.references() {
				uses = append(uses, ref.Use)
				switch ref.Use {
				case "source":
					assert.Equal(t, ref.Name, "repo")
					assert.Equal(t, ref.ChartPath, "./charts/app")
					assert.Diff(t, ref.Files, []string{"values.yaml"})
				case "valuesFrom":
					assert.Equal(t, ref.Name, "vars")
					assert.Equal(t, ref.Key, "values.yaml")
					assert.Equal(t, ref.Optional, true)
				}
			}
			assert.Equal(t, slices.Contains(uses, "source"), true)
			assert.Equal(t, slices.Contains(uses, "valuesFrom"), true)
			for ref := range fact.references() {
				if ref.Use == "valuesFrom" {
					assert.Equal(t, ref.Name, "${INPUT:-vars}")
					assert.Equal(t, ref.Key, "${KEY:-values.yaml}")
				}
			}
		} else {
			assert.Equal(t, projected.facts.targets[0].Name, "credentials")
			assert.Diff(t, projected.facts.targets[0].DeclaredKeys, []string{"token"})
			assert.Equal(t, fact.facts.targets[0].Name, "${SECRET:-credentials}")
		}
	}
}

func contextualSelectorFiles() fstest.MapFS {
	return fstest.MapFS{
		"root/kustomization.yaml": {Data: []byte("resources: [reader-owner.yaml, provider-owner.yaml]\n")},
		"root/reader-owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: reader-owner, namespace: apps}
spec:
  path: ./readers
  postBuild: {substitute: {KEY: role, OP: NotIn, VALUE: x}}
`)},
		"root/provider-owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: provider-owner, namespace: apps}
spec:
  path: ./providers
  postBuild: {substitute: {KEY: role, VALUE: y}}
`)},
		"readers/set.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: reader, namespace: apps}
spec:
  inputsFrom:
    - selector:
        matchExpressions: [{key: "${KEY}", operator: "${OP}", values: ["${VALUE}"]}]
`)},
		"providers/provider.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: provider, namespace: apps, labels: {"${KEY}": "${VALUE}"}}
spec: {type: Static}
`)},
	}
}
