package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestProjectOccurrence_StructuralIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, namespace string }{
		{name: "inherited"}, {name: "authored", namespace: "apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			namespace := tc.namespace
			t.Parallel()
			l := New(store.New())
			l.scopes = &scopeConstruction{}
			child := &manifest.Kustomization{Name: "child", Namespace: namespace}
			child.Path = "./work"
			original := compactOccurrence(child, manifest.TemplateProjection{})
			predicted := *child
			predicted.TargetNamespace = "apps"
			structured := withAuthoredStructure(&original, &predicted)
			visit := graphVisit{
				file: "child.yaml", owner: testutil.KustomizationID("apps", "parent"),
				namespace: "apps",
			}
			got, err := l.projectOccurrence(&structured, &visit, "v1")
			assert.NoError(t, err)
			assert.Equal(t, got.facts.structural.Namespace, "apps")
			assert.Equal(t, got.facts.structural.TargetNamespace, "apps")
			assert.Equal(t, got.facts.policy.Namespace, "apps")
			assert.Equal(t, &got.facts.roots[0] == &original.facts.roots[0], true)
			assert.Equal(t, child.Namespace, namespace)
			assert.Equal(t, predicted.TargetNamespace, "apps")
			l.locations = map[manifest.NamedResource]authoredOccurrence{got.Identity: got}
			repeated := predicted
			structured = withAuthoredStructure(&original, &repeated)
			again, err := l.projectOccurrence(&structured, &visit, "v1")
			assert.NoError(t, err)
			assert.Equal(t, again.facts.structural == got.facts.structural, true)
			physical, err := l.projectOccurrence(&original, &visit, "v1")
			assert.NoError(t, err)
			assert.Equal(t, physical.facts.structural == got.facts.structural, true)
			assert.Equal(t, physical.facts.structural.TargetNamespace, "apps")
			assert.Equal(t, physical.facts.policy.Namespace, "apps")
			if namespace == "" {
				resolution := substitutionResolution{loader: l}
				again, err = resolution.resolveOccurrence(t.Context(), &original, &visit, "v1")
				assert.NoError(t, err)
				key := occurrenceContext{
					original.Declaration, visit.owner, visit.source, visit.graphContext, visit.file,
				}
				l.scopes.contextual = map[occurrenceContext]authoredOccurrence{key: again}
				again, err = l.projectOccurrence(&structured, &visit, "v1")
				assert.NoError(t, err)
				assert.Equal(t, again.facts.structural == got.facts.structural, true)
				assert.Equal(t, again.facts.policy.Namespace, "apps")
			}
			l.scopes.contextual = nil
			updated := &manifest.Kustomization{Name: child.Name, Namespace: namespace}
			updated.Path, updated.Components = child.Path, []string{"./other"}
			fresh := compactOccurrence(updated, manifest.TemplateProjection{})
			changedPrediction := *updated
			changedPrediction.TargetNamespace = "apps"
			structured = withAuthoredStructure(&fresh, &changedPrediction)
			changed, err := l.projectOccurrence(&structured, &visit, "v1")
			assert.NoError(t, err)
			assert.Diff(t, changed.facts.structural.Components, []string{"./other"})
			assert.Equal(t, changed.facts.structural == got.facts.structural, false)
			unpredicted, err := l.projectOccurrence(&fresh, &visit, "v1")
			assert.NoError(t, err)
			assert.Equal(t, unpredicted.facts.structural.TargetNamespace, "")
			assert.Equal(t, unpredicted.facts.policy.Namespace, "")
		})
	}
}

func TestSubstitutionResolution_RestoredCoverage(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	l.documents.compact = true
	l.scopes = &scopeConstruction{}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	owner.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "known"}}
	l.Index.Records[owner.Named()] = manifest.DiscoveryRecord{Declaration: owner}
	child := &manifest.Kustomization{Name: "${VALUE}", Namespace: "apps"}
	child.Path = "./work"
	fact := compactOccurrence(child, manifest.TemplateProjection{})
	fact.Declaration = occurrenceID{Source: l.SourceRoot, File: "child.yaml"}
	file := filepath.Join(l.SourceRoot, "child.yaml")
	inputFS := &declarationReadFS{FS: fstest.MapFS{}}
	input := &store.SourceArtifact{LocalPath: l.SourceRoot, FS: inputFS}
	l.documents.setSources(map[string]*store.SourceArtifact{l.SourceRoot: input})
	l.documents.parsed = &ParsedInputs{
		compact: true,
		sources: map[string]parsedSource{
			l.SourceRoot: {
				input:   parsedInputIdentity(input),
				binding: new(input.Input(store.ArtifactContent | store.ArtifactRevision)),
				documents: map[string]*parsedDocument{
					file: {
						coverage: manifest.ApplicationDocuments,
						objects:  []parsedObject{{fact: &fact}},
					},
				},
			},
		},
	}
	visit := graphVisit{file: file, owner: owner.Named(), source: manifest.BootstrapSourceID, readable: true}
	resolution := substitutionResolution{loader: l}
	assert.NoError(t, resolution.resolveVisit(t.Context(), &visit))
	assert.Equal(t, inputFS.opens.Load(), int64(0))
	assert.Equal(t, l.documents.cache[file].coverage, manifest.ApplicationDocuments)
	assert.Equal(t, len(resolution.contextual), 1)
	for _, projected := range resolution.contextual {
		assert.Equal(t, projected.Identity.Name, "known")
	}
}

func TestProjectAuthored_UnownedStructure(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	l.documents.compact = true
	l.ensureScopes()
	child := &manifest.Kustomization{Name: "child"}
	child.Path = "./work"
	original := compactOccurrence(child, manifest.TemplateProjection{})
	predicted := *child
	predicted.TargetNamespace = "apps"
	structured := withAuthoredStructure(&original, &predicted)
	file := filepath.Join(l.SourceRoot, "child.yaml")
	l.documents.cache[file] = document{objects: []parsedObject{{fact: &original, version: "v1"}}}
	record := manifest.DiscoveryRecord{Path: file, Authored: child.Named()}
	authored := store.Child{Obj: &predicted}
	state := authoredState{
		records: map[manifest.NamedResource]manifest.DiscoveryRecord{},
		objects: map[manifest.NamedResource]store.Child{},
	}
	assert.Equal(t, l.projectAuthoredOccurrence(&state, &record, &authored, ""), true)
	first := l.locations[child.Named()]
	repeated := predicted
	authored.Obj = &repeated
	assert.Equal(t, l.projectAuthoredOccurrence(&state, &record, &authored, ""), true)
	again := l.locations[child.Named()]
	assert.Equal(t, first.facts.structural == again.facts.structural, true)
	assert.Equal(t, again.facts.structural.TargetNamespace, "apps")
	assert.Equal(t, again.facts.policy.Namespace, "apps")
	assert.Equal(t, child.TargetNamespace, "")
	assert.Equal(t, structured.facts.structural == &predicted, true)
}

func TestAuthoredNamespace_UnchangedStructure(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	child.Path = "./work"
	for _, target := range []string{"", "apps"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			authored := *child
			authored.TargetNamespace = target
			original := compactOccurrence(&authored, manifest.TemplateProjection{})
			projected := l.inAuthoredNamespace(&original, "apps")
			assert.Equal(t, projected.facts == original.facts, true)
			assert.Equal(t, projected.facts.structural == original.facts.structural, true)
		})
	}
}

func TestPublishAuthoredLocation_CatalogIdentity(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	child := &manifest.Kustomization{Name: "child", Path: "./work"}
	original := compactOccurrence(child, manifest.TemplateProjection{})
	projected := original
	projected.Identity.Namespace = "apps"
	l.publishAuthoredLocation(&projected)
	first := l.CatalogObject(projected.Identity)
	second := l.CatalogObject(projected.Identity)
	assert.Equal(t, first == second, true)
	assert.Equal(t, first.Named(), projected.Identity)
	assert.Equal(t, original.facts.catalog.Named(), child.Named())

	again := original
	again.Identity = projected.Identity
	l.publishAuthoredLocation(&again)
	assert.Equal(t, l.CatalogObject(projected.Identity) == first, true)
	assert.Equal(t, again.facts == projected.facts, true)

	updated := *child
	updated.Path = "./updated"
	fresh := compactOccurrence(&updated, manifest.TemplateProjection{})
	fresh.Identity = projected.Identity
	l.publishAuthoredLocation(&fresh)
	changed := l.CatalogObject(fresh.Identity)
	_, path, _ := changed.SourcePath()
	assert.Equal(t, path, "./updated")
	assert.Equal(t, changed == first, false)
	assert.Equal(t, child.Namespace, "")
	assert.Equal(t, child.Path, "./work")
}
