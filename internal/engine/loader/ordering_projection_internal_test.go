package loader

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxkustomize "github.com/fluxcd/pkg/apis/kustomize"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestQualifyOrderingOccurrences_Transformations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, configuration, incomplete, extraRecipe string
		patches                                      []fluxkustomize.Patch
		postBuild                                    *kustomizev1.PostBuild
		unknownNested                                bool
		want                                         bool
	}{
		{name: "local name reference", configuration: "names.yaml", want: true},
		{name: "name reference with unknown nested recipe", configuration: "names.yaml", unknownNested: true},
		{name: "owner patches", configuration: "names.yaml", patches: []fluxkustomize.Patch{{Patch: "patch"}}},
		{name: "owner substitution", configuration: "names.yaml", postBuild: &kustomizev1.PostBuild{}},
		{name: "transformer plugin", configuration: "names.yaml", extraRecipe: "transformers: [transformer.yaml]\n"},
		{name: "generator plugin", configuration: "names.yaml", extraRecipe: "generators: [generator.yaml]\n"},
		{name: "incomplete visit", configuration: "names.yaml", incomplete: "incomplete"},
		{name: "non-local configuration", configuration: "../names.yaml"},
		{name: "unreadable configuration", configuration: "absent.yaml"},
		{name: "no configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t, root, "names.yaml",
				"nameReference: [{kind: ConfigMap, fieldSpecs: [{path: spec/valuesFrom/name}]}]\n",
			)
			l := loaderFixture(store.New(), nil)
			l.ensureScopes()
			l.documents = readerForTest(root)
			owner := &manifest.Kustomization{
				Name:      "owner",
				Namespace: "apps",
				Patches:   tc.patches,
				PostBuild: tc.postBuild,
			}
			l.Store.AddObject(owner)
			reader := &manifest.Kustomization{
				Name: "reader", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: "target"}},
			}
			occurrence := compactOccurrence(reader, manifest.TemplateProjection{})
			occurrence.Declaration = occurrenceID{Source: root, File: "reader.yaml"}
			occurrence.requiresBuild = true
			l.locations = map[manifest.NamedResource]authoredOccurrence{reader.Named(): occurrence}
			recipe := "resources: [reader.yaml]\n"
			if tc.unknownNested {
				recipe = "resources: [reader.yaml, nested]\n"
				testutil.WriteFile(t, root, "nested/kustomization.yaml", "resources: [\n")
			}
			if tc.configuration != "" {
				recipe += "configurations: [" + tc.configuration + "]\n"
			}
			testutil.WriteFile(t, root, "kustomization.yaml", recipe+tc.extraRecipe)
			testutil.WriteFile(t, root, "reader.yaml", "")
			visit := graphVisit{root: root, dir: ".", owner: owner.Named(), role: kustomizeinput.Resource}
			visits := l.scopeWalk(&visit)
			if tc.unknownNested {
				assert.Equal(t, l.scopes.walks[visit.graphContext].unknownRecipe, true)
				for i := range visits {
					assert.Equal(t, visits[i].incomplete, "")
				}
			}
			if tc.incomplete != "" {
				visits = append(visits, graphVisit{
					file: filepath.Join(root, occurrence.Declaration.File), incomplete: tc.incomplete,
				})
			}
			l.recordScopeVisits(&visit, visits, false, false)
			l.scopes.contributions[0].active = true
			l.qualifyOrderingOccurrences()
			facts, known := l.Occurrence(reader.Named())
			assert.Equal(t, known, true)
			assert.Equal(t, facts.RequiresBuild, true)
			assert.Equal(t, facts.Parsed, manifest.BaseManifest(reader))
			assert.Equal(t, facts.OrderingKnown, tc.want)
		})
	}
}

func TestIndexTransformingProducers_Eligibility(t *testing.T) {
	t.Parallel()
	const (
		plain   = "resources: [reader.yaml]\n"
		patched = plain + "patches: [{target: {kind: ConfigMap}, patch: '- {op: add, path: /metadata/labels, value: {a: b}}'}]\n"
		nested  = "resources: [nested]\n"
		varName = "'${NAME}'"
		optOut  = ", annotations: {kustomize.toolkit.fluxcd.io/substitute: disabled}"
	)
	inline := &kustomizev1.PostBuild{Substitute: map[string]string{"NAME": "reader"}}
	for _, tc := range []struct {
		name, builder, child, namespace, visitNamespace, metadata, data            string
		full, ownerless, membershipOnly, inactive, nonapplying, catalog, suspended bool
		role                                                                       kustomizeinput.Role
		parsed                                                                     bool
		postBuild                                                                  *kustomizev1.PostBuild
		want                                                                       bool
	}{
		{name: "patch", builder: patched, want: true},
		{name: "full run", builder: patched, full: true},
		{name: "plain", builder: plain},
		{name: "data generators only", builder: "configMapGenerator: [{name: generated}]\n"},
		{name: "transforming child under a plain parent", builder: nested, child: patched, want: true},
		{name: "incomplete child", builder: nested, child: "resources: [", want: true},
		{name: "Flux namespace", builder: plain, namespace: "other", want: true},
		{name: "catalog-only owner", builder: patched, catalog: true, want: true},
		{name: "ownerless", builder: patched, ownerless: true},
		{name: "membership only", builder: patched, membershipOnly: true},
		{name: "nonapplying", builder: patched, nonapplying: true},
		{name: "inactive", builder: patched, inactive: true},
		{name: "suspended", builder: patched, suspended: true},
		{name: "configuration visit", builder: patched, role: kustomizeinput.Configuration},
		{name: "inline identity substitution", builder: plain, metadata: "name: " + varName, postBuild: inline, want: true},
		{
			name: "parsed namespace substitution", builder: plain, postBuild: inline,
			visitNamespace: "${NAME}", parsed: true, want: true,
		},
		{
			name: "parsed namespace substitution disabled", builder: plain, postBuild: inline,
			metadata: "name: reader" + optOut, visitNamespace: "${NAME}", parsed: true,
		},
		{
			name: "catalog identity substitution", builder: plain, metadata: "name: " + varName, catalog: true,
			postBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: "ConfigMap", Name: "vars"}}},
			want:      true,
		},
		{
			name: "always substitutes default", builder: plain, metadata: "name: '${NAME:=reader}'",
			postBuild: &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways}, want: true,
		},
		{name: "no post-build", builder: plain, metadata: "name: " + varName},
		{name: "inactive substitution", builder: plain, metadata: "name: " + varName, postBuild: &kustomizev1.PostBuild{}},
		{name: "substitution in data", builder: plain, metadata: "name: reader", data: "data: {value: " + varName + "}\n", postBuild: inline},
		{
			name: "disabled substitution", builder: plain, postBuild: inline,
			metadata: "name: " + varName + optOut,
		},
		{
			name: "namespace override removes expression", builder: plain, postBuild: inline,
			metadata: "name: reader, namespace: " + varName, visitNamespace: "apps",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			l := loaderFixture(store.New(), nil)
			l.ensureScopes()
			l.SourceRoot, l.documents = root, readerForTest(root)
			if !tc.full {
				l.Options.Changes = new(change.Set)
			}
			const configMap = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: reader}\n"
			testutil.WriteFile(t, root, "producer/kustomization.yaml", tc.builder)
			contents := configMap
			if tc.metadata != "" {
				contents = "apiVersion: v1\nkind: ConfigMap\nmetadata: {" + tc.metadata + "}\n" + tc.data
			}
			testutil.WriteFile(t, root, "producer/reader.yaml", contents)
			testutil.WriteFile(t, root, "other/kustomization.yaml", plain)
			testutil.WriteFile(t, root, "other/reader.yaml", configMap)
			if tc.child != "" {
				testutil.WriteFile(t, root, "producer/nested/kustomization.yaml", tc.child)
				testutil.WriteFile(t, root, "producer/nested/reader.yaml", configMap)
			}
			owner := &manifest.Kustomization{
				Name: "producer", Namespace: "apps", Path: "./producer",
				TargetNamespace: tc.namespace, Suspend: tc.suspended, PostBuild: tc.postBuild,
			}
			other := &manifest.Kustomization{Name: "other", Namespace: "apps", Path: "./other"}
			l.Store.AddObject(other)
			id := owner.Named()
			switch {
			case tc.ownerless:
				id = manifest.NamedResource{}
			case tc.catalog:
				l.Index.Records[id] = manifest.DiscoveryRecord{Declaration: owner}
				assert.Equal(t, l.Store.GetObject(id), nil)
			default:
				l.Store.AddObject(owner)
			}
			scope := graphVisit{root: root, dir: "producer", owner: id, role: tc.role, namespace: tc.visitNamespace}
			visits := l.scopeWalk(&scope)
			l.recordScopeVisits(&scope, visits, tc.membershipOnly, false)
			l.scopes.contributions[0].active = !tc.inactive
			l.scopes.contributions[0].applies = !tc.nonapplying
			neighbor := graphVisit{root: root, dir: "other", owner: other.Named()}
			l.recordScopeVisits(&neighbor, l.scopeWalk(&neighbor), false, false)
			l.scopes.contributions[1].active = true
			if tc.postBuild != nil || tc.metadata != "" {
				readTransformingResource(t, &l.documents, filepath.Join(root, "producer", "reader.yaml"), !tc.parsed)
			}

			l.indexTransformingProducers()
			var want []manifest.NamedResource
			if tc.want {
				want = []manifest.NamedResource{owner.Named()}
			}
			assert.Diff(t, l.selectionContext().TransformingProducers, want)
		})
	}
}

func readTransformingResource(t *testing.T, reader *documentReader, file string, compact bool) *parsedObject {
	t.Helper()
	reader.compact = compact
	parse := reader.fileObjects
	if compact {
		parse = reader.censusFile
	}
	objects, err := parse(t.Context(), file, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	assert.FatalIf(t, len(objects) != 1, "want one parsed object, got %d", len(objects))
	child := &objects[0]
	assert.Equal(t, child.fact != nil, compact)
	assert.Equal(t, child.Obj != nil, !compact)
	return child
}

func TestInputCache_RestoresTransformingProducer(t *testing.T) {
	t.Parallel()
	reason := cache.RenderPersistenceDisabledReason()
	t.Logf("render persistence reason=%q backing=disk", reason)
	const (
		resources   = "resources: [object.yaml]\n"
		substituted = "name: '${NAME}'"
	)
	for _, tc := range []transformingCacheCase{
		{name: "transforming builder", builder: "namePrefix: generated-\n" + resources, metadata: "name: object"},
		{name: "unknown resource", builder: "resources: [absent.yaml]\n", metadata: "name: object", unknown: true},
		{name: "substituted identity", builder: resources, metadata: substituted, substitution: true},
		{
			name: "disabled substitution", builder: resources,
			metadata:     substituted + ", annotations: {kustomize.toolkit.fluxcd.io/substitute: disabled}",
			substitution: true, disabled: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertTransformingProducerCache(t, tc, reason == "")
		})
	}
}

type transformingCacheCase struct {
	name, builder, metadata         string
	unknown, substitution, disabled bool
}

func assertTransformingProducerCache(t *testing.T, tc transformingCacheCase, persistent bool) {
	t.Helper()
	const builder, object = "apps/nested/kustomization.yaml", "apps/nested/object.yaml"
	root := t.TempDir()
	files := fstest.MapFS{
		"apps/kustomization.yaml": {Data: []byte("resources: [nested]\n")},
		builder:                   {Data: []byte(tc.builder)},
		object: {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {" + tc.metadata + "}\n"),
		},
	}
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, owner.Close()) })
	producer := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
	if tc.substitution {
		producer.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"NAME": "object"}}
	}
	var want []manifest.NamedResource
	if !tc.disabled {
		want = []manifest.NamedResource{producer.Named()}
	}
	for _, phase := range []string{"cold", "warm"} {
		l := cachedFixtureLoader(t, root, files, owner)
		l.ensureScopes()
		l.Options.Changes = new(change.Set)
		l.Store.AddObject(producer)
		scope := graphVisit{root: root, dir: "apps", owner: producer.Named()}
		visits := l.scopeWalk(&scope)
		assert.Equal(t, l.scopes.walks[scope.graphContext].unknownRecipe, tc.unknown)
		if tc.unknown {
			assert.Equal(t, l.documents.read(filepath.Join(root, "apps", "nested")).recipe, recipeResourceOnly)
		}
		l.recordScopeVisits(&scope, visits, false, false)
		l.scopes.contributions[len(l.scopes.contributions)-1].active = true
		if tc.substitution {
			child := readTransformingResource(t, &l.documents, filepath.Join(root, object), true)
			assert.Equal(t, child.fact.Authored.Name, "${NAME}")
			assert.Equal(t, child.fact.facts.substituteDisabled, tc.disabled)
		}

		l.indexTransformingProducers()
		assert.Diff(t, l.selectionContext().TransformingProducers, want)
		if persistent {
			_, restored := l.inputCaches[root].files[inputCoordinate{name: builder, builder: true}]
			assert.Equal(t, restored, phase == "warm")
			if tc.substitution {
				_, restored := l.inputCaches[root].files[inputCoordinate{name: object}]
				assert.Equal(t, restored, phase == "warm")
			}
			key := physicalContext{graphContext: scope.graphContext, sourceRoot: root}
			source := l.sourceFacts(root)
			physical, known := source.walk(&key)
			assert.Equal(t, known, true)
			assert.Equal(t, physical.imported, phase == "warm")
		}
		assert.NoError(t, l.PersistInputs(t.Context()))
		l.Close()
	}
}

func TestInputCache_TracksAlternateBuilderFiles(t *testing.T) {
	t.Parallel()
	reason := cache.RenderPersistenceDisabledReason()
	t.Logf("render persistence reason=%q backing=disk", reason)
	for _, tc := range []struct {
		name          string
		withAlternate bool
		want          recipeClass
	}{
		{name: "alternate added", want: 0},
		{name: "alternate removed", withAlternate: true, want: recipeResourceOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const primary, alternate = "apps/kustomization.yaml", "apps/kustomization.yml"
			root := t.TempDir()
			files := fstest.MapFS{
				primary:            {Data: []byte("resources: [object.yaml]\n")},
				"apps/object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
			}
			if tc.withAlternate {
				files[alternate] = files[primary]
			}
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, owner.Close()) })

			dir := filepath.Join(root, "apps")
			initial := cachedFixtureLoader(t, root, files, owner)
			assert.Equal(t, initial.documents.read(dir).recipe&recipeResourceOnly == 0, tc.withAlternate)
			assert.NoError(t, initial.PersistInputs(t.Context()))
			initial.Close()
			if tc.withAlternate {
				delete(files, alternate)
			} else {
				files[alternate] = files[primary]
			}

			cold := cachedFixtureLoader(t, root, files, nil)
			defer cold.Close()
			want := cold.documents.read(dir).recipe
			assert.Equal(t, want, tc.want)

			warm := cachedFixtureLoader(t, root, files, owner)
			assert.Equal(t, warm.documents.read(dir).recipe, want)
			assert.NoError(t, warm.PersistInputs(t.Context()))
			warm.Close()

			replay := cachedFixtureLoader(t, root, files, owner)
			defer replay.Close()
			if reason == "" {
				_, restored := replay.inputCaches[root].files[inputCoordinate{name: primary, builder: true}]
				assert.Equal(t, restored, true)
			}
			assert.Equal(t, replay.documents.read(dir).recipe, want)
		})
	}
}
