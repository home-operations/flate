package loader

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestEntryFrontier_ApplicationEvidence(t *testing.T) {
	t.Parallel()
	const syncInstance = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance, namespace: authored}
spec:
  sync: {kind: GitRepository, url: https://example.test/repo, ref: refs/heads/main, path: ./entry}
`
	for _, tc := range []struct {
		name     string
		builder  string
		files    map[string]string
		possible bool
	}{
		{name: "empty", builder: "resources: []\n"},
		{name: "ordinary", builder: "resources: [object.yaml]\n", files: map[string]string{
			"object.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ordinary}\n",
		}},
		{name: "application", builder: "resources: [object.yaml]\n", files: map[string]string{
			"object.yaml": testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"),
		}, possible: true},
		{name: "FluxInstance application", builder: "resources: [object.yaml]\n", files: map[string]string{
			"object.yaml": syncInstance,
		}, possible: true},
		{name: "nested application", builder: "resources: [nested]\n", files: map[string]string{
			"nested/kustomization.yaml": "resources: [object.yaml]\n",
			"nested/object.yaml":        testutil.KustomizationYAML("child", "apps", "  path: ./entry\n"),
		}, possible: true},
		{name: "HelmRelease continuation", builder: "resources: [object.yaml]\n", files: map[string]string{
			"object.yaml": "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: generator, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: chart}}\n",
		}, possible: true},
		{name: "ResourceSet continuation", builder: "resources: [object.yaml]\n", files: map[string]string{
			"object.yaml": "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: generator, namespace: apps}\nspec: {}\n",
		}, possible: true},
		{name: "missing resource", builder: "resources: [missing.yaml]\n", possible: true},
		{name: "remote resource", builder: "resources: [https://example.test/resource.yaml]\n", possible: true},
		{name: "generator", builder: "generators: [generator.yaml]\n", possible: true},
		{name: "remote generator input", builder: "configMapGenerator: [{name: data, envs: [https://example.test/data.env]}]\n", possible: true},
		{name: "missing generator input", builder: "configMapGenerator: [{name: data, files: [missing.txt]}]\n", possible: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := fstest.MapFS{"kustomization.yaml": {Data: []byte(tc.builder)}}
			for name, body := range tc.files {
				files[name] = &fstest.MapFile{Data: []byte(body)}
			}
			l := New(store.New())
			input := &store.SourceArtifact{LocalPath: t.TempDir(), FS: files}
			possible, err := l.EntryFrontier(t.Context(), input, ".", nil)
			assert.NoError(t, err)
			assert.Equal(t, possible, tc.possible)
			if _, exists := tc.files["object.yaml"]; exists {
				physical := New(store.New())
				physical.documents.setSources(map[string]*store.SourceArtifact{input.LocalPath: input})
				physical.documents.intent = manifest.IncidentalDocuments
				_, err = physical.documents.censusFile(
					t.Context(),
					filepath.Join(input.LocalPath, "object.yaml"),
					manifest.ParseDocOptions{},
				)
				assert.NoError(t, err)
				possible, err = physical.EntryFrontier(t.Context(), input, ".", nil)
				assert.NoError(t, err)
				assert.Equal(t, possible, tc.possible)
			}
		})
	}

	for _, tc := range []struct {
		name                  string
		compact, patched      bool
		repeatSourceNamespace string
	}{
		{name: "parsed", repeatSourceNamespace: "authored"},
		{name: "parsed with sync patches", patched: true, repeatSourceNamespace: "projected"},
		{name: "indexed", compact: true, repeatSourceNamespace: "projected"},
		{name: "indexed with sync patches", compact: true, patched: true, repeatSourceNamespace: "projected"},
	} {
		t.Run("FluxInstance repeat visit/"+tc.name, func(t *testing.T) {
			t.Parallel()
			instance := syncInstance
			if tc.patched {
				instance += "  kustomize: {patches: [{target: {kind: GitRepository}, patch: '[{op: replace, path: /spec/interval, value: 2m}]'}]}\n"
			}
			const objectFile = "object.yaml"
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
				objectFile: {Data: []byte(instance)},
				"consumer.yaml": {Data: []byte(testutil.KustomizationYAML(
					"consumer",
					"projected",
					"  path: ./payload\n  sourceRef: {kind: GitRepository, name: projected}\n  dependsOn: [{name: projected}]\n",
				))},
			}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := New(st)
			l.SourceRoot, l.entryPath = root, filepath.Join(root, "entry")
			l.ensureScopes()
			l.documents.compact = tc.compact
			l.documents.intent = manifest.IncidentalDocuments
			l.documents.setSources(map[string]*store.SourceArtifact{root: input})
			b := selfProduceBuilder{idx: l.Index, store: st, reader: l.documents, locations: l.locations}
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).
				Named()
			authored := manifest.NamedResource{
				Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "authored", Name: "instance",
			}
			projected := authored
			projected.Namespace = "projected"
			targetSource := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "projected",
				Name:      "projected",
			}
			targetRoot := testutil.KustomizationID("projected", "projected")
			consumer := graphVisit{file: filepath.Join(root, "consumer.yaml"), readable: true}
			want := make([]manifest.Production, 0, 2)
			for i, namespace := range []string{"projected", tc.repeatSourceNamespace} {
				source := targetSource
				source.Namespace, source.Name = namespace, namespace
				// Only the expected source can resolve ./entry to the bootstrap entry on this visit.
				st.SetArtifact(source, input)
				owner := testutil.KustomizationID("owners", []string{"first", "repeat"}[i])
				visit := graphVisit{
					namespace:  projected.Namespace,
					file:       filepath.Join(root, objectFile),
					coordinate: objectFile,
					owner:      owner,
					source:     manifest.BootstrapSourceID,
				}
				declarations, err := b.recordProduced(t.Context(), &visit, nil, l)
				assert.NoError(t, err)
				b.joinProductions(t.Context(), declarations, []graphVisit{consumer})
				st.SetArtifact(source, nil)
				want = append(want, manifest.Production{
					Owner:       owner,
					Declaration: projected,
					Authored:    authored,
					Source:      manifest.BootstrapSourceID,
					File:        objectFile,
				})
				assert.Equal(t, l.Index.Records[projected].Authored, authored)
				assert.Diff(t, l.Index.Records[entry].Productions, want)
				if tc.patched {
					assert.Diff(t, l.Index.Records[targetSource].Productions, want)
					assert.Diff(t, l.Index.Records[targetRoot].Productions, want)
				}
			}
		})
	}
}

func TestEntryFrontier_ReadIntentReuse(t *testing.T) {
	t.Parallel()
	for _, physicalFirst := range []bool{false, true} {
		name := "application first"
		if physicalFirst {
			name = "physical first"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{
				"kustomization.yaml": {Data: []byte("resources: [objects.yaml, negative.yaml, empty.yaml]\n")},
				"objects.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							"entry",
							"apps",
							"  path: ./entry\n",
						) + "---\n" + testutil.KustomizationYAML(
							"supplier",
							"apps",
							"  postBuild: {substitute: {PREFIX: prod-}}\n",
						) + `---
apiVersion: v1
kind: List
items:
  - apiVersion: v1
    kind: List
    items:
      - apiVersion: v1
        kind: ConfigMap
        metadata: {name: data}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: projected, namespace: apps}
    spec: {path: ./entry}
`,
					),
				},
				"negative.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: negative}\n")},
				"empty.yaml":    {Data: []byte("# empty\n")},
			}
			reads := map[string]*atomic.Int64{}
			for file := range files {
				reads[file] = &atomic.Int64{}
			}
			var directories atomic.Int64
			input := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				FS:        parsedReadFS{FS: files, reads: reads, directories: &directories},
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})

			owner := testutil.KustomizationID("apps", "owner")
			load := func() { _, err := l.Load(t.Context(), root, owner, nil); assert.NoError(t, err) }
			if physicalFirst {
				_, committed, commitErr := requireSelection(t, l, []manifest.NamedResource{owner}, false)
				assert.NoError(t, commitErr)
				assert.Equal(t, committed, true)
				load()
			}
			if !physicalFirst {
				load()
			}
			count := reads["objects.yaml"].Load()
			listings := directories.Load()
			possible, err := l.EntryFrontier(t.Context(), input, ".", nil)
			assert.NoError(t, err)
			assert.Equal(t, possible, true)
			load()
			assert.Equal(t, reads["objects.yaml"].Load(), count)
			assert.Equal(t, directories.Load(), listings)
			if !physicalFirst {
				cached := l.documents.cache[filepath.Join(root, "objects.yaml")]
				assert.Equal(t, cached.coverage, manifest.ApplicationDocuments)
				assert.Equal(t, len(cached.objects), 1)
				assert.Equal(t, cached.declarations.Complete, false)
				assert.Equal(t, len(cached.objects[0].fact.facts.targets), 0)
				assert.Equal(t, len(l.inputVisibility()), 0)
				assert.Equal(t, l.documents.cache[filepath.Join(root, "negative.yaml")].objects != nil, true)
				assert.Equal(t, l.documents.cache[filepath.Join(root, "empty.yaml")].objects != nil, true)
				_, committed, commitErr := requireSelection(t, l, []manifest.NamedResource{owner}, false)
				assert.NoError(t, commitErr)
				assert.Equal(t, committed, true)
				load()
				assert.Equal(t, reads["objects.yaml"].Load(), count+1)
			}
			assert.Equal(t, directories.Load(), listings)
			cached := l.documents.cache[filepath.Join(root, "objects.yaml")]
			assert.Equal(t, cached.coverage, manifest.IncidentalDocuments)
			assert.Equal(t, len(cached.objects), 4)
			supplier := l.CatalogObject(testutil.KustomizationID("apps", "supplier")).(*manifest.Kustomization)
			assert.Equal(t, supplier.PostBuild.Substitute["PREFIX"], "prod-")
			count = reads["objects.yaml"].Load()
			load()
			assert.Equal(t, reads["objects.yaml"].Load(), count)
			assert.Equal(t, directories.Load(), listings)
		})
	}
}

func TestEntryFrontier_RequiredDocumentCoverage(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var reads, directories atomic.Int64
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [objects.yaml]\n")},
		"objects.yaml": {Data: []byte(
			testutil.KustomizationYAML("application", "apps", "  path: ./entry\n") +
				"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: pinned, namespace: apps}\nspec: {url: https://example.test/repo, ref: {tag: pin}}\n",
		)},
	}
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: parsedReadFS{
		FS: files, reads: map[string]*atomic.Int64{"objects.yaml": &reads}, directories: &directories,
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := New(st)
	l.documents.intent = manifest.RequiredDocuments
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	before := reads.Load()
	possible, err := l.EntryFrontier(t.Context(), input, ".", nil)
	assert.NoError(t, err)
	assert.Equal(t, possible, true)
	assert.Equal(t, reads.Load(), before)
	cached := l.documents.cache[filepath.Join(root, "objects.yaml")]
	assert.Equal(t, cached.coverage, manifest.RequiredDocuments)
	assert.Equal(t, len(cached.objects), 2)
	child := l.authoredObject(
		filepath.Join(root, "objects.yaml"),
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "apps",
			Name:      "pinned",
		},
	)
	assert.FatalIf(t, child.Obj == nil, "required source declaration disappeared")
	assert.Equal(t, child.Obj.(*manifest.GitRepository).Reference.Tag, "pin")
}

func TestEntryFrontier_SourceLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var reads atomic.Int64
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: parsedReadFS{
		FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
			"object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ordinary}\n")},
		},
		reads: map[string]*atomic.Int64{"object.yaml": &reads},
	}}
	l := New(store.New())
	for range 2 {
		possible, err := l.EntryFrontier(t.Context(), input, ".", nil)
		assert.NoError(t, err)
		assert.Equal(t, possible, false)
	}
	assert.Equal(t, reads.Load(), int64(1))
	replacement := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
		"object.yaml":        {Data: []byte(testutil.KustomizationYAML("applier", "apps", "  path: ./entry\n"))},
	}}
	possible, err := l.EntryFrontier(t.Context(), replacement, ".", nil)
	assert.NoError(t, err)
	assert.Equal(t, possible, true)
	possible, err = l.EntryFrontier(t.Context(), input, ".", nil)
	assert.NoError(t, err)
	assert.Equal(t, possible, false)
	assert.Equal(t, reads.Load(), int64(2))
}

func TestEntryFrontier_FrozenOwnership(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	file := filepath.Join(root, "source.yaml")
	input := &store.SourceArtifact{
		Provider: store.ProviderInput, LocalPath: root,
		FS: fstest.MapFS{"source.yaml": {Data: []byte(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\n",
		)}},
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := New(st)
	l.documents.setSources(map[string]*store.SourceArtifact{root: input})
	owners := []selection.RootOwnerFacts{{Identity: testutil.KustomizationID("apps", "blocked")}}
	l.documents.cache[file] = document{eligible: selection.RootEligible, rootOwners: owners, rootKnown: true}
	l.frozen = true
	replacement := &store.SourceArtifact{
		Provider: store.ProviderAlias, LocalPath: root,
		FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [entry.yaml]\n")},
			"entry.yaml":         {Data: []byte(testutil.KustomizationYAML("entry", "apps", "  path: ./cluster\n"))},
		},
	}

	possible, err := l.EntryFrontier(t.Context(), replacement, ".", nil)
	assert.NoError(t, err)
	assert.Equal(t, possible, true)
	assert.Equal(t, l.documents.artifacts[root], input)
	frozen := l.documents.cache[file]
	assert.Equal(t, frozen.eligible, selection.RootEligible)
	assert.Diff(t, frozen.rootOwners, owners)
}

func TestEnclosingPackages_DeclarationCandidates(t *testing.T) {
	t.Parallel()
	const fluxInstance = "kind: FluxInstance\nmetadata: {name: cluster, namespace: flux-system}\n"
	const sync = "spec:\n  sync: {kind: GitRepository, url: 'https://example.invalid/repo', ref: main, path: ./apps}\n"
	target := testutil.KustomizationID("requested", "")
	matchKind := func(id manifest.NamedResource) bool { return id.Group == target.Group && id.Kind == target.Kind }
	matchGenerator := manifest.NamedResource.CanDeclareGeneratedEntry
	for _, tc := range []struct {
		name, declaration, transform string
		match                        func(manifest.NamedResource) bool
		want                         []string
	}{
		{
			name: "ordinary declaration in another namespace", match: matchKind, want: []string{"apps"},
			declaration: testutil.KustomizationYAML("app", "different", "  path: ./workload\n"),
		},
		{
			name:        "ordinary declaration without a predicate",
			declaration: testutil.KustomizationYAML("app", "requested", "  path: ./workload\n"),
		},
		{
			name: "opaque ancestor without a predicate", transform: "namePrefix: install-\n", want: []string{"apps"},
			declaration: testutil.KustomizationYAML("app", "requested", "  path: ./workload\n"),
		},
		{
			name: "same kind in a different group", match: matchGenerator,
			declaration: "apiVersion: example.invalid/v1\n" + fluxInstance + sync,
		},
		{
			name: "FluxInstance without sync", match: matchGenerator, want: []string{"apps"},
			declaration: "apiVersion: " + manifest.FluxOperatorDomain + "/v1\n" + fluxInstance,
		},
		{
			name: "FluxInstance with sync", match: matchGenerator, want: []string{"apps"},
			declaration: "apiVersion: " + manifest.FluxOperatorDomain + "/v1\n" + fluxInstance + sync,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{
				"apps/kustomization.yaml":       {Data: []byte("resources: [candidate.yaml, entry]\n" + tc.transform)},
				"apps/candidate.yaml":           {Data: []byte(tc.declaration)},
				"apps/entry/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"apps/entry/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: entry}\n"),
				},
			}}
			l := New(store.New())
			got, err := l.EnclosingPackages(t.Context(), input, "apps/entry", tc.match)
			assert.NoError(t, err)
			assert.Diff(t, got, tc.want)
		})
	}
}

func TestEnclosingApplications_ResourceMembership(t *testing.T) {
	t.Parallel()
	const definition = "clusters/app"
	const entry = definition + "/leaf"
	reference := "  path: ./" + definition + "\n  sourceRef: {kind: GitRepository, name: example}\n"
	for _, tc := range []struct {
		name          string
		extra         map[string]string
		paths         []string
		requiresBuild bool
	}{
		{
			name: "listed reach",
			extra: map[string]string{
				"kustomization.yaml":                     "resources: [clusters/app, clusters/management]\n",
				"clusters/management/kustomization.yaml": "resources: [../app/listed.yaml]\n",
			},
			paths: []string{"."},
		},
		{
			name: "configuration changes the enclosing recipe",
			extra: map[string]string{
				"kustomization.yaml":                            "resources: [clusters/app, clusters/management]\n",
				"clusters/management/kustomization.yaml":        "resources: [../app/listed.yaml, config]\n",
				"clusters/management/config/kustomization.yaml": "resources: []\nconfigurations: [config.yaml]\n",
			},
			paths:         []string{"."},
			requiresBuild: true,
		},
		{
			name: "separate enclosing build contexts for one resource",
			extra: map[string]string{
				"clusters/first/kustomization.yaml":  "resources: [../app/listed.yaml]\n",
				"clusters/second/kustomization.yaml": "resources: [../app/listed.yaml]\n",
			},
			paths: []string{"clusters/first", "clusters/second"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := fstest.MapFS{
				definition + "/kustomization.yaml": {Data: []byte("resources: [leaf/release.yaml]\n")},
				definition + "/listed.yaml": {
					Data: []byte(testutil.KustomizationYAML("listed", "flux-system", reference)),
				},
				definition + "/unlisted.yaml": {
					Data: []byte(testutil.KustomizationYAML("unlisted", "flux-system", reference)),
				},
				entry + "/release.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n"),
				},
			}
			for name, body := range tc.extra {
				files[name] = &fstest.MapFile{Data: []byte(body)}
			}
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir(), FS: files}
			standalone, recipes, err := ApplicationPackages(t.Context(), input, ".", entry)
			assert.NoError(t, err)
			assert.Equal(t, len(standalone), 0)
			assert.FatalIf(t, len(recipes) != len(tc.paths), "enclosing recipes = %v", recipes)
			for i, pkg := range recipes {
				assert.Equal(t, pkg.Path, tc.paths[i])
				assert.Equal(t, pkg.RequiresBuild, tc.requiresBuild)
				assert.FatalIf(t, len(pkg.Owners) != 1, "enclosing applications = %v", pkg.Owners)
				assert.Equal(t, pkg.Owners[0].Obj.Named(), testutil.KustomizationID("flux-system", "listed"))
			}
		})
	}
}
