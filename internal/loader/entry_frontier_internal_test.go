package loader

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
)

func TestEntryFrontier_ApplicationEvidence(t *testing.T) {
	t.Parallel()
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
		{name: "remote generator input", builder: "configMapGenerator: [{name: data, envs: [https://example.test/data.env]}]\n"},
		{name: "missing generator input", builder: "configMapGenerator: [{name: data, files: [missing.txt]}]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := fstest.MapFS{"kustomization.yaml": {Data: []byte(tc.builder)}}
			for name, body := range tc.files {
				files[name] = &fstest.MapFile{Data: []byte(body)}
			}
			l := New(store.New())
			input := &store.SourceArtifact{LocalPath: t.TempDir(), FS: files}
			possible, err := l.EntryFrontier(t.Context(), input, ".")
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
				possible, err = physical.EntryFrontier(t.Context(), input, ".")
				assert.NoError(t, err)
				assert.Equal(t, possible, tc.possible)
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
			possible, err := l.EntryFrontier(t.Context(), input, ".")
			assert.NoError(t, err)
			assert.Equal(t, possible, true)
			load()
			assert.Equal(t, reads["objects.yaml"].Load(), count)
			assert.Equal(t, directories.Load(), listings)
			if !physicalFirst {
				cached := l.documents.cache[filepath.Join(root, "objects.yaml")]
				assert.Equal(t, cached.coverage, manifest.ApplicationDocuments)
				assert.Equal(t, len(cached.objects), 1)
				assert.Equal(t, cached.objects[0].fact.facts.declared, "")
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
	input := &store.SourceArtifact{LocalPath: root, FS: parsedReadFS{
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
	possible, err := l.EntryFrontier(t.Context(), input, ".")
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
	input := &store.SourceArtifact{LocalPath: root, FS: parsedReadFS{
		FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
			"object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ordinary}\n")},
		},
		reads: map[string]*atomic.Int64{"object.yaml": &reads},
	}}
	l := New(store.New())
	for range 2 {
		possible, err := l.EntryFrontier(t.Context(), input, ".")
		assert.NoError(t, err)
		assert.Equal(t, possible, false)
	}
	assert.Equal(t, reads.Load(), int64(1))
	replacement := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
		"object.yaml":        {Data: []byte(testutil.KustomizationYAML("applier", "apps", "  path: ./entry\n"))},
	}}
	possible, err := l.EntryFrontier(t.Context(), replacement, ".")
	assert.NoError(t, err)
	assert.Equal(t, possible, true)
	possible, err = l.EntryFrontier(t.Context(), input, ".")
	assert.NoError(t, err)
	assert.Equal(t, possible, false)
	assert.Equal(t, reads.Load(), int64(2))
}
