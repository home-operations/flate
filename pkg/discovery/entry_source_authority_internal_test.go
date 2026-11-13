package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_EntrySourceAuthority(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [source.yaml, cluster.yaml]\n")
			testutil.WriteFile(t, root, "entry/source.yaml", fmt.Sprintf(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: %s\nmetadata: {name: entry, namespace: apps}\nspec: {url: 'https://declared.invalid/source', endpoint: declared.invalid, bucketName: entry, ref: {branch: main}}\n",
				kind,
			))
			testutil.WriteFile(t, root, "entry/cluster.yaml", testutil.KustomizationYAML("cluster", "apps",
				"  path: ./entry\n  sourceRef: {kind: "+kind+", name: entry}\n"))
			testutil.WriteFile(t, root, "payload/file", "selected local material")
			st := store.New()
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "entry"}
			st.SetArtifact(id, &store.SourceArtifact{
				Kind: kind, LocalPath: root, LocalRoot: root, FS: os.DirFS(root), InputAlias: true,
			})
			result, err := Run(t.Context(), Config{
				Path: filepath.Join(root, "entry"), RepoRoot: root, Store: st,
				SelfURLs: []string{"https://mirror.invalid/source"}, Trees: testTrees(t),
			})
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			child, material, err := result.Admission.Resolve(t.Context(), id, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, child.Obj.Named(), id)
			assert.Equal(t, material != nil, true)
			assert.Equal(t, material.Reads.Valid(), true)
			data, err := fs.ReadFile(material.FS, "payload/file")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "selected local material")
			status, _ := st.GetStatus(id)
			assert.Equal(t, status.Status, store.StatusReady)
			assert.Equal(t, result.Admission.resolvedSources.snapshot()[id].synthetic, false)
			assert.Equal(t, result.Admission.resolvedSources.snapshot()[id].pinned, false)
		})
	}
}

func TestAdmission_ProjectedEntrySourceAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, path, remote string
		local, entry             bool
	}{
		{name: "Git entry and self", kind: manifest.KindGitRepository, path: "entry/config", remote: "https://declared.invalid/source", local: true, entry: true},
		{name: "Git entry without self", kind: manifest.KindGitRepository, path: "entry/config", remote: "https://other.invalid/source", local: true, entry: true},
		{name: "Git self without entry", kind: manifest.KindGitRepository, path: "payload", remote: "https://declared.invalid/source", local: true},
		{name: "OCI entry", kind: manifest.KindOCIRepository, path: "entry/config"},
		{name: "Bucket entry", kind: manifest.KindBucket, path: "entry/config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/config/kustomization.yaml", "resources: [flux.yaml, consumer.yaml]\n")
			testutil.WriteFile(t, root, "entry/config/flux.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync:
    name: entry
    kind: %s
    url: https://declared.invalid/source
    ref: refs/heads/main
    path: %s
`, tc.kind, tc.path))
			testutil.WriteFile(t, root, "entry/config/consumer.yaml", testutil.KustomizationYAML("consumer", "apps",
				"  path: ./payload\n  sourceRef: {kind: "+tc.kind+", name: entry}\n"))
			testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
			testutil.WriteFile(t, root, "payload/file", "working tree")
			st := store.New()
			result, err := Run(t.Context(), Config{
				Path: filepath.Join(root, "entry", "config"), RepoRoot: root, Store: st,
				SelfURLs: []string{tc.remote}, Trees: testTrees(t),
			})
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Namespace: "apps", Name: "entry"}
			if tc.name == "Git entry without self" {
				resolved, resolveErr := result.Admission.resolve(t.Context(), id, &sourceRequest{
					object: st.GetDeclaration(id), entry: true, projected: true,
				})
				assert.NoError(t, resolveErr)
				result.Admission.resolvedSources.record(id, &resolved)
			}
			child, material, err := result.Admission.Resolve(t.Context(), id, nil, nil)
			assert.NoError(t, err)
			resolution := result.Admission.resolvedSources.snapshot()[id]
			assert.Equal(t, resolution.entry, tc.entry)
			assert.Equal(t, resolution.pinned, false)
			assert.Equal(t, resolution.synthetic, false)
			if tc.local {
				assert.FatalIf(
					t,
					material == nil || material.FS == nil,
					"projected entry source lost checkout authority",
				)
				if tc.name == "Git entry and self" {
					assert.Equal(t, st.GetDeclaration(id) == child.Obj, true)
					status, _ := st.GetStatus(id)
					assert.Equal(t, status.Status, store.StatusReady)
				}
				data, readErr := fs.ReadFile(material.FS, "payload/file")
				assert.NoError(t, readErr)
				assert.Equal(t, string(data), "working tree")

				return
			}
			assert.Equal(t, material == nil, true)
			assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
			status, _ := st.GetStatus(id)
			assert.Equal(t, status.Status == store.StatusReady, false)
			declaration := resolution.object
			assert.FatalIf(t, declaration == nil, "produced source lost its projected declaration")
			assert.Equal(t, st.GetDeclaration(id) == nil, true)
			for _, warning := range result.Admission.Warnings() {
				assert.Equal(t, warning.Category == manifest.WarnBootstrapAliases, false)
			}
			for range 2 {
				child, artifact, resolveErr := result.Admission.Resolve(
					t.Context(),
					id,
					declaration,
					aliasRequestReads(t, st, id),
				)
				assert.NoError(t, resolveErr)
				assert.Equal(t, child.Obj == declaration, true)
				assert.Equal(t, artifact == nil, true)
			}
			assert.Equal(t, manifest.Equal(result.Admission.resolvedSources.snapshot()[id].object, declaration), true)
			published, err := result.Admission.Publish(
				t.Context(),
				id,
				aliasRequestReads(t, st, id),
				func(store.Child) (bool, error) {
					t.Error("unsupported projection reached synthetic acceptance")
					return true, nil
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, published, false)
		})
	}
}

func TestAdmission_IndexOnlyProducerAuthority(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run(kind+" index-only producer", func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			document := retainedApplicationDocument("FluxInstance", ".")
			document["spec"].(map[string]any)["sync"].(map[string]any)["kind"] = kind
			parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			application, ok := manifest.ApplicationFor(parsed.Object)
			assert.Equal(t, ok, true)
			encoded, err := json.Marshal(document)
			assert.NoError(t, err)
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"owner.yaml": {Data: encoded}}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := loader.New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			t.Cleanup(l.Retire)
			assert.Equal(t, l.IsFileIndexed(parsed.Object.Named()), true)
			assert.Equal(t, st.GetDeclaration(parsed.Object.Named()) == nil, true)
			assert.Equal(t, st.GetDeclaration(application.Source) == nil, true)
			a := &Admission{
				input:  input,
				path:   root,
				store:  st,
				loader: l,
				resolvedSources: &entrySourcePublisher{
					store:   st,
					mutable: map[manifest.NamedResource]entryResolution{},
				},
			}
			id := application.Source
			resolved, err := a.resolve(
				t.Context(),
				id,
				&sourceRequest{object: application.Declaration, entry: true, projected: true},
			)
			assert.NoError(t, err)
			assert.Equal(t, resolved.artifact == nil, true)
			assert.Equal(t, resolved.entry, false)
			assert.Equal(t, resolved.projected, true)
			a.resolvedSources.record(id, &resolved)
			a.resolvedSources.freeze()
			_, material, err := a.Resolve(t.Context(), id, nil, aliasRequestReads(t, st, id))
			assert.NoError(t, err)
			assert.Equal(t, material == nil, true)
			published, err := a.Publish(t.Context(), id, aliasRequestReads(t, st, id), func(store.Child) (bool, error) {
				t.Error("index-only producer allowed a synthetic declaration")
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, false)
			assert.Equal(t, st.GetDeclaration(application.Source) == nil, true)
			assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
			_, ready := st.GetStatus(id)
			assert.Equal(t, ready, false)
			t.Run("input replacement and repeated resolution", func(t *testing.T) {
				t.Parallel()
				replacement := &Admission{
					input: &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}}, store: store.New(),
					resolvedSources: a.resolvedSources,
				}
				for range 2 {
					current, resolveErr := replacement.resolve(t.Context(), id, &sourceRequest{
						object: application.Declaration, entry: true, projected: true, scoped: true,
					})
					assert.NoError(t, resolveErr)
					assert.Equal(t, current.entry, false)
					assert.Equal(t, current.artifact == nil, true)
					assert.Equal(t, current.synthetic, false)
				}
			})
			t.Run("independent declaration replacement", func(t *testing.T) {
				t.Parallel()
				actual, parseErr := manifest.ParseDoc(map[string]any{
					"apiVersion": manifest.SourceDomain + "/v1",
					"kind":       kind,
					"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
					"spec": map[string]any{
						"url":        "https://independent.invalid/source",
						"endpoint":   "independent.invalid",
						"bucketName": "independent",
					},
				}, manifest.ParseDocOptions{})
				assert.NoError(t, parseErr)
				replacementStore := store.New()
				replacementStore.AddObject(actual.Object)
				acquired := &store.SourceArtifact{
					LocalPath: t.TempDir(),
					FS:        fstest.MapFS{"file": {Data: []byte("acquired")}},
				}
				replacementStore.SetArtifact(id, acquired)
				replacement := &Admission{input: input, store: replacementStore, resolvedSources: a.resolvedSources}
				current := replacement.classifySource(
					id,
					&sourceRequest{object: application.Declaration, projected: true},
				)
				assert.Equal(t, current.object == actual.Object, true)
				assert.Equal(t, current.projected, false)
				assert.Equal(t, current.artifact == nil, true)
				assert.Equal(t, replacementStore.GetArtifact(id, store.ArtifactContent) == acquired, true)
			})
			t.Run("synthetic declaration replacement", func(t *testing.T) {
				t.Parallel()
				staleStore := store.New()
				session := &Session{cfg: Config{Path: t.TempDir(), Store: staleStore}, loader: loader.New(staleStore)}
				t.Cleanup(session.Close)
				t.Cleanup(session.loader.Close)
				_, resolveErr := session.resolveTestSources(t.Context(), false)
				assert.NoError(t, resolveErr)
				admission := session.admission()
				accepted, publishErr := admission.Publish(t.Context(), id, aliasRequestReads(t, staleStore, id),
					func(store.Child) (bool, error) { return true, nil })
				assert.NoError(t, publishErr)
				assert.Equal(t, accepted, true)
				assert.NoError(t, constructAdmissionTest(t.Context(), admission, id))
				before, _ := staleStore.GetStatus(id)
				assert.Equal(t, before.Status, store.StatusReady)
				_, resolveErr = session.resolveEntrySource(t.Context(), admission, id, &entrySourceGroup{
					object:    application.Declaration,
					projected: true, scoped: true, required: true,
					producers: []occurrenceKey{{identity: parsed.Object.Named(), object: parsed.Object}},
				})
				assert.NoError(t, resolveErr)
				assert.Equal(t, staleStore.GetDeclaration(id) == nil, true)
				assert.Equal(t, staleStore.GetArtifact(id, store.ArtifactContent) == nil, true)
				after, _ := staleStore.GetStatus(id)
				assert.Equal(t, after.Status == store.StatusReady, false)
			})
			t.Run("stale local classification", func(t *testing.T) {
				t.Parallel()
				stale := &Admission{input: input, store: store.New(), resolvedSources: &entrySourcePublisher{
					mutable: map[manifest.NamedResource]entryResolution{id: {
						input: input, object: application.Declaration, entry: true, projected: true, artifact: input,
					}},
				}}
				current, resolveErr := stale.resolve(t.Context(), id, &sourceRequest{object: application.Declaration})
				assert.NoError(t, resolveErr)
				assert.Equal(t, current.entry, false)
				assert.Equal(t, current.artifact == nil, true)
				assert.Equal(t, current.synthetic, false)
			})
		})
	}
}

func TestPlan_ProducedSourceEvidenceTransfer(t *testing.T) {
	t.Parallel()
	for _, producer := range []string{"direct", "template", "target only"} {
		t.Run(producer, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			entryPath := filepath.Join(root, "kubernetes", "clusters", "cluster-00")
			testutil.WriteFile(t, root, "kubernetes/clusters/cluster-00/cluster/ks.yaml",
				testutil.KustomizationYAML("cluster", "flux-system",
					"  path: ./apps/overlays/cluster-00\n  sourceRef: {kind: OCIRepository, name: flux-system}\n"))
			document := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux-system, namespace: flux-system}
spec:
  sync:
    kind: OCIRepository
    name: flux-system
    url: oci://registry.invalid/cluster
    ref: latest
    path: ./clusters/cluster-00
`
			if producer != "direct" {
				if producer == "target only" {
					document = "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: flux-system, namespace: flux-system}\nspec: {url: oci://registry.invalid/cluster}\n"
				}
				document = "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: sync-producer, namespace: flux-system}\nspec:\n  resourcesTemplate: |\n    " + strings.ReplaceAll(
					strings.TrimSuffix(document, "\n"),
					"\n",
					"\n    ",
				) + "\n"
			}
			testutil.WriteFile(t, root, "kubernetes/clusters/cluster-00/flux-instance/app/producer.yaml", document)
			entries, err := Plan(t.Context(), Config{Path: entryPath, RepoRoot: root})
			assert.NoError(t, err)
			assert.Equal(t, len(entries), 1)
			entry := &entries[0]
			id := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository,
				Namespace: "flux-system", Name: "flux-system",
			}
			resolution, retained := entry.prepared.sources[id]
			assert.Equal(t, retained, true)
			assert.Equal(t, resolution.synthetic, false)
			assert.Equal(t, len(resolution.producers) != 0, true)
			if producer == "target only" {
				assert.Equal(t, resolution.object == nil, true)
				assert.Equal(t, resolution.artifact == nil, true)
				assert.Equal(t, resolution.viewKey, "")
			}
			st := store.New()
			session, err := beginTest(t.Context(), Config{
				Path: entry.Path, RepoRoot: root, Input: entry.Input, Entry: entry, Store: st,
			})
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			a := session.finishAdmission(t.Context())
			published, err := a.Publish(t.Context(), id, aliasRequestReads(t, st, id), func(store.Child) (bool, error) {
				t.Error("planning producer evidence allowed synthetic source acceptance")
				return true, nil
			})
			assert.NoError(t, err)
			assert.Equal(t, published, false)
			assert.Equal(t, st.GetDeclaration(id) == nil, true)
			assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
		})
	}
}

func TestPlan_GroupedProducerEvidence(t *testing.T) {
	t.Parallel()
	for _, producerFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("producer_first_%t", producerFirst), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			producer, consumer := "a/01-producer", "a/99-consumer"
			if !producerFirst {
				producer, consumer = "a/99-producer", "a/01-consumer"
			}
			for _, dir := range []string{consumer, "b/consumer"} {
				testutil.WriteFile(t, root, dir+"/kustomization.yaml", "resources: [consumer.yaml]\n")
				testutil.WriteFile(t, root, dir+"/consumer.yaml", testutil.KustomizationYAML("consumer", "apps",
					"  path: ./workload\n  sourceRef: {kind: OCIRepository, name: content}\n"))
			}
			testutil.WriteFile(t, root, producer+"/producer.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: content, namespace: apps}
    spec: {url: oci://registry.invalid/content}
`)
			testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: []\n")
			entries, err := Plan(t.Context(), Config{Path: root})
			assert.NoError(t, err)
			assert.Equal(t, len(entries), 2)
			id := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository,
				Namespace: "apps", Name: "content",
			}
			for _, entry := range entries {
				produced := entry.Path == filepath.Join(root, "a")
				assert.Equal(t, len(entry.prepared.sources[id].producers) != 0, produced)
				assert.Equal(t, entry.prepared.sources[id].synthetic, !produced)
				st := store.New()
				session, beginErr := beginTest(
					t.Context(),
					Config{Path: entry.Path, Input: entry.Input, Entry: &entry, Store: st},
				)
				assert.NoError(t, beginErr)
				t.Cleanup(session.Close)
				admission := session.finishAdmission(t.Context())
				published, publishErr := admission.Publish(t.Context(), id, aliasRequestReads(t, st, id),
					func(store.Child) (bool, error) { return true, nil })
				assert.NoError(t, publishErr)
				assert.Equal(t, published, !produced)
				assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, produced)
			}
		})
	}
}

func TestPlan_ProducerRemovalAndUnmatchedInput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	consumer := testutil.KustomizationYAML("consumer", "apps",
		"  path: ./workload\n  sourceRef: {kind: OCIRepository, name: content}\n")
	files := fstest.MapFS{
		"entry/consumer.yaml": {Data: []byte(consumer)},
		"entry/producer.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: content, namespace: apps}
    spec: {url: oci://registry.invalid/content}
`)},
		"workload/kustomization.yaml": {Data: []byte("resources: []\n")},
	}
	input := &store.SourceArtifact{LocalPath: root, FS: files}
	entries, err := Plan(t.Context(), Config{Path: filepath.Join(root, "entry"), Input: input})
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	entry := &entries[0]
	id := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository,
		Namespace: "apps", Name: "content",
	}
	assert.Equal(t, len(entry.prepared.sources[id].producers) != 0, true)
	remaining := fstest.MapFS{
		"entry/consumer.yaml":         files["entry/consumer.yaml"],
		"workload/kustomization.yaml": files["workload/kustomization.yaml"],
	}
	replacement := &store.SourceArtifact{LocalPath: root, FS: remaining}
	for _, tc := range []struct {
		name  string
		input *store.SourceArtifact
		path  string
	}{
		{name: "producer removed with surviving consumer", input: replacement, path: entry.Path},
		{name: "different entry", input: input, path: filepath.Join(root, "workload")},
		{name: "absent side", path: entry.Path},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := newDiscoverer(&Config{Path: tc.path, Input: tc.input, Entry: entry, Store: store.New()})
			t.Cleanup(d.Close)
			assert.Equal(t, d.cfg.Entry == nil, true)
			assert.Equal(t, d.loader.Options.ParsedInputs == nil, true)
		})
	}
	changed, err := Plan(t.Context(), Config{Path: entry.Path, Input: replacement})
	assert.NoError(t, err)
	assert.Equal(t, len(changed), 1)
	assert.Equal(t, len(changed[0].prepared.sources[id].producers), 0)
	assert.Equal(t, changed[0].prepared.sources[id].synthetic, true)
	st := store.New()
	session, err := beginTest(t.Context(), Config{Path: entry.Path, Input: replacement, Entry: entry, Store: st})
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	published, err := session.finishAdmission(t.Context()).Publish(t.Context(), id, aliasRequestReads(t, st, id),
		func(store.Child) (bool, error) { return true, nil })
	assert.NoError(t, err)
	assert.Equal(t, published, true)
}

func TestPlan_ProducerEvidenceReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var reads atomic.Int64
	files := fstest.MapFS{"producer.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec: {}
`)}}
	input := &store.SourceArtifact{
		LocalPath: root,
		FS:        entryReadFS{FS: files, reads: map[string]*atomic.Int64{"producer.yaml": &reads}},
	}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	packages, err := d.loader.Packages(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, len(packages), 1)
	scope := partitionScope{occurrences: slices.Clone(packages[0].Occurrences)}
	planner := entryPlanner{session: d, inputs: map[string]*store.SourceArtifact{root: input}}
	assert.NoError(t, planner.prepareScope(t.Context(), &scope))
	assert.Equal(t, len(scope.occurrences[0].TemplateObjects), 0)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "content",
	}
	reference := aliasRequestReference(t, d.cfg.Store, id)
	visit := partitionVisit{occurrence: &scope.occurrences[0], scope: &scope}
	_, err = planner.source(t.Context(), &entryRoot{}, &visit, &reference)
	assert.Equal(t, errors.Is(err, errPendingSources), true)
	assert.NoError(t, constructTestSources(t.Context(), d))
	before := reads.Load()
	assert.Equal(t, len(planner.objects), 1)
	planner.parsed = d.loader.ParsedInputs()
	parsed := planner.parsed
	for range 2 {
		_, sourceErr := planner.source(t.Context(), &entryRoot{}, &visit, &reference)
		assert.NoError(t, sourceErr)
	}
	assert.Equal(t, reads.Load(), before)
	assert.Equal(t, planner.parsed == parsed, true)
	assert.Equal(t, len(planner.objects), 1)
	witness := scope.occurrences[0]
	witness.Witness = new(int)
	scope = partitionScope{occurrences: []selection.OccurrenceFacts{witness}}
	assert.NoError(t, planner.prepareScope(t.Context(), &scope))
	assert.Equal(t, len(planner.objects), 2)
	assert.Equal(t, planner.parsed == nil, true)
	replaced := &store.SourceArtifact{
		LocalPath: root,
		FS: fstest.MapFS{"producer.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: added}
    spec: {url: oci://registry.invalid/added}
`)}},
	}
	planner.inputs[root] = replaced
	replacement := scope.occurrences[0]
	replacement.Witness = new(int)
	replacement.Identity.Namespace = "other"
	scope = partitionScope{occurrences: []selection.OccurrenceFacts{replacement}}
	assert.NoError(t, planner.prepareScope(t.Context(), &scope))
	assert.Equal(t, len(planner.objects), 3)
	assert.Equal(t, planner.parsed == nil, true)
	id = manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository,
		Namespace: "other", Name: "added",
	}
	_, _, producers := scope.source(id)
	assert.Equal(t, len(producers), 1)
	id.Namespace = "apps"
	_, _, producers = scope.source(id)
	assert.Equal(t, len(producers), 0)
}

func TestPlan_RetainedObjectEvidenceReuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, declaration string
	}{
		{"structural", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: apps}
spec: {path: ./leaf, sourceRef: {kind: GitRepository, name: cluster}}
`},
		{"source only", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: owner, namespace: apps}
spec: {chart: {spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}}}
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{
				LocalPath: root,
				FS: fstest.MapFS{
					"kustomization.yaml": {Data: []byte("resources: [owner.yaml]\n")},
					"owner.yaml":         {Data: []byte(tc.declaration)},
				},
			}
			d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
			t.Cleanup(d.Close)
			_, err := d.resolveTestSources(t.Context(), false)
			assert.NoError(t, err)
			d.loader.SourceRoot = root
			_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			packages, err := d.loader.Packages(t.Context())
			assert.NoError(t, err)
			assert.FatalIf(t, len(packages) != 1, "packages = %d, want 1", len(packages))
			scope := partitionScope{occurrences: slices.Clone(packages[0].Occurrences)}
			assert.Equal(t, scope.occurrences[0].Object != nil, true)
			planner := entryPlanner{session: d, inputs: map[string]*store.SourceArtifact{root: input}}
			planner.parsed = d.loader.ParsedInputs()
			parsed := planner.parsed
			assert.NoError(t, planner.prepareScope(t.Context(), &scope))
			assert.Equal(t, len(planner.objects), 0)
			assert.Equal(t, planner.parsed == parsed, true)
			assert.NoError(t, planner.prepareScope(t.Context(), &scope))
			assert.Equal(t, len(planner.objects), 0)
			assert.Equal(t, planner.parsed == parsed, true)
			facts := d.loader.ProducerFacts(partitionObject(&scope.occurrences[0]))
			assert.Equal(t, facts.Derived, true)
		})
	}
}

func TestPlan_ObjectMaterialReplacement(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	d := newDiscoverer(&Config{Path: root, Store: store.New()})
	t.Cleanup(d.Close)
	planner := entryPlanner{session: d, inputs: map[string]*store.SourceArtifact{}}
	var occurrence selection.OccurrenceFacts
	for _, content := range []string{"initial", "replacement"} {
		document := map[string]any{
			"apiVersion": manifest.FluxOperatorDomain + "/v1",
			"kind":       "ResourceSet",
			"metadata":   map[string]any{"name": "producer", "namespace": "apps"},
			"spec": map[string]any{"resourcesTemplate": fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: %s}
spec: {url: oci://registry.invalid/%s}
`, content, content)},
		}
		parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
		assert.NoError(t, err)
		encoded, err := json.Marshal(document)
		assert.NoError(t, err)
		input := &store.SourceArtifact{LocalPath: root, Content: content, FS: fstest.MapFS{
			"producer.yaml": {Data: encoded},
		}}
		if occurrence.Object == nil {
			occurrence = selection.OccurrenceFacts{
				Declaration: selection.OccurrenceID{Source: root, File: "producer.yaml"},
				Identity:    parsed.Object.Named(), Authored: parsed.Object.Named(),
				Object: parsed.Object, Witness: new(int), Mounted: input,
			}
		}
		planner.inputs[root] = input
		child, err := planner.object(t.Context(), &occurrence)
		assert.NoError(t, err)
		assert.Diff(t, child.Doc, document)
		assert.FatalIf(t, child.Obj == nil, "replacement lost producer object")
		facts := d.loader.ProducerFacts(child.Obj).Occurrence
		scope := partitionScope{occurrences: []selection.OccurrenceFacts{facts}}
		for _, name := range []string{"initial", "replacement"} {
			_, _, producers := scope.source(manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: name,
			})
			assert.Equal(t, len(producers) == 1, name == content)
		}
	}
}

func TestPlan_ProjectionMaterialReplacement(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	sourceRoot := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	parsed, err := manifest.ParseDoc(
		retainedApplicationDocument("FluxInstance", "./payload"),
		manifest.ParseDocOptions{},
	)
	assert.NoError(t, err)
	application, valid := manifest.ApplicationFor(parsed.Object)
	assert.Equal(t, valid, true)
	d.cfg.Store.AddObject(parsed.Object)
	d.cfg.Store.AddObject(application.Declaration)
	d.loader.SourceRoot = root
	occurrence := selection.OccurrenceFacts{Object: parsed.Object, Identity: parsed.Object.Named()}
	scope := partitionScope{occurrences: []selection.OccurrenceFacts{occurrence}}
	visit := partitionVisit{occurrence: &occurrence, scope: &scope}
	planner := entryPlanner{session: d, inputs: map[string]*store.SourceArtifact{root: input}}
	for _, content := range []string{"initial", "replacement"} {
		material := &store.SourceArtifact{LocalPath: sourceRoot, Content: content, FS: fstest.MapFS{
			"payload/producer.yaml": {Data: fmt.Appendf(nil, `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: %s}
    spec: {url: oci://registry.invalid/%s}
`, content, content, content)},
		}}
		d.cfg.Store.SetArtifact(application.Source, material)
		children, followErr := planner.follow(t.Context(), &entryRoot{}, &visit, false)
		assert.NoError(t, followErr)
		assert.FatalIf(t, len(children) != 1, "projected children: %+v", children)
		assert.Equal(t, children[0].occurrence.Identity.Name, content)
		assert.Equal(t, children[0].occurrence.Identity.Namespace, "apps")
		assert.Equal(t, children[0].occurrence.Declaration.Source, material.LocalPath)
		assert.Equal(t, children[0].occurrence.Declaration.File, "payload/producer.yaml")
		for _, name := range []string{"initial", "replacement"} {
			_, _, producers := children[0].scope.source(manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: name,
			})
			assert.Equal(t, len(producers) == 1, name == content)
		}
	}
}

func TestAdmission_ConstructionViewReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"payload/file": {Data: []byte("initial")},
	}}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	var previous fs.FS
	for _, content := range []string{"initial", "replacement"} {
		if content == "replacement" {
			replaced := *input
			replaced.Content = content
			replaced.FS = fstest.MapFS{"payload/file": {Data: []byte(content)}}
			d.cfg.Store.SetArtifact(manifest.BootstrapSourceID, &replaced)
			_, err = d.resolveTestSources(t.Context(), false)
			assert.NoError(t, err)
		}
		var first fs.FS
		for range 2 {
			parsed, parseErr := manifest.ParseDoc(
				retainedApplicationDocument("FluxInstance", "."),
				manifest.ParseDocOptions{},
			)
			assert.NoError(t, parseErr)
			application, valid := manifest.ApplicationFor(parsed.Object)
			assert.Equal(t, valid, true)
			_, resolveErr := d.resolveEntrySource(t.Context(), d.admission(), application.Source, &entrySourceGroup{
				object: application.Declaration, entry: true, projected: true,
			})
			assert.Equal(t, resolveErr == nil || errors.Is(resolveErr, errPendingSources), true)
			assert.NoError(t, constructTestSources(t.Context(), d))
			material := d.resolvedSources.snapshot()[application.Source].artifact
			assert.FatalIf(t, material == nil, "projected source lost local material")
			data, readErr := fs.ReadFile(material.FS, "payload/file")
			assert.NoError(t, readErr)
			assert.Equal(t, string(data), content)
			if first == nil {
				first = material.FS
			}
			assert.Equal(t, material.FS == first, true)
		}
		assert.Equal(t, first == previous, false)
		previous = first
	}
}
