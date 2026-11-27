package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmission_SyncPairFollowsSelectedDeclaration(t *testing.T) {
	t.Parallel()
	source := &manifest.OCIRepository{Name: "sync", Namespace: "apps", URL: "oci://fixture.invalid/repo"}
	authored := *source
	app := manifest.NewEntryKustomization(source.Named(), "./entry", manifest.NamedResource{})
	app.Name, app.Namespace = source.Name, source.Namespace
	st := store.New()
	st.AddObject(&authored)
	a := &Admission{store: st, input: &store.SourceArtifact{LocalPath: t.TempDir()}}
	for _, tc := range []struct {
		name   string
		source *manifest.OCIRepository
		want   bool
	}{
		{name: "selected declaration", source: &authored, want: true},
		{name: "different declaration with the same identity", source: source},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pair := &syncSource{source: tc.source, application: app}
			got := a.classifySource(source.Named(), &sourceRequest{object: tc.source, sync: pair, projected: true})
			assert.Equal(t, got.object, manifest.BaseManifest(&authored))
			assert.Equal(t, got.sync == pair, tc.want)
			assert.Equal(t, got.provider == store.ProviderAlias, tc.want)
		})
	}
}

func TestAdmission_SyncInheritance(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{LocalPath: t.TempDir()}
	source := &manifest.OCIRepository{Name: "sync", Namespace: "apps", URL: "oci://fixture.invalid/repo"}
	app := manifest.NewEntryKustomization(source.Named(), "./entry", manifest.NamedResource{})
	app.Name, app.Namespace = source.Name, source.Namespace
	prior := entryResolution{
		input: input, object: source, entry: true,
		sync:      &syncSource{source: source, application: app},
		producers: []occurrenceKey{{identity: testutil.HelmReleaseID("apps", "generator")}},
	}
	next := entryResolution{input: input, object: source}
	next.inheritEntry(&prior, false)
	assert.FatalIf(t, next.sync != prior.sync, "sync pair was lost during inheritance")
	assert.Equal(t, next.entry, true)
	assert.Equal(t, slices.Equal(next.producers, prior.producers), true)
}

func TestAdmission_SyncInheritanceRetainsProducerAuthority(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{LocalPath: t.TempDir()}
	source := &manifest.OCIRepository{Name: "sync", Namespace: "apps", URL: "oci://fixture.invalid/repo"}
	app := manifest.NewEntryKustomization(source.Named(), "./entry", manifest.NamedResource{})
	app.Name, app.Namespace = source.Name, source.Namespace
	prior := entryResolution{
		input: input, object: source, entry: true,
		sync:      &syncSource{source: source, application: app},
		producers: []occurrenceKey{{identity: testutil.HelmReleaseID("apps", "generator")}},
	}
	for _, tc := range []struct {
		producer string
		want     bool
	}{
		{producer: "generator", want: true},
		{producer: "replacement"},
	} {
		t.Run(tc.producer, func(t *testing.T) {
			t.Parallel()
			producers := []occurrenceKey{{identity: testutil.HelmReleaseID("apps", tc.producer)}}
			next := entryResolution{input: input, object: source, producers: slices.Clone(producers)}
			next.inheritEntry(&prior, false)
			assert.Equal(t, next.sync == prior.sync, tc.want)
			assert.Equal(t, next.entry, tc.want)
			assert.Equal(t, slices.Equal(next.producers, producers), true)
		})
	}
}

func TestAdmission_ClassificationReuseRequiresEquivalentSync(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{LocalPath: t.TempDir(), Provider: store.ProviderInput}
	source := &manifest.OCIRepository{Name: "sync", Namespace: "apps", URL: "oci://fixture.invalid/repo"}
	app := manifest.NewEntryKustomization(source.Named(), "./entry", manifest.NamedResource{})
	app.Name, app.Namespace = source.Name, source.Namespace
	pair := &syncSource{source: source, application: app}
	st := store.New()
	st.AddObject(source)
	a := &Admission{store: st, input: input, resolvedSources: &entrySourcePublisher{
		mutable: map[manifest.NamedResource]entryResolution{source.Named(): {
			object: source, input: input, artifact: input, provider: store.ProviderAlias, sync: pair, entry: true,
		}},
	}}
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "equivalent pair"},
		{name: "changed application", path: "./elsewhere"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			next := *pair
			want, material := pair, input
			if tc.path != "" {
				changed := app.Clone()
				changed.Path = tc.path
				next.application, want, material = changed, &next, nil
			}
			got := a.classifySource(
				source.Named(),
				&sourceRequest{object: source, scoped: true, sync: &next, entry: true},
			)
			assert.Equal(t, got.sync, want)
			assert.Equal(t, got.artifact, material)
		})
	}
}

func TestAdmission_PendingProjectionRequiresCheckoutEvidence(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{LocalPath: t.TempDir()}
	oci := &manifest.OCIRepository{Name: "apps", Namespace: "apps", URL: "oci://fixture.invalid/repo"}
	app := manifest.NewEntryKustomization(oci.Named(), "./entry", manifest.NamedResource{})
	app.Name, app.Namespace = oci.Name, oci.Namespace
	git := &manifest.GitRepository{Name: "sync", Namespace: "apps", URL: "https://fixture.invalid/repo"}
	a := &Admission{store: store.New(), input: input}
	for _, tc := range []struct {
		name   string
		object manifest.BaseManifest
		pair   *syncSource
		want   store.SourceProvider
		role   checkoutRole
	}{
		{name: "descriptor", object: git, want: store.ProviderAlias},
		{
			name: "admitted pair", object: oci, pair: &syncSource{source: oci, application: app},
			want: store.ProviderAlias, role: checkoutDefaultSync,
		},
		{
			name: "predicted pair", object: oci, pair: &syncSource{source: oci, application: app, predicted: true},
			want: store.ProviderAlias,
		},
		{name: "unsupported prediction", object: oci, want: store.ProviderGenerator},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := a.classifySource(tc.object.Named(), &sourceRequest{
				object: tc.object, sync: tc.pair, entry: true, scoped: true, projected: true,
			})
			assert.Equal(t, got.provider, tc.want)
			assert.Equal(t, got.entry, tc.want == store.ProviderAlias)
			assert.Equal(t, got.checkout, tc.role)
		})
	}
}

func TestAdmission_ConstructionViewReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"payload/file": {Data: []byte("initial")},
	}}
	d := newDiscoverer(
		&Config{Path: root, Input: input, Store: store.New(), SelfURLs: []string{"https://example.test/self"}},
	)
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	var previous fs.FS
	for _, content := range []string{"initial", "replacement"} {
		if content == "replacement" {
			replaced := *input
			replaced.Provider, replaced.Content = store.ProviderInput, content
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
			generatedSource := testGeneratedSync(t, parsed.Object).Objects[0]
			_, resolveErr := d.resolveEntrySource(t.Context(), d.admission(), application.Source, &entrySourceGroup{
				object: generatedSource, entry: true, projected: true,
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

func TestAdmission_ProjectedEntrySourceAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, path, url, ref, remote string
		local, crossNamespace, mismatched  bool
	}{
		{name: "Git entry and self", kind: manifest.KindGitRepository, path: "entry/config", url: "https://declared.invalid/source", ref: "refs/heads/main", remote: "https://declared.invalid/source", local: true},
		{name: "Git entry without self", kind: manifest.KindGitRepository, path: "entry/config", url: "https://declared.invalid/source", ref: "refs/heads/main", remote: "https://other.invalid/source", local: true},
		{name: "Git self without entry path", kind: manifest.KindGitRepository, path: "payload", url: "https://declared.invalid/source", ref: "refs/heads/main", remote: "https://declared.invalid/source", local: true},
		{name: "OCI default name", kind: manifest.KindOCIRepository, path: "entry/config", url: "oci://declared.invalid/source", ref: "latest", local: true},
		{name: "Bucket explicit cross-namespace reference", kind: manifest.KindBucket, path: "payload", url: "storage.invalid", ref: "my-bucket", local: true, crossNamespace: true},
		{name: "invalid OCI", kind: manifest.KindOCIRepository, path: "entry/config", url: "https://declared.invalid/source", ref: "latest"},
		{name: "invalid Bucket", kind: manifest.KindBucket, path: "entry/config", url: "storage.invalid", ref: "invalid/bucket"},
		{name: "mismatched pair with same source in another cluster", kind: manifest.KindOCIRepository, path: "entry/config", url: "oci://declared.invalid/source", ref: "latest", mismatched: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/config/kustomization.yaml", "resources: [flux.yaml, consumer.yaml]\n")
			sourceName, syncName := "entry", "    name: entry\n"
			if tc.name == "OCI default name" {
				sourceName, syncName = "apps", ""
			}
			instance := fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync:
%s    kind: %s
    url: %s
    ref: %s
    path: %s
`, syncName, tc.kind, tc.url, tc.ref, tc.path)
			if tc.mismatched {
				testutil.WriteFile(t, root, "other/flux.yaml", strings.ReplaceAll(instance, "entry/config", "other"))
				instance += `  kustomize:
    patches:
      - target: {kind: Kustomization, name: entry}
        patch: |
          - op: replace
            path: /spec/sourceRef/name
            value: other
`
			}
			testutil.WriteFile(t, root, "entry/config/flux.yaml", instance)
			namespace, referenceNamespace := "apps", ""
			if tc.crossNamespace {
				namespace, referenceNamespace = "workloads", ", namespace: apps"
			}
			testutil.WriteFile(t, root, "entry/config/consumer.yaml", testutil.KustomizationYAML("consumer", namespace,
				"  path: ./payload\n  sourceRef: {kind: "+tc.kind+", name: "+sourceName+referenceNamespace+"}\n"))

			testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
			testutil.WriteFile(t, root, "payload/file", "working tree")
			st := store.New()
			result, err := Run(t.Context(), testConfig(t, Config{
				Path: filepath.Join(root, "entry", "config"), RepoRoot: root, Store: st,
				SelfURLs: []string{tc.remote},
			}))
			assert.NoError(t, err)
			t.Cleanup(result.Close)
			id := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      tc.kind,
				Namespace: "apps",
				Name:      sourceName,
			}
			child, material, _, err := result.Admission.Resolve(t.Context(), id, nil, nil)
			assert.NoError(t, err)
			resolution := result.Admission.resolvedSources.snapshot()[id]
			assert.Equal(t, resolution.entry, tc.local)
			assert.Equal(t, resolution.provider == store.ProviderCommit, false)
			assert.Equal(t, resolution.synthetic, false)
			if tc.local {
				assert.FatalIf(
					t,
					material == nil || material.FS == nil,
					"projected entry source lost checkout authority",
				)
				assert.Equal(t, child.Obj, resolution.object)
				assert.Equal(t, result.Admission.Provider(id, nil, nil), store.ProviderAlias)
				assert.Equal(t, material.Provider, store.ProviderAlias)
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
			assertUnsupportedProjection(t, result.Admission, id, resolution.object)
		})
	}
}

func assertUnsupportedProjection(
	t *testing.T, admission *Admission, id manifest.NamedResource, declaration manifest.BaseManifest,
) {
	t.Helper()
	st := admission.store
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
	status, _ := st.GetStatus(id)
	assert.Equal(t, status.Status == store.StatusReady, false)
	assert.FatalIf(t, declaration == nil, "produced source lost its projected declaration")
	assert.Equal(t, st.GetDeclaration(id) == nil, true)
	warnings := admission.Warnings(st.Warnings())
	for i := range warnings {
		assert.Equal(t, warnings[i].Category == manifest.WarnBootstrapAliases, false)
	}
	for range 2 {
		child, artifact, _, resolveErr := admission.Resolve(
			t.Context(),
			id,
			declaration,
			aliasRequestReads(t, st, id),
		)
		assert.NoError(t, resolveErr)
		assert.Equal(t, child.Obj == declaration, true)
		assert.Equal(t, artifact == nil, true)
	}
	assert.Equal(t, manifest.Equal(admission.resolvedSources.snapshot()[id].object, declaration), true)
	published, err := admission.Publish(
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
}

func TestEntryPlanner_SyncPairScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                          string
		prior, reached, foreign, hidden, switchSource bool
		want                                          bool
	}{
		{name: "visible application on the selected mount", want: true},
		{name: "visible application on a foreign mount", foreign: true},
		{name: "hidden application on the selected mount", hidden: true},
		{name: "reached prior source", prior: true, reached: true, want: true},
		{name: "prior source outside selected scope", prior: true},
		{name: "replacement on the prior mount", prior: true, reached: true, switchSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			mount := &store.SourceArtifact{
				Provider: store.ProviderAlias, LocalPath: root, LocalRoot: root, FS: input.FS,
			}
			d := newDiscoverer(&Config{Store: store.New(), Input: input, Path: filepath.Join(root, "entry")})
			t.Cleanup(d.Close)
			source := &manifest.OCIRepository{Name: "sync", Namespace: "apps", URL: "oci://fixture.invalid/repo"}
			app := manifest.NewEntryKustomization(source.Named(), "./entry", manifest.NamedResource{})
			app.Name, app.Namespace = source.Name, source.Namespace
			declaration := selection.OccurrenceID{Source: root, File: "entry/instance.yaml"}
			pair := &syncSource{source: source, application: app, occurrence: declaration}
			selected := selection.OccurrenceFacts{
				Identity: source.Named(), Object: source, Declaration: declaration, Mounted: input,
			}
			if tc.prior {
				selected.Mounted = mount
				d.resolvedSources = &entrySourcePublisher{mutable: map[manifest.NamedResource]entryResolution{
					source.Named(): {
						object: source, input: input, artifact: mount, sync: pair, projected: true, entry: true,
						provider: store.ProviderAlias,
					},
				}}
			}
			if tc.switchSource {
				changed := *source
				selected.Object = &changed
			}
			scope := rootScope{occurrences: []selection.OccurrenceFacts{selected}}
			p := entryPlanner{
				session: d, inputs: map[string]*store.SourceArtifact{root: selected.Mounted},
				objects: make(map[occurrenceKey]store.Child),
			}
			if !tc.prior {
				application := selected
				application.Identity, application.Object = app.Named(), app
				if tc.foreign {
					application.Mounted = mount
				}
				scope.occurrences = append(scope.occurrences, application)
				key := occurrenceIdentity(&application)
				key.input = p.inputs[root]
				p.objects[key] = store.Child{Obj: app, Generated: !tc.hidden, VisibleGeneration: !tc.hidden}
			}
			child := store.Child{Obj: selected.Object, Generated: true, VisibleGeneration: true}
			got := p.scopedSyncSource(&selected, &child, &scope, tc.reached)
			assert.FatalIf(t, (got != nil) != tc.want, "sync pair present = %t, want %t", got != nil, tc.want)
			if tc.want {
				assert.Equal(t, *got, *pair)
			}
		})
	}
}

type emittedEntrySourceCase struct {
	name, path  string
	repoRoot    bool
	moved       bool
	defaultSync bool
	ownerDir    string
	want        store.SourceProvider
}

func TestAdmission_EmittedEntrySourceAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []emittedEntrySourceCase{
		{name: "entry", path: "entry", want: store.ProviderAlias},
		{name: "descendant without declaration", path: "entry", repoRoot: true, want: store.ProviderFetch},
		{name: "OCI descendant with declaration", path: "workload", ownerDir: "entry", repoRoot: true, want: store.ProviderAlias},
		{name: "OCI default sync", path: "entry", ownerDir: "entry", defaultSync: true, want: store.ProviderAlias},
		{name: "OCI application outside entry", path: "workload", ownerDir: "entry", want: store.ProviderAlias},
		{name: "OCI application without an origin", path: "workload/../../outside", ownerDir: "entry", want: store.ProviderFetch},
		{name: "OCI declaration outside entry", path: "workload", ownerDir: "foreign", want: store.ProviderFetch},
		{name: "unrelated", path: "unrelated", want: store.ProviderFetch},
		{name: "moved", path: "entry", moved: true, want: store.ProviderAlias},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEmittedEntrySourceAuthority(t, &tc)
		})
	}
}

func testEmittedEntrySourceAuthority(t *testing.T, tc *emittedEntrySourceCase) {
	t.Helper()
	root := t.TempDir()
	id := manifest.BootstrapSourceID
	bootstrap, _ := id.BindLocal(root)
	files := fstest.MapFS{"workload/resource.yaml": {Data: []byte("working tree bytes")}}
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files}
	st := store.New()
	st.AddObject(bootstrap)
	st.SetArtifact(id, input)
	a := &Admission{
		input: input, path: filepath.Join(root, "entry"), store: st,
		resolvedSources: &entrySourcePublisher{
			bootstrap: bootstrap,
			mutable: map[manifest.NamedResource]entryResolution{id: {
				object:    bootstrap,
				input:     input,
				artifact:  input,
				synthetic: true,
				provider:  store.ProviderInput,
			}},
		},
	}
	if tc.repoRoot {
		a.path = root
	}
	owner := &manifest.HelmRelease{Name: "generator", Namespace: "apps"}
	var source manifest.BaseManifest = &manifest.GitRepository{
		Name: id.Name, Namespace: id.Namespace, URL: "https://fixture.invalid/generated",
	}
	if tc.ownerDir == "" {
		st.DeleteObject(id)
	} else {
		id.Kind, id.Name = manifest.KindOCIRepository, "sync-source"
		if tc.defaultSync {
			id.Name = id.Namespace
		}
		source = &manifest.OCIRepository{
			Name:      id.Name,
			Namespace: id.Namespace,
			URL:       "oci://fixture.invalid/generated",
		}
		files[tc.ownerDir+"/release.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: generator, namespace: apps}
spec:
  interval: 1h
  chartRef: {kind: OCIRepository, name: chart}
`)}
		l := loader.New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{})
		t.Cleanup(l.Retire)
		_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
		a.loader = l
		evidence := l.RootEvidence(owner.Named(), nil)
		assert.Equal(t, evidence.Occurrence.Declaration.File, tc.ownerDir+"/release.yaml")
	}
	st.AddObject(owner)
	entry := manifest.NewEntryKustomization(id, tc.path, owner.Named())
	entry.Name, entry.Namespace = "sync", id.Namespace
	if tc.defaultSync {
		entry.Name = id.Name
	}
	output := &store.Emission{Complete: true, Children: []store.Child{
		{Obj: source, Generated: true, VisibleGeneration: true},
		{Obj: entry, Generated: true, VisibleGeneration: true},
	}}
	var support store.ReadSet
	support.Init(st, owner.Named(), nil)
	defer support.Retire()
	support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	support.Bind(output)
	st.RecordEmitted(owner.Named(), output, nil)

	var before store.ReadSet
	before.Init(st, id, nil)
	defer before.Retire()
	assert.Equal(t, before.GeneratedDocument(id), true)
	resolved := a.classifySource(id, &sourceRequest{object: source, reads: &before})
	assert.Equal(t, resolved.provider, tc.want)

	if tc.ownerDir == "entry" && tc.want == store.ProviderAlias {
		material, err := a.resolve(t.Context(), id, &sourceRequest{object: source, reads: &before})
		assert.NoError(t, err)
		assert.Equal(t, material.object, source)
		assert.Equal(t, material.provider, store.ProviderAlias)
		assert.FatalIf(t, material.artifact == nil, "emitted OCI source has no checkout")
		data, err := fs.ReadFile(material.artifact.FS, "workload/resource.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "working tree bytes")
		if tc.defaultSync {
			assert.FatalIf(t, material.sync == nil, "default sync evidence is absent")
			assert.Equal(t, material.sync.owner, owner.Named())
			testEmittedSyncAliasWarning(t, a, &material, output.Children)
			return
		}
	}

	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}},
	)
	assert.Equal(t, support.Valid(), false)
	var after store.ReadSet
	after.Init(st, id, nil)
	defer after.Retire()
	assert.Equal(t, after.GeneratedDocument(id), true)
	resolved = a.classifySource(id, &sourceRequest{object: source, reads: &after})
	assert.Equal(t, resolved.provider, tc.want)
	if tc.moved {
		moved := *entry
		moved.Path = "./unrelated"
		st.AddObject(&moved)
		assert.Equal(t, after.Valid(), false)
		var live store.ReadSet
		live.Init(st, id, nil)
		defer live.Retire()
		assert.Equal(t, live.GeneratedDocument(id), true)
		resolved = a.classifySource(id, &sourceRequest{object: source, reads: &live})
		assert.Equal(t, resolved.provider, store.ProviderFetch)
	}
	if tc.name == "entry" {
		assert.FatalIf(t, !resolved.sync.emitted(), "unscoped candidate lost emitted sync authority")
		scoped := entryResolution{object: source, input: input, projected: true, entryScoped: true}
		a.resolvedSources.record(id, &scoped)
		reclassified := a.classifySource(id, &sourceRequest{object: source, reads: &after})
		assert.Equal(t, reclassified.sync, (*syncSource)(nil))
		assert.Equal(t, reclassified.entry, false)
		assert.Equal(t, reclassified.provider.WorkingTree(), false)

		resolved.artifact = input
		a.resolvedSources.record(id, &resolved)
		st.RecordEmitted(owner.Named(), &store.Emission{Children: output.Children[:1]}, nil)
		partial := a.classifySource(id, &sourceRequest{object: source, entry: true})
		assert.Equal(t, partial.artifact == nil, true)
		assert.Equal(t, partial.provider, store.ProviderGenerator)
		assert.Equal(t, a.Provider(id, nil, nil), partial.provider)

		st.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
		withdrawn := a.classifySource(id, &sourceRequest{})
		assert.Equal(t, withdrawn.artifact == nil, true)
		assert.Equal(t, withdrawn.object == nil, true)
		assert.Equal(t, withdrawn.provider, store.ProviderAbsent)
		assert.Equal(t, a.Provider(id, nil, nil), withdrawn.provider)

		st.RecordEmitted(owner.Named(), &store.Emission{Complete: true, Children: output.Children}, nil)
		var pairReads store.ReadSet
		pairReads.Init(st, id, nil)
		defer pairReads.Retire()
		pair := a.classifySource(id, &sourceRequest{object: source, reads: &pairReads})
		assert.Equal(t, pair.provider, store.ProviderAlias)

		unrelated := &manifest.GitRepository{Name: "unrelated", Namespace: id.Namespace}
		children := append(slices.Clone(output.Children), store.Child{Obj: unrelated})
		st.RecordEmitted(owner.Named(), &store.Emission{Complete: true, Children: children}, nil)
		assert.Equal(t, st.GetDeclaration(unrelated.Named()), manifest.BaseManifest(unrelated))
		assert.Equal(t, pairReads.Valid(), true)

		st.RecordEmitted(owner.Named(), &store.Emission{Complete: true}, nil)
		assert.Equal(t, pairReads.Valid(), false)
	}
}

func testEmittedSyncAliasWarning(
	t *testing.T, a *Admission, accepted *entryResolution, children []store.Child,
) {
	t.Helper()
	id := accepted.object.Named()
	owner := accepted.sync.owner
	p := a.resolvedSources
	p.store, p.root = a.store, a.input.LocalPath
	p.freeze()
	p.publish(accepted, false)
	a.store.SetArtifact(id, accepted.artifact)
	other := id
	other.Name = "another-alias"
	p.record(other, &entryResolution{})
	a.store.SetArtifact(other, &store.SourceArtifact{Provider: store.ProviderAlias})
	assert.Equal(t, accepted.checkout, checkoutDefaultSync)
	assert.Equal(t, p.aliasWarning(), (*manifest.Warning)(nil))

	app := accepted.sync.application.(*manifest.Kustomization).Clone()
	app.Name = "named-sync"
	a.store.RecordEmitted(owner, &store.Emission{Complete: true, Children: []store.Child{
		children[0], {Obj: app, Generated: true, VisibleGeneration: true},
	}}, nil)
	want := []string{other.String(), id.String(), p.root}
	assertAliasWarningDetails(t, p.aliasWarning(), want)
	replaced := a.classifySource(id, &sourceRequest{object: accepted.object})
	assert.FatalIf(t, replaced.sync == nil, "replacement application was not admitted")
	assert.Equal(t, replaced.sync.application, manifest.BaseManifest(app))
	assert.Equal(t, replaced.checkout, checkoutAlias)

	a.store.RecordEmitted(owner, &store.Emission{Complete: true, Children: children}, nil)
	assert.Equal(t, p.aliasWarning(), (*manifest.Warning)(nil))
	replacement := *accepted.object.(*manifest.OCIRepository)
	replacement.URL = "oci://fixture.invalid/ordinary"
	replacedChildren := slices.Clone(children)
	replacedChildren[0].Obj = &replacement
	a.store.RecordEmitted(owner, &store.Emission{Complete: true, Children: replacedChildren}, nil)
	assert.Equal(t, a.store.GetDeclaration(id), manifest.BaseManifest(&replacement))
	assert.Equal(t, a.store.GetDeclaration(accepted.sync.application.Named()), accepted.sync.application)
	assertAliasWarningDetails(t, p.aliasWarning(), want)

	a.store.RecordEmitted(owner, &store.Emission{Complete: true}, nil)
	a.store.AddObject(&replacement)
	a.store.SetArtifact(id, accepted.artifact.WithReads(nil))
	assertAliasWarningDetails(t, p.aliasWarning(), want)
}

func assertAliasWarningDetails(t *testing.T, warning *manifest.Warning, want []string) {
	t.Helper()
	assert.FatalIf(t, warning == nil, "missing bootstrap alias warning, want %v", want)
	assert.Equal(t, warning.Category, manifest.WarnBootstrapAliases)
	assert.Equal(t, warning.Count, 1)
	assert.FatalIf(t, !slices.Equal(warning.Detail, want), "alias warning details = %v, want %v", warning.Detail, want)
}
