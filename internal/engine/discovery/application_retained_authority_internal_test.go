package discovery

import (
	"cmp"
	"encoding/json"
	"io/fs"
	"maps"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/selection"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/sourceignore"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestBindEntry_AuthoritativeReplacement(t *testing.T) {
	t.Parallel()
	for _, authority := range []string{"runtime", "index", "index over runtime"} {
		t.Run(authority, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: []\n")}}
			input := &store.SourceArtifact{LocalPath: root, FS: files}
			st := store.New()
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			old := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "."}
			current := old.Clone()
			current.Path = "./elsewhere"
			a := &Admission{input: input, path: root}
			a.AddApplication(st, old)
			if authority == "runtime" {
				st.AddObject(current)
			} else {
				files["kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [owner.yaml]\n")}
				files["owner.yaml"] = &fstest.MapFile{
					Data: []byte(testutil.KustomizationYAML("root", "apps", "  path: ./elsewhere\n")),
				}
				l := loader.New(st)
				l.SourceRoot = root
				configureSelection(t, l, selection.Request{Changed: true})
				_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
				assert.NoError(t, err)
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
				a.loader, a.entryDemand = l, true
				t.Cleanup(l.Retire)
				if authority == "index over runtime" {
					st.AddObject(old)
				}
			}
			selected, binding, err := a.BindEntry(t.Context(), st)
			defer st.Retire(binding.Reads)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				selected == nil || !selected.Named().ReportHidden() || binding.Producer == old.Named(),
				"stale application restored replaced root: %+v",
				binding,
			)
		})
	}
}

func TestReadApplication_GeneratedRootCoverage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, scope, edge, component string
		object                             manifest.BaseManifest
		recipe, staleRecipe                bool
		external, generated, ignored       bool
		want, descendant                   bool
	}{
		{name: "covering root", path: "./apps", generated: true, want: true, descendant: true},
		{
			name: "covering non-Kustomization application", path: "./apps", generated: true, want: true, descendant: true,
			object: &manifest.RawObject{
				APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance",
				Name: "instance", Namespace: "apps",
				Spec: map[string]any{"sync": map[string]any{
					"kind": manifest.KindGitRepository, "name": "self", "path": "./apps",
					"url": "https://github.com/example/fleet.git", "ref": "refs/heads/main",
				}},
			},
		},
		{name: "contained generated root", path: "./apps", scope: ".", generated: true, want: true},
		{name: "contained authored root", path: "./apps", scope: "."},
		{name: "contained root from another source", path: "./apps", scope: ".", external: true, generated: true},
		{name: "generator-only package", path: "./apps", scope: "apps/media/generator", generated: true, want: true, descendant: true},
		{name: "component edge", path: "./apps", generated: true, edge: "components: [media]"},
		{
			name: "component resource edge", path: "./apps", generated: true, edge: "components: [media]",
			component: "resources: [app.yaml]\n", want: true, descendant: true,
		},
		{
			name: "effective recipe component", path: "./apps", generated: true, edge: "resources: []",
			component: "resources: [app.yaml]\n", recipe: true, want: true, descendant: true,
		},
		{
			name: "stale recipe component", path: "./apps", generated: true, edge: "resources: []",
			component: "resources: [app.yaml]\n", staleRecipe: true,
		},
		{name: "ignored resource edge", path: "./apps", generated: true, edge: "resources: [media/ignored.yaml]", ignored: true},
		{name: "moved root", path: "./retired", generated: true},
		{name: "component-prefix sibling", path: "./app", generated: true},
		{name: "different source", path: "./apps", external: true, generated: true},
		{name: "ordinary application", path: "./apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := generatedRootCoverageFiles(t, tc.edge, tc.component, tc.ignored)
			input := &store.SourceArtifact{LocalPath: root, FS: files}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
			st.AddObject(source)
			artifact := input
			if tc.external {
				artifact = &store.SourceArtifact{LocalPath: t.TempDir(), FS: files}
			}
			st.SetArtifact(source.Named(), artifact)
			l := loader.New(st)
			l.SourceRoot = root
			defer l.Close()
			scope := tc.scope
			if scope == "" {
				scope = "apps/media"
			}
			entry := filepath.Join(root, scope)
			_, err := l.Load(t.Context(), entry, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			a := &Admission{
				input: input, path: entry, store: st,
				qualification: &entryQualification{loader: l},
			}
			object := &manifest.Kustomization{Name: "sync", Namespace: "apps", Path: tc.path}
			object.SourceRef.Kind, object.SourceRef.Name = source.Named().Kind, source.Name
			if tc.recipe || tc.staleRecipe {
				stale := *object
				if tc.staleRecipe {
					stale.Components = []string{"./media"}
				}
				st.AddObject(&stale)
				if tc.recipe {
					object.Components = []string{"./media"}
				}
			}
			evidence := &applicationEvidence{
				object:            cmp.Or(tc.object, manifest.BaseManifest(object)),
				visibleGeneration: tc.generated,
			}
			var reads store.ReadSet
			reads.Init(st, evidence.object.Named(), nil)
			defer reads.Close()
			matched, err := a.readApplication(t.Context(), st, &reads, evidence)
			assert.NoError(t, err)
			assert.Equal(t, matched, tc.want)
			assert.Equal(t, evidence.descendant, tc.descendant)
		})
	}
}

func generatedRootCoverageFiles(t *testing.T, edge, component string, ignored bool) fstest.MapFS {
	t.Helper()
	files := fstest.MapFS{
		"apps/kustomization.yaml":       {Data: []byte("resources: [media]\n")},
		"apps/media/kustomization.yaml": {Data: []byte("resources: [app.yaml, generator]\n")},
		"apps/media/generator/kustomization.yaml": {Data: []byte(`configMapGenerator:
- name: generated
  literals: [MODE=enabled]
`)},
		"apps/media/app.yaml": {
			Data: []byte(testutil.KustomizationYAML("app", "apps", "  path: ./workload\n")),
		},
	}
	if edge != "" {
		files["apps/kustomization.yaml"] = &fstest.MapFile{Data: []byte(edge + "\n")}
	}
	if strings.HasPrefix(edge, "components:") || component != "" {
		files["apps/media/kustomization.yaml"].Data = []byte(
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n" + component,
		)
	}
	if ignored {
		const excluded = "apps/media/ignored.yaml"
		files[excluded] = files["apps/media/app.yaml"]
		files[".sourceignore"] = &fstest.MapFile{Data: []byte("/" + excluded + "\n")}
		policy, err := sourceignore.NewFS(files, []string{".sourceignore"}, "", false)
		assert.NoError(t, err)
		for name := range files {
			if policy.Match(name, false) {
				delete(files, name)
			}
		}
		_, present := files[excluded]
		assert.Equal(t, present, false)
	}
	return files
}

func TestBindEntry_IndexOnlyProducer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	document := retainedApplicationDocument("FluxInstance", ".")
	encoded, err := json.Marshal(document)
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"owner.yaml": {Data: encoded}}}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	defer l.Retire()
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "self",
	}
	st.SetArtifact(source, &store.SourceArtifact{LocalPath: root, InputAlias: true, FS: input.FS})
	a := &Admission{input: input, path: root, loader: l, entryDemand: true}
	selected, binding, err := a.BindEntry(t.Context(), st)
	defer st.Retire(binding.Reads)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		selected == nil || selected.Named().ReportHidden(),
		"indexed instance did not propose its public sync root",
	)
	generation := testGeneratedSync(t, parsed.Object)
	assert.Equal(t, manifest.Equal(selected, generation.Objects[1]), true)
	assert.Equal(t, st.GetObject(selected.Named()), manifest.BaseManifest(nil))
	assert.Equal(t, st.GetObject(parsed.Object.Named()), manifest.BaseManifest(nil))
	assert.Equal(t, len(a.EntryProducers()), 0)
	st.AddObject(parsed.Object, parsed.Document)
	assert.Equal(t, st.GetObject(parsed.Object.Named()), parsed.Object)
	_, actual, emission := st.GetEmission(parsed.Object.Named())
	defer emission.Finish()
	assert.Diff(t, actual, document)
	assert.Equal(t, len(st.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)), 0)
	assert.Equal(t, a.entryBound.Load(), false)
}

func TestBindEntry_IndexOnlyGeneratorPackage(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"index only", "planner frame", "accepted history"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testBindEntryIndexOnlyGeneratorPackage(t, mode)
		})
	}
}

func testBindEntryIndexOnlyGeneratorPackage(t *testing.T, mode string) {
	t.Helper()
	const syncPath = "apps"
	const entry = "apps/media/generator"
	root := t.TempDir()
	owner := retainedApplicationDocument("FluxInstance", syncPath)
	document, err := json.Marshal(owner)
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"owner.yaml":                    {Data: document},
		"apps/kustomization.yaml":       {Data: []byte("resources: [media]\n")},
		"apps/media/kustomization.yaml": {Data: []byte("resources: [generator]\n")},
		entry + "/kustomization.yaml": {Data: []byte(`configMapGenerator:
  - name: generator-only
    literals: [MODE=generator-only]
`)},
	}}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	t.Cleanup(l.Retire)
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))

	source := manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "self",
	}
	st.SetArtifact(source, &store.SourceArtifact{LocalPath: root, InputAlias: true, FS: input.FS})
	a := &Admission{input: input, path: filepath.Join(root, entry), loader: l, entryDemand: true}
	key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, entry, manifest.NamedResource{})
	var recorder manifest.WorkRecorder
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	if mode != "index only" {
		fixture := testReceiptTrees(t, t.TempDir(), persistent)
		cfg := Config{
			Input: input, Store: st, Loader: l, Trees: fixture.trees, Cache: fixture.owner,
			PrepareDocuments: base.PrepareDocuments, StrictSubstitutions: true, SkipKinds: []string{"Secret"},
		}
		session := newDiscoverer(&cfg)
		t.Cleanup(session.Close)
		frame, frameErr := session.prepareDocuments(t.Context(), key, input, nil)
		assert.NoError(t, frameErr)
		a.trees, a.packageRender = fixture.trees, cfg.packagePreparation()
		a.qualification = &entryQualification{loader: l, trees: a.trees}
		switch mode {
		case "planner frame":
			a.qualification.prepared = session.prepared
		case "accepted history":
			assert.NoError(t, fixture.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
				Owner: key.Named(), Receipt: frame.receipt, Content: frame.content,
				Source:  input.HistoryIdentity(manifest.BootstrapSourceID),
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
			}))
		}
		l.Options.Work = recorder.Scope()
	}

	selected, binding, err := a.BindEntry(t.Context(), st)
	defer st.Retire(binding.Reads)
	assert.NoError(t, err)
	assert.FatalIf(t, selected == nil, "%s", "no entry proposed")
	accepted, err := a.AcceptEntry(t.Context(), binding)
	assert.NoError(t, err)
	assert.Equal(t, accepted.Producer, testutil.KustomizationID("apps", "self"))
	assert.Equal(t, accepted.Source, source)
	assert.Equal(t, accepted.Path, syncPath)
	assert.Equal(t, accepted.Descendant, entry)
	assert.Equal(t, accepted.Key, key.Named())
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generator-only-f4856c7cm8"}
	_, indexed := l.Index.Lookup(id)
	assert.Equal(t, indexed, false)
	if mode == "index only" {
		return
	}
	assert.FatalIf(t, binding.documents == nil, "missing inventory for %s", mode)
	_, contained := (*binding.documents)[id]
	assert.Equal(t, contained, true)
	counts := make(map[manifest.WorkPhase]manifest.WorkCounts)
	records := recorder.Scope().Snapshot(0)
	for i := range records {
		record := &records[i]
		total := counts[record.Key.Phase]
		total.Add(&record.Counts)
		counts[record.Key.Phase] = total
	}
	if mode == "planner frame" {
		assert.Equal(t, counts[manifest.WorkKustomize].Attempts, uint64(0))
		assert.Equal(t, counts[manifest.WorkPreparation].OwnerPrepares, uint64(0))
		assert.Equal(t, counts[manifest.WorkCache].CacheService, uint64(0))
	} else if persistent {
		assert.Equal(t, counts[manifest.WorkKustomize].Attempts, uint64(0))
		assert.Equal(t, counts[manifest.WorkPreparation].OwnerPrepares, uint64(1))
		assert.Equal(t, counts[manifest.WorkCache].CacheService, uint64(1))
	}
}

func TestBindEntry_CurrentDeclarationPrecedesEvidence(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindKustomization, "FluxInstance"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, historyFirst := range []bool{true, false} {
				t.Run(map[bool]string{true: "history first", false: "history last"}[historyFirst], func(t *testing.T) {
					t.Parallel()
					testCurrentDeclarationPrecedesEvidence(t, kind, historyFirst)
				})
			}
		})
	}
}

func TestResolve_UnrelatedApplicationAdmission(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{
		LocalPath: root,
		FS:        fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: []\n")}},
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://example.test/self"}
	application := &manifest.Kustomization{Name: "unrelated", Namespace: "apps", Path: "./leaf"}
	st.AddObject(source)
	st.AddObject(application)
	a := &Admission{store: st, input: input, path: root, remotes: map[string]struct{}{"example.test/self": {}}}
	_, artifact, err := a.Resolve(t.Context(), source.Named(), source, nil)
	assert.NoError(t, err)
	assert.FatalIf(t, artifact == nil, "bootstrap source did not resolve")
	assert.Equal(t, artifact.InputAlias, true)
	st.SetArtifact(source.Named(), artifact)
	var reads store.ReadSet
	reads.Init(st, source.Named(), nil)
	defer reads.Close()
	reads.GetDeclaration(source.Named())
	reads.GetArtifact(source.Named(), store.ArtifactContent)
	reads.Accept(store.AssessmentReconciled)
	assert.Equal(t, reads.Valid(), true)
	st.SetIncluded(application.Named(), false, false)
	assert.Equal(t, reads.Valid(), true)
	for changed := range st.Sweep() {
		assert.ErrorIf(t, changed == source.Named(), "unrelated application admission invalidated bootstrap source")
	}
}

func TestBindEntry_TerminalBranchRemainsUnresolved(t *testing.T) {
	t.Parallel()
	for _, status := range []store.StatusInfo{
		{Status: store.StatusFailed, Message: "parent failed"},
		{Status: store.StatusReady, Reason: store.ReasonSuspended},
		{Status: store.StatusReady, Reason: store.ReasonSkipped},
	} {
		t.Run(string(status.Outcome()), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{
				LocalPath: root,
				FS:        fstest.MapFS{"opaque/kustomization.yaml": {Data: []byte("generators: [generator.yaml]\n")}},
			}
			st := store.New()
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./opaque"}
			st.AddObject(parent)
			st.AddObject(&manifest.Kustomization{Name: "z", Namespace: "apps", Path: "."})
			st.SetArtifact(manifest.BootstrapSourceID, input)
			st.UpdateStatus(parent.Named(), status)
			l := loader.New(st)
			l.SourceRoot = root
			a := &Admission{input: input, path: root, loader: l, entryDemand: true}
			selected, binding, err := a.BindEntry(t.Context(), st)
			st.Retire(binding.Reads)
			assert.NoError(t, err)
			assert.Equal(t, selected, (*manifest.Kustomization)(nil))
			assert.Equal(t, binding, Binding{})
			assert.Equal(t, len(a.EntryProducers()), 0)
			current, ok := st.GetStatus(parent.Named())
			assert.Equal(t, ok, true)
			assert.Equal(t, current.Outcome(), status.Outcome())
			assert.Equal(t, len(st.Warnings()), 0)
		})
	}
}

func retainedApplicationDocument(kind, path string) map[string]any {
	spec := map[string]any{"path": path, "interval": "42m"}
	api := manifest.FluxKustomizeDomain + "/v1"
	if kind == "FluxInstance" {
		api = manifest.FluxOperatorDomain + "/v1"
		spec = map[string]any{
			"sync": map[string]any{
				"kind": manifest.KindGitRepository,
				"name": "self",
				"url":  "https://example.test/self",
				"ref":  "refs/heads/main",
				"path": path,
			},
			"distribution": map[string]any{"version": "2.x"},
		}
	}
	return map[string]any{
		"apiVersion": api,
		"kind":       kind,
		"metadata":   map[string]any{"name": "owner", "namespace": "apps"},
		"spec":       spec,
	}
}

func TestApplications_RecapturesChangedQuery(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "./parent"}
	old := &manifest.Kustomization{Name: "selected", Namespace: "apps", Path: "./outside"}
	current := old.Clone()
	current.Path = "./entry"
	observed := &rebindingEntryFS{FS: fstest.MapFS{"outside/kustomization.yaml": {Data: []byte("resources: []\n")}}}
	input := &store.SourceArtifact{LocalPath: root, FS: observed}
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.AddObject(parent)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	st.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: old}}, Complete: true}, nil)
	observed.rebind = sync.OnceFunc(func() {
		observed.changed.Store(true)
		st.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: current}}, Complete: true}, nil)
	})
	a := &Admission{input: input, path: filepath.Join(root, "entry")}
	a.AddApplication(st, parent)
	selected, stale, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	st.Retire(stale.Reads)
	assert.Equal(t, observed.changed.Load(), true)
	assert.Equal(t, selected, (*manifest.Kustomization)(nil))
	selected, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	defer st.Retire(binding.Reads)
	assert.FatalIf(t, selected == nil, "changed query did not recapture current application")
	assert.Equal(t, selected.Named(), current.Named())
	assert.Equal(t, selected.Path, current.Path)
	assert.Equal(t, st.EmissionOwner(selected.Named()), parent.Named())
}

type rebindingEntryFS struct {
	fs.FS

	rebind  func()
	changed atomic.Bool
}

func (f *rebindingEntryFS) Open(name string) (fs.File, error) {
	if name == "outside/kustomization.yaml" && f.rebind != nil {
		f.rebind()
	}
	return f.FS.Open(name)
}

func TestBindEntry_AcceptedRootOrder(t *testing.T) {
	t.Parallel()
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{true: "direct root", false: "emitted roots"}[direct], func(t *testing.T) {
			t.Parallel()
			var previous []manifest.Warning
			for range 2 {
				for _, order := range [][2]string{{"z", "a"}, {"a", "z"}} {
					warnings := retainedAcceptedRootOrder(t, direct, order)
					if previous != nil {
						assert.Diff(t, warnings, previous)
					}
					previous = warnings
				}
			}
		})
	}
	t.Run("package priority", testBindEntryPackageRootOrder)
}

func testBindEntryPackageRootOrder(t *testing.T) {
	t.Parallel()
	for _, first := range []string{"a", "z"} {
		t.Run(first+" publishes first", func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte("resources: []\n")},
			}}
			st := store.New()
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			l := loader.New(st)
			l.SourceRoot = root
			t.Cleanup(l.Close)
			hinted := &manifest.ResourceSet{Name: "hinted", Namespace: "apps"}
			hinted.ResourcesTemplate = testutil.KustomizationYAML("z", "apps", "  path: ./entry\n")
			opaque := &manifest.HelmRelease{Name: "opaque", Namespace: "apps"}
			hintFacts := []selection.OccurrenceFacts{{Identity: hinted.Named(), Object: hinted}}
			opaqueFacts := []selection.OccurrenceFacts{{Identity: opaque.Named(), Object: opaque}}
			roots := []clusterRoot{
				{Path: "opaque", Occurrences: opaqueFacts, seeds: opaqueFacts},
				{Path: "hinted", Occurrences: hintFacts, seeds: hintFacts},
			}
			var inputs entryInputs
			inputs.collectEntryOperands(&Entry{aggregate: true, roots: []int{0, 1}}, roots, "entry")
			assert.FatalIf(t, len(inputs.packagesToQualify) != 2, "expected two package priority groups")
			assert.Equal(t, inputs.packagesToQualify[0][0].Identity, hinted.Named())
			assert.Equal(t, inputs.packagesToQualify[1][0].Identity, opaque.Named())
			q := &entryQualification{loader: l, packages: inputs.packagesToQualify}
			a := &Admission{
				input:         input,
				path:          filepath.Join(root, "entry"),
				loader:        l,
				qualification: q,
				entryDemand:   true,
			}
			selected, binding, err := a.BindEntry(t.Context(), st)
			assert.NoError(t, err)
			assert.Equal(t, selected, (*manifest.Kustomization)(nil))
			assert.Equal(t, binding, Binding{})
			assert.Diff(t, a.PrivateEntryOperands(), []manifest.NamedResource{hinted.Named(), opaque.Named()})

			owners := map[string]manifest.NamedResource{"a": opaque.Named(), "z": hinted.Named()}
			second := "a"
			if first == second {
				second = "z"
			}
			for index, name := range [2]string{first, second} {
				winner := &manifest.Kustomization{Name: name, Namespace: "apps", Path: "./entry"}
				st.RecordEmitted(owners[name], &store.Emission{
					Children: []store.Child{{Obj: winner, VisibleGeneration: true}}, Complete: true,
				}, nil)
				if index == 0 {
					incomplete, partial, bindErr := a.BindEntry(t.Context(), st)
					assert.NoError(t, bindErr)
					assert.Equal(t, incomplete, (*manifest.Kustomization)(nil))
					assert.Equal(t, partial, Binding{})
					assert.Equal(t, len(st.Warnings()), 0)
				}
			}
			selected, binding, err = a.BindEntry(t.Context(), st)
			assert.NoError(t, err)
			assert.FatalIf(t, selected == nil, "no root after both packages published")
			assert.Equal(t, selected.Named(), testutil.KustomizationID("apps", "a"))
			assert.Equal(t, binding.Producer, selected.Named())
			assert.Equal(t, st.EmissionOwner(selected.Named()), opaque.Named())
			acceptEntryForTest(t, a, &binding)
			warnings := st.Warnings()
			assert.FatalIf(t, len(warnings) != 1, "expected one producer warning: %+v", warnings)
			assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
			assert.Equal(t, warnings[0].Count, 1)
		})
	}
}

func retainedAcceptedRootOrder(t *testing.T, direct bool, order [2]string) []manifest.Warning {
	t.Helper()
	root := t.TempDir()
	input := &store.SourceArtifact{
		LocalPath: root,
		FS:        fstest.MapFS{"elsewhere/kustomization.yaml": {Data: []byte("generators: [opaque.yaml]\n")}},
	}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://unreachable.invalid/self"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), input)
	l := loader.New(st)
	l.SourceRoot = root
	a := &Admission{input: input, path: root, loader: l, entryDemand: true}
	for _, name := range order {
		st.AddObject(&manifest.Kustomization{Name: "owner-" + name, Namespace: "apps", Path: "./elsewhere"})
	}
	candidate := func(name string) *manifest.Kustomization {
		object := &manifest.Kustomization{Name: name, Namespace: "apps", Path: "."}
		object.SourceRef.Kind, object.SourceRef.Name = manifest.KindGitRepository, source.Name
		return object
	}
	if direct {
		st.AddObject(candidate("z"))
		selected, binding, err := a.BindEntry(t.Context(), st)
		st.Retire(binding.Reads)
		assert.NoError(t, err)
		assert.Equal(t, selected, (*manifest.Kustomization)(nil))
		assert.Equal(t, len(st.Warnings()), 0)
	}
	for i, name := range order {
		owner := testutil.KustomizationID("apps", "owner-"+name)
		st.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: candidate(name)}}, Complete: true}, nil)
		if i == 0 && name == "z" {
			selected, binding, err := a.BindEntry(t.Context(), st)
			st.Retire(binding.Reads)
			assert.NoError(t, err)
			assert.Equal(t, selected, (*manifest.Kustomization)(nil))
			assert.Equal(t, binding, Binding{})
			assert.Equal(t, len(st.Warnings()), 0)
		}
	}
	selected, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, selected == nil, "accepted root order did not bind")
	assert.Equal(t, selected.Named(), testutil.KustomizationID("apps", "a"))
	assert.Equal(t, binding.Producer, selected.Named())
	assert.Equal(t, binding.Source, source.Named())
	assert.Equal(t, st.EmissionOwner(selected.Named()), testutil.KustomizationID("apps", "owner-a"))
	assert.Equal(t, len(st.Warnings()), 0)
	acceptEntryForTest(t, a, &binding)
	warnings := st.Warnings()
	assert.FatalIf(t, len(warnings) != 1, "accepted conflict warning missing: %+v", warnings)
	assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
	assert.Equal(t, warnings[0].Count, 1)
	return warnings
}

func testCurrentDeclarationPrecedesEvidence(t *testing.T, kind string, historyFirst bool) {
	t.Helper()
	root := t.TempDir()
	document := retainedApplicationDocument(kind, ".")
	encoded, err := json.Marshal(document)
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"owner.yaml": {Data: encoded}}}
	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
	st.AddObject(bootstrap)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "self",
	}
	st.SetArtifact(source, &store.SourceArtifact{LocalPath: root, InputAlias: true, FS: input.FS})
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	defer l.Retire()
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	oldDocument := retainedApplicationDocument(kind, "./retired")
	old, err := manifest.ParseDoc(oldDocument, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	history, err := json.Marshal(oldDocument)
	assert.NoError(t, err)
	a := &Admission{input: input, path: root, loader: l, entryDemand: true}
	addHistory := func() { a.AddHistory(st, old.Object.Named(), manifest.DiscoveryRecord{Application: string(history)}) }
	if historyFirst {
		addHistory()
	}
	st.AddObject(old.Object, old.Document)
	a.AddApplication(st, old.Object)
	if !historyFirst {
		addHistory()
	}
	selected, binding, err := a.BindEntry(t.Context(), st)
	defer st.Retire(binding.Reads)
	assert.NoError(t, err)
	assert.Equal(t, st.GetObject(parsed.Object.Named()), old.Object)
	st.AddObject(parsed.Object, parsed.Document)
	assert.Equal(t, st.GetObject(parsed.Object.Named()), parsed.Object)
	_, actual, emission := st.GetEmission(parsed.Object.Named())
	emission.Finish()
	assert.Diff(t, actual, document)
	if kind == manifest.KindKustomization {
		assert.FatalIf(t, selected == nil, "current indexed declaration lost its entry")
		assert.Equal(t, selected.Named(), parsed.Object.Named())
		assert.Equal(t, manifest.Equal(selected, parsed.Object), true)
		assert.Equal(t, binding.Producer, parsed.Object.Named())
	} else {
		assert.FatalIf(
			t,
			selected == nil || selected.Named().ReportHidden(),
			"current instance did not propose its public sync root",
		)
		generation := testGeneratedSync(t, parsed.Object)
		assert.Equal(t, manifest.Equal(selected, generation.Objects[1]), true)
		assert.Equal(t, st.GetObject(selected.Named()), manifest.BaseManifest(nil))
		assert.Equal(t, st.GetArtifact(parsed.Object.Named(), store.ArtifactContent), store.Artifact(nil))
		_, scheduled := st.GetStatus(parsed.Object.Named())
		assert.Equal(t, scheduled, false)
	}
}

func TestBindEntry_PackageOperandTerminalFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: []\n")},
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	t.Cleanup(l.Close)
	parent := &manifest.Kustomization{Name: "opaque-package", Namespace: "apps", Path: "elsewhere"}
	st.AddObject(parent)
	st.UpdateFailure(parent.Named(), manifest.NewError(manifest.ErrInput, nil, "package cannot render"))
	q := &entryQualification{loader: l, active: []manifest.NamedResource{parent.Named()}}
	a := &Admission{input: input, path: root, loader: l, qualification: q, entryDemand: true}

	selected, binding, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.Equal(t, selected, (*manifest.Kustomization)(nil))
	assert.Equal(t, binding, Binding{})
	assert.Diff(t, a.EntryProducers(), []manifest.NamedResource{parent.Named()})
	assert.Equal(t, q.pending, true)
	assert.Equal(t, len(a.PrivateEntryOperands()), 0)
}
