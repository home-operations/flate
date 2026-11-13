package discovery

import (
	"encoding/json"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
		selected == nil || !selected.Named().ReportHidden(),
		"raw application did not propose its ordinary literal",
	)
	assert.Equal(t, manifest.DiagnosticFor(selected.Named(), selected).Resource, parsed.Object.Named())
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
			selected == nil || !selected.Named().ReportHidden(),
			"current raw application did not propose its ordinary literal",
		)
		assert.Equal(t, manifest.DiagnosticFor(selected.Named(), selected).Resource, parsed.Object.Named())
		assert.Equal(t, st.GetObject(selected.Named()), manifest.BaseManifest(nil))
		assert.Equal(t, st.GetArtifact(parsed.Object.Named(), store.ArtifactContent), store.Artifact(nil))
		_, scheduled := st.GetStatus(parsed.Object.Named())
		assert.Equal(t, scheduled, false)
	}
}
