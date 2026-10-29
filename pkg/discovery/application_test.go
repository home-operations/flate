package discovery

import (
	"encoding/json"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestBindEntry_AuthoritativeReplacement(t *testing.T) {
	t.Parallel()
	for _, authority := range []string{"runtime", "index", "index over runtime"} {
		t.Run(authority, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/application-replacement"
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: []\n")}}}
			s := store.New()
			s.SetArtifact(manifest.BootstrapSourceID, input)
			old := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "."}
			current := *old
			current.Path = "./elsewhere"
			a := &Admission{input: input, path: root}
			a.AddApplication(s, old)
			if authority == "runtime" {
				s.AddObject(&current)
			} else {
				a.declarations = []store.Child{{Obj: &current}}
				if authority == "index over runtime" {
					s.AddObject(old)
				}
			}
			selected, binding, err := a.BindEntry(t.Context(), s)
			assert.NoError(t, err)
			if selected == nil || !selected.Named().ReportHidden() || binding.Producer == old.Named() {
				t.Fatalf("stale accepted evidence restored the replaced root: %+v", binding)
			}
		})
	}
}

func TestBindEntry_IndexOnlyProducer(t *testing.T) {
	t.Parallel()
	const root = "/virtual/indexed-producer"
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	doc := map[string]any{"apiVersion": manifest.FluxOperatorDomain + "/v1", "kind": "FluxInstance", "metadata": map[string]any{"name": "flux", "namespace": "control"}, "spec": map[string]any{"sync": map[string]any{"kind": "GitRepository", "name": "self", "url": "https://example.test/self", "ref": "refs/heads/main", "path": "."}}}
	obj, doc, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	s := store.New()
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "control", Name: "self"}
	s.SetArtifact(source, &store.SourceArtifact{LocalPath: root, InputAlias: true, FS: input.FS})
	a := &Admission{input: input, path: root, declarations: []store.Child{{Obj: obj, Doc: doc}}}
	selected, binding, err := a.BindEntry(t.Context(), s)
	assert.NoError(t, err)
	assert.Equal(t, selected, (*manifest.Kustomization)(nil))
	assert.Equal(t, binding, Binding{})
	assert.Equal(t, s.GetObject(obj.Named()), obj)
	_, actual, emission := s.GetEmission(obj.Named())
	defer emission.Finish()
	assert.Diff(t, actual, doc)
	assert.Equal(t, len(s.ListObjects(manifest.FluxKustomizeDomain, manifest.KindKustomization)), 0)
	assert.Equal(t, a.entryBound.Load(), false)
}

func TestBindEntry_CurrentDeclarationPrecedesEvidence(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"Kustomization", "FluxInstance"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, reverse := range []bool{false, true} {
				t.Run(map[bool]string{false: "history first", true: "history last"}[reverse], func(t *testing.T) {
					t.Parallel()
					const root = "/virtual/current-declaration"
					input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
					api, spec := manifest.FluxKustomizeDomain+"/v1", map[string]any{"path": ".", "interval": "42m"}
					if kind == "FluxInstance" {
						api = manifest.FluxOperatorDomain + "/v1"
						spec = map[string]any{"sync": map[string]any{"kind": "GitRepository", "name": "self", "url": "https://example.test/self", "ref": "refs/heads/main", "path": "."}, "distribution": map[string]any{"version": "2.x"}}
					}
					doc := map[string]any{"apiVersion": api, "kind": kind, "metadata": map[string]any{"name": "owner", "namespace": "apps"}, "spec": spec}
					current, doc, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
					assert.NoError(t, err)
					oldDoc := manifest.DeepCopyMap(doc)
					oldSpec := oldDoc["spec"].(map[string]any)
					if kind == "FluxInstance" {
						oldSpec["sync"].(map[string]any)["path"] = "./retired"
					} else {
						oldSpec["path"] = "./retired"
					}
					old, _, _, err := manifest.ParseDoc(oldDoc, manifest.ParseDocOptions{})
					assert.NoError(t, err)
					encoded, err := json.Marshal(oldDoc)
					assert.NoError(t, err)
					s := store.New()
					s.SetArtifact(manifest.BootstrapSourceID, input)
					s.SetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "self"}, &store.SourceArtifact{LocalPath: root, FS: input.FS, InputAlias: true})
					a := &Admission{input: input, path: root, declarations: []store.Child{{Obj: current, Doc: doc}}}
					add := []func(){func() { a.AddHistory(old.Named(), manifest.DiscoveryRecord{Application: string(encoded)}) }, func() { s.AddObject(old, oldDoc); a.AddApplication(s, old) }}
					if reverse {
						add[0], add[1] = add[1], add[0]
					}
					for _, apply := range add {
						apply()
					}
					selected, binding, err := a.BindEntry(t.Context(), s)
					assert.NoError(t, err)
					assert.Equal(t, s.GetObject(current.Named()), current)
					if kind == "Kustomization" {
						assert.Equal(t, a.selected.object, current)
						assert.Equal(t, selected.Named(), current.Named())
						assert.Equal(t, binding.Producer, current.Named())
					} else {
						assert.Equal(t, selected, (*manifest.Kustomization)(nil))
						assert.Equal(t, binding, Binding{})
					}
				})
			}
		})
	}
}

func TestBindEntry_AcceptedRootOrder(t *testing.T) {
	t.Parallel()
	var previous []manifest.Warning
	for _, order := range [][]string{{"z", "a"}, {"a", "z"}} {
		const path = "/virtual/accepted-root-order"
		s := store.New()
		input := &store.SourceArtifact{LocalPath: path, FS: fstest.MapFS{}}
		s.SetArtifact(manifest.BootstrapSourceID, input)
		source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://unreachable.invalid/self"}
		s.AddObject(source)
		for _, name := range order {
			owner := &manifest.Kustomization{Name: "owner-" + name, Namespace: "apps", Path: "./elsewhere"}
			s.AddObject(owner)
			root := &manifest.Kustomization{Name: name, Namespace: "apps", Path: ".", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name}}
			s.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: root}}, Complete: true}, nil)
		}
		a := &Admission{input: input, path: path}
		root, binding, err := a.BindEntry(t.Context(), s)
		assert.NoError(t, err)
		assert.Equal(t, root.Name, "a")
		assert.Equal(t, binding.Producer, root.Named())
		assert.Equal(t, binding.Source, source.Named())
		assert.Equal(t, a.selected.owner.Name, "owner-a")
		warnings := s.Warnings()
		assert.Equal(t, len(warnings), 1)
		assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
		assert.Equal(t, warnings[0].Count, 1)
		if previous != nil {
			assert.Diff(t, warnings, previous)
		}
		previous = warnings
	}
}

func TestResolve_UnrelatedApplicationAdmission(t *testing.T) {
	t.Parallel()
	const root = "/virtual/unrelated-application"
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"kustomization.yaml": {Data: []byte("resources: []\n")}}}
	s := store.New()
	source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://example.test/self"}
	application := &manifest.Kustomization{Name: "unrelated", Namespace: "apps", Path: "./leaf", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "self"}}
	s.AddObject(source)
	s.AddObject(application)
	a := &Admission{store: s, input: input, path: root, declarations: []store.Child{{Obj: application}}, remotes: map[string]struct{}{"example.test/self": {}}}
	var reads store.ReadSet
	reads.Init(s, source.Named(), nil)
	defer reads.Close()
	_, artifact, err := a.Resolve(t.Context(), &reads, ResolveRequest{ID: source.Named(), Object: source})
	assert.NoError(t, err)
	assert.Equal(t, artifact.InputAlias, true)
	assert.Equal(t, reads.Valid(), true)
	s.SetIncluded(application.Named(), false)
	assert.Equal(t, reads.Valid(), true)
	for changed := range s.Sweep() {
		if changed == source.Named() {
			t.Fatal("non-entry application admission invalidated the bootstrap source")
		}
	}
}
