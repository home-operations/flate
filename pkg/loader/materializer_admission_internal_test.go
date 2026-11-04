package loader

import (
	"encoding/json"
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ProjectedReaderNamespace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	input := &manifest.ConfigMap{Name: "settings", Namespace: "apps"}
	consumer := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: input.Name}},
		},
	}
	st.AddObject(consumer)
	owner := (&manifest.Kustomization{Name: "owner", Namespace: "apps"}).Named()
	l := New(st)
	l.SourceRoot = root
	l.Options.Changes = change.NewSet([]string{"consumer.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Filter = change.NewFilter()
	l.Index.Records[consumer.Named()] = manifest.DiscoveryRecord{File: "consumer.yaml"}
	l.Index.Records[input.Named()] = manifest.DiscoveryRecord{
		Productions: []manifest.Production{
			{Owner: owner, Source: manifest.BootstrapSourceID, Path: filepath.Join(root, "shared.yaml")},
		},
	}
	l.Index.Readers = map[string][]manifest.FileReader{}
	l.candidates = map[manifest.NamedResource]store.Child{}
	for _, namespace := range []string{"apps", "other"} {
		reader := &manifest.Kustomization{Name: namespace, Namespace: "flux-system", TargetNamespace: namespace}
		l.candidates[reader.Named()] = store.Child{Obj: reader}
		l.Index.Records[reader.Named()] = manifest.DiscoveryRecord{
			Parent: (&manifest.ResourceSet{Name: "templates", Namespace: "flux-system"}).Named(),
		}
		l.Index.Readers["shared.yaml"] = append(
			l.Index.Readers["shared.yaml"],
			manifest.FileReader{Reader: reader.Named(), Source: manifest.BootstrapSourceID},
		)
	}
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, input.Named()), true)
	for _, namespace := range []string{"apps", "other"} {
		reader := (&manifest.Kustomization{Name: namespace, Namespace: "flux-system"}).Named()
		assert.Equal(t, slices.Contains(proposal.Needed, reader), namespace == "apps")
	}
}

type countedProducer struct {
	*manifest.ResourceSet

	classifications atomic.Int64
}

func (p *countedProducer) SourcePath() (manifest.NamedResource, string, bool) {
	p.classifications.Add(1)
	return p.ResourceSet.SourcePath()
}

func TestAdmission_MaterializerClassificationReused(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer := &countedProducer{ResourceSet: &manifest.ResourceSet{Name: "producer", Namespace: "apps"}}
	st.AddObject(producer)
	l := New(st)
	l.Options.Filter = change.NewFilter()
	l.Index.Records[producer.Named()] = manifest.DiscoveryRecord{Builder: "generated"}
	l.candidates = map[manifest.NamedResource]store.Child{producer.Named(): {Obj: producer}}
	l.freezeConstruction()
	c := admissionClosure{loader: l, objects: st}
	assert.Equal(t, len(c.materializerCandidates(false)[0]), 1)
	updated := *producer.ResourceSet
	updated.Suspend = true
	st.AddObject(&updated)
	start := make(chan struct{})
	var workers sync.WaitGroup
	read := func(ordering bool) {
		<-start
		c := admissionClosure{loader: l, objects: st}
		candidates := c.materializerCandidates(ordering)
		assert.Equal(t, len(candidates[0]), 1)
		if ordering {
			assert.Equal(t, len(candidates[1]), 1)
			assert.Equal(t, candidates[1][0].facts.catalog.(*manifest.ResourceSet).Suspend, true)
		}
	}
	workers.Go(func() { read(false) })
	workers.Go(func() { read(true) })
	close(start)
	workers.Wait()
	assert.Equal(t, producer.classifications.Load(), int64(1))
}

func TestAdmission_MaterializerInventoryAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []applicationInventoryCase{{"unknown", "unknown", true, true}, {"known_owner", "known_owner", true, false}, {"bound_entry", "bound_entry", false, false}, {"static_positive", "static", false, false}, {"retired_static", "static", true, true}, {"complete_positive", "emission", false, false}, {"complete_negative", "emission", false, true}, {"partial_positive", "emission", true, true}, {"stale", "emission", true, true}, {"history_positive", "history", false, false}, {"history_negative", "history", false, true}, {"history_partial", "history", true, true}, {"foreign", "emission", false, true}} {
		t.Run("application/"+tc.name, func(t *testing.T) {
			t.Parallel()
			testApplicationInventoryCase(t, &tc)
		})
	}
	for _, scope := range []string{"named", "namespace"} {
		t.Run(scope, func(t *testing.T) { t.Parallel(); testAdmissionMaterializerInventoryAuthorityCase(t, &scope) })
	}
	t.Run(
		"namespace generated root",
		func(t *testing.T) { t.Parallel(); testAdmissionMaterializerInventoryAuthorityNamespaceGeneratedRoot(t) },
	)
}

func testAdmissionMaterializerInventoryAuthorityCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	for _, mode := range []string{"unknown", "complete", "incomplete", "stale", "history", "positive_history", "unknown_history", "prepared", "prepared_foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			runMaterializerMode(t, testCaseInput, mode)
		})
	}
}

func testAdmissionMaterializerInventoryAuthorityNamespaceGeneratedRoot(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"indexed.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: indexed, namespace: apps}\nspec: {path: ./payload}\n",
			),
		},
		"producer.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: producer, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: demo, sourceRef: {kind: GitRepository, name: charts}}}\n",
			),
		},
		"demo/Chart.yaml": {Data: []byte("apiVersion: v2\nname: demo\nversion: 0.1.0\n")},
		"demo/templates/generated.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: generated, namespace: apps}\nspec: {path: ./generated}\n",
			),
		},
	}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	charts := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(charts)
	st.SetArtifact(charts.Named(), input)
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps"},
		},
	}
	_, err := l.Load(t.Context(), root, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	producer := (&manifest.HelmRelease{Name: "producer", Namespace: "apps"}).Named()
	assert.Equal(t, slices.Contains(proposal.Needed, producer), true)
	assert.Equal(t, slices.Contains(proposal.Selected, producer), false)
}

func runMaterializerMode(t *testing.T, testCaseInput *string, mode string) {
	t.Helper()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./payload\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: missing}]}\n",
			),
		},
		"producer.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: producer, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: demo, sourceRef: {kind: GitRepository, name: charts}}}\n",
			),
		},
	}})
	input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	files := input.FS.(fstest.MapFS)
	files["demo/Chart.yaml"] = &fstest.MapFile{
		Data: []byte("apiVersion: v2\nname: demo\nversion: 0.1.0\n"),
	}
	files["demo/templates/cm.yaml"] = &fstest.MapFile{
		Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: missing, namespace: apps}\n"),
	}
	charts := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(charts)
	st.SetArtifact(charts.Named(), input)
	producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
	st.AddObject(producer)
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	consumer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	target := consumer
	if (*testCaseInput) == "namespace" {
		target.Name = ""
	}
	l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{target}}
	loadPath := root
	if mode == "prepared" || mode == "prepared_foreign" {
		input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		input.FS.(fstest.MapFS)["entry/consumer.yaml"] = input.FS.(fstest.MapFS)["consumer.yaml"]
		loadPath = filepath.Join(root, "entry")
	}
	_, err := l.Load(t.Context(), loadPath, nil)
	assert.NoError(t, err)
	if mode == "prepared" || mode == "prepared_foreign" {
		registerMaterializerPrepared(t, preparedMaterializerInput{root: root, mode: mode, store: st, loader: l})
	}
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	if mode == "complete" || mode == "stale" {
		_, _, next := st.GetEmission(producer.Named())
		next.Complete = true
		st.RecordEmitted(producer.Named(), &next, nil)
		next.Finish()
		st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	if mode == "incomplete" || mode == "stale" {
		if mode == "stale" {
			fresh := producer.Clone()
			fresh.Values = map[string]any{"changed": true}
			st.AddObject(fresh)
		}
		_, _, next := st.GetEmission(producer.Named())
		st.RecordEmitted(producer.Named(), &next, nil)
		next.Finish()
		if mode == "stale" {
			st.UpdateStatus(
				producer.Named(),
				store.StatusInfo{Status: store.StatusFailed, Message: "failed successor"},
			)
		}
	}
	history := map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{}
	if mode == "history" || mode == "positive_history" || mode == "unknown_history" {
		var records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		if mode != "unknown_history" {
			records = func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}
		}
		history[producer.Named()] = struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{Records: records, Complete: mode != "positive_history"}
	}
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, history)
	assert.NoError(t, err)
	assert.Equal(
		t,
		slices.Contains(proposal.Needed, producer.Named()),
		mode != "complete" && mode != "history" && mode != "prepared_foreign",
	)
	assert.Equal(t, slices.Contains(proposal.Selected, producer.Named()), false)
	assert.Equal(t, slices.Contains(proposal.Dirty, producer.Named()), false)
}

type preparedMaterializerInput struct {
	root, mode string
	store      *store.Store
	loader     *Loader
}

func registerMaterializerPrepared(t *testing.T, fixture preparedMaterializerInput) {
	t.Helper()
	input := fixture.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	reader := documentReader{}
	reader.setSources(map[string]*store.SourceArtifact{fixture.root: input})
	docs, parseFileErr := reader.parseFile(
		t.Context(),
		filepath.Join(fixture.root, "consumer.yaml"),
		manifest.ParseDocOptions{},
	)
	assert.NoError(t, parseFileErr)
	producers, parseFileErr := reader.parseFile(
		t.Context(),
		filepath.Join(fixture.root, "producer.yaml"),
		manifest.ParseDocOptions{},
	)
	assert.NoError(t, parseFileErr)
	docs = append(docs, producers...)
	source, builtRoot := manifest.BootstrapSourceID, fixture.root
	if fixture.mode == "prepared_foreign" {
		source = manifest.NamedResource{
			Group: manifest.SourceDomain,
			Kind:  manifest.KindGitRepository,
			Name:  "foreign",
		}
		builtRoot = t.TempDir()
		input := fixture.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		fixture.store.SetArtifact(source, &store.SourceArtifact{LocalPath: builtRoot, FS: input.FS})
	}
	fixture.loader.RegisterBuilt(
		BuiltInput{
			Root:   builtRoot,
			Source: source,
			Owner: manifest.NamedResource{
				Group: manifest.FluxKustomizeDomain,
				Kind:  manifest.KindKustomization,
				Name:  "entry",
			},
		},
		docs,
	)
}

func TestAdmission_ApplicationProducerEvidence(t *testing.T) {
	t.Parallel()
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%t", compact), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{
				LocalPath: root,
				FS: fstest.MapFS{"flux/owner.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: factory, namespace: apps}
spec: {path: ./application}
`)}, "application/kustomization.yaml": {Data: []byte("resources: [flux.yaml]\n")}, "application/flux.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync: {name: self, kind: GitRepository, url: https://fixture.invalid/repo, ref: main, path: ./flux}
`)}},
			}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
			st.AddObject(source)
			st.SetArtifact(source.Named(), input)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Selection = manifest.Selection{Changed: compact}
			_, err := l.Load(t.Context(), filepath.Join(root, "flux"), nil)
			assert.NoError(t, err)
			_, err = l.Load(t.Context(), filepath.Join(root, "application"), nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{}).
				Named()
			owner := manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Name:      "factory",
				Namespace: "apps",
			}
			application := manifest.NamedResource{
				Group:     manifest.FluxOperatorDomain,
				Kind:      "FluxInstance",
				Name:      "flux",
				Namespace: "apps",
			}
			assert.Equal(
				t,
				slices.ContainsFunc(
					l.Index.Records[entry].Productions,
					func(p manifest.Production) bool { return p.Owner == owner && p.Declaration == application },
				),
				true,
			)
			assert.Equal(
				t,
				slices.ContainsFunc(
					l.Index.Records[source.Named()].Productions,
					func(p manifest.Production) bool { return p.Declaration == application },
				),
				false,
			)
			closure := admissionClosure{loader: l, objects: st, proposal: &Admission{}}
			closure.materializers(
				&materializerDemand{
					target:      source.Named(),
					source:      root,
					application: filepath.Join(root, "flux"),
					required:    true,
				},
				manifest.NamedResource{},
				closure.materializerCandidates(false),
			)
			assert.Equal(t, closure.role(owner).needed, true)
		})
	}
	for _, namespace := range []string{"apps", ""} {
		t.Run("resourceset/namespace="+namespace, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				root := t.TempDir()
				st := store.New()
				input := &store.SourceArtifact{
					LocalPath: root,
					FS: fstest.MapFS{
						"application/kustomization.yaml": {Data: []byte("namespace: apps\nresources: [set.yaml]\n")},
						"application/set.yaml": {Data: fmt.Appendf(nil, `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: factory, namespace: %q}
spec:
  inputs: [{}]
  resourcesTemplate: |
    apiVersion: fluxcd.controlplane.io/v1
    kind: FluxInstance
    metadata: {name: flux}
    spec:
      sync: {name: self, kind: GitRepository, url: https://fixture.invalid/repo, ref: main, path: ./flux}
`, namespace)},
					},
				}
				st.SetArtifact(manifest.BootstrapSourceID, input)
				source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
				st.AddObject(source)
				st.SetArtifact(source.Named(), input)
				l := New(st)
				l.SourceRoot = root
				l.Options.Filter = change.NewFilter()
				l.Options.Selection = manifest.Selection{Changed: true}
				_, err := l.Load(t.Context(), filepath.Join(root, "application"), nil)
				assert.NoError(t, err)
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
				owner := manifest.NamedResource{
					Group:     manifest.FluxOperatorDomain,
					Kind:      manifest.KindResourceSet,
					Name:      "factory",
					Namespace: "apps",
				}
				record, indexed := l.Index.Lookup(owner)
				assert.Equal(t, indexed, true)
				assert.Equal(t, record.Authored.Namespace, namespace)
				closure := admissionClosure{loader: l, objects: st, proposal: &Admission{}}
				pending := closure.materializers(
					&materializerDemand{
						target:      source.Named(),
						source:      root,
						application: filepath.Join(root, "flux"),
						required:    true,
					},
					manifest.NamedResource{},
					closure.materializerCandidates(false),
				)
				assert.Equal(t, pending, true)
				assert.Equal(t, closure.role(owner).needed, true)
				owner.Namespace = ""
				assert.Equal(t, closure.role(owner).needed, false)
			}
		})
	}
}

type applicationInventoryCase struct {
	name, fixture          string
	wantProducer, wantPeer bool
}

func testApplicationInventoryCase(t *testing.T, tc *applicationInventoryCase) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{
		LocalPath: root,
		FS: fstest.MapFS{
			"entry/literal.yaml": {
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal}\n"),
			},
		},
	}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), input)
	producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
	other := &manifest.HelmRelease{Name: "other", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{producer, other})
	application := &manifest.RawObject{
		APIVersion: "fluxcd.controlplane.io/v1",
		Kind:       "FluxInstance",
		Namespace:  "apps",
		Name:       "flux",
		Spec: map[string]any{
			"sync": map[string]any{
				"name": source.Name,
				"kind": source.Named().Kind,
				"url":  "https://fixture.invalid/repo",
				"ref":  "main",
				"path": "./entry",
			},
		},
	}
	if tc.name == "foreign" {
		foreign := &manifest.GitRepository{Name: "foreign", Namespace: "apps"}
		st.AddObject(foreign)
		st.SetArtifact(foreign.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: input.FS})
		application.Spec["sync"].(map[string]any)["name"] = foreign.Name
	}
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	occurrence := compactOccurrence(producer, manifest.TemplateProjection{})
	occurrence.Declaration.Source = root
	l.locations = map[manifest.NamedResource]Occurrence{producer.Named(): occurrence}
	peer := compactOccurrence(other, manifest.TemplateProjection{})
	peer.Declaration.Source = root
	l.locations[other.Named()] = peer
	if tc.fixture == "bound_entry" {
		st.AddObject(manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", application.Named()))
	}
	if tc.fixture == "static" {
		fact := compactOccurrence(application, manifest.TemplateProjection{})
		fact.Declaration.Source = root
		l.locations[application.Named()] = fact
		l.Index.Records[application.Named()] = manifest.DiscoveryRecord{
			Authored: application.Named(),
			Path:     filepath.Join(root, "entry", "application.yaml"),
			File:     "entry/application.yaml",
		}
		l.roots = map[manifest.NamedResource]bool{application.Named(): true}
		current := application.Clone()
		if tc.name == "retired_static" {
			current.Spec = manifest.DeepCopyMap(application.Spec)
			current.Spec["sync"].(map[string]any)["path"] = "./elsewhere"
		}
		st.AddObject(current)
	}
	if tc.fixture == "known_owner" {
		entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).
			Named()
		l.Index.Records[entry] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: producer.Named()}},
		}
	}
	if tc.fixture == "emission" {
		children := []store.Child{{Obj: application}}
		if tc.name == "complete_negative" {
			children = nil
		}
		st.RecordEmitted(
			producer.Named(),
			&store.Emission{Children: children, Complete: tc.name != "partial_positive"},
			nil,
		)
		st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
		if tc.name == "stale" {
			fresh := producer.Clone()
			fresh.Values = map[string]any{"changed": true}
			st.AddObject(fresh)
			st.RecordEmitted(producer.Named(), &store.Emission{}, nil)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusFailed})
		}
	}
	closure := admissionClosure{loader: l, objects: st, proposal: &Admission{}}
	if tc.fixture == "history" {
		document := map[string]any{
			"apiVersion": application.APIVersion,
			"kind":       application.Kind,
			"metadata":   map[string]any{"name": application.Name, "namespace": application.Namespace},
			"spec":       application.Spec,
		}
		data, err := json.Marshal(document)
		assert.NoError(t, err)
		closure.history = map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{producer.Named(): {Complete: tc.name != "history_partial", Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
			if tc.name != "history_negative" {
				yield(application.Named(), manifest.DiscoveryRecord{Application: string(data)})
			}
		}}}
		closure.readHistory()
	}
	if st.GetArtifact(source.Named(), store.ArtifactContent) == nil {
		t.Fatal("application fixture requires physical source bytes")
	}
	closure.materializers(
		&materializerDemand{
			target:      source.Named(),
			source:      root,
			application: filepath.Join(root, "entry"),
			required:    true,
		},
		manifest.NamedResource{},
		closure.materializerCandidates(false),
	)
	assert.Equal(t, closure.role(producer.Named()).needed, tc.wantProducer)
	assert.Equal(t, closure.role(producer.Named()).selected, false)
	assert.Equal(t, closure.role(producer.Named()).dirty, false)
	assert.Equal(t, closure.role(other.Named()).needed, tc.wantPeer)
}
