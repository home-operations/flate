package store

import (
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func projectionSource(t *testing.T, s *Store, name string) manifest.NamedResource {
	t.Helper()
	source := &manifest.GitRepository{Name: name, Namespace: "apps"}
	s.AddObject(source)
	s.SetArtifact(source.Named(), &SourceArtifact{LocalPath: "/" + name, Key: name, FS: fstest.MapFS{}})
	return source.Named()
}

func projectionOwner(s *Store, name string, source manifest.NamedResource) *manifest.Kustomization {
	owner := &manifest.Kustomization{
		Name:      name,
		Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      source.Kind,
			Name:      source.Name,
			Namespace: source.Namespace,
		},
	}
	s.AddObject(owner)
	return owner
}

func TestStore_IncomingProductionsIdentity(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"inherited_namespace", "explicit_namespace", "group", "source", "contributor", "unknown_evidence", "producer_file", "derived_raw_file", "own_file", "own_no_path"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testStoreIncomingProductionsIdentityCase(t, &mode)
		},
		)
	}
}

func TestStore_IncomingProductionsIgnoresStoredChild(t *testing.T) {
	t.Parallel()
	s := New()
	source := projectionSource(t, s, "source")
	owner := projectionOwner(s, "owner", source)
	stored := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "stale"}}
	s.AddObject(stored)
	proof := manifest.Production{
		Owner:    owner.Named(),
		Source:   source,
		Authored: stored.Named(),
		Path:     "/source/stale.yaml",
		File:     "stale.yaml",
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		stored.Named(): {
			Authored:    stored.Named(),
			Path:        proof.Path,
			File:        proof.File,
			Productions: []manifest.Production{proof},
		},
	}}
	s.SetDiscoveryIndex(index)
	for _, mode := range []string{"unknown_evidence", "own_evidence"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			incoming := Child{
				Obj: &manifest.ConfigMap{
					Name:      stored.Name,
					Namespace: stored.Namespace,
					Data:      map[string]any{"value": "incoming"},
				},
			}
			if mode == "own_evidence" {
				incoming.Productions = []manifest.Production{
					{Owner: owner.Named(), Source: source, File: "incoming.yaml"},
				}
			}
			assert.Diff(t, s.IncomingProductions(incoming, index), incoming.Productions)
			assert.Equal(t, s.GetObject(stored.Named()), manifest.BaseManifest(stored))
		})
	}
}

func TestStore_IncomingProductionsSuppressesRawBridge(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"shadowed", "hidden"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := projectionSource(t, s, "source")
			renderer := projectionOwner(s, "renderer", source)
			fileOwner := projectionOwner(s, "file-owner", source)
			ancestor := projectionOwner(s, "ancestor", source)
			incoming := Child{
				Obj:         &manifest.ConfigMap{Name: "input", Namespace: "apps"},
				Productions: []manifest.Production{{Owner: renderer.Named(), Source: source}},
			}
			bare := incoming.Obj.Named()
			bare.Namespace = ""
			proof := manifest.Production{
				Owner:    fileOwner.Named(),
				Source:   source,
				Authored: bare,
				Path:     "/source/input.yaml",
				File:     "input.yaml",
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				bare: {
					Authored:    bare,
					Path:        proof.Path,
					File:        proof.File,
					Productions: []manifest.Production{proof},
				},
				ownerID("marker"): {Productions: []manifest.Production{{Owner: renderer.Named(), Source: source}}},
			}}
			if mode == "hidden" {
				index.Records[fileOwner.Named()] = manifest.DiscoveryRecord{
					Productions: []manifest.Production{{Owner: ancestor.Named(), Source: source}},
				}
			}
			s.SetDiscoveryIndex(index)
			want := append(slices.Clone(incoming.Productions), proof)
			slices.SortFunc(want, compareProduction)
			assert.Diff(t, s.IncomingProductions(incoming, index), want)
			owner := fileOwner
			if mode == "hidden" {
				owner = ancestor
			}
			_, _, attempt := s.GetEmission(owner.Named())
			defer attempt.Finish()
			attempt.Complete = true
			s.RecordEmitted(owner.Named(), &attempt, nil)
			assert.Diff(t, s.IncomingProductions(incoming, index), incoming.Productions)
			incoming.Productions = []manifest.Production{proof}
			assert.Diff(t, s.IncomingProductions(incoming, index), incoming.Productions)
		})
	}
}

func TestIncomingProductions_RegisteredEvidence(t *testing.T) {
	t.Parallel()
	for _, mutation := range []string{"publication", "source", "contributor"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			testIncomingProductionsRegisteredEvidenceCase(
				t,
				&mutation,
			)
		},
		)
	}
	for _, mode := range []string{"first", "strengthen", "content_only"} {
		t.Run("completed/"+mode, func(t *testing.T) {
			t.Parallel()
			testIncomingProductionsRegisteredEvidenceCase2(
				t,
				&mode,
			)
		},
		)
	}
}

func TestChildFootprint_MatchesEmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"index", "superseded", "foreign", "previous", "retained", "empty", "static", "entry", "application-kustomization", "application-fluxinstance"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testChildFootprintMatchesEmissionCase(t, &mode)
		},
		)
	}
}

func testStoreIncomingProductionsIdentityCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	source := projectionSource(t, s, "source")
	other := projectionSource(t, s, "other-source")
	rendererSource := source
	if (*testCaseInput) == "contributor" {
		rendererSource = other
	}
	renderer := projectionOwner(s, "renderer", rendererSource)
	fileOwner := projectionOwner(s, "file-owner", source)
	incoming := Child{Obj: &manifest.ConfigMap{Name: "input", Namespace: "apps"}}
	bare := incoming.Obj.Named()
	bare.Namespace = ""
	if (*testCaseInput) == "group" {
		bare.Group = "foreign.example"
	}
	if (*testCaseInput) == "explicit_namespace" {
		bare.Namespace = "other"
	}
	proof := manifest.Production{
		Owner:    fileOwner.Named(),
		Source:   source,
		Authored: bare,
		Path:     "/source/input.yaml",
		File:     "input.yaml",
	}
	if (*testCaseInput) == "source" {
		proof.Source = other
	}
	if (*testCaseInput) == "derived_raw_file" {
		proof.Authored = renderer.Named()
		proof.Authored.Namespace = ""
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {
			Authored:    bare,
			Path:        proof.Path,
			File:        proof.File,
			Productions: []manifest.Production{proof},
		},
		ownerID("marker"): {
			Productions: []manifest.Production{{Owner: renderer.Named(), Source: rendererSource}},
		},
	}}
	s.SetDiscoveryIndex(index)
	incoming.Productions = []manifest.Production{{Owner: renderer.Named(), Source: source}}
	if (*testCaseInput) == "producer_file" || (*testCaseInput) == "own_file" ||
		(*testCaseInput) == "own_no_path" {
		incoming.Productions[0].Path = "/source/producer.yaml"
		incoming.Productions[0].File = "producer.yaml"
		incoming.Productions[0].Authored = renderer.Named()
		if (*testCaseInput) == "own_file" || (*testCaseInput) == "own_no_path" {
			incoming.Productions[0].Authored = bare
		}
		if (*testCaseInput) == "own_no_path" {
			incoming.Productions[0].Path = ""
		}
	}
	if (*testCaseInput) == "unknown_evidence" {
		incoming.Productions = nil
	}
	want := slices.Clone(incoming.Productions)
	if (*testCaseInput) == "inherited_namespace" || (*testCaseInput) == "producer_file" ||
		(*testCaseInput) == "own_no_path" {
		want = append(want, proof)
	}
	slices.SortFunc(want, compareProduction)
	assert.Diff(t, s.IncomingProductions(incoming, index), want)
}

func testIncomingProductionsRegisteredEvidenceCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	source := projectionSource(t, s, "selected")
	producer := projectionOwner(s, "producer", source)
	fileOwner := projectionOwner(s, "raw-owner", source)
	stale := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "stale"}}
	s.AddObject(stale)
	raw := stale.Named()
	raw.Namespace = ""
	proof := manifest.Production{
		Owner:    fileOwner.Named(),
		Source:   source,
		Authored: raw,
		File:     "input.yaml",
		Path:     "/selected/input.yaml",
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		raw:              {Productions: []manifest.Production{proof}},
		producer.Named(): {Productions: []manifest.Production{{Owner: producer.Named(), Source: source}}},
	}}
	s.SetDiscoveryIndex(index)
	incoming := Child{
		Obj: &manifest.ConfigMap{
			Name:      stale.Name,
			Namespace: stale.Namespace,
			Data:      map[string]any{"value": "incoming"},
		},
		Productions: []manifest.Production{{Owner: producer.Named(), Source: source}},
	}
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	defer reads.Retire()
	want := append(slices.Clone(incoming.Productions), proof)
	slices.SortFunc(want, compareProduction)
	assert.Diff(t, reads.IncomingProductions(incoming, index), want)
	assert.Equal(t, reads.Valid(), true)
	switch *testCaseInput {
	case "publication":
		next := &Emission{Children: []Child{incoming}, Complete: true}
		reads.Bind(next)
		s.RecordEmitted(producer.Named(), next, nil)
		assert.Equal(t, reads.Valid(), true)
		assert.Equal(t, s.GetObject(stale.Named()), incoming.Obj)
	case "source":
		s.SetArtifact(source, &SourceArtifact{Key: "replacement", LocalPath: "/selected", FS: fstest.MapFS{}})
		assert.Equal(t, reads.Valid(), false)
	case "contributor":
		s.RecordEmitted(fileOwner.Named(), &Emission{Complete: true}, nil)
		assert.Equal(t, reads.Valid(), false)
	}
}

func testIncomingProductionsRegisteredEvidenceCase2(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	source := projectionSource(t, s, "selected")
	producer := projectionOwner(s, "producer", source)
	contributor := projectionOwner(s, "contributor", source)
	child := emissionChild("input", "same")
	payload := &SourceArtifact{Content: "stable", FS: fstest.MapFS{}}
	raw := child.Obj.Named()
	raw.Namespace = ""
	proof := manifest.Production{
		Owner:    contributor.Named(),
		Source:   source,
		Authored: raw,
		File:     "input.yaml",
		Path:     "/selected/input.yaml",
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		raw:                 {Productions: []manifest.Production{proof}},
		contributor.Named(): {Productions: []manifest.Production{{Owner: contributor.Named(), Source: source}}},
	}}
	s.SetDiscoveryIndex(index)
	publish := func(fingerprint string) {
		s.RecordEmitted(
			contributor.Named(),
			&Emission{Children: []Child{child}, Complete: true, Fingerprint: fingerprint},
			map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): payload},
		)
	}
	publish("first")
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	defer reads.Close()
	if (*testCaseInput) != "first" {
		assert.Equal(t, reads.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(payload))
	}
	if (*testCaseInput) != "content_only" {
		incoming := Child{
			Obj:         child.Obj,
			Productions: []manifest.Production{{Owner: producer.Named(), Source: source}},
		}
		assert.Diff(t, reads.IncomingProductions(incoming, index), incoming.Productions)
	}
	assert.Equal(t, reads.Valid(), true)
	publish("second")
	changed := slices.Collect(s.Sweep())
	if (*testCaseInput) == "content_only" {
		assert.Equal(t, len(changed), 0)
	} else {
		assert.Diff(t, changed, []manifest.NamedResource{reads.ID()})
	}
	assert.Equal(t, reads.Valid(), (*testCaseInput) == "content_only")
}

func testChildFootprintMatchesEmissionCase(t *testing.T, testCaseInput *string) {
	t.Helper()
	s := New()
	source := projectionSource(t, s, "content")
	owner := projectionOwner(s, "owner", source)
	if (*testCaseInput) == "entry" {
		owner = manifest.NewEntryKustomization(source, ".", manifest.NamedResource{})
		s.AddObject(owner)
	}
	generator := Child{
		Obj: &manifest.RawObject{
			APIVersion: "source.extensions.fluxcd.io/v1beta1",
			Kind:       "ArtifactGenerator",
			Name:       "generator",
			Namespace:  "apps",
			Spec: map[string]any{
				"pathPattern": "@repo/{app}",
				"artifacts":   []any{map[string]any{"name": "app-{app}"}},
			},
		},
	}
	consumer := Child{
		Obj: &manifest.Kustomization{
			Name:      "consumer",
			Namespace: "apps",
			SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: "app-api"},
		},
	}
	consumer.Obj.(*manifest.Kustomization).PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "settings"}},
	}
	settings := (&manifest.ConfigMap{Name: "settings", Namespace: "apps"}).Named()
	target := consumer.Obj.(*manifest.Kustomization).SourceID()
	production := manifest.Production{
		Owner:  owner.Named(),
		Source: source,
		File:   "consumer.yaml",
		Path:   "/content/consumer.yaml",
	}
	decl := manifest.Production{Owner: owner.Named(), Declaration: generator.Obj.Named(), Source: source}
	if (*testCaseInput) == "foreign" {
		production.Source.Name, decl.Source.Name = "other", "other"
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		settings: {Productions: []manifest.Production{{Source: source, File: "settings.yaml"}}},
		consumer.Obj.Named(): {
			File:        production.File,
			Path:        production.Path,
			Refs:        []manifest.NamedResource{target},
			Productions: []manifest.Production{production},
		},
		target: {Productions: []manifest.Production{decl}},
	}}
	if (*testCaseInput) == "static" || (*testCaseInput) == "entry" {
		index.Records[owner.Named()] = manifest.DiscoveryRecord{StaticOutput: true}
	}
	recordProjectionHistory(s, owner.Named(), []Child{generator, consumer}, *testCaseInput)
	children := []Child{generator}
	application := (*testCaseInput) == "application-kustomization" || (*testCaseInput) == "application-fluxinstance"
	if application {
		children = append(children, projectionApplicationChild(t, source, *testCaseInput))
	}
	if (*testCaseInput) == "superseded" || (*testCaseInput) == "retained" {
		replacement := *consumer.Obj.(*manifest.Kustomization)
		replacement.SourceRef.Name = "app-web"
		children = append(children, Child{Obj: &replacement})
	}
	if (*testCaseInput) == "empty" {
		children = nil
	}
	prior := s.GetArtifact(owner.Named(), ArtifactContent)
	prospective := s.ChildFootprint(owner.Named(), children, index)
	sources := s.ChildSourcePaths(owner.Named(), children, index)
	if prospective == nil {
		t.Fatal("complete batch supplied unavailable history")
	}
	before := maps.Collect(prospective)
	_, predicted := before[target]
	assert.Equal(
		t,
		predicted,
		slices.Contains([]string{"index", "previous", "retained", "static", "entry"}, *testCaseInput) || application,
	)
	assert.Equal(t, s.GetArtifact(owner.Named(), ArtifactContent), prior)
	assert.Equal(t, s.index.Load() == nil, true)
	assert.Diff(t, index.Records[target].Productions, []manifest.Production{decl})
	assert.Diff(t, index.Records[consumer.Obj.Named()].Refs, []manifest.NamedResource{target})
	s.SetDiscoveryIndex(index)
	_, _, next := s.GetEmission(owner.Named())
	defer next.Finish()
	var reads ReadSet
	reads.Init(s, owner.Named(), nil)
	defer reads.Retire()
	reads.GetArtifact(source, ArtifactContent)
	next.Children, next.Complete, next.Reads = children, true, &reads
	s.RecordEmitted(owner.Named(), &next, nil)
	if next.Footprint() == nil {
		t.Fatal("real emission was not accepted")
	}
	assert.Diff(t, maps.Collect(next.Footprint()), before)
	if application {
		id := children[1].Obj.Named()
		if before[id].Application == "" {
			t.Fatal("application document missing from prospective footprint")
		}
		assert.Equal(t, maps.Collect(next.Footprint())[id].Application, before[id].Application)
	}
	if (*testCaseInput) == "static" {
		assert.Equal(t, next.SourcePaths() == nil, true)
	} else {
		assert.Diff(t, next.SourcePaths(), sources)
	}
	input := manifest.SourceEvidence{
		Source: s.GetArtifact(source, ArtifactContent).(*SourceArtifact).HistoryIdentity(source),
		Path:   "settings.yaml",
	}
	assert.Equal(t, slices.Contains(sources, input), (*testCaseInput) == "retained" || (*testCaseInput) == "superseded")
	if application {
		id := children[1].Obj.Named()
		replacement := s.ChildFootprint(owner.Named(), []Child{generator}, index)
		_, retained := maps.Collect(replacement)[id]
		assert.Equal(t, retained, false)
		next.Children = []Child{generator}
		s.RecordEmitted(owner.Named(), &next, nil)
		_, retained = maps.Collect(next.Footprint())[id]
		assert.Equal(t, retained, false)
	}
}

func recordProjectionHistory(s *Store, owner manifest.NamedResource, children []Child, mode string) {
	if mode == "previous" || mode == "retained" {
		_, _, prior := s.GetEmission(owner)
		prior.Children, prior.Complete = children, true
		if mode == "retained" {
			retained := *children[1].Obj.(*manifest.Kustomization)
			retained.Name = "retained"
			prior.Children = append(prior.Children, Child{Obj: &retained})
		}
		s.RecordEmitted(owner, &prior, nil)
		prior.Finish()
	}
}

func projectionApplicationChild(t *testing.T, source manifest.NamedResource, mode string) Child {
	t.Helper()
	document := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "application", "namespace": "apps"},
		"spec": map[string]any{
			"path":      "./entry",
			"sourceRef": map[string]any{"kind": source.Kind, "name": source.Name},
		},
	}
	if mode == "application-fluxinstance" {
		document["apiVersion"], document["kind"] = "fluxcd.controlplane.io/v1", "FluxInstance"
		document["spec"] = map[string]any{"sync": map[string]any{
			"kind": source.Kind,
			"name": source.Name,
			"path": "./entry",
			"url":  "https://fixture.invalid/self",
			"ref":  "main",
		}}
	}
	object, _, _, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	return Child{Obj: object, Doc: document}
}
