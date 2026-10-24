package store

import (
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
		Name: name, Namespace: "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name, Namespace: source.Namespace},
	}
	s.AddObject(owner)
	return owner
}

func TestStore_IncomingProductionsIdentity(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"inherited_namespace", "explicit_namespace", "group", "source", "contributor", "unknown_evidence", "producer_file", "derived_raw_file", "own_file", "own_no_path"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := projectionSource(t, s, "source")
			other := projectionSource(t, s, "other-source")
			rendererSource := source
			if mode == "contributor" {
				rendererSource = other
			}
			renderer := projectionOwner(s, "renderer", rendererSource)
			fileOwner := projectionOwner(s, "file-owner", source)
			incoming := Child{Obj: &manifest.ConfigMap{Name: "input", Namespace: "apps"}}
			bare := incoming.Obj.Named()
			bare.Namespace = ""
			if mode == "group" {
				bare.Group = "foreign.example"
			}
			if mode == "explicit_namespace" {
				bare.Namespace = "other"
			}
			proof := manifest.Production{Owner: fileOwner.Named(), Source: source, Authored: bare, Path: "/source/input.yaml", File: "input.yaml"}
			if mode == "source" {
				proof.Source = other
			}
			if mode == "derived_raw_file" {
				proof.Authored = renderer.Named()
				proof.Authored.Namespace = ""
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				bare:              {Authored: bare, Path: proof.Path, File: proof.File, Productions: []manifest.Production{proof}},
				ownerID("marker"): {Productions: []manifest.Production{{Owner: renderer.Named(), Source: rendererSource}}},
			}}
			s.SetDiscoveryIndex(index)
			incoming.Productions = []manifest.Production{{Owner: renderer.Named(), Source: source}}
			if mode == "producer_file" || mode == "own_file" || mode == "own_no_path" {
				incoming.Productions[0].Path = "/source/producer.yaml"
				incoming.Productions[0].File = "producer.yaml"
				incoming.Productions[0].Authored = renderer.Named()
				if mode == "own_file" || mode == "own_no_path" {
					incoming.Productions[0].Authored = bare
				}
				if mode == "own_no_path" {
					incoming.Productions[0].Path = ""
				}
			}
			if mode == "unknown_evidence" {
				incoming.Productions = nil
			}
			want := slices.Clone(incoming.Productions)
			if mode == "inherited_namespace" || mode == "producer_file" || mode == "own_no_path" {
				want = append(want, proof)
			}
			slices.SortFunc(want, compareProduction)
			assert.Diff(t, s.IncomingProductions(incoming, index), want)
		})
	}
}

func TestStore_IncomingProductionsIgnoresStoredChild(t *testing.T) {
	t.Parallel()
	s := New()
	source := projectionSource(t, s, "source")
	owner := projectionOwner(s, "owner", source)
	stored := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "stale"}}
	s.AddObject(stored)
	proof := manifest.Production{Owner: owner.Named(), Source: source, Authored: stored.Named(), Path: "/source/stale.yaml", File: "stale.yaml"}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		stored.Named(): {Authored: stored.Named(), Path: proof.Path, File: proof.File, Productions: []manifest.Production{proof}},
	}}
	s.SetDiscoveryIndex(index)
	for _, mode := range []string{"unknown_evidence", "own_evidence"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			incoming := Child{Obj: &manifest.ConfigMap{Name: stored.Name, Namespace: stored.Namespace, Data: map[string]any{"value": "incoming"}}}
			if mode == "own_evidence" {
				incoming.Productions = []manifest.Production{{Owner: owner.Named(), Source: source, File: "incoming.yaml"}}
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
			incoming := Child{Obj: &manifest.ConfigMap{Name: "input", Namespace: "apps"}, Productions: []manifest.Production{{Owner: renderer.Named(), Source: source}}}
			bare := incoming.Obj.Named()
			bare.Namespace = ""
			proof := manifest.Production{Owner: fileOwner.Named(), Source: source, Authored: bare, Path: "/source/input.yaml", File: "input.yaml"}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				bare:              {Authored: bare, Path: proof.Path, File: proof.File, Productions: []manifest.Production{proof}},
				ownerID("marker"): {Productions: []manifest.Production{{Owner: renderer.Named(), Source: source}}},
			}}
			if mode == "hidden" {
				index.Records[fileOwner.Named()] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: ancestor.Named(), Source: source}}}
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
			s := New()
			source := projectionSource(t, s, "selected")
			producer := projectionOwner(s, "producer", source)
			fileOwner := projectionOwner(s, "raw-owner", source)
			stale := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "stale"}}
			s.AddObject(stale)
			raw := stale.Named()
			raw.Namespace = ""
			proof := manifest.Production{Owner: fileOwner.Named(), Source: source, Authored: raw, File: "input.yaml", Path: "/selected/input.yaml"}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw:              {Productions: []manifest.Production{proof}},
				producer.Named(): {Productions: []manifest.Production{{Owner: producer.Named(), Source: source}}},
			}}
			s.SetDiscoveryIndex(index)
			incoming := Child{Obj: &manifest.ConfigMap{Name: stale.Name, Namespace: stale.Namespace, Data: map[string]any{"value": "incoming"}}, Productions: []manifest.Production{{Owner: producer.Named(), Source: source}}}
			var reads ReadSet
			reads.Init(s, producer.Named(), nil)
			defer reads.Retire()
			want := append(slices.Clone(incoming.Productions), proof)
			slices.SortFunc(want, compareProduction)
			assert.Diff(t, reads.IncomingProductions(incoming, index), want)
			assert.Equal(t, reads.Valid(), true)
			switch mutation {
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
		})
	}
}
