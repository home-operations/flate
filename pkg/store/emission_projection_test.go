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
	for _, mode := range []string{"first", "strengthen", "content_only"} {
		t.Run("completed/"+mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := projectionSource(t, s, "selected")
			producer := projectionOwner(s, "producer", source)
			contributor := projectionOwner(s, "contributor", source)
			child := emissionChild("input", "same")
			payload := &SourceArtifact{Content: "stable", FS: fstest.MapFS{}}
			raw := child.Obj.Named()
			raw.Namespace = ""
			proof := manifest.Production{Owner: contributor.Named(), Source: source, Authored: raw, File: "input.yaml", Path: "/selected/input.yaml"}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw:                 {Productions: []manifest.Production{proof}},
				contributor.Named(): {Productions: []manifest.Production{{Owner: contributor.Named(), Source: source}}},
			}}
			s.SetDiscoveryIndex(index)
			publish := func(fingerprint string) {
				s.RecordEmitted(contributor.Named(), &Emission{Children: []Child{child}, Complete: true, Fingerprint: fingerprint}, map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): payload})
			}
			publish("first")
			var reads ReadSet
			reads.Init(s, producer.Named(), nil)
			defer reads.Close()
			if mode != "first" {
				assert.Equal(t, reads.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(payload))
			}
			if mode != "content_only" {
				incoming := Child{Obj: child.Obj, Productions: []manifest.Production{{Owner: producer.Named(), Source: source}}}
				assert.Diff(t, reads.IncomingProductions(incoming, index), incoming.Productions)
			}
			assert.Equal(t, reads.Valid(), true)
			publish("second")
			changed := slices.Collect(s.Sweep())
			if mode == "content_only" {
				assert.Equal(t, len(changed), 0)
			} else {
				assert.Diff(t, changed, []manifest.NamedResource{reads.ID()})
			}
			assert.Equal(t, reads.Valid(), mode == "content_only")
		})
	}

}

func TestChildFootprint_MatchesEmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"index", "superseded", "foreign", "previous", "retained", "empty"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := projectionSource(t, s, "content")
			owner := projectionOwner(s, "owner", source)
			generator := Child{Obj: &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "app-{app}"}}}}}
			consumer := Child{Obj: &manifest.Kustomization{Name: "consumer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: "app-api"}}}
			target := consumer.Obj.(*manifest.Kustomization).SourceID()
			production := manifest.Production{Owner: owner.Named(), Source: source, File: "consumer.yaml", Path: "/content/consumer.yaml"}
			decl := manifest.Production{Owner: owner.Named(), Declaration: generator.Obj.Named(), Source: source}
			if mode == "foreign" {
				production.Source.Name, decl.Source.Name = "other", "other"
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				consumer.Obj.Named(): {File: production.File, Path: production.Path, Refs: []manifest.NamedResource{target}, Productions: []manifest.Production{production}},
				target:               {Productions: []manifest.Production{decl}},
			}}
			if mode == "previous" || mode == "retained" {
				_, _, prior := s.GetEmission(owner.Named())
				prior.Children, prior.Complete = []Child{generator, consumer}, true
				if mode == "retained" {
					retained := *consumer.Obj.(*manifest.Kustomization)
					retained.Name = "retained"
					prior.Children = append(prior.Children, Child{Obj: &retained})
				}
				s.RecordEmitted(owner.Named(), &prior, nil)
				prior.Finish()
			}
			children := []Child{generator}
			if mode == "superseded" || mode == "retained" {
				replacement := *consumer.Obj.(*manifest.Kustomization)
				replacement.SourceRef.Name = "app-web"
				children = append(children, Child{Obj: &replacement})
			}
			if mode == "empty" {
				children = nil
			}
			prior := s.GetArtifact(owner.Named(), ArtifactContent)
			prospective := s.ChildFootprint(owner.Named(), children, index)
			if prospective == nil {
				t.Fatal("complete batch supplied unavailable history")
			}
			before := maps.Collect(prospective)
			_, predicted := before[target]
			assert.Equal(t, predicted, mode == "index" || mode == "previous" || mode == "retained")
			assert.Equal(t, s.GetArtifact(owner.Named(), ArtifactContent), prior)
			assert.Equal(t, s.index.Load() == nil, true)
			assert.Diff(t, index.Records[target].Productions, []manifest.Production{decl})
			assert.Diff(t, index.Records[consumer.Obj.Named()].Refs, []manifest.NamedResource{target})
			s.SetDiscoveryIndex(index)
			_, _, next := s.GetEmission(owner.Named())
			defer next.Finish()
			next.Children, next.Complete = children, true
			s.RecordEmitted(owner.Named(), &next, nil)
			if next.Footprint() == nil {
				t.Fatal("real emission was not accepted")
			}
			assert.Diff(t, maps.Collect(next.Footprint()), before)
		})
	}
}
