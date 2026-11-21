package selection

import (
	"encoding/json"
	"hash/maphash"
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type peerDocumentScopeCase struct {
	name       string
	wantPeer   bool
	incomplete bool
}

func TestProject_PeerDocumentScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []peerDocumentScopeCase{
		{name: "different parent without a roster row", wantPeer: true},
		{name: "selected emitter under an unselected parent", wantPeer: true},
		{name: "selected entry emitter under an unselected parent", wantPeer: true},
		{name: "named root"},
		{name: "other namespace"},
		{name: "other label"},
		{name: "outside entry"},
		{name: "absent"},
		{name: "incomplete", incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testProjectPeerDocumentScope(t, &tc)
		})
	}
}

func testProjectPeerDocumentScope(t *testing.T, tc *peerDocumentScopeCase) {
	t.Helper()
	root := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps"}
	if tc.name == "named root" {
		root.Name = "local"
	}
	namespace := "apps"
	if tc.name == "other namespace" {
		namespace = "other"
	}
	doc := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "moved", "namespace": namespace},
	}
	id := manifest.DocIdentity(doc)
	request := Request{Roots: []manifest.NamedResource{root}}
	if tc.name == "other label" {
		request.Labels = map[string]string{"slot": "local"}
	}
	var runs [2]Snapshot
	for i, name := range [2]string{"local", "peer"} {
		runs[i] = peerDocumentScopeSnapshot(tc, name, i == 1, doc)
	}
	runs[1].Absent = tc.name == "absent"
	runs[1].Complete = !tc.incomplete
	results, errs := Project(runs[:], nil, request)
	assert.Equal(t, errs.Selected != nil, tc.incomplete)
	assert.Equal(t, errs.Remaining != nil, tc.incomplete)
	assert.FatalIf(t, len(results) != 2, "got %d results, want 2", len(results))
	assert.Equal(t, len(results[0].Objects), 0)
	assert.Equal(t, len(results[0].Counts), 0)
	if !tc.wantPeer {
		assert.Equal(t, len(results[0].Manifests), 0)
		return
	}
	assert.Equal(t, len(results[1].Objects), 0)
	for i, result := range results {
		assert.FatalIf(t, len(result.Manifests) != 1, "side %d has %d batches, want 1", i, len(result.Manifests))
		assert.Diff(t, result.Manifests[0].Identities, []manifest.NamedResource{id})
	}
}

func peerDocumentScopeSnapshot(tc *peerDocumentScopeCase, name string, peer bool, doc map[string]any) Snapshot {
	id := manifest.DocIdentity(doc)
	parent := &manifest.HelmRelease{
		Name: name, Namespace: "apps", Labels: map[string]string{"slot": name},
	}
	st := store.New()
	st.AddObject(parent)
	var emitter manifest.BaseManifest = parent
	admission := &Admission{Needed: []manifest.NamedResource{parent.Named()}}
	if peer {
		admission.Selected = []manifest.NamedResource{id}
		if tc.name == "selected emitter under an unselected parent" ||
			tc.name == "selected entry emitter under an unselected parent" {
			emitter = &manifest.ResourceSet{Name: "driver", Namespace: "apps"}
			st.AddObject(emitter)
			admission.Selected, admission.Needed = []manifest.NamedResource{emitter.Named()}, nil
		}
	}
	decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(admission, true)
	snapshot := Snapshot{
		Store: st, Complete: true, Decision: decision,
		Emissions: []EmissionFacts{{
			Emitter: emitter, Parent: parent, Documents: []DocumentFact{{Document: doc, Identity: id}},
		}},
	}
	if peer && tc.name == "outside entry" {
		snapshot.Entry = EntryIdentity{Producer: parent.Named(), Descendant: "entry"}
	}
	if peer && tc.name == "selected entry emitter under an unselected parent" {
		snapshot.Entry = EntryIdentity{
			Producer: emitter.Named(), Descendant: "entry", Documents: map[manifest.NamedResource]struct{}{id: {}},
		}
	}
	return snapshot
}

func TestProject_BatchIdentityMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		targetNamespace string
		selectedSide    int
		supplied        bool
		count           int
	}{
		{name: "orig counterpart omitted", targetNamespace: "apps", selectedSide: 0, count: 2},
		{name: "current counterpart omitted", targetNamespace: "apps", selectedSide: 1, count: 2},
		{name: "supplied emitter under reporting parent", targetNamespace: "reporting", selectedSide: 1, supplied: true, count: 2},
		{name: "restricted membership", targetNamespace: "apps", count: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			documents := []map[string]any{
				{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "kept"}},
				{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "excluded"}},
			}
			identities := []manifest.NamedResource{
				{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "kept"},
				{Kind: manifest.KindSecret, Namespace: "apps", Name: "excluded"},
			}
			runs := make([]Snapshot, tc.count)
			for i := range runs {
				parent := &manifest.Kustomization{
					Name:            [2]string{"original", "current"}[i],
					Namespace:       "reporting",
					TargetNamespace: tc.targetNamespace,
				}
				admission := &Admission{Needed: []manifest.NamedResource{parent.Named()}}
				if i == tc.selectedSide {
					admission.Selected = identities[:1]
				}
				decision, _ := (&Decision{seed: maphash.MakeSeed()}).include(admission, true)
				st := store.New()
				st.AddObject(parent)
				batch := manifest.DocumentBatch{Parent: parent, Documents: documents, Identities: identities}
				if !tc.supplied && i == tc.selectedSide {
					batch.Identities = nil
				}
				runs[i] = Snapshot{
					Store:     st,
					Complete:  true,
					Decision:  decision,
					Manifests: []manifest.DocumentBatch{batch},
				}
			}
			before := make([][]byte, tc.count)
			for i := range runs {
				var err error
				before[i], err = json.Marshal(runs[i].Manifests)
				assert.NoError(t, err)
			}
			results, errs := Project(
				runs,
				nil,
				Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "")}},
			)
			assert.NoError(t, errs.Selected)
			assert.NoError(t, errs.Remaining)
			assert.Equal(t, len(results), tc.count)
			for i, result := range results {
				assert.FatalIf(
					t,
					len(result.Manifests) != 1,
					"side %d has %d batches, want 1",
					i,
					len(result.Manifests),
				)
				assert.Diff(t, result.Manifests[0].Documents, documents[:1])
				assert.Diff(t, result.Manifests[0].Identities, identities[:1])
				assert.Diff(t, result.Manifests[0].Parent.Named(), runs[i].Manifests[0].Parent.Named())
				after, err := json.Marshal(runs[i].Manifests)
				assert.NoError(t, err)
				assert.Diff(t, after, before[i])
			}
		})
	}
}
