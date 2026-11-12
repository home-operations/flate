package store

import (
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_ReferenceFacts_ReplacementConsistency(t *testing.T) {
	t.Parallel()

	for _, namespace := range []string{"apps", ""} {
		t.Run("authored_namespace="+namespace, func(t *testing.T) {
			t.Parallel()

			s := New()
			owner := ownerID("owner")
			s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			secret := &manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"VALUE": "visible"}}
			target := secret.Named()
			authored := target
			authored.Namespace = namespace
			proof := manifest.Production{
				Owner:    owner,
				Source:   manifest.BootstrapSourceID,
				Authored: authored,
				Path:     "input.yaml",
				File:     "input.yaml",
			}
			records := map[manifest.NamedResource]manifest.DiscoveryRecord{
				authored: {Productions: []manifest.Production{proof}},
			}
			proofs := []manifest.Production{proof}
			if namespace == "" {
				scoped := manifest.Production{Owner: owner, Source: manifest.BootstrapSourceID}
				records[target] = manifest.DiscoveryRecord{Productions: []manifest.Production{scoped}}
				proofs = append([]manifest.Production{scoped}, proof)
			}
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
			s.AddObject(secret)
			before := s.ReferenceFacts(target, nil, manifest.NamedResource{})

			assert.FatalIf(t, before.Data != secret || before.Accepted, "file-backed facts=%+v", before)

			assert.Diff(t, before.Productions, proofs)

			external := &manifest.RawObject{
				Kind:       "ExternalSecret",
				APIVersion: "external-secrets.io/v1",
				Name:       "external",
				Namespace:  target.Namespace,
				Spec: map[string]any{
					"target": map[string]any{"name": target.Name},
					"data":   []any{map[string]any{"secretKey": "VALUE"}},
				},
			}
			_, _, next := s.GetEmission(owner)
			defer next.Finish()
			next.Children, next.Complete = []Child{{Obj: external}}, true
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				s.ReferenceFacts(target, nil, manifest.NamedResource{})
			})
			wg.Go(func() {
				<-start
				s.RecordEmitted(owner, &next, nil)
			})
			close(start)
			wg.Wait()

			want := []manifest.Production{
				{Owner: owner, Source: manifest.BootstrapSourceID, Declaration: external.Named()},
			}
			assert.Diff(
				t,
				s.ReferenceFacts(target, nil, manifest.NamedResource{}),
				ReferenceFacts{
					Generated:     true,
					Materializers: []manifest.NamedResource{owner},
					Productions:   want,
					ReferenceData: []manifest.ReferenceData{{Production: want[0], DeclaredKeys: []string{"VALUE"}}},
				},
			)
			assert.Diff(t, s.Productions(target, nil), want)
			assert.FatalIf(
				t,
				s.GetObject(target) != nil || s.GetObject(external.Named()) != external,
				"%s",
				"replacement did not leave only the declaration",
			)
			parent, ok := s.RenderedParent(target, nil)
			assert.Equal(t, parent, manifest.NamedResource{})
			assert.Equal(t, ok, false)
		})
	}
}

func TestReferenceFacts_PendingPeerAfterEmptySupplier(t *testing.T) {
	t.Parallel()
	s := New()
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	empty := &manifest.Kustomization{Name: "empty", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{first, empty})
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{{Owner: first.Named()}, {Owner: empty.Named()}}},
	}})
	s.UpdateStatus(first.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "pending supplier"})
	_, _, next := s.GetEmission(empty.Named())
	defer next.Finish()
	next.Complete = true
	s.RecordEmitted(empty.Named(), &next, nil)
	facts := s.ReferenceFacts(target, nil, manifest.NamedResource{})

	assert.FatalIf(
		t,
		len(facts.Productions) != 1 || facts.Productions[0].Owner != first.Named() || facts.Data != nil ||
			facts.Accepted,
		"pending peer lost after empty supplier: facts=%+v",
		facts,
	)
}

func TestStore_ReferenceFacts_WarningOnlyReplacement(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"repository", "emitted", "supplied", "admitted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testStoreReferenceFactsWarningOnlyReplacement(t, mode)
		})
	}
}

func testStoreReferenceFactsWarningOnlyReplacement(t *testing.T, mode string) {
	t.Helper()
	st := New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(owner)
	st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Content: "source"})
	data := &manifest.Secret{Name: "input", Namespace: "apps"}
	warning := manifest.Warning{
		Resource: data.Named(),
		Category: manifest.WarnSOPSUnreadable,
		Detail:   []string{"VALUE"},
	}
	publish := func(warnings []manifest.Warning) {
		switch mode {
		case "emitted":
			st.RecordEmitted(
				owner.Named(),
				&Emission{Children: []Child{{Obj: data, Warnings: warnings}}, Complete: true},
				nil,
			)
		case "admitted":
			var admission ReadSet
			admission.Init(st, ownerID("admission"), nil)
			defer admission.Retire()
			admission.Admit(Child{Obj: data, Warnings: warnings}, manifest.BootstrapSourceID, manifest.NamedResource{})
		default:
			origin := manifest.RepositoryDeclaration
			if mode == "supplied" {
				origin = manifest.SuppliedDeclaration
			}
			st.AddDeclaration(Declaration{Obj: data, Origin: origin, Warnings: warnings})
		}
	}
	var previous *ReadSet
	for _, warnings := range [][]manifest.Warning{{warning}, nil, {warning}} {
		publish(warnings)
		if previous != nil {
			assert.Equal(t, previous.Valid(), false)
			var changed ReadSet
			changed.Init(st, ownerID("consumer"), nil)
			assert.Equal(t, changed.Matches(previous), false)
			assert.Equal(t, changed.matches(previous, true), false)
			changed.Close()
			previous.Close()
		}
		reads := new(ReadSet)
		reads.Init(st, ownerID("consumer"), nil)
		captured := reads.ReferenceFacts(data.Named(), nil, reads.ID())
		live := st.ReferenceFacts(data.Named(), nil, reads.ID())
		assert.Equal(t, captured.Data, live.Data)
		assert.Equal(t, captured.Original, live.Original)
		assert.Diff(t, captured.Warnings, live.Warnings)
		assert.Diff(t, captured.Warnings, warnings)
		if mode == "supplied" {
			assert.Equal(t, captured.Original, manifest.Data(nil))
		} else {
			assert.Equal(t, captured.Original, manifest.Data(data))
		}
		assert.Equal(t, reads.Valid(), true)
		var same ReadSet
		same.Init(st, ownerID("consumer"), nil)
		assert.Equal(t, same.Matches(reads), true)
		assert.Equal(t, same.matches(reads, true), true)
		same.Close()
		previous = reads
	}
	previous.Close()
}
