package depwait

import (
	"errors"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAssess_DeclarationPublication(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"republished", "omitted", "unowned"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			source := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
			id, owner := source.Named(), producer.Named()
			if outcome != "unowned" {
				s.AddObject(producer)
				s.SetDiscoveryIndex(
					&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						id: {Productions: []manifest.Production{{Owner: owner}}},
					}},
				)
				s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: source}}, Complete: true}, nil)
				assert.Equal(t, s.GetDeclaration(id), manifest.BaseManifest(source))
				s.RecordEmitted(owner, &store.Emission{Complete: true}, nil)
				s.UpdateStatus(owner, store.StatusInfo{Status: store.StatusPending})
			}
			s.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady})
			s.SetArtifact(id, &store.SourceArtifact{FS: fstest.MapFS{}})
			assert.Equal(t, s.GetDeclaration(id), nil)
			var before store.ReadSet
			before.Init(s, id, nil)
			defer before.Retire()
			waiter := Waiter{Store: &before, Parent: id, Missing: NewMissingPolicy(nil)}

			start, updated := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(updated)
				<-start
				switch outcome {
				case "republished":
					replacement := *source
					replacement.URL = "oci://example.test/updated"
					s.RecordEmitted(
						owner,
						&store.Emission{Children: []store.Child{{Obj: &replacement}}, Complete: true},
						nil,
					)
				case "omitted":
					s.RecordEmitted(owner, &store.Emission{Complete: true}, nil)
					s.UpdateStatus(owner, store.StatusInfo{Status: store.StatusReady})
				}
			}()
			err := waiter.Assess(t.Context(), &before, manifest.DrainNone, AssessDeclaration)
			close(start)
			<-updated
			blocked, ok := errors.AsType[*BlockedError](err)
			assert.FatalIf(t, !ok, "expected a blocked assessment, got %v", err)
			if outcome == "unowned" {
				assert.Diff(t, blocked.Deps, []manifest.NamedResource{id})
				assert.Equal(t, before.Valid(), true)
			} else {
				assert.Diff(t, blocked.Deps, []manifest.NamedResource{owner, id})
				assert.Equal(t, before.Valid(), false)
			}

			var current store.ReadSet
			current.Init(s, id, nil)
			defer current.Retire()
			waiter.Store = &current
			err = waiter.Assess(t.Context(), &current, manifest.DrainForce, AssessDeclaration)
			switch outcome {
			case "republished":
				assert.NoError(t, err)
				target, ok := current.GetDeclaration(id).(*manifest.OCIRepository)
				assert.FatalIf(t, !ok, "missing republished declaration")
				assert.Equal(t, target.URL, "oci://example.test/updated")
			case "omitted":
				failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
				assert.FatalIf(t, !ok, "expected dependency failure, got %v", err)
				assert.Diff(t, failed.Failed, []manifest.NamedResource{owner})
				assert.Equal(t, failed.Reasons[owner].Message, "declaration not emitted")
			case "unowned":
				assert.Equal(t, errors.Is(err, manifest.ErrSourceSkipped), true)
			}
		})
	}
}

func TestAssess_DeclarationPublicationDispatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		bootstrap bool
	}{
		{name: "dispatch with exact bootstrap artifact", bootstrap: true},
		{name: "dispatch with different readable artifact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			source := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
			id := source.Named()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(producer)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{{Owner: producer.Named()}}},
			}})
			s.AddObject(source)
			s.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady})
			input := &store.SourceArtifact{FS: fstest.MapFS{}}
			s.SetArtifact(manifest.BootstrapSourceID, input)
			artifact := input
			if !tc.bootstrap {
				distinct := *input
				artifact = &distinct
			}
			s.SetArtifact(id, artifact)
			assert.Equal(t, s.GetObject(id), manifest.BaseManifest(source))
			assert.Equal(t, s.GetDeclaration(id), nil)
			assert.Equal(t, s.GetArtifact(id, store.ArtifactContent), store.Artifact(artifact))
			var reads store.ReadSet
			reads.Init(s, id, nil)
			defer reads.Retire()
			waiter := Waiter{Store: &reads, Parent: id, Missing: NewMissingPolicy(nil)}
			err := waiter.Assess(t.Context(), &reads, manifest.DrainNone, AssessDispatch)
			if tc.bootstrap {
				assert.NoError(t, err)
			} else {
				blocked, ok := errors.AsType[*BlockedError](err)
				assert.FatalIf(t, !ok, "expected a blocked dispatch, got %v", err)
				assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer.Named(), id})
			}
		})
	}
}
