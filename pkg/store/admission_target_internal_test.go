package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_AdmissionTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                  string
		seed, retire, producer, parked, valid bool
	}{
		{name: "missing", valid: true},
		{name: "retired", seed: true, retire: true, valid: true},
		{name: "retired producer", seed: true, retire: true, producer: true, valid: true},
		{name: "consumed content", seed: true},
		{name: "parked declaration", parked: true, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Content: "source"})
			previous, next := emissionChild("input", "previous"), emissionChild("input", "next")
			id := next.Obj.Named()
			if tc.seed {
				st.AddObject(previous.Obj, previous.Doc)
			}
			if tc.producer {
				st.RecordEmitted(id, &Emission{Complete: true}, nil)
			}
			if tc.retire {
				assert.Equal(t, st.DeleteObject(id), true)
			}
			if tc.parked {
				st.SetDiscoveryIndex(
					&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						id: {
							BuilderOwner: ownerID("builder"),
							File:         "kustomization.yaml",
							Builder:      "kustomization.yaml",
						},
					}},
				)
				var initial ReadSet
				initial.Init(st, ownerID("initial"), nil)
				initial.Admit(previous, manifest.BootstrapSourceID, manifest.NamedResource{})
				initial.Retire()
				assert.Equal(t, st.GetObject(id), previous.Obj)
			}
			var proposal, observer ReadSet
			proposal.Init(st, ownerID("proposal"), nil)
			observer.Init(st, ownerID("observer"), nil)
			t.Cleanup(proposal.Close)
			t.Cleanup(observer.Close)
			captured := proposal.GetDeclaration(id)
			assert.Equal(t, observer.GetDeclaration(id), captured)
			assert.Equal(t, captured == nil, tc.valid)
			next.SourceSupport = manifest.BootstrapSourceID
			proposal.Admit(next, manifest.BootstrapSourceID, manifest.NamedResource{})
			assert.Equal(t, proposal.Valid(), tc.valid)
			assert.Equal(t, observer.Valid(), false)
			assert.Equal(t, st.GetObject(id), next.Obj)
			if !tc.parked {
				assert.Equal(t, st.GetDeclaration(id), next.Obj)
			}
		})
	}
}
