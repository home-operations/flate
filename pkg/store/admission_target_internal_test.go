package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_AdmissionTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                          string
		seed, retire, producer, valid bool
	}{
		{name: "missing", valid: true},
		{name: "retired", seed: true, retire: true, valid: true},
		{name: "retired producer", seed: true, retire: true, producer: true, valid: true},
		{name: "consumed content", seed: true},
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
			assert.Equal(t, st.GetDeclaration(id), next.Obj)
		})
	}
}
