package store

import (
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_ReceiptOnly(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	child := emissionChild("output", "same")
	next := Emission{
		Children:  []Child{child},
		Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
		Complete:  true,
		Render:    manifest.NewRenderReceipt("first", func(a, b any) bool { return a == b }),
	}
	st.RecordEmitted(owner.Named(), &next, nil)
	var observer ReadSet
	observer.Init(st, ownerID("observer"), nil)
	defer observer.Retire()
	before := observer.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	observer.GetObject(child.Obj.Named())
	var objects, artifacts atomic.Int32
	stopObjects := st.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { objects.Add(1) }, false)
	defer stopObjects()
	stopArtifacts := st.AddListener(EventArtifactUpdated, func(manifest.NamedResource, any) { artifacts.Add(1) }, false)
	defer stopArtifacts()
	next.Render = manifest.NewRenderReceipt("second", func(a, b any) bool { return a == b })
	st.RecordEmitted(owner.Named(), &next, nil)
	after := st.GetArtifact(owner.Named(), ArtifactContent).(*Emission)
	{
		assert.FatalIf(
			t,
			before == after || before.revision != after.revision || !observer.Valid() || objects.Load() != 0 ||
				artifacts.Load() != 0,
			"%s",
			"receipt-only replacement changed semantic publication",
		)
	}
	assert.Equal(t, before.Render.Value().(string), "first")
	assert.Equal(t, after.Render.Value().(string), "second")
	assert.Equal(t, next.Render.Value().(string), "second")
	{
		assert.FatalIf(t, next.Footprint() == nil, "%s", "receipt-only replacement lost the accepted caller handoff")
	}
}
