package selection

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmit_FirstPresenceSurvivesIdentityPruning(t *testing.T) {
	t.Parallel()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: t.TempDir(), Content: "before", FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	present := &manifest.Kustomization{Name: "z-present", Namespace: "apps", Path: "payload"}
	st.AddObject(present)
	session := newSolverSession(t, st, nil, Request{Changed: true})
	trigger := &manifest.ConfigMap{Name: "a-trigger", Namespace: "apps"}
	incoming := present.Clone()
	incoming.Path = "updated"
	published := false
	stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if published || id != trigger.Named() {
			return
		}
		published = true
		assert.Equal(t, st.GetObject(present.Named()) != nil, true)
		st.DeleteObject(present.Named())
		replacement := *input
		replacement.Content = "after"
		st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	}, false)
	defer stop()
	result, accepted, err := session.admitTest(t.Context(), AdmissionOptions{
		Request: PrepareRequest{
			Invocation: true,
			Generation: []store.Child{
				{Obj: trigger, Entry: &manifest.EntryApplication{}},
				{Obj: incoming, Entry: &manifest.EntryApplication{}},
			},
		},
		Stage: Construction,
	})
	assert.NoError(t, err)
	assert.Equal(t, accepted, true)
	assert.Equal(t, published, true)
	assert.Equal(t, slices.ContainsFunc(result.Objects, func(child store.Child) bool {
		return child.Obj.Named() == present.Named()
	}), true)
	assert.Equal(t, slices.Contains(result.Absent, trigger.Named()), true)
	assert.Equal(t, slices.Contains(result.Absent, present.Named()), false)
	assert.Equal(t, st.GetDeclaration(present.Named()), manifest.BaseManifest(incoming))
}

func TestAdmit_RejectedPublicationDiscardsPresence(t *testing.T) {
	t.Parallel()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: t.TempDir(), Content: "before", FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	session := newSolverSession(t, st, nil, Request{Changed: true})
	trigger := &manifest.ConfigMap{Name: "trigger", Namespace: "apps"}
	var guard store.ReadSet
	guard.Init(st, manifest.NamedResource{}, nil)
	guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	defer guard.Retire()
	published := false
	stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if published || id != trigger.Named() {
			return
		}
		published = true
		replacement := *input
		replacement.Content = "after"
		st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	}, false)
	defer stop()
	result, accepted, err := session.admitTest(t.Context(), AdmissionOptions{
		Request: PrepareRequest{
			Reads:      &guard,
			Generation: []store.Child{{Obj: trigger, Entry: &manifest.EntryApplication{}}},
		},
		Stage: Construction,
	})
	assert.NoError(t, err)
	assert.Equal(t, accepted, false)
	assert.Equal(t, published, true)
	assert.Equal(t, guard.Valid(), false)
	assert.Equal(t, result.Objects == nil, true)
	assert.Equal(t, result.Absent == nil, true)
	assert.Equal(t, result.Demands == nil, true)
	assert.Equal(t, result.EntryReceipt, nil)
	assert.Equal(t, result.Request.Generation == nil, true)
}
