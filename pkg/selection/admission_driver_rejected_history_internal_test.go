package selection

import (
	"context"
	"iter"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmit_PublicationRetryDiscardsRejectedOwnerHistory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		owner    manifest.NamedResource
		records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		complete bool
		retained bool
	}{
		{
			name:     "distinct_owner",
			owner:    testutil.KustomizationID("apps", "borrowed-owner"),
			records:  func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {},
			complete: true,
		},
		{
			name:     "same_owner_unavailable",
			owner:    testutil.KustomizationID("apps", "old-owner"),
			retained: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &store.SourceArtifact{
				LocalPath: t.TempDir(), Content: "source", FS: fstest.MapFS{},
			}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "old-owner", Namespace: "apps", Path: "old"}
			root := &manifest.Kustomization{
				Name: "root", Namespace: "apps",
				DependsOn: []kustomizev1.DependencyReference{{Name: owner.Name}},
			}
			rejectedInput := &manifest.ConfigMap{Name: "rejected-input", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{owner, root, rejectedInput})
			session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{root.Named()}})
			borrowedOwner := tc.owner
			borrowed := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{borrowedOwner: {
				Records: tc.records, Complete: tc.complete,
			}}
			trigger := &manifest.ConfigMap{Name: "a-trigger", Namespace: "apps"}
			reads := 0
			published := false
			stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if published || id != trigger.Named() {
					return
				}
				published = true
				assert.Equal(t, reads, 1)
				replacement := root.Clone()
				replacement.DependsOn = nil
				st.AddObject(replacement)
				st.DeleteObject(owner.Named())
			}, false)
			defer stop()
			result, accepted, err := session.admitTest(t.Context(), AdmissionOptions{
				Request: PrepareRequest{
					Invocation: true,
					Generation: []store.Child{{Obj: trigger, Entry: &manifest.EntryApplication{}}},
					History:    borrowed,
					HistoryOwners: map[manifest.NamedResource][]manifest.SourceEvidence{owner.Named(): {{
						Source: input.HistoryIdentity(manifest.BootstrapSourceID), Path: "old/child.yaml",
					}}},
					ReadHistory: func(
						_ context.Context,
						demand *Demand,
					) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
						reads++
						assert.Equal(t, demand.Identity, owner.Named())
						return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
							yield(root.Named(), manifest.DiscoveryRecord{
								Refs: []manifest.NamedResource{rejectedInput.Named()},
							})
						}, false
					},
				},
				Stage: Construction,
			})
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			assert.Equal(t, published, true)
			assert.Equal(t, reads, 1)
			assert.Equal(t, slices.Contains(result.Needed, owner.Named()), false)
			_, retained := result.Request.History[owner.Named()]
			assert.Equal(t, retained, tc.retained)
			assert.Equal(t, slices.Contains(result.Needed, rejectedInput.Named()), false)
			assert.Equal(t, len(borrowed), 1)
			assert.Equal(t, borrowed[borrowedOwner].Complete, tc.complete)
			assert.Equal(t, result.Request.History[borrowedOwner].Complete, tc.complete)
			assert.Equal(t, borrowed[borrowedOwner].Records == nil, tc.records == nil)
			assert.Equal(t, result.Request.History[borrowedOwner].Records == nil, tc.records == nil)
		})
	}
}
