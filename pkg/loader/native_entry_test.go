package loader

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_NativeEntryNeedsOwnObligation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, file   string
		failed, want bool
	}{
		{name: "child_payload", file: "workload/value.yaml"},
		{name: "entry_declaration", file: "entry/child.yaml", want: true},
		{name: "entry_failure", file: "workload/value.yaml", failed: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			root := t.TempDir()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			child := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./workload"}
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", owner.Named())
			st.AddObjects([]manifest.BaseManifest{owner, child, entry})
			if tc.failed {
				st.UpdateStatus(entry.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "invalid entry package"})
			}
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Changes = change.NewSet([]string{tc.file})
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner.Named(): {File: "entry/owner.yaml", Declaration: owner},
				child.Named(): {File: "entry/child.yaml", Declaration: child, Parent: owner.Named()},
			}}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, entry.Named()), tc.want)
		})
	}
}
