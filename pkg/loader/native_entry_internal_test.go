package loader

import (
	"fmt"
	"iter"
	"path/filepath"
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
	for _, file := range []string{"workload/value.yaml", "entry/child.yaml", "entry/kustomization.yaml", "entry/patch.yaml", "entry/input.env", "entry/component/kustomization.yaml", "entry/missing/data.yaml"} {
		t.Run("construction/"+file, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{
					LocalPath: root,
					FS: fstest.MapFS{"entry/kustomization.yaml": {Data: []byte(`resources: [child.yaml, missing]
patches: [{path: patch.yaml}]
components: [component]
configMapGenerator: [{name: settings, envs: [input.env]}]
`)}, "entry/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: ./workload}
`)}, "entry/patch.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {interval: 1h}
`)}, "entry/input.env": {Data: []byte("VALUE=one\n")}, "entry/component/kustomization.yaml": {Data: []byte("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nnamespace: apps\n")}, "workload/kustomization.yaml": {Data: []byte("resources: [value.yaml]\n")}, "workload/value.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: value}\n")}},
				},
			)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{file})
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Blocked, "entry"), file != "workload/value.yaml")
		})
	}
	for _, tc := range []struct {
		name, file   string
		failed, want bool
	}{{name: "child_payload", file: "workload/value.yaml"}, {name: "entry_declaration", file: "entry/child.yaml", want: true}, {name: "entry_failure", file: "workload/value.yaml", failed: true, want: true}} {
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
				st.UpdateStatus(
					entry.Named(),
					store.StatusInfo{Status: store.StatusFailed, Message: "invalid entry package"},
				)
			}
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Changes = change.NewSet([]string{tc.file})
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Index = &manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner.Named(): {File: "entry/owner.yaml", Declaration: owner},
					child.Named(): {File: "entry/child.yaml", Declaration: child, Parent: owner.Named()},
				},
			}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Selected, child.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
			assert.Equal(t, slices.Contains(proposal.Needed, entry.Named()), tc.want)
		})
	}
}

func TestAdmission_UnreferencedEntryFile(t *testing.T) {
	t.Parallel()
	for _, historical := range []bool{false, true} {
		t.Run(fmt.Sprintf("history=%t", historical), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{
					LocalPath: root,
					FS: fstest.MapFS{
						"entry/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
						"entry/child.yaml": {Data: []byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec: {path: ./workload}
`)},
						"entry/unreferenced.yaml": {
							Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n"),
						},
					},
				},
			)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"entry/unreferenced.yaml"})
			_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
			var history map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}
			if historical {
				history = map[manifest.NamedResource]struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{entry.Named(): {Complete: true, Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					yield(
						entry.Named(),
						manifest.DiscoveryRecord{
							ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"entry"}},
						},
					)
				}}}
			}
			proposal, err := l.Prepare(t.Context(), store.Child{Obj: entry}, nil, history)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, entry.Named()), false)
			assert.Equal(t, slices.Contains(proposal.Blocked, "entry"), false)
		})
	}
}
