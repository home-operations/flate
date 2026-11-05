package loader

import (
	"context"
	"iter"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAdmission_EffectiveProviderHistory(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, child                        string
		complete, missing, invalid, needed bool
	}{
		{name: "complete unrelated", child: "unrelated", complete: true},
		{name: "matching", child: "wanted", complete: true, needed: true},
		{name: "complete empty", complete: true},
		{name: "incomplete", child: "unrelated", needed: true},
		{name: "missing", missing: true, needed: true},
		{name: "invalid roots", child: "unrelated", complete: true, invalid: true, needed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l, owner := effectiveProviderHistoryLoader(t)
			var records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			if !tc.missing {
				records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					if tc.child == "" {
						return
					}
					id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: tc.child}
					record := manifest.DiscoveryRecord{Parent: owner}
					if tc.invalid {
						record.ContentRoots = map[manifest.NamedResource][]string{
							manifest.BootstrapSourceID: {"../outside"},
						}
					}
					yield(id, record)
				}
			}
			history := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{owner: {Records: records, Complete: tc.complete}}
			proposal, err := l.Prepare(t.Context(), PrepareRequest{History: history, Invocation: true})
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, owner), tc.needed)
			assert.Equal(t, l.Options.Filter.ShouldReconcile(owner), false)
			_, retry := l.Options.Filter.AddEmitted(
				change.MembershipUpdate{
					Needed:   proposal.Needed,
					Selected: proposal.Selected,
					Dirty:    proposal.Dirty,
					Ordinary: proposal.Ordinary,
				},
				proposal.Revision,
			)
			assert.Equal(t, retry, false)
			assert.Equal(t, l.Options.Filter.ShouldReconcile(owner), tc.needed)
			assert.Equal(
				t,
				slices.ContainsFunc(
					proposal.Objects,
					func(
						child store.Child,
					) bool {
						return child.Obj.Named() ==
							owner
					},
				),
				tc.needed,
			)
		})
	}
	t.Run("invalidated before publication", func(t *testing.T) {
		t.Parallel()
		l, owner := effectiveProviderHistoryLoader(t)
		var reads store.ReadSet
		reads.Init(l.Store, testutil.KustomizationID("apps", "consumer"), nil)
		defer reads.Retire()
		reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		history := map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{owner: {Records: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}, Complete: true}}
		lookedUp, invalidated := make(chan struct{}), make(chan struct{})
		tasks := task.NewBounded(2)
		tasks.Go(t.Context(), "lookup", func(ctx context.Context) {
			proposal, err := l.Prepare(ctx, PrepareRequest{Reads: &reads, History: history})
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, owner), false)
			close(lookedUp)
			<-invalidated
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, l.Options.Filter.ShouldReconcile(owner), false)
			proposal, err = l.Prepare(ctx, PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			_, retry := l.Options.Filter.AddEmitted(
				change.MembershipUpdate{
					Needed:   proposal.Needed,
					Selected: proposal.Selected,
					Dirty:    proposal.Dirty,
					Ordinary: proposal.Ordinary,
				},
				proposal.Revision,
			)
			assert.Equal(t, retry, false)
			assert.Equal(t, l.Options.Filter.ShouldReconcile(owner), true)
		})
		tasks.Go(t.Context(), "invalidate", func(context.Context) {
			<-lookedUp
			input := *l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			input.Content = "changed"
			l.Store.SetArtifact(manifest.BootstrapSourceID, &input)
			close(invalidated)
		})
		tasks.BlockTillDone()
		assert.Equal(t, tasks.Failures(), int64(0))
	})
}

func effectiveProviderHistoryLoader(t *testing.T) (*Loader, manifest.NamedResource) {
	t.Helper()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"consumer.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\n" +
					"kind: Kustomization\n" +
					"metadata: {name: consumer, namespace: apps}\n" +
					"spec:\n" +
					" path: ./payload\n" +
					" postBuild: {substituteFrom: [{kind: ConfigMap, name: wanted}]}\n",
			),
		},
		"producer.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\n" +
					"kind: HelmRelease\n" +
					"metadata: {name: producer, namespace: apps}\n" +
					"spec:\n" +
					" interval: 1m\n" +
					" chart: {spec: {chart: demo, sourceRef: {kind: GitRepository, name: charts}}}\n",
			),
		},
	}})
	input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	files := input.FS.(fstest.MapFS)
	files["demo/Chart.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v2\nname: demo\nversion: 0.1.0\n")}
	files["demo/templates/cm.yaml"] = &fstest.MapFile{
		Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: wanted, namespace: apps}\n"),
	}
	charts := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(charts)
	st.SetArtifact(charts.Named(), input)
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "consumer")},
	}
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	owner := testutil.HelmReleaseID("apps", "producer")
	assert.Equal(t, st.GetObject(owner) == nil, true)
	assert.Equal(t, l.IsFileIndexed(owner), true)
	return l, owner
}
