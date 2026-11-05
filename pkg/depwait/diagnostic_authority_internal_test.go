package depwait

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestClassify_DiagnosticAuthority(t *testing.T) {
	t.Parallel()
	for _, status := range []store.Status{store.StatusReady, store.StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "first"}
			source := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/repo"}
			st.AddObjects([]manifest.BaseManifest{owner, source})
			st.SetDiscoveryIndex(
				&manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						source.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
					},
				},
			)
			output := store.Emission{Children: []store.Child{{Obj: source}}, Complete: true}
			assert.Equal(t, st.GetObject(source.Named()) != nil, true)
			assert.Equal(t, st.GetDeclaration(source.Named()) == nil, true)
			st.UpdateStatus(source.Named(), store.StatusInfo{Status: status})
			st.SetArtifact(source.Named(), &store.SourceArtifact{FS: fstest.MapFS{}})
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{Kind: manifest.KindHelmChart, Name: "consumer"}, nil)
			defer reads.Close()
			dep := manifest.DependencyRef{NamedResource: source.Named(), Artifact: true}
			waiter := Waiter{Store: &reads}
			tasks := task.NewBounded(2)
			captured := make(chan struct{})
			tasks.Go(t.Context(), "capture", func(ctx context.Context) {
				assert.Equal(t, waiter.Classify(ctx, dep, manifest.DrainNone).Kind, schedule.Blocked)
				assert.Equal(t, waiter.Classify(ctx, dep, manifest.DrainCascade).Kind, schedule.DependencyFailed)
				close(captured)
			})
			tasks.Go(t.Context(), "publish", func(context.Context) {
				tasks.YieldSlot(func() { <-captured })
				st.RecordEmitted(owner.Named(), &store.Emission{Children: output.Children, Complete: true}, nil)
			})
			tasks.BlockTillDone()
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, reads.GetDeclaration(source.Named()) == nil, true)
			var fresh store.ReadSet
			fresh.Init(st, reads.ID(), nil)
			defer fresh.Close()
			assert.Equal(t, fresh.GetDeclaration(source.Named()) != nil, true)
		})
	}
}

func TestClassify_DiagnosticSkipTransition(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "first"}
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObjects([]manifest.BaseManifest{owner, source})
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				source.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
			},
		},
	)
	st.UpdateStatus(
		source.Named(),
		store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "credentials unavailable"},
	)
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{Kind: manifest.KindKustomization, Name: "consumer"}, nil)
	defer reads.Close()
	dep := manifest.DependencyRef{NamedResource: source.Named(), Artifact: true}
	waiter := Waiter{Store: &reads}
	assert.Equal(
		t,
		errors.Is(waiter.Classify(t.Context(), dep, manifest.DrainNone).Err, manifest.ErrSourceSkipped),
		true,
	)
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusPending})
	assert.Equal(t, reads.Valid(), false)
	var fresh store.ReadSet
	fresh.Init(st, reads.ID(), nil)
	defer fresh.Close()
	waiter.Store = &fresh
	result := waiter.Classify(t.Context(), dep, manifest.DrainNone)
	assert.Equal(t, result.Err == nil, true)
	assert.Equal(t, result.Kind, schedule.Blocked)
}
