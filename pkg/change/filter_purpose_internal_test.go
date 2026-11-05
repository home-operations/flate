package change

import (
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFilter_OrdinaryDemandPromotion(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "candidate",
	}
	filter := NewFilter()
	included, retry := filter.AddEmitted(MembershipUpdate{Needed: []manifest.NamedResource{id}}, 0)
	assert.Equal(t, retry, false)
	assert.Diff(t, included, []manifest.NamedResource{id})
	assert.Equal(t, filter.OrdinaryDemand(id), false)
	before := filter.Membership(id).Revision
	promoted, retry := filter.AddEmitted(MembershipUpdate{Ordinary: []manifest.NamedResource{id}}, before)
	assert.Equal(t, retry, false)
	assert.Diff(t, promoted, []manifest.NamedResource{id})
	assert.Equal(t, filter.OrdinaryDemand(id), true)
	assert.Equal(t, filter.Membership(id).Selected, false)
	assert.Equal(t, filter.Membership(id).Dirty, true)
	_, retry = filter.AddEmitted(MembershipUpdate{Selected: []manifest.NamedResource{id}}, before)
	assert.Equal(t, retry, true)
	assert.Equal(t, filter.Membership(id).Selected, false)
	included, retry = filter.AddEmitted(
		MembershipUpdate{Selected: []manifest.NamedResource{id}},
		filter.Membership(id).Revision,
	)
	assert.Equal(t, retry, false)
	assert.Equal(t, len(included), 0)
	assert.Equal(t, filter.Membership(id).Selected, true)
	assert.Equal(t, filter.OrdinaryDemand(id), true)
	filter = NewFilter()
	included, retry = filter.AddEmitted(MembershipUpdate{
		Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id},
	}, 0)
	assert.Equal(t, retry, false)
	assert.Diff(t, included, []manifest.NamedResource{id})
	assert.Equal(t, filter.OrdinaryDemand(id), true)
	assert.Equal(t, filter.Membership(id).Dirty, false)
}

func TestFilter_ConcurrentPurposeTransaction(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "apps", Name: "candidate"}
	filter := NewFilter()
	_, retry := filter.AddEmitted(MembershipUpdate{Needed: []manifest.NamedResource{id}}, 0)
	assert.Equal(t, retry, false)
	revision := filter.Membership(id).Revision
	updates := [2]MembershipUpdate{
		{Ordinary: []manifest.NamedResource{id}},
		{Selected: []manifest.NamedResource{id}},
	}
	var retries [2]bool
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range updates {
		workers.Go(func() {
			<-start
			_, retries[i] = filter.AddEmitted(updates[i], revision)
		})
	}
	close(start)
	workers.Wait()
	assert.Equal(t, retries[0] != retries[1], true)
	loser := 0
	if retries[1] {
		loser = 1
	}
	_, retry = filter.AddEmitted(updates[loser], filter.Membership(id).Revision)
	assert.Equal(t, retry, false)
	assert.Equal(t, filter.OrdinaryDemand(id), true)
	assert.Equal(t, filter.Membership(id).Selected, true)
	assert.Equal(t, filter.Membership(id).Dirty, true)
}
