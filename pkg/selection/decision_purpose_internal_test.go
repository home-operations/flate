package selection

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
	filter := newPublicationSession(t)
	included, retry := publishDecision(t, filter, new(Admission{Needed: []manifest.NamedResource{id}}), 0)
	assert.Equal(t, retry, false)
	assert.Diff(t, included, []manifest.NamedResource{id})
	assert.Equal(t, filter.Snapshot().OrdinaryDemand(id), false)
	before := filter.Snapshot().version()
	promoted, retry := publishDecision(t, filter, new(Admission{Ordinary: []manifest.NamedResource{id}}), before)
	assert.Equal(t, retry, false)
	assert.Diff(t, promoted, []manifest.NamedResource{id})
	assert.Equal(t, filter.Snapshot().OrdinaryDemand(id), true)
	assert.Equal(t, filter.Snapshot().Output(id), false)
	assert.Equal(t, filter.Snapshot().Changed(id), true)
	_, retry = publishDecision(t, filter, new(Admission{Selected: []manifest.NamedResource{id}}), before)
	assert.Equal(t, retry, true)
	assert.Equal(t, filter.Snapshot().Output(id), false)
	included, retry = publishDecision(t, filter,
		new(Admission{Selected: []manifest.NamedResource{id}}),
		filter.Snapshot().version(),
	)
	assert.Equal(t, retry, false)
	assert.Equal(t, len(included), 0)
	assert.Equal(t, filter.Snapshot().Output(id), true)
	assert.Equal(t, filter.Snapshot().OrdinaryDemand(id), true)
	filter = newPublicationSession(t)
	included, retry = publishDecision(t, filter, new(Admission{
		Needed: []manifest.NamedResource{id}, Selected: []manifest.NamedResource{id},
	}), 0)
	assert.Equal(t, retry, false)
	assert.Diff(t, included, []manifest.NamedResource{id})
	assert.Equal(t, filter.Snapshot().OrdinaryDemand(id), true)
	assert.Equal(t, filter.Snapshot().Changed(id), false)
}

func TestFilter_ConcurrentPurposeTransaction(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "apps", Name: "candidate"}
	filter := newPublicationSession(t)
	_, retry := publishDecision(t, filter, new(Admission{Needed: []manifest.NamedResource{id}}), 0)
	assert.Equal(t, retry, false)
	revision := filter.Snapshot().version()
	updates := [2]Admission{
		{Ordinary: []manifest.NamedResource{id}},
		{Selected: []manifest.NamedResource{id}},
	}
	var retries [2]bool
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range updates {
		workers.Go(func() {
			<-start
			_, retries[i] = publishDecision(t, filter, &updates[i], revision)
		})
	}
	close(start)
	workers.Wait()
	assert.Equal(t, retries[0] != retries[1], true)
	loser := 0
	if retries[1] {
		loser = 1
	}
	_, retry = publishDecision(t, filter, &updates[loser], filter.Snapshot().version())
	assert.Equal(t, retry, false)
	assert.Equal(t, filter.Snapshot().OrdinaryDemand(id), true)
	assert.Equal(t, filter.Snapshot().Output(id), true)
	assert.Equal(t, filter.Snapshot().Changed(id), true)
}
