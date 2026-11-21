package store

import (
	"iter"
	"maps"

	"github.com/home-operations/flate/pkg/manifest"
)

// ProducerBindings borrows immutable comparison coordinates from an accepted discovery index.
type ProducerBindings struct {
	keys map[manifest.NamedResource]manifest.NamedResource
}

// Bindings borrows immutable coordinates from the accepted index.
func Bindings(index *manifest.DiscoveryIndex) ProducerBindings {
	if index == nil {
		return ProducerBindings{}
	}
	return ProducerBindings{keys: index.ProducerKeys}
}

// Empty lets callers skip translating inputs when discovery has no binding.
func (b ProducerBindings) Empty() bool {
	return len(b.keys) == 0
}

// Slot returns the comparison identity and whether discovery mapped the producer.
func (b ProducerBindings) Slot(producer manifest.NamedResource) (manifest.NamedResource, bool) {
	if slot, found := b.keys[producer]; found {
		return slot, true
	}
	return producer, false
}

// Producer returns the producer for the comparison identity, or the identity itself.
func (b ProducerBindings) Producer(slot manifest.NamedResource) (manifest.NamedResource, bool) {
	var producer manifest.NamedResource
	for actual, key := range b.keys {
		if key == slot && (producer == (manifest.NamedResource{}) || actual.Compare(producer) < 0) {
			producer = actual
		}
	}
	if producer == (manifest.NamedResource{}) {
		return slot, false
	}
	return producer, true
}

// Peer keeps unmapped identities and requires an accepted peer for mapped identities.
func (b ProducerBindings) Peer(id manifest.NamedResource, other ProducerBindings) manifest.NamedResource {
	slot, mapped := b.Slot(id)
	if !mapped {
		return id
	}
	producer, found := other.Producer(slot)
	if !found {
		return manifest.NamedResource{}
	}
	return producer
}

// All iterates the immutable coordinates in unspecified order.
func (b ProducerBindings) All() iter.Seq2[manifest.NamedResource, manifest.NamedResource] {
	return maps.All(b.keys)
}
