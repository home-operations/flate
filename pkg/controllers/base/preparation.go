package base

type preparation interface{ retire() }

// Preparation MUST remain inline in its final consumer's storage.
type Preparation[T any] struct {
	value T
}

func (p *Preparation[T]) retire() {
	var zero T
	p.value = zero
}

// Prepare MUST NOT transfer input captures or grant publication authority.
func (e *Execution) Prepare[T any](destination *Preparation[T], discovery *T, matches func(*T) bool) *T {
	previous := e.preparation
	if !matches(&destination.value) {
		destination.retire()
		if prior, ok := previous.(*Preparation[T]); ok && prior != destination && matches(&prior.value) {
			destination.value = prior.value
		} else if discovery != nil && discovery != &destination.value && matches(discovery) {
			destination.value, *discovery = *discovery, destination.value
		}
	}
	if previous != nil && previous != destination {
		previous.retire()
	}
	e.preparation = destination
	return &destination.value
}
