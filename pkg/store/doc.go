// Package store is the central, in-memory state container for the
// controller pipeline. It tracks three orthogonal kinds of information
// for each Kubernetes-style resource (keyed by manifest.NamedResource):
//
//   - The parsed manifest object itself.
//   - The processing status (Pending / Ready / Failed) with an optional
//     error message.
//   - The artifact produced by a controller (e.g. a checked-out git
//     working tree, a rendered manifest, a downloaded chart).
//
// Three event types are dispatched as state evolves: ObjectAdded,
// StatusUpdated, ArtifactUpdated. Callers subscribe via AddListener (sync,
// returns an unsubscribe function) or block on the high-level Watch*
// helpers, which return a single value once the awaited state is reached.
//
// Store is safe for concurrent use. Listeners run inline on the goroutine
// that triggered the event — they MUST NOT block on Store operations, or
// they'll deadlock. The Watch* helpers handle this correctly by sending to
// a buffered channel from inside the listener.
package store
