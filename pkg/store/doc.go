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
// ObjectAdded, StatusUpdated, ArtifactUpdated and WarningAdded events are
// observed through AddListener. WarningAdded carries each newly recorded
// origin/payload contribution with Count 1; Warnings returns aggregate counts
// and derived conflicting-producer diagnostics.
//
// Store is safe for concurrent use. Listeners run inline after state locks
// are released. Event payloads are immutable; copy Warning.Detail before mutation.
package store
