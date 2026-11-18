// Package orchestrator exposes supported render inputs and completed results.
// Together with pkg/manifest, it forms the public SDK boundary.
//
// A non-nil Run requires Close, including returns with a render error. Keep the
// run open through every result and selection consumer. The caller serializes
// closure after those consumers and joins teardown errors with earlier errors.
// Repeated closure is safe. Validation failures before allocation return nil.
//
// Run.Results contains reporting snapshots. Select resolves original owned
// identities independently of changes to that slice. Select accepts one head
// or an ordered base/head pair from the same cluster. Complete means that a
// selection snapshot exists, including completed renders with resource failures.
// Keep borrowed maps, slices, documents, and manifest objects immutable until
// closure. Other flate implementation packages are outside the supported SDK.
package orchestrator
