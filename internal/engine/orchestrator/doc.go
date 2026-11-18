// Package orchestrator owns rendering and the resources of each invocation.
// The CLI and the public SDK use this same execution owner.
//
// A non-nil Invocation requires Close after every result and selection consumer,
// including returns with an error. Workers and callbacks drain before Render
// returns. Completed stores, catalogs, and lazy sources remain readable until
// Close releases catalogs and stores before the owned cache and network.
// The caller serializes closure after all consumers. Repeated closure is safe
// and returns the same teardown error.
//
// Injected Cache, Network, and HelmClient resources remain caller-owned unless
// ownership explicitly transfers to the invocation. The CLI initializes that
// owner before baseline resolution and retains it through the final report.
package orchestrator
