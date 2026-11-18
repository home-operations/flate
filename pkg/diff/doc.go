// Package diff compares groups of rendered Kubernetes manifests.
//
// Compare matches normalized resources by canonical producer and resource identity.
// Parent group, kind, namespace, and name distinguish producers.
// Parent.Path supplies display metadata.
// A resource moved between producers becomes a removal and an addition.
//
// Each Group contains two Snapshot values and any selected render error.
// Compare withholds unavailable producer batches and returns grouped warnings
// and structured suppressions alongside prepared output.
// Comparison.Write formats that output without repeating normalization.
//
// Options.StripAttrs and Options.StripFields remove volatile fields from copies.
// ConfigMap binaryData becomes a content hash summary.
// Options.Normalize runs after these operations on the copied manifests.
// Original chart labels remain available in the structured JSON output.
// The package example shows grouped comparison with retained chart metadata.
//
// Native formats use dyff to compare resource contents and named list entries.
// Unified, HTML, and JSON formats present the same canonical resource pairs.
// CompareImages applies the same producer availability policy and returns
// sorted image changes with structured suppressions.
package diff
