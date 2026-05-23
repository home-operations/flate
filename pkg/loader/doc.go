// Package loader implements Loader — the entry point that scans a
// local filesystem path, decodes every YAML document found, and adds
// the recognized Flux objects to a Store.
//
// Loader honors a `.krmignore`-style ignore file at the scan root.
// Unrecognized objects (Pods, Deployments, etc.) are silently skipped
// to match flux-local's pre-filtering: flate derives them later from
// the rendered output of Kustomizations and HelmReleases.
package loader
