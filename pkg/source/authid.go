package source

import (
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// AuthIdentity returns a deterministic, opaque tag identifying the
// auth context bound to a source fetch. It is appended to the cache
// key so two source CRs that target the same (URL, ref) but reference
// different SecretRefs do not collide on the same on-disk slot.
//
// Inputs are usually `<ns>/<secretRef.Name>` strings; the helper trims
// empties and joins the rest with NULs so adding a new auth dimension
// later (cert secret, proxy secret, …) is a one-arg append.
//
// Returns "" when every input is empty — the caller passes "" to
// Cache.Slot in that case, and slots match the legacy unauthenticated
// layout so existing caches survive.
//
// Fetchers should prefer AuthIdentityFromRefs when their inputs are
// already *manifest.LocalObjectReference values; reach for this
// lower-level entry point only when a non-secret-ref dimension (e.g.
// a hashed cert thumbprint) needs to participate in the key.
func AuthIdentity(parts ...string) string {
	nonEmpty := slices.DeleteFunc(slices.Clone(parts), func(p string) bool { return p == "" })
	return strings.Join(nonEmpty, "\x00")
}

// AuthIdentityFromRefs is the typed entry point fetchers use to build
// a cache auth identity from their namespaced SecretRefs. Each fetcher
// (git, oci, bucket) used to reimplement the same nil-check + format +
// AuthIdentity call sequence; collapsing them here keeps the format
// stable as new ref dimensions get added.
//
// Refs are consumed in caller-defined order; nil entries slot in as
// "" so AuthIdentity strips them. ns is the owning CR's namespace
// (SecretRefs are LocalObjectReferences — they inherit it).
func AuthIdentityFromRefs(ns string, refs ...*manifest.LocalObjectReference) string {
	parts := make([]string, len(refs))
	for i, ref := range refs {
		if ref != nil {
			parts[i] = secretRefID(ns, ref.Name)
		}
	}
	return AuthIdentity(parts...)
}

// secretRefID renders a Flux LocalObjectReference into the
// `<namespace>/<name>` shape AuthIdentity expects. Returns "" when
// name is empty so optional refs slot in as no-op zero values.
func secretRefID(ns, name string) string {
	if name == "" {
		return ""
	}
	return ns + "/" + name
}
