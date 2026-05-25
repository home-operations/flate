package source

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
func AuthIdentity(parts ...string) string {
	out := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		if out != "" {
			out += "\x00"
		}
		out += p
	}
	return out
}

// SecretRefID renders a Flux LocalObjectReference into the
// `<namespace>/<name>` shape AuthIdentity expects. ns is the owning
// CR's namespace (SecretRefs are LocalObjectReferences — they inherit
// it). Returns "" when name is empty so optional refs slot in as
// no-op zero values.
func SecretRefID(ns, name string) string {
	if name == "" {
		return ""
	}
	return ns + "/" + name
}
