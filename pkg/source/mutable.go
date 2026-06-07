package source

// MutableCacheKey returns a one-shot cache key for mutable source refs.
// Mutable refs must refresh instead of serving a stale slot, but a failed
// refresh must also leave any previously committed artifact intact.
func MutableCacheKey(base string) string {
	return base + "#mutable:" + randomHex(16)
}
