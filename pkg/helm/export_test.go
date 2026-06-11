package helm

// Size reports the running total of cached entry costs (sum of
// value sizes). Used by tests to verify eviction accounting.
func (c *templateCache) Size() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// Len returns the number of currently-cached entries. Test affordance.
func (c *templateCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}
