package helm

import "time"

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

// SweepBlocking forces a synchronous eviction pass. Test affordance —
// production callers use the async trigger inside Put. Useful for
// asserting eviction ordering without flake.
func (c *diskRenderCache) SweepBlocking() {
	if c == nil {
		return
	}
	// Wait for any in-flight async sweep to drain before kicking off
	// our own so the synchronous call sees a stable view.
	for !c.sweepBusy.CompareAndSwap(0, 1) {
		time.Sleep(time.Millisecond)
	}
	c.sweep()
}
