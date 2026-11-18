package orchestrator

// Close MUST follow the final selection consumer of the completed result.
func (r *Result) Close() {
	if r == nil || r.catalog == nil {
		return
	}
	r.catalog.Close()
	r.catalog = nil
}
