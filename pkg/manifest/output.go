package manifest

// DocumentBatch contains ordered output documents and their actual producer.
// Borrowed payloads MUST remain immutable.
type DocumentBatch struct {
	Parent    BaseManifest
	Documents []map[string]any
}
