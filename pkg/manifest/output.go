package manifest

// DocumentBatch contains ordered output documents and their actual producer.
// Parent is nil for ownerless documents; borrowed payloads MUST remain immutable.
type DocumentBatch struct {
	Parent    BaseManifest
	Documents []map[string]any
}
