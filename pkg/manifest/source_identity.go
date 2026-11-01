package manifest

// SourceIdentity describes a source family independently of its content generation.
type SourceIdentity struct {
	Kind, URL, Key, Root string
}

// SourceEvidence identifies one source-relative file or subtree from accepted output.
type SourceEvidence struct {
	Source  SourceIdentity
	Path    string
	Subtree bool
}
