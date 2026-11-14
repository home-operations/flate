package manifest

// DocumentBatch borrows immutable output slices. Absent Identities use the parent's emission policy,
// or the empty policy without a parent.
// Supplied identities MUST match Documents in length and order. Both slices MUST remain immutable.
type DocumentBatch struct {
	Parent     BaseManifest     `json:"Parent"`
	Documents  []map[string]any `json:"Documents"`
	Identities []NamedResource  `json:"Identities"`
}

// IdentityAccessor borrows both slices. Indices MUST come from this batch's immutable Documents.
func (b DocumentBatch) IdentityAccessor() (func(int) NamedResource, error) {
	if len(b.Identities) != 0 {
		if len(b.Identities) != len(b.Documents) {
			return nil, inputf(
				"identity count %d does not match document count %d",
				len(b.Identities),
				len(b.Documents),
			)
		}
		return func(i int) NamedResource { return b.Identities[i] }, nil
	}
	var policy EmissionPolicy
	if b.Parent != nil {
		policy = b.Parent.EmissionPolicy()
	}
	return func(i int) NamedResource { return policy.EmittedIdentity(b.Documents[i]) }, nil
}
