package manifest

// DeclarationOrigin distinguishes repository production from invocation references.
type DeclarationOrigin uint8

const (
	// RepositoryDeclaration is eligible for ordinary production.
	RepositoryDeclaration DeclarationOrigin = iota
	// SuppliedDeclaration provides references without production authority.
	SuppliedDeclaration
)

// DeclarationPolicy classifies root, reconciliation and reporting eligibility.
type DeclarationPolicy struct {
	Origin                  DeclarationOrigin
	Root, Reconcile, Report bool
}

// Policy derives declaration roles from origin and object capabilities when known.
func (o DeclarationOrigin) Policy(id NamedResource, obj BaseManifest) DeclarationPolicy {
	info := KindOf(obj)
	if obj == nil {
		info = id.KindInfo()
	}
	return DeclarationPolicy{
		Origin:    o,
		Root:      o == RepositoryDeclaration,
		Reconcile: info.Runnable && (o == RepositoryDeclaration || info.IsSource()),
		Report:    o == RepositoryDeclaration && !id.ReportHidden(),
	}
}
