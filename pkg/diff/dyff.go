package diff

import "github.com/homeport/dyff/pkg/dyff"

// isFileLevelOrderChange reports whether d is dyff's document-SET "order
// changed" note: a nil path (dyff renders it as "(file level)") whose details
// are all ORDERCHANGE. A nil path uniquely marks that top-level
// document-sequence diff — every document add/remove/change and every
// in-resource diff carries a non-nil path.
//
// flate emits documents in a deterministic sorted order, so the document
// sequence never reorders on its own. dyff raises this note only when an
// add/remove/rename keeps the document COUNT equal but shifts identities in the
// list: its order check excludes modified documents but NOT added/removed ones
// (see homeport/dyff core.go), so a single rename — already reported as the
// remove + add — explodes into a wall of "order changed" spanning every
// untouched resource. Dropping it removes that noise. In-resource list reorders
// (container/env order) carry a non-nil path and are kept — that K8s-aware
// signal is exactly why dyff's order detection stays enabled.
func isFileLevelOrderChange(d dyff.Diff) bool {
	if d.Path != nil || len(d.Details) == 0 {
		return false
	}
	for _, detail := range d.Details {
		if detail.Kind != dyff.ORDERCHANGE {
			return false
		}
	}
	return true
}
