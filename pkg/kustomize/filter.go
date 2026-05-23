package kustomize

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// FilterKinds returns a new slice with only the documents whose `kind`
// is in keep.
func FilterKinds(docs []map[string]any, keep []string) []map[string]any {
	if len(keep) == 0 {
		return docs
	}
	return filter(docs, func(doc map[string]any) bool {
		return slices.Contains(keep, manifest.DocKind(doc))
	})
}

// DropKinds returns docs with every entry whose `kind` appears in drop
// removed. drop=nil is a no-op (returns docs unchanged). The symmetric
// counterpart of FilterKinds.
//
// Used by the CLI's build/diff paths to honor --skip-secrets /
// --skip-crds / --skip-kinds against Kustomization-rendered output.
// (HelmRelease output is already filtered inside helm.TemplateDocs
// before reaching the store — see pkg/helm/template.go.)
func DropKinds(docs []map[string]any, drop []string) []map[string]any {
	if len(drop) == 0 {
		return docs
	}
	return filter(docs, func(doc map[string]any) bool {
		return !slices.Contains(drop, manifest.DocKind(doc))
	})
}


// filter returns the elements of docs for which pred is true, without
// mutating the input slice.
func filter(docs []map[string]any, pred func(map[string]any) bool) []map[string]any {
	out := make([]map[string]any, 0, len(docs))
	for _, doc := range docs {
		if pred(doc) {
			out = append(out, doc)
		}
	}
	return out
}
