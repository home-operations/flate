package manifest

import (
	"maps"
	"strings"
)

// FlattenLists expands Kubernetes List wrappers (a kind ending in "List"
// carrying a top-level items sequence — e.g. the ConfigMapList some Helm
// charts emit to bundle Grafana dashboards, or the generic `kind: List`)
// into their individual items. It mirrors what the Kubernetes client does
// when a List is applied to a cluster: the wrapper never exists as an
// object, only its items do.
//
// The helmrelease and kustomization controllers call this on their
// rendered output so flate's model stays consistent across build/diff:
// every rendered document is an individual, name-able resource. It also
// lets each item flow through ParseDoc as its real kind — a List wrapper
// has no metadata.name, so ParseDoc rejects it today and the resources
// inside go untracked in the store. The loader's own SplitDocs/DecodeDocs
// of user CR files is deliberately left un-flattened.
//
// Returns docs unchanged (no allocation) when no List is present;
// otherwise lazily allocates on the first List, mirroring DropKinds.
func FlattenLists(docs []map[string]any) []map[string]any {
	var out []map[string]any
	for i, d := range docs {
		items, ok := listItems(d)
		if !ok {
			if out != nil {
				out = append(out, d)
			}
			continue
		}
		if out == nil {
			out = make([]map[string]any, 0, len(docs))
			out = append(out, docs[:i]...)
		}
		itemKind := strings.TrimSuffix(DocKind(d), "List")
		apiVersion := DocAPIVersion(d)
		for _, it := range items {
			if im, ok := it.(map[string]any); ok {
				out = append(out, promoteListItem(im, itemKind, apiVersion))
			}
		}
	}
	if out == nil {
		return docs
	}
	return out
}

// listItems returns doc's items when doc is a Kubernetes List wrapper: a
// kind ending in "List" whose top-level items is a sequence of resource
// maps.
//
// The kind-suffix requirement (rather than apimachinery's items-presence-
// only IsList rule) keeps detection conservative on raw rendered YAML: the
// `<Kind>List` name is reserved by Kubernetes for auto-generated collection
// types, so no well-behaved singular resource ends in "List", and ordinary
// resources keep their data under spec rather than a top-level items of
// objects. The resource-maps requirement guards the remaining edge — a CRD
// like `AllowList` carrying a top-level items of scalars is left untouched
// rather than silently dropped.
func listItems(doc map[string]any) ([]any, bool) {
	kind := DocKind(doc)
	if kind == "" || !strings.HasSuffix(kind, "List") {
		return nil, false
	}
	items, ok := doc["items"].([]any)
	if !ok {
		return nil, false
	}
	for _, it := range items {
		if _, isMap := it.(map[string]any); !isMap && it != nil {
			return nil, false
		}
	}
	return items, true
}

// promoteListItem returns a List item as a standalone document. Items in
// typed lists (ConfigMapList → ConfigMap) usually already carry their own
// apiVersion/kind; when they don't, derive them from the List so the
// promoted document still has a Kubernetes identity. The generic "List"
// kind seeds no item kind (itemKind == ""), so its items — which must
// self-describe — are left as-is. A copy is taken only when a field must
// be injected, so the common already-typed path keeps aliasing the
// original map.
func promoteListItem(item map[string]any, itemKind, apiVersion string) map[string]any {
	needKind := itemKind != "" && DocKind(item) == ""
	needAPI := apiVersion != "" && DocAPIVersion(item) == ""
	if !needKind && !needAPI {
		return item
	}
	out := make(map[string]any, len(item)+2)
	maps.Copy(out, item)
	if needKind {
		out["kind"] = itemKind
	}
	if needAPI {
		out["apiVersion"] = apiVersion
	}
	return out
}
