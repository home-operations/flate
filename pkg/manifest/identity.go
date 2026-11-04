package manifest

import (
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

// ValidateIdentity requires authored identity, using CRD definitions when scope is known.
func ValidateIdentity(doc map[string]any, definitions []map[string]any) error {
	id := DocIdentity(doc)
	version := DocAPIVersion(doc)
	if id.Kind == "" || version == "" || id.Name == "" {
		return inputf("missing required object identity")
	}
	if gv, err := schema.ParseGroupVersion(version); err != nil || gv.Version == "" {
		return inputf("invalid apiVersion")
	}
	if id.Namespace != "" {
		return nil
	}
	namespaced, known := scopes.ResolveScope(yaml.TypeMeta{APIVersion: version, Kind: id.Kind})
	known = known || id.KindInfo().Tracked
	for _, definition := range definitions {
		if !DocIdentity(definition).IsCRD() {
			continue
		}
		spec, _ := definition["spec"].(map[string]any)
		group, _ := spec["group"].(string)
		names, _ := spec["names"].(map[string]any)
		kind, _ := names["kind"].(string)
		if group != id.Group || kind != id.Kind {
			continue
		}
		scope, _ := spec["scope"].(string)
		if scope == "Cluster" || scope == "Namespaced" {
			namespaced, known = scope == "Namespaced", true
		}
	}
	if known && namespaced {
		return inputf("missing required object identity")
	}
	return nil
}

func validateDataDocument(doc map[string]any, data Data) error {
	for _, field := range []string{dataKey, binaryDataKey, stringDataKey} {
		if value := doc[field]; value != nil {
			if _, ok := value.(map[string]any); !ok {
				return inputf("invalid data mapping")
			}
		}
	}
	for _, key := range slices.Sorted(data.Keys()) {
		if _, _, err := data.Value(key); err != nil {
			return inputf("invalid data value")
		}
	}
	return nil
}
