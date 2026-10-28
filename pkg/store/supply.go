package store

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// SuppliedObject associates a supplied Secret or ConfigMap with its input file.
type SuppliedObject struct {
	Object manifest.BaseManifest
	Path   string
}

// Supplied owns an immutable invocation snapshot separate from canonical objects.
type Supplied struct {
	objects map[manifest.NamedResource]SuppliedObject
}

// NewSupplied validates and snapshots inputs with whole-object last-input precedence.
func NewSupplied(inputs []SuppliedObject) (*Supplied, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	s := &Supplied{objects: make(map[manifest.NamedResource]SuppliedObject, len(inputs))}
	for i, input := range inputs {
		data, ok := input.Object.(manifest.Data)
		origin := fmt.Sprintf("supply %q input %d", cmp.Or(input.Path, "SDK input"), i+1)
		if !ok || reflect.ValueOf(data).Kind() == reflect.Pointer && reflect.ValueOf(data).IsNil() || input.Object.Named().Name == "" || input.Object.Named().Namespace == "" {
			return nil, fmt.Errorf("%w: %s: expected a core v1 Secret or ConfigMap with name and namespace", manifest.ErrInput, origin)
		}
		id := input.Object.Named()
		for _, key := range slices.Sorted(data.Keys()) {
			if _, _, err := data.Value(key); err != nil {
				return nil, fmt.Errorf("%w: %s %s key %q: invalid supplied data", manifest.ErrInput, origin, id, key)
			}
		}
		obj, _, _, err := manifest.ParseDoc(data.Snapshot(), manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
		if err != nil {
			return nil, fmt.Errorf("%w: %s %s: invalid supplied object", manifest.ErrInput, origin, id)
		}
		s.objects[id] = SuppliedObject{Object: obj, Path: cmp.Or(input.Path, "SDK input")}
	}
	return s, nil
}

// SetSupplied installs the immutable snapshot before publication or concurrent reads.
func (s *Store) SetSupplied(supplied *Supplied) { s.supplied = supplied }

func (s *Store) suppliedObject(id manifest.NamedResource) (manifest.Data, string) {
	if s.supplied == nil {
		return nil, ""
	}
	input := s.supplied.objects[id]
	data, _ := input.Object.(manifest.Data)
	return data, input.Path
}
