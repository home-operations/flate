package manifest

import fluxapis "github.com/fluxcd/pkg/apis/kustomize"

// Generation carries immutable auxiliary objects, visible documents, and ordered patches.
type Generation struct {
	Objects   []BaseManifest
	Documents []DocOccurrence
	Patches   []fluxapis.Patch
}

type objectGenerator interface {
	GeneratedObjects() (Generation, error)
	GeneratedTargets() ([]ProducerTarget, error)
}

// GeneratedObjects derives immutable auxiliary declarations when the manifest owns a rule.
func GeneratedObjects(obj BaseManifest) (Generation, error) {
	if generator, ok := obj.(objectGenerator); ok {
		return generator.GeneratedObjects()
	}
	return Generation{}, nil
}

// GeneratedTargets MUST share validation and identity with full auxiliary generation.
func GeneratedTargets(obj BaseManifest) ([]ProducerTarget, error) {
	if generator, ok := obj.(objectGenerator); ok {
		return generator.GeneratedTargets()
	}
	return nil, nil
}
