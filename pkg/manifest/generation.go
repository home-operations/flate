package manifest

type objectGenerator interface {
	GeneratedObjects() ([]BaseManifest, error)
	GeneratedTargets() ([]ProducerTarget, error)
}

// GeneratedObjects derives immutable auxiliary declarations when the manifest owns a rule.
func GeneratedObjects(obj BaseManifest) ([]BaseManifest, error) {
	if generator, ok := obj.(objectGenerator); ok {
		return generator.GeneratedObjects()
	}
	return nil, nil
}

// GeneratedTargets MUST share validation and identity with full auxiliary generation.
func GeneratedTargets(obj BaseManifest) ([]ProducerTarget, error) {
	if generator, ok := obj.(objectGenerator); ok {
		return generator.GeneratedTargets()
	}
	return nil, nil
}
