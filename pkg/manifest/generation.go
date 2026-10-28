package manifest

type objectGenerator interface {
	GeneratedObjects() ([]BaseManifest, error)
}

// GeneratedObjects derives immutable auxiliary declarations when the manifest owns a rule.
func GeneratedObjects(obj BaseManifest) ([]BaseManifest, error) {
	if generator, ok := obj.(objectGenerator); ok {
		return generator.GeneratedObjects()
	}
	return nil, nil
}
