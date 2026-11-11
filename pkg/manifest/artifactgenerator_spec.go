package manifest

import (
	"encoding/json"
	"fmt"
)

// ArtifactGeneratorSpec must retain its JSON layout because it participates in content fingerprints.
type ArtifactGeneratorSpec struct {
	CommonMetadata struct {
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"commonMetadata"`
	Sources     []ArtifactGeneratorSource `json:"sources"`
	PathPattern string                    `json:"pathPattern"`
	Artifacts   []ArtifactGeneratorOutput `json:"artifacts"`
}

// ArtifactGeneratorSource keeps namespace presence outside its serialized content fingerprint.
type ArtifactGeneratorSource struct {
	Alias        string `json:"alias"`
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	Kind         string `json:"kind"`
	NamespaceSet bool   `json:"-"`
}

// Supported keeps generator input capability narrower than source-group membership.
func (s ArtifactGeneratorSource) Supported() bool {
	switch s.Kind {
	case KindGitRepository, KindOCIRepository, KindBucket, KindHelmChart, KindExternalArtifact:
		return true
	default:
		return false
	}
}

// ArtifactGeneratorOutput must retain authored revision selectors in content fingerprints.
type ArtifactGeneratorOutput struct {
	Name           string                  `json:"name"`
	Revision       string                  `json:"revision"`
	OriginRevision string                  `json:"originRevision"`
	Copy           []ArtifactGeneratorCopy `json:"copy"`
}

// ArtifactGeneratorCopy must retain excluded paths in the serialized execution specification.
type ArtifactGeneratorCopy struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Strategy string   `json:"strategy"`
	Exclude  []string `json:"exclude"`
}

// Declaration reads must borrow containers and ignore unrelated execution fields.
type artifactGeneratorView struct {
	fields map[string]any
}

func (r *RawObject) artifactGeneratorView() artifactGeneratorView {
	return artifactGeneratorView{fields: r.Spec}
}

func (v artifactGeneratorView) str(key string) string {
	value, _ := v.fields[key].(string)
	return value
}

func (v artifactGeneratorView) entries(key string) []any {
	value, _ := v.fields[key].([]any)
	return value
}

func (v artifactGeneratorView) child(value any) artifactGeneratorView {
	fields, _ := value.(map[string]any)
	return artifactGeneratorView{fields: fields}
}

func (v artifactGeneratorView) source(value any) ArtifactGeneratorSource {
	child := v.child(value)
	_, namespaceSet := child.fields[namespaceKey]
	return ArtifactGeneratorSource{
		Alias: child.str("alias"), Name: child.str(nameKey), Namespace: child.str(namespaceKey),
		Kind: child.str(kindKey), NamespaceSet: namespaceSet,
	}
}

// ArtifactGeneratorSpec must retain encoding/json field matching and type acceptance.
func (r *RawObject) ArtifactGeneratorSpec() (ArtifactGeneratorSpec, error) {
	v := r.artifactGeneratorView()
	var spec ArtifactGeneratorSpec
	data, err := json.Marshal(v.fields)
	if err != nil {
		return spec, fmt.Errorf("marshal spec: %w", err)
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("decode spec: %w", err)
	}
	sources := v.entries("sources")
	if len(sources) != len(spec.Sources) {
		return spec, fmt.Errorf("sources must be a list")
	}
	for i, raw := range sources {
		spec.Sources[i].NamespaceSet = v.source(raw).NamespaceSet
	}
	return spec, nil
}
