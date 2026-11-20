package manifest

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
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

type artifactGeneratorDecode struct {
	spec ArtifactGeneratorSpec
	err  error
	// Only the canonical sources list supplies indexed declarations.
	sources []ArtifactGeneratorSource
}

func decodeArtifactGeneratorSpec(fields map[string]any) *artifactGeneratorDecode {
	result := &artifactGeneratorDecode{}
	data, err := json.Marshal(fields)
	if err != nil {
		result.err = fmt.Errorf("marshal spec: %w", err)
		return result
	}
	if err := json.Unmarshal(data, &result.spec); err != nil {
		result.err = fmt.Errorf("decode spec: %w", err)
	}
	sources, _ := fields["sources"].([]any)
	if len(sources) != len(result.spec.Sources) {
		if result.err == nil {
			result.err = fmt.Errorf("sources must be a list")
		}
		return result
	}
	for i, source := range sources {
		fields, _ := source.(map[string]any)
		_, result.spec.Sources[i].NamespaceSet = fields[namespaceKey]
	}
	result.sources = result.spec.Sources
	return result
}

func (r *RawObject) initArtifactGeneratorSpec() *RawObject {
	if r.isArtifactGenerator() {
		r.generator = decodeArtifactGeneratorSpec(r.Spec)
	}
	return r
}

func (r *RawObject) artifactGeneratorSpec() *artifactGeneratorDecode {
	if r.generator != nil {
		return r.generator
	}
	return decodeArtifactGeneratorSpec(r.Spec)
}

// ArtifactGeneratorSpec returns an independent snapshot, including partial data on decode errors.
func (r *RawObject) ArtifactGeneratorSpec() (ArtifactGeneratorSpec, error) {
	decoded := r.artifactGeneratorSpec()
	if r.generator == nil {
		return decoded.spec, decoded.err
	}
	spec := decoded.spec
	spec.CommonMetadata.Labels = maps.Clone(spec.CommonMetadata.Labels)
	spec.CommonMetadata.Annotations = maps.Clone(spec.CommonMetadata.Annotations)
	spec.Sources = slices.Clone(spec.Sources)
	spec.Artifacts = slices.Clone(spec.Artifacts)
	for i := range spec.Artifacts {
		artifact := &spec.Artifacts[i]
		artifact.Copy = slices.Clone(artifact.Copy)
		for j := range artifact.Copy {
			artifact.Copy[j].Exclude = slices.Clone(artifact.Copy[j].Exclude)
		}
	}
	return spec, decoded.err
}
