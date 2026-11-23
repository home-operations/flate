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
	Sources            []ArtifactGeneratorSource           `json:"sources"`
	PathPattern        string                              `json:"pathPattern"`
	Artifacts          []ArtifactGeneratorOutput           `json:"artifacts"`
	ManagedNamespaces  *ArtifactGeneratorManagedNamespaces `json:"managedNamespaces,omitempty"`
	ServiceAccountName string                              `json:"serviceAccountName,omitempty"`
}

// ArtifactGeneratorManagedNamespaces is present even when its name policy is empty.
type ArtifactGeneratorManagedNamespaces struct {
	AllowedNames []string                            `json:"allowedNames"`
	Prune        bool                                `json:"prune"`
	Metadata     *ArtifactGeneratorNamespaceMetadata `json:"metadata,omitempty"`
}

// ArtifactGeneratorNamespaceMetadata applies From entries in declaration order after FromSource.
type ArtifactGeneratorNamespaceMetadata struct {
	FromSource *ArtifactGeneratorNamespaceMetadataSource `json:"fromSource,omitempty"`
	From       []ArtifactGeneratorNamespaceMetadataEntry `json:"from,omitempty"`
}

// ArtifactGeneratorNamespaceMetadataSource only admits keys listed in its assertions.
type ArtifactGeneratorNamespaceMetadataSource struct {
	Path               string            `json:"path"`
	AllowedAnnotations map[string]string `json:"allowedAnnotations,omitempty"`
	AllowedLabels      map[string]string `json:"allowedLabels,omitempty"`
}

// ArtifactGeneratorNamespaceMetadataEntry can use "*" to apply to every managed namespace.
type ArtifactGeneratorNamespaceMetadataEntry struct {
	Strategy    string            `json:"strategy"`
	Namespace   string            `json:"namespace"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
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
	Namespace      string                  `json:"namespace"`
	InputsFrom     string                  `json:"inputsFrom"`
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
	Optional bool     `json:"optional"`
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
	spec.ManagedNamespaces = spec.ManagedNamespaces.clone()
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

func (m *ArtifactGeneratorManagedNamespaces) clone() *ArtifactGeneratorManagedNamespaces {
	if m == nil {
		return nil
	}
	out := *m
	out.AllowedNames = slices.Clone(m.AllowedNames)
	if m.Metadata == nil {
		return &out
	}
	metadata := *m.Metadata
	out.Metadata = &metadata
	if source := metadata.FromSource; source != nil {
		cloned := *source
		cloned.AllowedAnnotations = maps.Clone(source.AllowedAnnotations)
		cloned.AllowedLabels = maps.Clone(source.AllowedLabels)
		metadata.FromSource = &cloned
	}
	metadata.From = slices.Clone(metadata.From)
	for i := range metadata.From {
		entry := &metadata.From[i]
		entry.Annotations = maps.Clone(entry.Annotations)
		entry.Labels = maps.Clone(entry.Labels)
	}
	return &out
}

func (s *ArtifactGeneratorSpec) namespaceMetadataPath() string {
	m := s.ManagedNamespaces
	if m == nil || m.Metadata == nil || m.Metadata.FromSource == nil {
		return ""
	}
	return m.Metadata.FromSource.Path
}
