package manifest

import (
	"bytes"
	"cmp"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
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
	kind, apiVersion string
	spec             ArtifactGeneratorSpec
	err              error
	compact          *artifactGeneratorDecode
	// Only the canonical sources list supplies indexed declarations.
	sources []ArtifactGeneratorSource
}

type artifactGeneratorCatalogRule struct {
	key    string
	marker func(full, compact *ArtifactGeneratorSpec) bool
}

func artifactGeneratorCatalogRules() [3]artifactGeneratorCatalogRule {
	return [...]artifactGeneratorCatalogRule{
		{key: "artifacts"},
		{key: "pathPattern"},
		{
			key: "managedNamespaces",
			marker: func(full, compact *ArtifactGeneratorSpec) bool {
				if full.ManagedNamespaces == nil {
					return false
				}
				if compact != nil {
					compact.ManagedNamespaces = &ArtifactGeneratorManagedNamespaces{}
				}
				return true
			},
		},
	}
}

func retainedArtifactGeneratorCatalogField(key string) int {
	rules := artifactGeneratorCatalogRules()
	return slices.IndexFunc(rules[:], func(rule artifactGeneratorCatalogRule) bool {
		return rule.marker == nil && strings.EqualFold(key, rule.key)
	})
}

func (d *artifactGeneratorDecode) catalogFields(fields map[string]any) map[string]any {
	rules := artifactGeneratorCatalogRules()
	catalog := make(map[string]any, len(rules))
	var present [len(rules)]bool
	for key, value := range fields {
		if i := retainedArtifactGeneratorCatalogField(key); i >= 0 {
			catalog[key], present[i] = value, true
		}
	}
	for i, rule := range rules {
		if rule.marker == nil {
			if !present[i] {
				catalog[rule.key] = nil
			}
		} else if rule.marker(&d.spec, nil) {
			catalog[rule.key] = map[string]any{}
		}
	}
	return catalog
}

func (r *RawObject) decodeArtifactGeneratorSpec() *artifactGeneratorDecode {
	decoded := new(struct {
		full, compact artifactGeneratorDecode
	})
	result, compact := &decoded.full, &decoded.compact
	result.kind, result.apiVersion = r.Kind, r.APIVersion
	compact.kind, compact.apiVersion = r.Kind, r.APIVersion
	result.compact, compact.compact = compact, compact
	fields := r.Spec
	data, err := json.Marshal(fields)
	if err != nil {
		result.err = fmt.Errorf("marshal spec: %w", err)
		projected, catalogErr := json.Marshal(compact.catalogFields(fields))
		if catalogErr != nil {
			compact.err = fmt.Errorf("marshal spec: %w", catalogErr)
		} else {
			compact.err = decodeArtifactGeneratorFields(projected, &compact.spec)
		}
		return result
	}

	partition := artifactGeneratorPartition{data: data}
	if err := jsonv2.Unmarshal(data, &partition, json.DefaultOptionsV1()); err != nil {
		result.err = fmt.Errorf("decode spec: %w", err)
		return result
	}
	retained := decodeArtifactGeneratorFields(partition.retained, &compact.spec)
	result.spec = compact.spec
	omitted := decodeArtifactGeneratorFields(partition.omitted, &result.spec)
	if retained != nil || omitted != nil {
		result.spec = ArtifactGeneratorSpec{}
		result.err = decodeArtifactGeneratorFields(data, &result.spec)
	}
	for _, rule := range artifactGeneratorCatalogRules() {
		if rule.marker != nil {
			rule.marker(&result.spec, &compact.spec)
		}
	}
	if retained != nil {
		projected, err := json.Marshal(compact.catalogFields(fields))
		if err != nil {
			compact.err = fmt.Errorf("marshal spec: %w", err)
		} else {
			compact.spec = ArtifactGeneratorSpec{}
			compact.err = decodeArtifactGeneratorFields(projected, &compact.spec)
		}
	}

	sources, _ := fields["sources"].([]any)
	if len(sources) != len(result.spec.Sources) {
		result.err = cmp.Or(result.err, errors.New("sources must be a list"))
		return result
	}
	for i, source := range sources {
		fields, _ := source.(map[string]any)
		_, result.spec.Sources[i].NamespaceSet = fields[namespaceKey]
	}
	result.sources = result.spec.Sources
	return result
}

type artifactGeneratorPartition struct {
	data              []byte
	retained, omitted []byte
	buffer            [2][256]byte
}

//nolint:wrapcheck // The caller wraps decoder errors with the specification operation.
func (p *artifactGeneratorPartition) UnmarshalJSONFrom(input *jsontext.Decoder) error {
	// Marshal produced one sorted object. Partition its original members so
	// colliding spellings reach the same typed field in their original order.
	root, rootErr := input.ReadToken()
	if rootErr != nil || root.Kind() == 'n' {
		return rootErr
	}
	p.retained, p.omitted = p.buffer[0][:0], p.buffer[1][:0]
	for input.PeekKind() == '"' {
		start := input.InputOffset()
		key, err := input.ReadToken()
		if err != nil {
			return err
		}
		part := &p.omitted
		if retainedArtifactGeneratorCatalogField(key.String()) >= 0 {
			part = &p.retained
		}
		if _, err := input.ReadValue(); err != nil {
			return err
		}
		separator := byte('{')
		if len(*part) != 0 {
			separator = ','
		}
		*part = append(append(*part, separator), bytes.TrimPrefix(p.data[start:input.InputOffset()], []byte(","))...)
	}
	if len(p.retained) != 0 {
		p.retained = append(p.retained, '}')
	}
	if len(p.omitted) != 0 {
		p.omitted = append(p.omitted, '}')
	}
	_, closeErr := input.ReadToken()
	return closeErr
}

func decodeArtifactGeneratorFields(data []byte, spec *ArtifactGeneratorSpec) error {
	if len(data) != 0 {
		if err := json.Unmarshal(data, spec); err != nil {
			return fmt.Errorf("decode spec: %w", err)
		}
	}
	return nil
}

func (r *RawObject) initArtifactGeneratorSpec() *RawObject {
	if r.Named().IsArtifactGenerator() {
		r.generator = r.decodeArtifactGeneratorSpec()
	}
	return r
}

func (r *RawObject) artifactGeneratorSpec() *artifactGeneratorDecode {
	if r.generator != nil {
		return r.generator
	}
	return r.decodeArtifactGeneratorSpec()
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
