package manifest

import (
	"bytes"
	"cmp"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"
)

// ArtifactGeneratorSpec must retain its JSON layout because it participates in content fingerprints.
//
//nolint:recvcheck // Value snapshots need the JSON method set, while decoder helpers mutate through pointers.
type ArtifactGeneratorSpec struct {
	sourcewatcherv1.ArtifactGeneratorSpec

	sourceNamespaces []bool
	emptyStrategies  map[[2]int]struct{}
}

// SourceNamespaceSet reports whether the canonical source at position declares a namespace.
func (s *ArtifactGeneratorSpec) SourceNamespaceSet(position int) bool {
	return position >= 0 && position < len(s.sourceNamespaces) && s.sourceNamespaces[position]
}

// EmptyCopyStrategy reports whether an explicitly empty string survives at the copy position.
func (s *ArtifactGeneratorSpec) EmptyCopyStrategy(artifact, operation int) bool {
	_, present := s.emptyStrategies[[2]int{artifact, operation}]
	return present
}

// SupportsArtifactGeneratorSource keeps input capability narrower than source-group membership.
func SupportsArtifactGeneratorSource(kind string) bool {
	switch kind {
	case KindGitRepository, KindOCIRepository, KindBucket, KindHelmChart, KindExternalArtifact:
		return true
	default:
		return false
	}
}

type artifactGeneratorDecodeView struct {
	sourcewatcherv1.ArtifactGeneratorSpec

	// A JSON null must leave metadata from an earlier spelling of the key intact.
	CommonMetadata sourcewatcherv1.CommonMetadata `json:"commonMetadata"`
}

type artifactGeneratorDecode struct {
	kind, apiVersion string
	spec             ArtifactGeneratorSpec
	err              error
	compact          *artifactGeneratorDecode
	// Only the canonical sources list supplies indexed declarations.
	sources []sourcewatcherv1.SourceReference
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
					compact.ManagedNamespaces = &sourcewatcherv1.ManagedNamespaces{}
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
		view          artifactGeneratorDecodeView
	})
	result, compact, view := &decoded.full, &decoded.compact, &decoded.view
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
			compact.err = view.decode(projected, &compact.spec, false)
			compact.spec.recordEmptyCopyStrategies(projected)
		}
		return result
	}

	partition := artifactGeneratorPartition{data: data}
	if err := jsonv2.Unmarshal(data, &partition, json.DefaultOptionsV1()); err != nil {
		result.err = fmt.Errorf("decode spec: %w", err)
		return result
	}
	retained := view.decode(partition.retained, &compact.spec, false)
	compact.spec.recordEmptyCopyStrategies(partition.retained)
	result.spec = compact.spec
	omitted := view.decode(partition.omitted, &result.spec, partition.commonMetadata)
	if retained != nil || omitted != nil {
		result.spec = ArtifactGeneratorSpec{emptyStrategies: compact.spec.emptyStrategies}
		result.err = view.decode(data, &result.spec, partition.commonMetadata)
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
			compact.err = view.decode(projected, &compact.spec, false)
			compact.spec.recordEmptyCopyStrategies(projected)
		}
	}

	sources, _ := fields["sources"].([]any)
	if len(sources) != len(result.spec.Sources) {
		result.err = cmp.Or(result.err, errors.New("sources must be a list"))
		return result
	}
	for i, source := range sources {
		fields, _ := source.(map[string]any)
		if _, present := fields[namespaceKey]; !present {
			continue
		}
		if result.spec.sourceNamespaces == nil {
			result.spec.sourceNamespaces = make([]bool, len(sources))
		}
		result.spec.sourceNamespaces[i] = true
	}
	result.sources = result.spec.Sources
	return result
}

type artifactGeneratorPartition struct {
	data              []byte
	retained, omitted []byte
	buffer            [2][256]byte
	commonMetadata    bool
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
		name := key.String()
		commonMetadata := strings.EqualFold(name, "commonMetadata")
		part := &p.omitted
		if retainedArtifactGeneratorCatalogField(name) >= 0 {
			part = &p.retained
		}
		value, err := input.ReadValue()
		if err != nil {
			return err
		}
		if commonMetadata && value.Kind() == '{' {
			p.commonMetadata = true
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

func (v *artifactGeneratorDecodeView) decode(data []byte, spec *ArtifactGeneratorSpec, commonMetadata bool) error {
	if len(data) == 0 {
		return nil
	}
	*v = artifactGeneratorDecodeView{ArtifactGeneratorSpec: spec.ArtifactGeneratorSpec}
	err := json.Unmarshal(data, v)
	spec.ArtifactGeneratorSpec = v.ArtifactGeneratorSpec
	if commonMetadata {
		metadata := v.CommonMetadata
		spec.CommonMetadata = &metadata
	}
	if err != nil {
		return fmt.Errorf("decode spec: %w", err)
	}
	return nil
}

type emptyCopyStrategy bool

func (s *emptyCopyStrategy) UnmarshalJSON(data []byte) error {
	if data[0] == '"' {
		*s = emptyCopyStrategy(bytes.Equal(data, []byte(`""`)))
	}
	return nil
}

func (s *ArtifactGeneratorSpec) recordEmptyCopyStrategies(data []byte) {
	// Marshal compacts JSON before partitioning it. Only an empty string can
	// leave a copy strategy that differs from the omitted default.
	if !bytes.Contains(data, []byte(`:""`)) {
		return
	}
	var presence struct {
		Artifacts []struct {
			Copy []struct {
				Strategy emptyCopyStrategy `json:"strategy"`
			} `json:"copy"`
		} `json:"artifacts"`
	}
	// Use the same matching and slice reuse rules as the specification decode.
	// Type errors belong to the specification decode, which also retains partial data.
	_ = json.Unmarshal(data, &presence)
	for i, artifact := range presence.Artifacts {
		for j, operation := range artifact.Copy {
			if operation.Strategy {
				if s.emptyStrategies == nil {
					s.emptyStrategies = make(map[[2]int]struct{})
				}
				s.emptyStrategies[[2]int{i, j}] = struct{}{}
			}
		}
	}
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
	decoded.spec.ArtifactGeneratorSpec.DeepCopyInto(&spec.ArtifactGeneratorSpec)
	return spec, decoded.err
}

func (s *ArtifactGeneratorSpec) namespaceMetadataPath() string {
	m := s.ManagedNamespaces
	if m == nil || m.Metadata == nil || m.Metadata.FromSource == nil {
		return ""
	}
	return m.Metadata.FromSource.Path
}
