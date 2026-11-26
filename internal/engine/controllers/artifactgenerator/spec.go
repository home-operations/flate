package artifactgenerator

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
)

const (
	maxPatternLength = 1024
	maxGlobCommas    = 20
	maxEntries       = 1000
	maxResourceName  = 253
	maxAliasLength   = 63
)

var (
	aliasName  = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]*[a-z0-9])?$`)
	sourceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
)

type generatorSpec struct {
	declaration    manifest.ArtifactGeneratorSpec
	selectors      selectorResults
	copyModes      map[string]cache.CopyMode
	namespaceRules []namespaceMetadataRule
}

func validateSpec(spec *manifest.ArtifactGeneratorSpec, namespace string) (generatorSpec, error) {
	sourcesOutsideBounds := len(spec.Sources) < 1 || len(spec.Sources) > maxEntries
	artifactsOutsideBounds := len(spec.Artifacts) < 1 || len(spec.Artifacts) > maxEntries
	if sourcesOutsideBounds || artifactsOutsideBounds || len(spec.PathPattern) > maxPatternLength {
		return generatorSpec{}, fmt.Errorf("sources, artifacts or pathPattern outside API bounds")
	}
	aliases := make(map[string]bool, len(spec.Sources))
	for i := range spec.Sources {
		if err := validateSource(&spec.Sources[i], aliases); err != nil {
			return generatorSpec{}, err
		}
	}
	prepared := generatorSpec{declaration: *spec}
	identities := make(map[manifest.NamedResource]bool, len(spec.Artifacts))
	for i := range spec.Artifacts {
		artifact := &spec.Artifacts[i]
		resolved, _ := spec.ResolveOutput(artifact, namespace, nil, nil)
		id := resolved.Artifact
		if identities[id] {
			return generatorSpec{}, fmt.Errorf(
				"duplicate artifact name '%s' found in namespace '%s'",
				id.Name,
				id.Namespace,
			)
		}
		identities[id] = true
		if err := prepared.validateArtifact(artifact, id, aliases); err != nil {
			return generatorSpec{}, err
		}
	}
	ns := spec.ManagedNamespaces
	if ns == nil || ns.Metadata == nil {
		return prepared, nil
	}
	if source := ns.Metadata.FromSource; source != nil {
		if err := prepared.validateFileSelector(source.Path, aliases); err != nil {
			return generatorSpec{}, fmt.Errorf("namespaces metadata fromSource path %w", err)
		}
	}
	rules, err := resolveNamespaceMetadataRules(ns.Metadata)
	if err != nil {
		return generatorSpec{}, err
	}
	prepared.namespaceRules = rules
	return prepared, nil
}

func validateSource(input *manifest.ArtifactGeneratorSource, aliases map[string]bool) error {
	if aliases[input.Alias] {
		return fmt.Errorf("duplicate source alias '%s' found", input.Alias)
	}
	invalidAlias := len(input.Alias) > maxAliasLength || !aliasName.MatchString(input.Alias)
	invalidName := len(input.Name) > maxResourceName || !sourceName.MatchString(input.Name)
	invalidNamespace := input.NamespaceSet && (len(input.Namespace) > 63 || !sourceName.MatchString(input.Namespace))
	if invalidAlias || invalidName || invalidNamespace {
		return fmt.Errorf("invalid or duplicate source %q", input.Alias)
	}
	if !input.Supported() {
		return fmt.Errorf("unsupported source kind %q", input.Kind)
	}
	aliases[input.Alias] = true
	return nil
}

func (s *generatorSpec) validateArtifact(
	artifact *manifest.ArtifactGeneratorOutput, id manifest.NamedResource, aliases map[string]bool,
) error {
	if artifact.Name == "" || len(artifact.Name) > maxPatternLength || len(artifact.Namespace) > maxPatternLength ||
		len(artifact.Copy) == 0 {
		return fmt.Errorf("artifact %q outside API bounds", artifact.Name)
	}
	if s.declaration.PathPattern == "" {
		if err := validateArtifactIdentity(id, artifact.Namespace != "", "artifact"); err != nil {
			return err
		}
	}
	if err := s.validateRevision(artifact.Name, "revision", artifact.Revision, aliases); err != nil {
		return err
	}
	if err := s.validateRevision(artifact.Name, "origin revision", artifact.OriginRevision, aliases); err != nil {
		return err
	}
	if from := artifact.InputsFrom; from != "" {
		if len(from) > maxPatternLength {
			return fmt.Errorf("artifact %s inputsFrom outside API bounds", artifact.Name)
		}
		if selectorErr := s.validateFileSelector(from, aliases); selectorErr != nil {
			return fmt.Errorf("artifact %s inputsFrom %w", artifact.Name, selectorErr)
		}
	}
	for i := range artifact.Copy {
		if err := s.prepareOperation(&artifact.Copy[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *generatorSpec) validateRevision(name, kind, text string, aliases map[string]bool) error {
	if text == "" {
		return nil
	}
	result, err := s.selectors.resolve(text, revisionSelector, true)
	if err != nil {
		return err
	}
	if alias := result.selector.Alias; !aliases[alias] {
		return fmt.Errorf("artifact %s %s source alias '%s' not found", name, kind, alias)
	}
	return nil
}

func validateArtifactIdentity(id manifest.NamedResource, explicitNamespace bool, subject string) error {
	if errs := apivalidation.NameIsDNSSubdomain(id.Name, false); len(errs) != 0 {
		return fmt.Errorf(
			"%s name %q is not a valid Kubernetes object name: %s",
			subject,
			id.Name,
			strings.Join(errs, "; "),
		)
	}
	if explicitNamespace {
		if errs := apivalidation.ValidateNamespaceName(id.Namespace, false); len(errs) != 0 {
			return fmt.Errorf(
				"%s namespace %q is not a valid Kubernetes namespace: %s",
				subject,
				id.Namespace,
				strings.Join(errs, "; "),
			)
		}
	}
	return nil
}

func (s *generatorSpec) validateFileSelector(text string, aliases map[string]bool) error {
	result, err := s.selectors.resolve(text, fileSelector, true)
	if err != nil {
		return fmt.Errorf("%q: %w", text, err)
	}
	if selector := result.selector; !selector.StagedFile() && !aliases[selector.Alias] {
		return fmt.Errorf("%q: source alias '%s' not found", text, selector.Alias)
	}
	if s.declaration.PathPattern == "" && strings.ContainsAny(text, "{}") {
		return fmt.Errorf("%q uses capture placeholders but pathPattern is not set", text)
	}
	return nil
}

func (s *generatorSpec) prepareOperation(op *manifest.ArtifactGeneratorCopy) error {
	invalidFrom := len(op.From) < 1 || len(op.From) > maxPatternLength
	invalidTo := len(op.To) < 1 || len(op.To) > maxPatternLength
	if invalidFrom || invalidTo || len(op.Exclude) > 100 {
		return fmt.Errorf("copy operation outside API bounds")
	}
	if _, err := s.selectors.resolve(op.From, copySourceSelector, true); err != nil {
		return fmt.Errorf("invalid copy source '%s': %w", op.From, err)
	}
	if _, err := s.selectors.resolve(op.To, destinationSelector, true); err != nil {
		return fmt.Errorf("invalid copy destination '%s': %w", op.To, err)
	}
	var mode cache.CopyMode
	switch op.Strategy {
	case "", "Overwrite":
		mode = cache.CopyFiles
	case "Merge":
		mode = cache.MergeYAML
	case strategyExtract:
		mode = cache.ExtractArchive
	default:
		return fmt.Errorf("unsupported strategy %q", op.Strategy)
	}
	if mode != cache.CopyFiles {
		if s.copyModes == nil {
			s.copyModes = make(map[string]cache.CopyMode)
		}
		s.copyModes[op.Strategy] = mode
	}
	for _, pattern := range op.Exclude {
		if len(pattern) > maxPatternLength {
			return fmt.Errorf("exclude pattern outside API bounds")
		}
		if err := validateGlob(pattern); err != nil {
			return fmt.Errorf("invalid exclude pattern '%s': %w", pattern, err)
		}
		if !doublestar.ValidatePattern(pattern) {
			return fmt.Errorf("invalid exclude pattern '%s'", pattern)
		}
	}
	return nil
}

func validateGlob(pattern string) error {
	commas, depth, escaped := 0, 0, false
	for _, c := range pattern {
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		switch c {
		case '{':
			depth++
		case '}':
			depth--
		case ',':
			if depth > 0 {
				commas++
			}
		}
	}
	if commas > maxGlobCommas {
		return fmt.Errorf("glob contains more than 20 brace commas")
	}
	return nil
}
