package artifactgenerator

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"

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

func validateSpec(spec *manifest.ArtifactGeneratorSpec, namespace string) error {
	sourcesOutsideBounds := len(spec.Sources) < 1 || len(spec.Sources) > maxEntries
	artifactsOutsideBounds := len(spec.Artifacts) < 1 || len(spec.Artifacts) > maxEntries
	if sourcesOutsideBounds || artifactsOutsideBounds || len(spec.PathPattern) > maxPatternLength {
		return fmt.Errorf("sources, artifacts or pathPattern outside API bounds")
	}
	aliases := make(map[string]bool, len(spec.Sources))
	for i := range spec.Sources {
		if err := validateSource(&spec.Sources[i], aliases); err != nil {
			return err
		}
	}
	identities := make(map[manifest.NamedResource]bool, len(spec.Artifacts))
	for i := range spec.Artifacts {
		artifact := &spec.Artifacts[i]
		resolved, _ := spec.ResolveOutput(artifact, namespace, nil, nil)
		id := resolved.Artifact
		if identities[id] {
			return fmt.Errorf("duplicate artifact name '%s' found in namespace '%s'", id.Name, id.Namespace)
		}
		identities[id] = true
		if err := validateArtifact(artifact, id, spec.PathPattern, aliases); err != nil {
			return err
		}
	}
	if ns := spec.ManagedNamespaces; ns != nil && ns.Metadata != nil && ns.Metadata.FromSource != nil {
		if err := validateFileSelector(ns.Metadata.FromSource.Path, spec.PathPattern, aliases); err != nil {
			return fmt.Errorf("namespaces metadata fromSource path %w", err)
		}
	}
	return nil
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

func validateArtifact(
	artifact *manifest.ArtifactGeneratorOutput, id manifest.NamedResource, pattern string, aliases map[string]bool,
) error {
	if artifact.Name == "" || len(artifact.Name) > maxPatternLength || len(artifact.Namespace) > maxPatternLength ||
		len(artifact.Copy) == 0 {
		return fmt.Errorf("artifact %q outside API bounds", artifact.Name)
	}
	if pattern == "" {
		if err := validateArtifactIdentity(id, artifact.Namespace != "", "artifact"); err != nil {
			return err
		}
	}
	if err := validateRevisionSource(artifact.Name, "revision", artifact.Revision, aliases); err != nil {
		return err
	}
	if err := validateRevisionSource(artifact.Name, "origin revision", artifact.OriginRevision, aliases); err != nil {
		return err
	}
	if from := artifact.InputsFrom; from != "" {
		if len(from) > maxPatternLength {
			return fmt.Errorf("artifact %s inputsFrom outside API bounds", artifact.Name)
		}
		if err := validateFileSelector(from, pattern, aliases); err != nil {
			return fmt.Errorf("artifact %s inputsFrom %w", artifact.Name, err)
		}
	}
	for i := range artifact.Copy {
		if err := validateOperation(&artifact.Copy[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateRevisionSource(name, kind, selector string, aliases map[string]bool) error {
	if selector == "" {
		return nil
	}
	alias, marked := strings.CutPrefix(selector, "@")
	if !marked || len(alias) > maxAliasLength || !aliasName.MatchString(alias) {
		return fmt.Errorf("invalid revision selector %q", selector)
	}
	if !aliases[alias] {
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

func validateFileSelector(value, pattern string, aliases map[string]bool) error {
	selector, err := splitFileSelector(value)
	if err != nil {
		return fmt.Errorf("%q: %w", value, err)
	}
	if alias := selector.alias; alias != artifactAlias && !aliases[alias] {
		return fmt.Errorf("%q: source alias '%s' not found", value, alias)
	}
	if pattern == "" && strings.ContainsAny(value, "{}") {
		return fmt.Errorf("%q uses capture placeholders but pathPattern is not set", value)
	}
	return nil
}

func validateOperation(op *manifest.ArtifactGeneratorCopy) error {
	invalidFrom := len(op.From) < 1 || len(op.From) > maxPatternLength
	invalidTo := len(op.To) < 1 || len(op.To) > maxPatternLength
	if invalidFrom || invalidTo || len(op.Exclude) > 100 {
		return fmt.Errorf("copy operation outside API bounds")
	}
	if _, err := sourcePath(op.From); err != nil {
		return fmt.Errorf("invalid copy source '%s': %w", op.From, err)
	}
	if _, err := destinationPath(op.To); err != nil {
		return fmt.Errorf("invalid copy destination '%s': %w", op.To, err)
	}
	if op.Strategy != "" && op.Strategy != "Overwrite" && op.Strategy != "Merge" && op.Strategy != strategyExtract {
		return fmt.Errorf("unsupported strategy %q", op.Strategy)
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
