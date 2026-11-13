package artifactgenerator

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	maxPatternLength = 1024
	maxGlobCommas    = 20
	maxEntries       = 1000
	maxResourceName  = 253
)

var (
	aliasName  = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]*[a-z0-9])?$`)
	sourceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
)

func validateSpec(spec *manifest.ArtifactGeneratorSpec) error {
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
	names := map[string]bool{}
	for i := range spec.Artifacts {
		if err := validateArtifact(&spec.Artifacts[i], aliases, names); err != nil {
			return err
		}
	}
	return nil
}

func validateSource(input *manifest.ArtifactGeneratorSource, aliases map[string]bool) error {
	invalidAlias := len(input.Alias) > 63 || !aliasName.MatchString(input.Alias) || aliases[input.Alias]
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

func validateArtifact(artifact *manifest.ArtifactGeneratorOutput, aliases, names map[string]bool) error {
	if len(artifact.Name) < 1 || len(artifact.Name) > maxResourceName || names[artifact.Name] ||
		len(artifact.Copy) == 0 {
		return fmt.Errorf("invalid or duplicate artifact %q", artifact.Name)
	}
	names[artifact.Name] = true
	for _, selector := range []string{artifact.Revision, artifact.OriginRevision} {
		if selector != "" &&
			(len(selector) > 64 || !strings.HasPrefix(selector, "@") || !aliasName.MatchString(selector[1:])) {
			return fmt.Errorf("invalid revision selector %q", selector)
		}
	}
	if artifact.Revision != "" && !aliases[strings.TrimPrefix(artifact.Revision, "@")] {
		return fmt.Errorf("unknown revision source %q", artifact.Revision)
	}
	for i := range artifact.Copy {
		if err := validateOperation(&artifact.Copy[i]); err != nil {
			return err
		}
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
		return err
	}
	if _, err := destinationPath(op.To); err != nil {
		return err
	}
	if op.Strategy != "" && op.Strategy != "Overwrite" && op.Strategy != "Merge" && op.Strategy != strategyExtract {
		return fmt.Errorf("unsupported strategy %q", op.Strategy)
	}
	for _, pattern := range op.Exclude {
		if len(pattern) > maxPatternLength {
			return fmt.Errorf("exclude pattern outside API bounds")
		}
		if err := validateGlob(pattern); err != nil {
			return err
		}
		if _, err := doublestar.Match(pattern, "."); err != nil {
			return fmt.Errorf("match: %w", err)
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
