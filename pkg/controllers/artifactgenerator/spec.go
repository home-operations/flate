package artifactgenerator

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	maxPatternLength = 1024
	maxGlobCommas    = 20
)

type specification struct {
	CommonMetadata struct {
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"commonMetadata"`
	Sources     []source `json:"sources"`
	PathPattern string   `json:"pathPattern"`
	Artifacts   []output `json:"artifacts"`
}

type source struct {
	Alias     string `json:"alias"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
}

type output struct {
	Name           string      `json:"name"`
	Revision       string      `json:"revision"`
	OriginRevision string      `json:"originRevision"`
	Copy           []operation `json:"copy"`
}

type operation struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Strategy string   `json:"strategy"`
	Exclude  []string `json:"exclude"`
}

var (
	aliasName  = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]*[a-z0-9])?$`)
	sourceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
)

func decodeSpec(raw map[string]any) (specification, error) {
	var spec specification
	data, err := json.Marshal(raw)
	if err != nil {
		return spec, fmt.Errorf("marshal spec: %w", err)
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("decode spec: %w", err)
	}
	if len(spec.Sources) < 1 || len(spec.Sources) > 1000 || len(spec.Artifacts) < 1 || len(spec.Artifacts) > 1000 ||
		len(spec.PathPattern) > maxPatternLength {
		return spec, fmt.Errorf("sources, artifacts or pathPattern outside API bounds")
	}
	aliases := make(map[string]bool, len(spec.Sources))
	rawSources, ok := raw["sources"].([]any)
	if !ok {
		return spec, fmt.Errorf("sources must be a list")
	}
	for i := range spec.Sources {
		if err := validateSource(&spec.Sources[i], rawSources[i], aliases); err != nil {
			return spec, err
		}
	}
	names := map[string]bool{}
	for i := range spec.Artifacts {
		if err := validateArtifact(&spec.Artifacts[i], aliases, names); err != nil {
			return spec, err
		}
	}
	return spec, nil
}

func validateSource(input *source, raw any, aliases map[string]bool) error {
	fields, _ := raw.(map[string]any)
	_, explicitNamespace := fields["namespace"]
	if len(input.Alias) > 63 || !aliasName.MatchString(input.Alias) || aliases[input.Alias] || len(input.Name) > 253 ||
		!sourceName.MatchString(
			input.Name,
		) || explicitNamespace && (len(input.Namespace) > 63 || !sourceName.MatchString(input.Namespace)) {
		return fmt.Errorf("invalid or duplicate source %q", input.Alias)
	}
	if input.Kind != manifest.KindGitRepository && input.Kind != manifest.KindOCIRepository &&
		input.Kind != manifest.KindBucket &&
		input.Kind != manifest.KindHelmChart &&
		input.Kind != manifest.KindExternalArtifact {
		return fmt.Errorf("unsupported source kind %q", input.Kind)
	}
	aliases[input.Alias] = true
	return nil
}

func validateArtifact(artifact *output, aliases, names map[string]bool) error {
	if len(artifact.Name) < 1 || len(artifact.Name) > 253 || names[artifact.Name] || len(artifact.Copy) == 0 {
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

func validateOperation(op *operation) error {
	if len(op.From) < 1 || len(op.From) > maxPatternLength || len(op.To) < 1 || len(op.To) > maxPatternLength ||
		len(op.Exclude) > 100 {
		return fmt.Errorf("copy operation outside API bounds")
	}
	sourcePathResult1, sourcePathResult1Err := sourcePath(op.From)
	if _, _, err := sourcePathResult1.alias, sourcePathResult1.pattern, sourcePathResult1Err; err != nil {
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
