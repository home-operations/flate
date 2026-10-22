package artifactgenerator

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/home-operations/flate/pkg/manifest"
)

type specification struct {
	CommonMetadata struct{ Labels, Annotations map[string]string } `json:"commonMetadata"`
	Sources        []source                                        `json:"sources"`
	PathPattern    string                                          `json:"pathPattern"`
	Artifacts      []output                                        `json:"artifacts"`
}
type source struct{ Alias, Name, Namespace, Kind string }
type output struct {
	Name, Revision, OriginRevision string
	Copy                           []operation
}
type operation struct {
	From, To, Strategy string
	Exclude            []string
}

var aliasName = regexp.MustCompile(`^[a-z0-9]([a-z0-9_-]*[a-z0-9])?$`)
var sourceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func decodeSpec(raw map[string]any) (specification, error) {
	var spec specification
	data, err := json.Marshal(raw)
	if err != nil {
		return spec, fmt.Errorf("marshal spec: %w", err)
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("decode spec: %w", err)
	}
	if len(spec.Sources) < 1 || len(spec.Sources) > 1000 || len(spec.Artifacts) < 1 || len(spec.Artifacts) > 1000 || len(spec.PathPattern) > 1024 {
		return spec, fmt.Errorf("sources, artifacts or pathPattern outside API bounds")
	}
	aliases := make(map[string]bool, len(spec.Sources))
	rawSources, ok := raw["sources"].([]any)
	if !ok {
		return spec, fmt.Errorf("sources must be a list")
	}
	for i, source := range spec.Sources {
		fields, _ := rawSources[i].(map[string]any)
		_, explicitNamespace := fields["namespace"]
		if len(source.Alias) > 63 || !aliasName.MatchString(source.Alias) || aliases[source.Alias] || len(source.Name) > 253 || !sourceName.MatchString(source.Name) || explicitNamespace && (len(source.Namespace) > 63 || !sourceName.MatchString(source.Namespace)) {
			return spec, fmt.Errorf("invalid or duplicate source %q", source.Alias)
		}
		if source.Kind != manifest.KindGitRepository && source.Kind != manifest.KindOCIRepository && source.Kind != manifest.KindBucket && source.Kind != manifest.KindHelmChart && source.Kind != manifest.KindExternalArtifact {
			return spec, fmt.Errorf("unsupported source kind %q", source.Kind)
		}
		aliases[source.Alias] = true
	}
	names := map[string]bool{}
	for _, artifact := range spec.Artifacts {
		if len(artifact.Name) < 1 || len(artifact.Name) > 253 || names[artifact.Name] || len(artifact.Copy) == 0 {
			return spec, fmt.Errorf("invalid or duplicate artifact %q", artifact.Name)
		}
		names[artifact.Name] = true
		for _, selector := range []string{artifact.Revision, artifact.OriginRevision} {
			if selector != "" && (len(selector) > 64 || !strings.HasPrefix(selector, "@") || !aliasName.MatchString(selector[1:])) {
				return spec, fmt.Errorf("invalid revision selector %q", selector)
			}
		}
		if artifact.Revision != "" && !aliases[strings.TrimPrefix(artifact.Revision, "@")] {
			return spec, fmt.Errorf("unknown revision source %q", artifact.Revision)
		}
		for _, op := range artifact.Copy {
			if len(op.From) < 1 || len(op.From) > 1024 || len(op.To) < 1 || len(op.To) > 1024 || len(op.Exclude) > 100 {
				return spec, fmt.Errorf("copy operation outside API bounds")
			}
			if _, _, err := sourcePath(op.From); err != nil {
				return spec, err
			}
			if _, err := destinationPath(op.To); err != nil {
				return spec, err
			}
			if op.Strategy != "" && op.Strategy != "Overwrite" && op.Strategy != "Merge" && op.Strategy != "Extract" {
				return spec, fmt.Errorf("unsupported strategy %q", op.Strategy)
			}
			for _, pattern := range op.Exclude {
				if len(pattern) > 1024 {
					return spec, fmt.Errorf("exclude pattern outside API bounds")
				}
				if err := validateGlob(pattern); err != nil {
					return spec, err
				}
				if _, err := doublestar.Match(pattern, "."); err != nil {
					return spec, err
				}
			}
		}
	}
	return spec, nil
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
	if commas > 20 {
		return fmt.Errorf("glob contains more than 20 brace commas")
	}
	return nil
}
