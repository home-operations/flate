package artifactgenerator

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"

	"github.com/home-operations/flate/pkg/manifest"
)

type request struct {
	work                       manifest.AcquisitionWork
	Identity                   manifest.NamedResource
	Namespace                  manifest.NamedResource
	Output                     manifest.ArtifactGeneratorOutput
	Captures                   map[string]string
	NamespaceMetadataPath      string
	NamespaceMetadataPathError error
	stagedMetadata             stagedNamespaceMetadata
	stagedMetadataError        error
}

var captureName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func expand(
	ctx context.Context,
	spec *manifest.ArtifactGeneratorSpec,
	namespace string,
	inputs map[string]fs.FS,
) ([]request, error) {
	matcher, input, err := pathMatcher(spec.PathPattern, inputs)
	if err != nil {
		return nil, err
	}
	if matcher == nil {
		requests := make([]request, 0, len(spec.Artifacts))
		for i := range spec.Artifacts {
			output := &spec.Artifacts[i]
			resolved, _ := spec.ResolveOutput(output, namespace, nil, nil)
			requests = append(requests, request{
				Identity: resolved.Artifact, Namespace: resolved.Namespace, Output: resolved.Output,
				NamespaceMetadataPath: resolved.NamespaceMetadataPath,
			})
		}
		return requests, nil
	}
	x := requestExpansion{
		spec: spec, namespace: namespace, captures: matcher.SubexpNames()[1:],
		seen: make(map[manifest.NamedResource]string),
	}
	err = fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk source directory: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		if name != "." && entry.IsDir() {
			if matches := matcher.FindStringSubmatch(name); matches != nil {
				return x.add(name, matches[1:])
			}
		}
		return nil
	})
	if err != nil {
		//nolint:wrapcheck // The walk callback attaches the source or artifact operation to each error.
		return nil, err
	}
	return x.requests, nil
}

type requestExpansion struct {
	spec      *manifest.ArtifactGeneratorSpec
	namespace string
	captures  []string
	requests  []request
	seen      map[manifest.NamedResource]string
}

func (x *requestExpansion) add(dir string, values []string) error {
	raw := make(map[string]string, len(values))
	normalized := make(map[string]string, len(values))
	for i, key := range x.captures {
		raw[key] = values[i]
		value := strings.ToLower(values[i])
		if errs := content.IsLabelValue(value); len(errs) != 0 {
			return fmt.Errorf(
				"pathPattern %q: captured value %q for variable %q is not a valid Kubernetes label value: %s",
				x.spec.PathPattern,
				value,
				key,
				strings.Join(errs, "; "),
			)
		}
		normalized[key] = value
	}
	for i := range x.spec.Artifacts {
		output := &x.spec.Artifacts[i]
		resolved, err := x.spec.ResolveOutput(output, x.namespace, normalized, raw)
		if err != nil {
			return fmt.Errorf("pathPattern %q: failed to render artifact %s for match %s: %w",
				x.spec.PathPattern, output.Name, dir, err)
		}
		id := resolved.Artifact
		if err := validateArtifactIdentity(id, output.Namespace != "", "rendered artifact"); err != nil {
			return fmt.Errorf("pathPattern %q: %w", x.spec.PathPattern, err)
		}
		if previous, exists := x.seen[id]; exists {
			return fmt.Errorf("pathPattern %q: directories %q and %q both resolve to artifact name %q in namespace %q",
				x.spec.PathPattern, previous, dir, id.Name, id.Namespace)
		}
		x.seen[id] = dir
		x.requests = append(x.requests, request{
			Identity: id, Namespace: resolved.Namespace, Output: resolved.Output, Captures: normalized,
			NamespaceMetadataPath:      resolved.NamespaceMetadataPath,
			NamespaceMetadataPathError: resolved.NamespaceMetadataPathError,
		})
	}
	return nil
}

func pathMatcher(patternValue string, inputs map[string]fs.FS) (*regexp.Regexp, fs.FS, error) {
	var matcher *regexp.Regexp
	var input fs.FS
	if patternValue != "" {
		sourcePathResult1, sourcePathResult1Err := sourcePath(patternValue)
		alias, pattern, err := sourcePathResult1.alias, sourcePathResult1.pattern, sourcePathResult1Err
		if err != nil {
			return nil, nil, err
		}
		input = inputs[alias]
		if input == nil {
			return nil, nil, fmt.Errorf(
				"pathPattern %q: source alias %s not found in local sources",
				patternValue,
				alias,
			)
		}
		matcher, err = compilePathPattern(pattern)
		if err != nil {
			return nil, nil, err
		}
	}
	return matcher, input, nil
}

func compilePathPattern(pattern string) (*regexp.Regexp, error) {
	var expression strings.Builder
	expression.WriteByte('^')
	seen := map[string]bool{}
	for pattern != "" {
		i := strings.IndexAny(pattern, "{}")
		if i < 0 {
			expression.WriteString(regexp.QuoteMeta(pattern))
			break
		}
		expression.WriteString(regexp.QuoteMeta(pattern[:i]))
		name, rest, ok := strings.Cut(pattern[i+1:], "}")
		if pattern[i] != '{' || !ok || !captureName.MatchString(name) || seen[name] ||
			len(content.IsLabelKey(name)) != 0 {
			return nil, fmt.Errorf("invalid or repeated pathPattern capture %q", name)
		}
		seen[name] = true
		expression.WriteString("(?P<" + name + ">[^/]+)")
		pattern = rest
	}
	expression.WriteByte('$')
	matcher, err := regexp.Compile(expression.String())
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return matcher, nil
}
