package artifactgenerator

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"strings"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"
	"k8s.io/apimachinery/pkg/api/validate/content"

	"github.com/home-operations/flate/internal/core/artifactsyntax"
	"github.com/home-operations/flate/pkg/manifest"
)

type request struct {
	work                       manifest.AcquisitionWork
	Identity                   manifest.NamedResource
	Namespace                  manifest.NamedResource
	Output                     sourcewatcherv1.OutputArtifact
	Captures                   map[string]string
	namespaceMetadata          artifactsyntax.Selector
	inputsFrom                 artifactsyntax.Selector
	NamespaceMetadataPathError error
	stagedMetadata             stagedNamespaceMetadata
	stagedMetadataError        error
}

func (s *generatorSpec) expand(
	ctx context.Context,
	namespace string,
	inputs map[string]fs.FS,
) ([]request, error) {
	spec := &s.declaration
	matcher, input, err := pathMatcher(spec.PathPattern, inputs)
	if err != nil {
		return nil, err
	}
	if matcher == nil {
		requests := make([]request, 0, len(spec.OutputArtifacts))
		for i := range spec.OutputArtifacts {
			output := &spec.OutputArtifacts[i]
			resolved, _ := spec.ResolveOutput(output, namespace, nil, nil)
			requests = append(requests, s.request(&resolved))
		}
		return requests, nil
	}
	x := requestExpansion{
		spec: s, namespace: namespace, captures: matcher.SubexpNames()[1:],
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
	spec      *generatorSpec
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
				x.spec.declaration.PathPattern,
				value,
				key,
				strings.Join(errs, "; "),
			)
		}
		normalized[key] = value
	}
	declaration := &x.spec.declaration
	for i := range declaration.OutputArtifacts {
		output := &declaration.OutputArtifacts[i]
		resolved, err := declaration.ResolveOutput(output, x.namespace, normalized, raw)
		if err != nil {
			return fmt.Errorf("pathPattern %q: failed to render artifact %s for match %s: %w",
				x.spec.declaration.PathPattern, output.Name, dir, err)
		}
		id := resolved.Artifact
		if err := validateArtifactIdentity(id, output.Namespace != "", "rendered artifact"); err != nil {
			return fmt.Errorf("pathPattern %q: %w", x.spec.declaration.PathPattern, err)
		}
		if previous, exists := x.seen[id]; exists {
			return fmt.Errorf("pathPattern %q: directories %q and %q both resolve to artifact name %q in namespace %q",
				x.spec.declaration.PathPattern, previous, dir, id.Name, id.Namespace)
		}
		x.seen[id] = dir
		req := x.spec.request(&resolved)
		req.Captures = normalized
		x.requests = append(x.requests, req)
	}
	return nil
}

func (s *generatorSpec) request(resolved *manifest.ResolvedArtifactGeneratorOutput) request {
	return request{
		Identity:                   resolved.Artifact,
		Namespace:                  resolved.Namespace,
		Output:                     resolved.Output,
		inputsFrom:                 s.selectors.parsedFile(resolved.Output.InputsFrom),
		namespaceMetadata:          s.selectors.parsedFile(resolved.NamespaceMetadataPath),
		NamespaceMetadataPathError: resolved.NamespaceMetadataPathError,
	}
}

func pathMatcher(text string, inputs map[string]fs.FS) (*regexp.Regexp, fs.FS, error) {
	if text == "" {
		return nil, nil, nil
	}
	selector := artifactsyntax.ParseSelector(text)
	if err := validateSelector(selector, patternSelector); err != nil {
		return nil, nil, err
	}
	input := inputs[selector.Alias]
	if input == nil {
		return nil, nil, fmt.Errorf(
			"pathPattern %q: source alias %s not found in local sources",
			selector.Text,
			selector.Alias,
		)
	}
	matcher, err := artifactsyntax.CompilePathPattern(selector.Path)
	return matcher, input, err
}
