package artifactgenerator

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"
)

const maxInputsFileSize = 1 << 20

func (e *evaluation) exportedInputs(ctx context.Context, from string, output fs.FS) (map[string]any, error) {
	if from == "" {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	alias, name, _ := strings.Cut(from[1:], "/")
	input := e.inputs[alias]
	if alias == "artifact" {
		input = output
	}
	if input == nil {
		return nil, fmt.Errorf("invalid inputsFrom path: no local path found for alias %q", alias)
	}
	name = filepath.FromSlash(name)
	if !filepath.IsLocal(name) {
		return nil, fmt.Errorf("invalid inputsFrom path: path %q must stay within the source root", name)
	}
	file, err := input.Open(filepath.ToSlash(filepath.Clean(name)))
	if err != nil {
		return nil, fmt.Errorf("failed to read inputs file %q: %w", from, err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxInputsFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read inputs file %q: %w", from, err)
	}
	if len(data) > maxInputsFileSize {
		return nil, fmt.Errorf(
			"failed to read inputs file %q: file exceeds the maximum size of %d bytes",
			from,
			maxInputsFileSize,
		)
	}
	var values map[string]any
	if err := yaml.Unmarshal(data, &values); err != nil {
		return nil, fmt.Errorf("failed to parse inputs file %q: %w", from, err)
	}
	return values, nil
}
