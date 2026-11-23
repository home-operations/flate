package artifactgenerator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	maxInputsFileSize = 1 << 20
	artifactAlias     = "artifact"
	inputsFile        = "inputs"
	namespaceFile     = "namespace metadata"
)

var errSourceFileTooLarge = errors.New("file exceeds the maximum size")

func (e *evaluation) exportedInputs(ctx context.Context, from string, output fs.FS) (map[string]any, error) {
	if from == "" {
		return nil, nil
	}
	data, err := e.readSourceFile(ctx, from, output, inputsFile)
	if err != nil {
		return nil, err
	}
	var values map[string]any
	if err := yaml.Unmarshal(data, &values); err != nil {
		return nil, fmt.Errorf("failed to parse inputs file %q: %w", from, err)
	}
	return values, nil
}

func splitFileSelector(value string) (sourceSelection, error) {
	source, marked := strings.CutPrefix(value, "@")
	if !marked {
		return sourceSelection{}, fmt.Errorf("path must start with '@'")
	}
	alias, name, ok := strings.Cut(source, "/")
	if !ok || alias == "" || name == "" {
		return sourceSelection{}, fmt.Errorf("path must be in the format '@<alias>/<path>'")
	}
	return sourceSelection{alias: alias, pattern: name}, nil
}

func (e *evaluation) readSourceFile(ctx context.Context, from string, output fs.FS, kind string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	input, name, err := e.sourceFile(from, output)
	if err != nil {
		if kind == inputsFile {
			return nil, fmt.Errorf("invalid inputsFrom path: %w", err)
		}
		return nil, fmt.Errorf("invalid namespace metadata path %q: %w", from, err)
	}
	file, err := input.Open(name)
	if err != nil {
		return nil, sourceFileReadError(kind, from, err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxInputsFileSize+1))
	if err != nil {
		return nil, sourceFileReadError(kind, from, err)
	}
	if len(data) > maxInputsFileSize {
		return nil, fmt.Errorf(
			"failed to read %s file %q: %w of %d bytes",
			kind,
			from,
			errSourceFileTooLarge,
			maxInputsFileSize,
		)
	}
	return data, nil
}

type namespaceMetadataReadError struct{ error }

func (e *namespaceMetadataReadError) Unwrap() error { return e.error }

func sourceFileReadError(kind, from string, cause error) error {
	err := fmt.Errorf("failed to read %s file %q: %w", kind, from, cause)
	if kind == namespaceFile && !errors.Is(cause, fs.ErrNotExist) {
		return &namespaceMetadataReadError{err}
	}
	return err
}

func (e *evaluation) sourceFile(from string, output fs.FS) (fs.FS, string, error) {
	selector, err := splitFileSelector(from)
	if err != nil {
		return nil, "", err
	}
	alias := selector.alias
	input := output
	if alias != artifactAlias {
		var found bool
		input, found = e.inputs[alias]
		if !found {
			return nil, "", fmt.Errorf("source alias %q not found", alias)
		}
	}
	if input == nil {
		return nil, "", fmt.Errorf("no local path found for alias %q", alias)
	}
	name := filepath.FromSlash(selector.pattern)
	if !filepath.IsLocal(name) {
		return nil, "", fmt.Errorf("path %q must stay within the source root", name)
	}
	return input, filepath.ToSlash(filepath.Clean(name)), nil
}

type stagedNamespaceMetadata struct {
	Data    []byte `json:",omitempty"`
	Failure string `json:",omitempty"`
}

func (e *evaluation) captureInputs(ctx context.Context, path string, staged fs.FS) (string, error) {
	inputs, err := e.exportedInputs(ctx, path, staged)
	if err != nil {
		return "", manifest.NewError(manifest.ErrInput, err, "")
	}
	encoded, err := json.Marshal(inputs)
	if err != nil {
		return "", fmt.Errorf("encode staged inputs: %w", err)
	}
	return string(encoded), nil
}

func (e *evaluation) captureNamespaceMetadata(ctx context.Context, path string, staged fs.FS) (string, error) {
	data, err := e.readSourceFile(ctx, path, staged, namespaceFile)
	captured := stagedNamespaceMetadata{Data: data}
	if err != nil {
		if _, transient := errors.AsType[*namespaceMetadataReadError](err); transient || ctx.Err() != nil {
			return "", err
		}
		captured.Failure = err.Error()
	}
	encoded, err := json.Marshal(captured)
	if err != nil {
		return "", fmt.Errorf("encode staged namespace metadata: %w", err)
	}
	return string(encoded), nil
}
