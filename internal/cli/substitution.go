package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func supplyEnv(input string) ([]string, error) {
	if input == "" {
		return nil, nil
	}
	paths := filepath.SplitList(input)
	for i, path := range paths {
		if path == "" {
			return nil, fmt.Errorf("%w: %s path %d is empty", manifest.ErrInput, envKey("supply"), i+1)
		}
	}
	return paths, nil
}

func (c *commonFlags) loadInputs(ctx context.Context) error {
	for _, kind := range c.allowMissingKinds {
		if kind != manifest.KindSecret && kind != manifest.KindConfigMap && kind != manifest.KindCustomResourceDefinition {
			return fmt.Errorf("%w: --allow-missing-kinds: unsupported kind %q", manifest.ErrInput, kind)
		}
	}
	for _, path := range c.supply {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := os.ReadFile(path) //nolint:gosec // path is an explicitly requested substitution input
		if err != nil {
			return fmt.Errorf("%w: --supply %q: %w", manifest.ErrInput, path, err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for ordinal := 1; ; ordinal++ {
			var doc map[string]any
			err := decoder.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			// Decoder diagnostics can contain scalar values from Secret input.
			if err != nil {
				return fmt.Errorf("%w: --supply %q document %d: invalid YAML mapping",
					manifest.ErrInput, path, ordinal)
			}
			if doc == nil {
				continue
			}
			kind := manifest.DocKind(doc)
			name, namespace := manifest.DocMetadata(doc)
			if manifest.DocAPIVersion(doc) != manifest.CoreAPIVersion ||
				(kind != manifest.KindConfigMap && kind != manifest.KindSecret) || name == "" || namespace == "" {
				return fmt.Errorf("%w: --supply %q document %d: expected a core v1 ConfigMap or Secret with name and namespace",
					manifest.ErrInput, path, ordinal)
			}
			for _, field := range []string{"data", "binaryData", "stringData"} {
				if value, exists := doc[field]; exists && value != nil {
					if _, ok := value.(map[string]any); !ok {
						return fmt.Errorf("%w: --supply %q document %d: %s must be a mapping",
							manifest.ErrInput, path, ordinal, field)
					}
				}
			}
			obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""})
			if err != nil {
				return fmt.Errorf("%w: --supply %q document %d: invalid substitution source",
					manifest.ErrInput, path, ordinal)
			}
			c.suppliedObjects = append(c.suppliedObjects, store.SuppliedObject{Object: obj, Path: path})
		}
	}
	return nil
}
