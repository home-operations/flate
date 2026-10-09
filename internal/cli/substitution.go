package cli

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/values"
)

func substitutionPair(pair, origin string, position int) (string, string, error) {
	key, value, ok := strings.Cut(pair, "=")
	if !ok || !values.ValidSubstitutionName(key) {
		return "", "", fmt.Errorf("%w: %s pair %d: expected a valid variable name followed by =VALUE",
			manifest.ErrInput, origin, position)
	}
	return key, value, nil
}

func substitutionEnv(flag, input string) ([]string, error) {
	if input == "" {
		return nil, nil
	}
	key := envKey(flag)
	if flag == "substitute-from" {
		paths := strings.Split(input, ":")
		for i, path := range paths {
			if path == "" {
				return nil, fmt.Errorf("%w: %s path %d is empty", manifest.ErrInput, key, i+1)
			}
		}
		return paths, nil
	}
	r := csv.NewReader(strings.NewReader(input))
	r.FieldsPerRecord = -1
	pairs, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("%w: %s pair %d: invalid CSV", manifest.ErrInput, key, len(pairs)+1)
	}
	if _, err := r.Read(); err != io.EOF {
		return nil, fmt.Errorf("%w: %s pair %d: expected one CSV record", manifest.ErrInput, key, len(pairs)+1)
	}
	for i, pair := range pairs {
		if _, _, err := substitutionPair(pair, key, i+1); err != nil {
			return nil, err
		}
	}
	return pairs, nil
}

func (c *commonFlags) loadSubstitutions(ctx context.Context) error {
	if len(c.substitute) > 0 {
		c.substitutionValues = make(map[string]string, len(c.substitute))
		for i, pair := range c.substitute {
			key, value, err := substitutionPair(pair, "--substitute", i+1)
			if err != nil {
				return err
			}
			c.substitutionValues[key] = value
		}
	}
	for _, path := range c.substituteFrom {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := os.ReadFile(path) //nolint:gosec // path is an explicitly requested substitution input
		if err != nil {
			return fmt.Errorf("%w: --substitute-from %q: %w", manifest.ErrInput, path, err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		for ordinal := 1; ; ordinal++ {
			var doc map[string]any
			err := decoder.Decode(&doc)
			if err == io.EOF {
				break
			}
			// Decoder diagnostics can contain scalar values from Secret input.
			if err != nil {
				return fmt.Errorf("%w: --substitute-from %q document %d: invalid YAML mapping",
					manifest.ErrInput, path, ordinal)
			}
			if doc == nil {
				continue
			}
			kind := manifest.DocKind(doc)
			name, namespace := manifest.DocMetadata(doc)
			if manifest.DocAPIVersion(doc) != "v1" ||
				(kind != manifest.KindConfigMap && kind != manifest.KindSecret) || name == "" || namespace == "" {
				return fmt.Errorf("%w: --substitute-from %q document %d: expected a core v1 ConfigMap or Secret with name and namespace",
					manifest.ErrInput, path, ordinal)
			}
			for _, field := range []string{"data", "binaryData", "stringData"} {
				if value, exists := doc[field]; exists && value != nil {
					if _, ok := value.(map[string]any); !ok {
						return fmt.Errorf("%w: --substitute-from %q document %d: %s must be a mapping",
							manifest.ErrInput, path, ordinal, field)
					}
				}
			}
			obj, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: false})
			if err != nil {
				return fmt.Errorf("%w: --substitute-from %q document %d: invalid substitution source",
					manifest.ErrInput, path, ordinal)
			}
			c.substitutionSources = append(c.substitutionSources, orchestrator.SubstitutionSource{Object: obj, Path: path})
		}
	}
	return nil
}
