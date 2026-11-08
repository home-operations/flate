package values

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestExpandValueReferences_YAMLContainingPlaceholderIsMerged verifies that
// a ConfigMap whose values.yaml contains placeholder tokens inside valid YAML
// values (e.g. from postBuild substitution on a configMapGenerator) is correctly
// parsed and merged into hr.Values instead of being discarded.

func TestExpandValueReferences_MalformedYAMLClassified(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, document string
		typeError      bool
	}{
		{"syntax", "a: [", false},
		{"document type", "- name: demo", true},
	} {
		for _, optional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/optional=%t", tc.name, optional), func(t *testing.T) {
				t.Parallel()
				st := store.New()
				st.AddObject(
					&manifest.ConfigMap{
						Name:      "extra",
						Namespace: "default",
						Data:      map[string]any{"requested.yaml": tc.document},
					},
				)
				hr := &manifest.HelmRelease{
					Name:      "demo",
					Namespace: "default",
					ValuesFrom: []manifest.ValuesReference{
						{Kind: manifest.KindConfigMap, Name: "extra", ValuesKey: "requested.yaml", Optional: optional},
					},
				}
				err := ExpandValueReferences(hr, NewStoreProvider(st), &Cache{})

				assert.FatalIf(
					t,
					!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
					"malformed values must retain input/domain identity: %v",
					err,
				)

				for _, detail := range []string{"extra", "requested.yaml"} {
					assert.ErrorIf(t, !strings.Contains(err.Error(), detail), "error lacks %q: %v", detail, err)
				}
				wrapped, ok := errors.AsType[interface {
					error
					Unwrap() []error
				}](err)

				assert.FatalIf(t, !ok, "parser cause is not wrapped beside ErrInput: %v", err)

				causes := wrapped.Unwrap()
				var decoded map[string]any
				parseErr := yaml.Unmarshal([]byte(tc.document), &decoded)

				assert.FatalIf(
					t,
					len(causes) != 2 || parseErr == nil || errors.Unwrap(causes[1]).Error() != parseErr.Error(),
					"parser cause changed: %v",
					err,
				)

				if tc.typeError {
					{
						_, ok := errors.AsType[*json.UnmarshalTypeError](err)
						assert.FatalIf(t, !ok, "typed parser cause lost: %v", err)
					}
				}
			})
		}
	}
}
