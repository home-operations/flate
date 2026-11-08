package resourceset_test

import (
	"errors"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_ResourcesTemplate(t *testing.T) {
	t.Parallel()
	tmpl := `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: << inputs.name >>
  namespace: flux-system
---
apiVersion: v1
kind: Namespace
metadata:
  name: << inputs.name >>
`
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		Inputs: []fluxopv1.ResourceSetInput{
			{"name": jsonTmpl(t, `"a"`)},
			{"name": jsonTmpl(t, `"b"`)},
		},
		ResourcesTemplate: tmpl,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	assert.FatalIf(t, err != nil, "Render: %v", err)
	assert.ErrorIf(t, len(docs) != 4, "expected 4 docs (2 inputs × 2 docs each), got %d", len(docs))
	t.Run("invalid stream separator", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{
			Name:              "stream",
			ResourcesTemplate: "\n---oops: kept\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: rejected}",
		}
		_, _, err := renderProviders(candidate, nil, nil)
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
			"error = %v, want classified invalid stream separator",
			err,
		)
	})
	t.Run("duplicate mapping keys", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{Name: "duplicate-keys", ResourcesTemplate: `--- # comment
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: db}
spec:
  postgresql: {value: first}
  postgresql: {value: last}
`}
		docs, _, err := renderProviders(candidate, nil, nil)
		assert.NoError(t, err)
		assert.FatalIf(t, len(docs) != 1, "documents = %d, want 1", len(docs))
		assert.Diff(t, docs[0].Document["spec"], map[string]any{"postgresql": map[string]any{"value": "last"}})
	})
}

func TestRender_SlugifyLength(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, input, want string }{
		{"truncate", strings.Repeat("a", 64), strings.Repeat("a", 63)},
		{"smart truncate", strings.Repeat("a", 32) + " " + strings.Repeat("b", 32), strings.Repeat("a", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{
				Name: "slug", Namespace: "apps",
				Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"`+tc.input+`"`)}},
				ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: << inputs.name | slugify >>}`,
			}
			docs, _, err := renderProviders(rs, nil, nil)
			assert.NoError(t, err)
			assert.FatalIf(t, len(docs) != 1, "documents = %d, want 1", len(docs))
			assert.Equal(t, docs[0].Document["metadata"].(map[string]any)["name"].(string), tc.want)
		})
	}
}
