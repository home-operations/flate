package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseDoc_BuildDirectives(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"Kustomization", "Component"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			obj, _, _, err := ParseDoc(
				map[string]any{"kind": kind, "apiVersion": "kustomize.config.k8s.io/v1beta1"},
				ParseDocOptions{DefaultNamespace: "apps"},
			)
			{
				assert.FatalIf(t, err != nil || obj != nil, "ParseDoc = %T, %v; want nil, nil", obj, err)
			}
		})
	}
}
