package values

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestStoreProvider_DeclaredOutputsHaveNoData(t *testing.T) {
	t.Parallel()
	st := store.New()
	declaration := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "external",
		Namespace:  "apps",
		Spec:       map[string]any{"data": []any{map[string]any{"secretKey": "VALUE"}}},
	}
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "external"}
	st.AddObject(declaration)
	assert.FatalIf(
		t,
		NewStoreProvider(st).Data(target) != nil || st.GetObject(target) != nil,
		"%s",
		"declared output fabricated data",
	)
}
