package scope_test

import (
	"encoding/json"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func e2ERuntimeOwnerNamespaceChangedOnlyName(t *testing.T, transition *struct {
	name      string
	namespace string
	value     string
},
) {
	t.Helper()

	before := runtimeOwnerNamespaceFixture(t, "", "before")
	after := runtimeOwnerNamespaceFixture(t, transition.namespace, transition.value)
	for _, direction := range []struct{ name, current, original, old, new string }{
		{"forward", after, before, "before", transition.value},
		{"reverse", before, after, transition.value, "before"},
	} {
		t.Run(
			direction.name,
			func(t *testing.T) { e2ERuntimeOwnerNamespaceChangedOnlyNameName(t, &direction, &transition) },
		)
	}
}

func e2ERuntimeOwnerNamespaceChangedOnlyNameName(t *testing.T, direction *struct {
	name     string
	current  string
	original string
	old      string
	new      string
}, transition **struct {
	name      string
	namespace string
	value     string
},
) {
	t.Helper()

	flags := []string{
		"--path",
		direction.current,
		"--path-orig",
		direction.original,
		"--cache-dir",
		"",
		"--concurrency",
		"2",
		"--no-progress",
	}
	out := runtimeOwnerNamespaceOutput(t, append([]string{"build", "all"}, flags...)...)
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	counts := map[string]int{}
	for _, doc := range docs {
		kind := manifest.DocKind(doc)
		metadata := manifest.DocMetadata(doc)
		name, ns := metadata.Name, metadata.Namespace
		counts[kind]++
		if kind == manifest.KindHelmRelease || kind == manifest.KindConfigMap {
			assert.Equal(t, ns, "apps")
			if kind == manifest.KindConfigMap {
				assert.Equal(t, name, "demo-chart")
				assert.Equal(t, doc["data"].(map[string]any)["value"], any(direction.new))
			}
		}
	}
	assert.Equal(t, counts[manifest.KindHelmRelease], 1)
	assert.Equal(t, counts[manifest.KindConfigMap], 1)
	out = runtimeOwnerNamespaceOutput(t, append([]string{"diff", "all", "-o", "json"}, flags...)...)
	var result struct {
		Changes []struct {
			Kind      string `json:"Kind"`
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
			Status    string `json:"Status"`
			Parent    struct {
				Kind      string `json:"Kind"`
				Name      string `json:"Name"`
				Namespace string `json:"Namespace"`
			} `json:"Parent"`
			Old map[string]any `json:"Old"`
			New map[string]any `json:"New"`
		} `json:"Changes"`
		Suppressed []any `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.Equal(t, len(result.Suppressed), 0)
	if (*transition).namespace != "" {
		assert.Equal(t, len(result.Changes), 0)
		return
	}
	workloads := 0
	for changeIndex := range result.Changes {
		change := result.Changes[changeIndex]
		assert.ErrorIf(t, change.Parent.Namespace == "", "empty-namespace parent: %+v", change)
		if change.Kind == manifest.KindConfigMap {
			workloads++
			assert.Equal(t, change.Name, "demo-chart")
			assert.Equal(t, change.Namespace, "apps")
			assert.Equal(t, change.Status, "changed")
			assert.Equal(t, change.Parent.Kind, manifest.KindHelmRelease)
			assert.Equal(t, change.Parent.Name, "demo")
			assert.Equal(t, change.Parent.Namespace, "apps")
			assert.Equal(t, change.Old["data"].(map[string]any)["value"], any(direction.old))
			assert.Equal(t, change.New["data"].(map[string]any)["value"], any(direction.new))
		}
	}
	assert.Equal(t, workloads, 1)
}
