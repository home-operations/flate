package kustomize

import (
	"strings"
	"testing"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomizestate"
	"github.com/home-operations/flate/internal/testutil"
)

func closeTestTreeCache(t *testing.T, c *TreeCache) {
	t.Helper()
	c.Close()
	c.remoteInputs.Close()
	{
		err := c.owner.Close()
		assert.NoError(t, err)
	}
}

func buildTestTreeFS(view *treeFS) ([]byte, error) {
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	rm, err := fluxkustomize.Build(view, view.root)
	if err != nil {
		return nil, err
	}
	return rm.AsYaml()
}

func hasRenderField(value any, key, want string) bool {
	switch value := value.(type) {
	case string:
		return key == "" && strings.Contains(value, want)
	case []map[string]any:
		for _, doc := range value {
			if hasRenderField(doc, key, want) {
				return true
			}
		}
	case map[string]any:
		return hasRenderObjectField(value, key, want)
	case []any:
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	}
	return false
}

func testRenderEncoding(tb testing.TB, docs []map[string]any) []byte {
	tb.Helper()
	payload, err := testutil.EncodeRender(docs)
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}

func testRenderPayload(tb testing.TB, witness string) []byte {
	tb.Helper()
	return testRenderEncoding(tb, []map[string]any{{"witness": witness}})
}

func hasRenderObjectField(value map[string]any, key, want string) bool {
	if value[key] == want {
		return true
	}
	if items, ok := value[key].([]any); ok {
		for _, item := range items {
			if item == want {
				return true
			}
		}
	}
	for _, item := range value {
		if hasRenderField(item, key, want) {
			return true
		}
	}
	return false
}
