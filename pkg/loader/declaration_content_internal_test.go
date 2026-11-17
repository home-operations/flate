package loader

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type countedDeclarationValue struct{ calls atomic.Int64 }

func (v *countedDeclarationValue) MarshalJSON() ([]byte, error) {
	v.calls.Add(1)
	return []byte(`"value"`), nil
}

func TestDeclarationContents_DemandAndLifetime(t *testing.T) {
	t.Parallel()
	value := new(countedDeclarationValue)
	document := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata":  map[string]any{"name": "input", "namespace": "apps"},
		"unmodeled": value,
	}
	child, outcome, _ := Prepare(manifest.DocOccurrence{Document: document}, manifest.ParseDocOptions{})
	assert.NoError(t, outcome.Err)
	content := new(Loader).DeclarationContents()
	assert.Equal(t, value.calls.Load(), int64(0))
	first := content(child.Doc)
	assert.Equal(t, value.calls.Load(), int64(2))
	assert.Equal(t, first.Exact != "", true)
	assert.Equal(t, content(child.Doc), first)
	assert.Equal(t, value.calls.Load(), int64(2))
	replacement := manifest.DeepCopyMap(child.Doc)
	replacement["changed"] = true
	second := content(replacement)
	assert.Equal(t, first.Exact == second.Exact, false)
	assert.Equal(t, content(child.Doc), first)
	assert.Equal(t, content(replacement), second)
	assert.Equal(t, value.calls.Load(), int64(4))
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			local := new(Loader).DeclarationContents()
			assert.Equal(t, local(child.Doc), first)
			assert.Equal(t, local(child.Doc), first)
		})
	}
	workers.Wait()
	assert.Equal(t, value.calls.Load(), int64(8))
}
