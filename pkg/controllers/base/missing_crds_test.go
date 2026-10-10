package base_test

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestConfigure_MissingCRDs(t *testing.T) {
	c := base.New(store.New(), task.NewBounded(2), "test")
	id := manifest.NamedResource{Kind: manifest.KindResourceSet, Namespace: "ns", Name: "app"}
	deps := []manifest.DependencyRef{{Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}}
	err := c.Require(t.Context(), id, deps, manifest.DrainCascade)
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	c.Configure(base.Options{AllowMissingCRDs: true})
	assert.Equal(t, c.Require(t.Context(), id, deps, manifest.DrainCascade) == nil, true)
	c.StartLifecycle()
	defer c.Close()
	defer func() {
		if recover() == nil {
			t.Error("Configure after start must panic")
		}
		assert.Equal(t, c.Require(t.Context(), id, deps, manifest.DrainCascade) == nil, true)
	}()
	c.Configure(base.Options{})
}
