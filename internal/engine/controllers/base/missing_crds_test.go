package base_test

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/engine/depwait"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestConfigure_MissingCRDs(t *testing.T) {
	t.Parallel()
	c := base.New(store.New(), task.NewBounded(2))
	id := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "ns",
		Name:      "app",
	}
	deps := []manifest.DependencyRef{
		{Group: apix.GroupName, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"},
	}
	err := controllertest.NewExecution(c, id).Require(t.Context(), deps, manifest.DrainCascade)
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	owner := cachetest.New(t, "", cache.New)
	c.Configure(
		base.Options{Cache: owner, Missing: depwait.NewMissingPolicy([]string{manifest.KindCustomResourceDefinition})},
	)
	assert.Equal(t, controllertest.NewExecution(c, id).Require(t.Context(), deps, manifest.DrainCascade) == nil, true)
	assert.Equal(t, c.Cache, owner)
	c.StartLifecycle()
	defer func() {
		assert.ErrorIf(t, recover() == nil, "%v", "Configure after start must panic")
		assert.Equal(t, c.Cache, owner)
		assert.Equal(
			t,
			controllertest.NewExecution(c, id).Require(t.Context(), deps, manifest.DrainCascade) == nil,
			true,
		)
	}()
	c.Configure(base.Options{})
}
