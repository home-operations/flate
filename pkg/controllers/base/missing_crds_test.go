package base_test

import (
	"errors"
	"testing"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestConfigure_MissingCRDs(t *testing.T) {
	t.Parallel()
	c := base.New(store.New(), task.NewBounded(2), "test")
	id := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "ns", Name: "app"}
	deps := []manifest.DependencyRef{{Group: apix.GroupName, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com"}}
	err := c.Require(&depwait.Waiter{Parent: id, Dependencies: deps}, manifest.ReconcileAttempt{Drain: manifest.DrainCascade}, &depwait.Resolution{})
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	c.Configure(base.Options{AllowMissingCRDs: true})
	err = c.Require(&depwait.Waiter{Parent: id, Dependencies: deps}, manifest.ReconcileAttempt{Drain: manifest.DrainCascade}, &depwait.Resolution{})
	assert.NoError(t, err)
	c.StartLifecycle()
	defer c.Close()
	defer func() {
		if recover() == nil {
			t.Error("Configure after start must panic")
		}
		err = c.Require(&depwait.Waiter{Parent: id, Dependencies: deps}, manifest.ReconcileAttempt{Drain: manifest.DrainCascade}, &depwait.Resolution{})
		assert.NoError(t, err)
	}()
	c.Configure(base.Options{})
}
