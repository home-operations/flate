package artifactgenerator

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func consumedPublication(t *testing.T, publication, change string) []byte {
	t.Helper()
	var documents []byte
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		st, bodies := store.New(), task.NewBounded(2)
		st.SetWorkTree(manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, (&manifest.WorkRecorder{}).Scope())
		owner, err := cache.New(ctx, "")
		assert.FatalIf(t, err != nil, "%v", err)
		defer func() {
			{
				closeErr := owner.Close()
				assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
			}
		}()
		f := &consumedFixture{publication: publication, change: change, st: st, bodies: bodies, owner: owner}
		f.prepareInputs(ctx, t)
		f.prepareGenerator(t)
		trees := f.startControllers(t)
		defer trees.Close()
		var producer *base.Execution
		if publication == "emitted" {
			supplier := &manifest.ResourceSet{Name: "supplier", Namespace: "apps"}
			st.AddObject(supplier)
			producer = controllertest.NewExecution(f.generator.Controller, supplier.Named())
			defer producer.Close()
		}
		f.preparePublication(t, producer)
		documents = f.run(ctx, t, cancel)
	})
	return documents
}
