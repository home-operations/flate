package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCascadeParentFailures_RecordsParentAsBlocker(t *testing.T) {
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	o := &Orchestrator{store: store.New(), rendered: newRenderedSet()}
	o.store.AddObject(parent)
	o.store.AddObject(child)
	o.store.UpdateStatus(parent.Named(), store.StatusFailed, "build failed")
	o.store.UpdateStatus(child.Named(), store.StatusReady, "done")
	o.rendered.MarkRenderedBatch(parent.Named(), []manifest.NamedResource{child.Named()})
	failed := o.store.FailedResources()

	o.cascadeParentFailures(failed)

	info, ok := o.store.GetStatus(child.Named())
	assert.Equal(t, ok, true)
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, info.Message, "dependencies failed: "+parent.Named().String()+": build failed")
	assert.Diff(t, info.BlockedBy(), []manifest.NamedResource{parent.Named()})
	assert.Equal(t, failed[child.Named()].IsDependencyFailure(), true)
}
