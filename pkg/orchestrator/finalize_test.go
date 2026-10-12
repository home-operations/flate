package orchestrator

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCascadeParentFailures_RecordsParentAsBlocker(t *testing.T) {
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	o := &Orchestrator{store: store.New()}
	o.store.AddObject(parent)
	o.store.AddObject(child)
	o.store.UpdateStatus(parent.Named(), store.StatusFailed, "build failed")
	o.store.UpdateStatus(child.Named(), store.StatusReady, "done")
	o.store.RecordEmitted(parent.Named(), []manifest.NamedResource{child.Named()})
	failed := o.store.FailedResources()

	o.cascadeParentFailures(failed)

	info, ok := o.store.GetStatus(child.Named())
	assert.Equal(t, ok, true)
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, info.Message, "dependencies failed: "+parent.Named().String()+": build failed")
	assert.Diff(t, info.BlockedBy(), []manifest.NamedResource{parent.Named()})
	assert.Equal(t, failed[child.Named()].IsDependencyFailure(), true)
}

func TestCascadeParentFailures_AncestorGuards(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status store.Status
	}{
		{name: "ready descendant", status: store.StatusReady},
		{name: "already failed", status: store.StatusFailed},
		{name: "pending descendant", status: store.StatusPending},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
			middle := &manifest.Kustomization{Name: "middle", Namespace: "apps"}
			child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
			o := &Orchestrator{store: store.New()}
			for _, obj := range []manifest.BaseManifest{parent, middle, child} {
				o.store.AddObject(obj)
			}
			o.store.UpdateStatus(parent.Named(), store.StatusFailed, "build failed")
			o.store.UpdateStatus(middle.Named(), store.StatusPending, "waiting")
			o.store.UpdateStatus(child.Named(), tt.status, "own status")
			o.store.RecordEmitted(parent.Named(), []manifest.NamedResource{middle.Named()})
			o.store.RecordEmitted(middle.Named(), []manifest.NamedResource{child.Named()})
			before, _ := o.store.GetStatus(child.Named())
			failed := o.store.FailedResources()
			o.cascadeParentFailures(failed)
			got, _ := o.store.GetStatus(child.Named())
			if tt.status == store.StatusReady {
				assert.Equal(t, got.Status, store.StatusFailed)
				assert.Diff(t, got.BlockedBy(), []manifest.NamedResource{parent.Named()})
				assert.Equal(t, failed[child.Named()].IsDependencyFailure(), true)
			} else {
				assert.Equal(t, got.Status, before.Status)
				assert.Equal(t, got.Message, before.Message)
				assert.Diff(t, got.BlockedBy(), before.BlockedBy())
			}
			mid, _ := o.store.GetStatus(middle.Named())
			assert.Equal(t, mid.Status, store.StatusPending)
		})
	}
}

func TestCascadeParentFailures_EmissionCycle(t *testing.T) {
	a := &manifest.Kustomization{Name: "a", Namespace: "apps"}
	b := &manifest.Kustomization{Name: "b", Namespace: "apps"}
	o := &Orchestrator{store: store.New()}
	o.store.AddObject(a)
	o.store.AddObject(b)
	o.store.UpdateStatus(a.Named(), store.StatusReady, "done")
	o.store.UpdateStatus(b.Named(), store.StatusReady, "done")
	o.store.RecordEmitted(a.Named(), []manifest.NamedResource{b.Named()})
	o.store.RecordEmitted(b.Named(), []manifest.NamedResource{a.Named()})
	failed := o.store.FailedResources()
	o.cascadeParentFailures(failed)
	assert.Equal(t, len(failed), 0)
	for _, id := range []manifest.NamedResource{a.Named(), b.Named()} {
		info, _ := o.store.GetStatus(id)
		assert.Equal(t, info.Status, store.StatusReady)
	}
}

func TestJoinTreeErrors_Sides(t *testing.T) {
	base, head := errors.New("base failure"), errors.New("head failure")
	for _, tt := range []struct {
		name       string
		base, head error
		message    string
	}{
		{name: "neither"},
		{name: "base", base: base, message: "base tree: base failure"},
		{name: "head", head: head, message: "head tree: head failure"},
		{name: "both", base: base, head: head,
			message: "both trees had reconcile failures\nbase tree: base failure\nhead tree: head failure"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := joinTreeErrors(tt.base, tt.head)
			if tt.message == "" {
				assert.Equal(t, got, nil)
				return
			}
			if got == nil {
				t.Fatal("missing tree failure")
			}
			assert.Equal(t, got.Error(), tt.message)
			assert.Equal(t, errors.Is(got, base), tt.base != nil)
			assert.Equal(t, errors.Is(got, head), tt.head != nil)
		})
	}
}
