package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_EntryApplicationConflictingProducers(t *testing.T) {
	run := func() *Result {
		t.Helper()
		root := t.TempDir()
		const url = "https://example.test/entry/conflicts"
		testutil.WriteFile(
			t,
			root,
			"entry/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/literal.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal, namespace: apps}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/workload.yaml",
			testutil.KustomizationYAML("workload", "apps", "  path: ./workload\n"),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: []\n")
		for _, producer := range []struct{ name, path string }{
			{name: "a", path: "./entry"},
			{name: "z", path: "./elsewhere"},
		} {
			testutil.WriteFile(
				t,
				root,
				"entry/"+producer.name+".yaml",
				fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: apps}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: fluxcd.controlplane.io/v1
      kind: FluxInstance
      metadata: {name: flux, namespace: apps}
      spec:
        sync: {name: self, kind: GitRepository, url: '%s', ref: main, path: %s}
`, producer.name, url, producer.path),
			)
		}
		o, err := newTestOrchestrator(
			t,
			Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}},
			Config{CacheDir: t.TempDir(), Concurrency: 2},
		)
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		assert.Equal(t, o.entryBinding.Producer, manifest.NamedResource{})
		id := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      "FluxInstance",
			Namespace: "apps",
			Name:      "flux",
		}
		firstPublication := make(chan struct{})
		var released sync.Once
		o.controllers.ResourceSet = entryPublicationController{
			reconcileController: o.rsc,
			tasks:               o.tasks,
			first:               firstPublication,
		}
		var mu sync.Mutex
		var published []string
		unsubscribe := o.store.AddListener(store.EventObjectAdded, func(added manifest.NamedResource, payload any) {
			if added == id {
				obj := payload.(*manifest.RawObject)
				application, valid := obj.EntryApplication()
				if valid {
					mu.Lock()
					published = append(published, application.Path)
					mu.Unlock()
					if application.Path == "./entry" {
						released.Do(func() { close(firstPublication) })
					}
				}
			}
		}, false)
		defer unsubscribe()
		result, err := o.collectResult(o.run(t.Context()))
		assert.NoError(t, err)
		assert.Diff(t, published, []string{"./entry", "./elsewhere"})
		assert.Equal(t, o.entryBinding.Producer, manifest.NamedResource{})
		winner := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "z",
		}
		assert.Equal(t, o.store.EmissionOwner(id), winner)
		obj := o.store.GetObject(id).(*manifest.RawObject)
		application, valid := obj.EntryApplication()
		assert.Equal(t, valid, true)
		assert.Equal(t, application.Path, "./elsewhere")
		assertEntryApplicationConflicts(t, result, id, winner)
		return result
	}
	first, second := run(), run()
	assert.Diff(t, outputByParent(first.Manifests), outputByParent(second.Manifests))
	assert.Diff(t, first.Warnings, second.Warnings)
}

type entryPublicationController struct {
	reconcileController

	tasks *task.Service
	first <-chan struct{}
}

func (c entryPublicationController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
) *base.Execution {
	if id.Name == "z" {
		c.tasks.YieldSlot(func() {
			select {
			case <-c.first:
			case <-ctx.Done():
			}
		})
	}
	return c.reconcileController.ReconcileNode(ctx, id, level)
}

func assertEntryApplicationConflicts(t *testing.T, result *Result, id, winner manifest.NamedResource) {
	t.Helper()
	for _, docs := range outputByParent(result.Manifests) {
		for _, doc := range docs {
			name, _ := manifest.DocMetadata(doc)
			{
				assert.ErrorIf(
					t,
					manifest.DocKind(doc) == manifest.KindConfigMap && name == "literal",
					"%v",
					"retired application admitted the literal root document",
				)
			}
		}
	}
	if len(result.Warnings) != 1 {
		t.Errorf("warnings: %+v", result.Warnings)
	}
	if len(result.Warnings) == 1 {
		warning := result.Warnings[0]
		assert.Equal(t, warning.Resource, id)
		assert.Equal(t, warning.Category, manifest.WarnConflictingProducers)
		assert.Equal(t, warning.Count, 1)
		assert.Equal(t, warning.Message, "conflicting producers; using "+winner.String())
		loser := winner
		loser.Name = "a"
		assert.Diff(t, warning.Detail, []string{loser.String(), winner.String()})
	}
}
