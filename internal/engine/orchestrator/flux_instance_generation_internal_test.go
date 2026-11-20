package orchestrator

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The orchestrator fixture changes process-wide source policy.
func TestRender_FluxInstanceGenerationWithdrawal(t *testing.T) {
	root := t.TempDir()
	const url = "https://example.test/sync"
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [parent.yaml]\n")
	testutil.WriteFile(
		t, root, "entry/parent.yaml",
		testutil.KustomizationYAML("parent", "apps", "  path: ./initial\n"),
	)
	testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
	for _, dir := range []string{"initial", "replacement", "empty"} {
		resources := "resources: []\n"
		if dir != "empty" {
			resources = "resources: [instance.yaml]\n"
			testutil.WriteFile(t, root, dir+"/instance.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance, namespace: apps}
spec:
  sync: {name: `+dir+`, kind: GitRepository, url: `+url+`, ref: main, path: ./payload}
`)
		}
		testutil.WriteFile(t, root, dir+"/kustomization.yaml", resources)
	}
	o, err := newTestOrchestrator(t, Tree{
		Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url},
	}, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	parentID := testutil.KustomizationID("apps", "parent")
	instanceID := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "instance",
	}
	for _, step := range []struct {
		path, withdrawn string
	}{
		{path: "initial"},
		{path: "replacement", withdrawn: "initial"},
		{path: "empty", withdrawn: "replacement"},
	} {
		t.Run(step.path, func(t *testing.T) {
			parent := bindingLifecycleObject(
				t,
				testutil.KustomizationYAML("parent", "apps", "  path: ./"+step.path+"\n"),
			)
			o.store.AddObject(parent.Object, parent.Document)
			if step.withdrawn == "" {
				assert.NoError(t, o.run(t.Context()))
			} else {
				assertSyncWithdrawalBeforeConsumption(t, o, parentID, step.withdrawn)
			}
			for _, name := range []string{"initial", "replacement"} {
				if name == step.path || name == step.withdrawn {
					assertFluxInstanceSyncAuthority(t, o.store, &syncAuthority{
						name: name, present: name == step.path, parent: parentID, instance: instanceID,
					})
				}
			}
			assert.Equal(t, o.store.GetDeclaration(instanceID) != nil, step.path != "empty")
			output, ok := o.store.GetArtifact(parentID, store.ArtifactContent).(*store.Emission)
			assert.FatalIf(
				t,
				!ok || !output.Complete || !output.OutputReads.Valid(),
				"parent did not publish complete output",
			)
		})
	}
}

func assertSyncWithdrawalBeforeConsumption(
	t *testing.T,
	o *Orchestrator,
	parentID manifest.NamedResource,
	withdrawn string,
) {
	t.Helper()
	dependent := bindingLifecycleObject(t, testutil.KustomizationYAML(
		"dependent", "apps", "  dependsOn: [{name: parent}]\n  path: ./payload\n",
	))
	o.store.AddObject(dependent.Object, dependent.Document)
	o.store.UpdateStatus(parentID, store.StatusInfo{Status: store.StatusPending})
	probed := make(chan struct{}, 1)
	o.controllers.Kustomization = &syncWithdrawalController{
		reconcileController: o.ksc,
		base:                o.ksc.Controller,
		tasks:               o.tasks,
		parent:              parentID,
		dependent:           dependent.Object.Named(),
		assessed:            make(chan struct{}),
		consume: func(reads *store.ReadSet) {
			for _, id := range syncIdentities(withdrawn) {
				assert.ErrorIf(t, reads.GetDeclaration(id) != nil, "dependent can still consume %s", id)
			}
			select {
			case probed <- struct{}{}:
			default:
			}
		},
	}
	assert.NoError(t, o.runDAG(t.Context()))
	assert.Equal(t, len(probed), 1)
}

type syncAuthority struct {
	name             string
	present          bool
	parent, instance manifest.NamedResource
}

func assertFluxInstanceSyncAuthority(t *testing.T, st *store.Store, want *syncAuthority) {
	t.Helper()
	for _, id := range syncIdentities(want.name) {
		assert.ErrorIf(t, (st.GetDeclaration(id) != nil) != want.present,
			"authority for %s does not match the parent output", id)
		productions := st.Productions(id, nil)
		assert.Equal(t, slices.ContainsFunc(productions, func(p manifest.Production) bool {
			return p.Owner == want.parent && p.Declaration == want.instance
		}), want.present)
	}
}

func syncIdentities(name string) [2]manifest.NamedResource {
	return [2]manifest.NamedResource{
		testutil.KustomizationID("apps", name),
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: name},
	}
}

type syncWithdrawalController struct {
	reconcileController

	base              *base.Controller
	tasks             *task.Service
	parent, dependent manifest.NamedResource
	assessed          chan struct{}
	consume           func(*store.ReadSet)
}

func (c *syncWithdrawalController) ReconcileNode(
	ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, previous schedule.Result,
) *base.Execution {
	if id == c.parent {
		c.tasks.YieldSlot(func() {
			select {
			case <-c.assessed:
			case <-ctx.Done():
			}
		})
	}
	if id != c.dependent {
		return c.reconcileController.ReconcileNode(ctx, id, level, previous)
	}
	execution := new(base.Execution)
	execution.Init(c.base, id)
	execution.Take(previous)
	execution.Dispatch(ctx, level, execution,
		func(run *base.Execution, ctx context.Context, obj *manifest.Kustomization, level manifest.DrainLevel) error {
			_, err := run.RequireRefresh(ctx, obj, manifest.GateInputs, level)
			select {
			case <-c.assessed:
			default:
				close(c.assessed)
			}
			if err != nil {
				return err
			}
			c.consume(&run.Reads)
			return nil
		},
	)
	return execution
}
