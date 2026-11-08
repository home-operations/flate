package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/baseline"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/helm"
	"github.com/home-operations/flate/internal/source"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/helmchart"
	"github.com/home-operations/flate/internal/source/oci"
	"github.com/home-operations/flate/internal/source/ssrfguard"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testDirectoryInput(tb testing.TB, path string) *store.SourceArtifact {
	tb.Helper()
	input, _, err := baseline.AdmitDirectory(tb.Context(), path, path, manifest.WorkScope{})
	if err != nil {
		tb.Fatal(err)
	}
	return input
}

//nolint:gocritic // Admission mutates a private Tree snapshot and must leave the caller's value unchanged.
func newTestOrchestrator(tb testing.TB, tree Tree, cfg Config) (*Orchestrator, error) {
	tb.Helper()
	if tree.Path == "" {
		tree.Path = tree.RepoRoot
	}
	if tree.Path == "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, "tree path is required")
	}
	if tree.Input == nil {
		var err error
		tree.Input, tree.Path, err = baseline.AdmitDirectory(
			tb.Context(),
			tree.Path,
			tree.RepoRoot,
			manifest.WorkScope{},
		)
		if err != nil {
			return nil, err
		}
	}
	if tree.RepoRoot == "" {
		tree.RepoRoot = tree.Input.LocalPath
	}
	if tree.BaseInput != nil && tree.BaseInput != tree.Input && tree.ExternalChanges == nil {
		var err error
		tree.ExternalChanges, err = change.Detect(tb.Context(), tree.BaseInput, tree.Input, manifest.WorkScope{})
		if err != nil {
			return nil, err
		}
	}
	return newTestOrchestratorTail(&newTestOrchestratorTailInputs{cfg: &cfg, tb: &tb, tree: &tree})
}

func newTestOrchestratorTail(tailCaseInput *newTestOrchestratorTailInputs) (*Orchestrator, error) {
	if err := snapshotSubstitutions(tailCaseInput.cfg); err != nil {
		return nil, err
	}
	ownNetwork := tailCaseInput.cfg.Network == nil
	if ownNetwork {
		tailCaseInput.cfg.
			Network = new(source.Network)
	}
	ownCache := tailCaseInput.cfg.Cache == nil
	if ownCache {
		var err error
		tailCaseInput.cfg.
			Cache, err = sourcecache.New((*tailCaseInput.tb).Context(), tailCaseInput.cfg.CacheDir)
		if err != nil {
			return nil, err
		}
	}
	if tailCaseInput.cfg.HelmClient == nil {
		tailCaseInput.cfg.
			HelmClient = helm.NewClient(tailCaseInput.cfg.Cache)
	}
	ssrfguard.Restrict(tailCaseInput.cfg.RestrictEgress)
	tailCaseInput.cfg.
		chartFetcher = helmchart.New(
		&oci.Fetcher{
			Cache:          tailCaseInput.cfg.Cache,
			Network:        tailCaseInput.cfg.Network,
			RegistryConfig: tailCaseInput.cfg.RegistryConfig,
		},
		helmchart.Config{
			Cache:                      tailCaseInput.cfg.Cache,
			Network:                    tailCaseInput.cfg.Network,
			Concurrency:                tailCaseInput.cfg.Concurrency,
			DisableChartDigestTracking: tailCaseInput.cfg.HelmOptions.DisableChartDigestTracking,
		},
	)
	o, constructionErr := newOrchestrator((*tailCaseInput.tb).Context(),
		new(*tailCaseInput.cfg),
		new(*tailCaseInput.tree),
		task.NewBounded(tailCaseInput.cfg.Concurrency),
	)
	assert.NoError(*tailCaseInput.tb, constructionErr)
	(*tailCaseInput.tb).
		Cleanup(func() {
			o.stop()
			if ownNetwork {
				tailCaseInput.cfg.
					Network.Close()
			}
			if ownCache {
				if err := tailCaseInput.cfg.Cache.Close(); err != nil {
					(*tailCaseInput.tb).
						Error(err)
				}
			}
		})
	if o.cfg.Selection.Changed || o.cfg.ExternalChanges != nil || len(o.cfg.Selection.Roots) != 0 {
		planned, err := prepareTreeEntries(
			(*tailCaseInput.tb).Context(),
			[]Tree{o.cfg.Tree},
			tailCaseInput.cfg,
			&o.inputs,
		)
		if err != nil {
			return nil, err
		}
		if len(planned) != 1 {
			return nil, manifest.NewError(manifest.ErrInput, nil, "test orchestrator requires one entry")
		}
		o.cfg.Tree = planned[0]
	}
	return o, nil
}

func (o *Orchestrator) prepareBootstrap(ctx context.Context) error {
	if o.session == nil {
		if err := o.discover(ctx); err != nil {
			return err
		}
	}
	if o.session == nil {
		return nil
	}
	for {
		o.loader.Options.Changes = o.cfg.ExternalChanges
		if err := o.lookupHistory(ctx); err != nil {
			return err
		}
		proposal, err := o.prepareAdmission(
			ctx,
			selection.PrepareRequest{Generation: nil, Reads: nil, History: o.history, Invocation: true},
		)
		if err != nil {
			return err
		}
		advanced, err := o.session.Advance(ctx, proposal)
		if err != nil {
			return err
		}
		if !advanced {
			o.preparation = proposal
			break
		}
	}
	if err := o.discover(ctx); err != nil {
		return err
	}
	return nil
}

func (o *Orchestrator) bootstrap(ctx context.Context) error {
	if err := o.prepareBootstrap(ctx); err != nil {
		return err
	}
	o.completeBootstrap()
	if err := o.prepareInvocation(ctx); err != nil {
		return err
	}
	if (o.selection != nil) && len(o.admission.EntryProducers()) == 0 {
		proposal, err := o.prepareAdmission(ctx, selection.PrepareRequest{Invocation: true, History: o.history})
		if err != nil {
			return err
		}
		if _, _, err = o.selection.Commit(ctx, proposal, selection.Execution); err != nil {
			return err
		}
	}
	if o.selection != nil {
		return o.installAdmission(ctx)
	}
	return nil
}

func (o *Orchestrator) render(ctx context.Context, tb testing.TB) (*Result, error) {
	tb.Helper()
	defer o.stop()
	if err := o.bootstrap(ctx); err != nil {
		return nil, err
	}
	result, err := o.collectResult(o.run(ctx))
	tb.Cleanup(result.Close)
	return result, err
}

func reconcileEntryForTest(t *testing.T, o *Orchestrator) {
	t.Helper()
	if o.entryBinding.Producer == (manifest.NamedResource{}) {
		return
	}
	o.configureControllers()
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: o.tasks, Store: o.store, ID: o.entryBinding.Producer, ReconcileNode: o.ksc.ReconcileNode,
	})
	assert.FatalIf(t, info.Status != store.StatusReady, "entry staging failed: %v", info)
}

type newTestOrchestratorTailInputs struct {
	cfg *Config

	tb *testing.
		TB

	tree *Tree
}

func (o *Orchestrator) collectManifests() []manifest.DocumentBatch {
	manifests, _ := o.collectCompletedManifests()
	return manifests
}
