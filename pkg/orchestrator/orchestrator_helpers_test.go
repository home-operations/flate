package orchestrator

import (
	"context"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func testDirectoryInput(tb testing.TB, path string) *store.SourceArtifact {
	tb.Helper()
	input, _, err := baseline.AdmitDirectory(tb.Context(), path, path)
	if err != nil {
		tb.Fatal(err)
	}
	return input
}

func newTestOrchestrator(tb testing.TB, tree Tree, cfg Config) (*Orchestrator, error) {
	tb.Helper()
	if tree.Path == "" {
		tree.Path = tree.RepoRoot
	}
	if tree.Path == "" {
		return nil, fmt.Errorf("%w: tree path is required", manifest.ErrInput)
	}
	if tree.Input == nil {
		var err error
		tree.Input, tree.Path, err = baseline.AdmitDirectory(tb.Context(), tree.Path, tree.RepoRoot)
		if err != nil {
			return nil, err
		}
	}
	if tree.RepoRoot == "" {
		tree.RepoRoot = tree.Input.LocalPath
	}
	if tree.BaseInput != nil && tree.BaseInput != tree.Input && tree.ExternalChanges == nil {
		var err error
		tree.ExternalChanges, err = change.Detect(tb.Context(), tree.BaseInput, tree.Input)
		if err != nil {
			return nil, err
		}
	}
	if err := snapshotSubstitutions(&cfg); err != nil {
		return nil, err
	}
	ownNetwork := cfg.Network == nil
	if ownNetwork {
		cfg.Network = new(source.Network)
	}
	ownCache := cfg.Cache == nil
	if ownCache {
		var err error
		cfg.Cache, err = sourcecache.New(tb.Context(), cfg.CacheDir)
		if err != nil {
			return nil, err
		}
	}
	if cfg.HelmClient == nil {
		cfg.HelmClient = helm.NewClient(cfg.Cache)
	}
	ssrfguard.Restrict(cfg.RestrictEgress)
	cfg.chartFetcher = helmchart.New(&oci.Fetcher{Cache: cfg.Cache, Network: cfg.Network, RegistryConfig: cfg.RegistryConfig}, helmchart.Config{
		Cache: cfg.Cache, Network: cfg.Network, Concurrency: cfg.Concurrency, DisableChartDigestTracking: cfg.HelmOptions.DisableChartDigestTracking})
	o := newOrchestrator(cfg, tree, task.NewBounded(cfg.Concurrency))
	tb.Cleanup(func() {
		o.stop()
		if ownNetwork {
			cfg.Network.Close()
		}
		if ownCache {
			if err := cfg.Cache.Close(); err != nil {
				tb.Error(err)
			}
		}
	})
	return o, nil
}

func (o *Orchestrator) prepareBootstrap(ctx context.Context) error {
	if err := o.discover(ctx); err != nil {
		return err
	}
	if o.session != nil {
		for {
			o.loader.Options.Changes = o.cfg.ExternalChanges
			proposal, err := o.loader.Prepare(ctx, store.Child{}, nil, nil)
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
	}
	return nil
}

func (o *Orchestrator) bootstrap(ctx context.Context) error {
	if err := o.prepareBootstrap(ctx); err != nil {
		return err
	}
	o.completeBootstrap()
	if err := o.buildChangeFilter(ctx); err != nil {
		return err
	}
	if o.loader != nil {
		return o.installAdmission(ctx)
	}
	return nil
}

func (o *Orchestrator) render(ctx context.Context) (*Result, error) {
	defer o.stop()
	if err := o.bootstrap(ctx); err != nil {
		return nil, err
	}
	return o.collectResult(o.run(ctx))
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
	if info.Status != store.StatusReady {
		t.Fatalf("entry staging failed: %v", info)
	}
}
