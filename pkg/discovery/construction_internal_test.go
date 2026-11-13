package discovery

import (
	"context"
	"errors"
	"slices"

	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func constructTestSources(ctx context.Context, session *Session) error {
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(session.cfg.Store, tasks, session.cfg.Cache)
	controller.Configure(
		sourcectrl.FetchOptions{Admission: session.Admission()},
		cache.RetryConfig{},
	)
	pending := session.PendingSources()
	failures := make([]error, len(pending))
	for i, id := range pending {
		tasks.Go(ctx, "construct source", func(ctx context.Context) { failures[i] = controller.Construct(ctx, id) })
	}
	tasks.BlockTillDone()
	for _, err := range failures {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

//nolint:gocritic // Test admission MUST preserve the caller configuration snapshot.
func beginTest(ctx context.Context, cfg Config) (*Session, error) {
	session, err := Begin(ctx, cfg)
	if err != nil {
		return nil, err
	}
	for {
		if err := constructTestSources(ctx, session); err != nil {
			return nil, err
		}
		ready, err := session.Load(ctx)
		if err != nil {
			return nil, err
		}
		if ready {
			return session, nil
		}
	}
}

//nolint:gocritic // Test admission MUST preserve the caller configuration snapshot.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	session, err := Begin(ctx, cfg)
	if err != nil {
		return nil, err
	}
	for {
		if err := constructTestSources(ctx, session); err != nil {
			return nil, err
		}
		result, err := session.Run(ctx)
		if result != nil || err != nil {
			return result, err
		}
	}
}

//nolint:gocritic // Test admission MUST preserve the caller configuration snapshot.
func Plan(ctx context.Context, cfg Config) ([]Entry, error) {
	if cfg.Input == nil && cfg.Store != nil {
		cfg.Input, _ = cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	cfg.Store = store.New()
	cfg.Store.SetWorkTree(cfg.Work.Identity().Tree, cfg.Work)
	session, err := Begin(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer session.Close()
	for {
		if err := constructTestSources(ctx, session); err != nil {
			return nil, err
		}
		entries, err := session.Plan(ctx)
		if entries != nil || err != nil {
			return entries, err
		}
	}
}

//nolint:gocritic // Test admission MUST preserve the caller configuration snapshot.
func BeginTest(ctx context.Context, cfg Config) (*Session, error) { return beginTest(ctx, cfg) }

func (d *Session) resolveTestSources(ctx context.Context, allowMissing bool) (int, error) {
	total := 0
	for {
		added, err := d.resolveEntrySources(ctx, allowMissing)
		total += added
		if err != nil && !errors.Is(err, errPendingSources) {
			return total, err
		}
		if err == nil {
			return total, nil
		}
		if err := constructTestSources(ctx, d); err != nil {
			return total, err
		}
	}
}

func (d *Session) loadManifestsTest(ctx context.Context, root string, build bool) error {
	for {
		err := d.loadManifests(ctx, root, build)
		if !errors.Is(err, errPendingSources) {
			return err
		}
		if err := constructTestSources(ctx, d); err != nil {
			return err
		}
	}
}

func (d *Session) partitionRootsTest(ctx context.Context) ([]entryRoot, error) {
	for {
		roots, err := d.partitionRoots(ctx)
		if !errors.Is(err, errPendingSources) {
			return roots, err
		}
		if err := constructTestSources(ctx, d); err != nil {
			return nil, err
		}
	}
}

func constructAdmissionTest(ctx context.Context, a *Admission, ids ...manifest.NamedResource) error {
	ids = slices.Compact(slices.SortedFunc(slices.Values(ids), manifest.NamedResource.Compare))
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(a.store, tasks, nil)
	controller.Configure(sourcectrl.FetchOptions{Admission: a}, cache.RetryConfig{})
	failures := make([]error, len(ids))
	for i, id := range ids {
		tasks.Go(ctx, "construct source", func(ctx context.Context) { failures[i] = controller.Construct(ctx, id) })
	}
	tasks.BlockTillDone()
	for _, err := range failures {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (d *Session) advanceTest(ctx context.Context, admission *selection.AdmissionResult) (bool, error) {
	changed := false
	for {
		advanced, err := d.Advance(ctx, admission)
		changed = changed || advanced
		if err != nil || len(d.PendingSources()) == 0 {
			return changed, err
		}
		if err := constructTestSources(ctx, d); err != nil {
			return changed, err
		}
	}
}
