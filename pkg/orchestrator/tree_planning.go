package orchestrator

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/change"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

func (r *renderInvocation) prepareTrees(
	ctx context.Context,
	trees []Tree,
	inputs *source.HTTPInputs,
) ([]Tree, error) {
	trees = slices.Clone(trees)
	slices.SortFunc(trees, func(a, b Tree) int { return a.ID.Compare(b.ID) })
	admitted := make(map[string]*store.SourceArtifact)
	if err := prepareTreeInputs(ctx, trees, admitted, r.cfg); err != nil {
		return trees, err
	}
	changes := make(map[[2]*store.SourceArtifact]*change.Set)
	for i, tree := range trees {
		for _, opposite := range trees {
			if opposite.ID.Cluster == tree.ID.Cluster && opposite.ID.Side != tree.ID.Side {
				tree.BaseInput = opposite.Input
			}
		}
		if err := prepareTreeChanges(ctx, &tree, changes); err != nil {
			return trees, err
		}
		trees[i] = tree
	}
	expanded, err := r.prepareTreeEntries(ctx, trees, inputs)
	if err != nil {
		return trees, err
	}
	if err := validateTrees(expanded); err != nil {
		return trees, manifest.NewError(
			manifest.ErrInput,
			err,
			err.Error()+"; use --path to select distinct entry directories",
		)
	}
	trees = expanded
	trees = pairTreeSides(trees)
	slices.SortFunc(trees, func(a, b Tree) int { return a.ID.Compare(b.ID) })
	return trees, nil
}

func (r *renderInvocation) prepareTreeEntries(
	ctx context.Context,
	trees []Tree,
	inputs *source.HTTPInputs,
) ([]Tree, error) {
	entries := make([][]discovery.Entry, len(trees))
	failures := make([]error, len(trees))
	var wg sync.WaitGroup
	for i, tree := range trees {
		wg.Go(func() {
			var err error
			entries[i], err = r.planTree(ctx, &tree, inputs)
			if err != nil {
				failures[i] = &treeError{id: tree.ID, err: err}
			}
		})
	}
	wg.Wait()
	if err := errors.Join(failures...); err != nil {
		return nil, err
	}
	labels := map[manifest.ClusterID]map[manifest.ClusterID]manifest.ClusterID{}
	for i, tree := range trees {
		if labels[tree.ID.Cluster] == nil {
			labels[tree.ID.Cluster] = map[manifest.ClusterID]manifest.ClusterID{}
		}
		for _, entry := range entries[i] {
			labels[tree.ID.Cluster][entry.ID] = entry.ID
		}
	}
	for i, tree := range trees {
		automatic := tree.ID.Cluster == "." && tree.Path == tree.RepoRoot && len(labels[tree.ID.Cluster]) > 1
		if len(entries[i]) == 1 && !automatic {
			labels[tree.ID.Cluster][entries[i][0].ID] = tree.ID.Cluster
		}
	}
	var expanded []Tree
	for i, tree := range trees {
		for j := range entries[i] {
			entry := &entries[i][j]
			current := tree
			current.ID.Cluster = labels[tree.ID.Cluster][entry.ID]
			current.Path, current.Input, current.entry = entry.Path, entry.Input, entry
			expanded = append(expanded, current)
		}
	}
	slices.SortFunc(expanded, func(a, b Tree) int { return a.ID.Compare(b.ID) })
	return expanded, nil
}

func (r *renderInvocation) planTree(
	ctx context.Context,
	tree *Tree,
	inputs *source.HTTPInputs,
) ([]discovery.Entry, error) {
	cfg := r.cfg
	request := cfg.Selection
	request.Changed = request.Changed || tree.ExternalChanges != nil
	st := store.New()
	st.SetWorkTree(tree.work.Scope().Identity().Tree, tree.work.Scope())
	declarations := loader.New(st)
	defer declarations.Close()
	declarations.Options.Missing = cfg.missing
	session, err := discovery.Begin(ctx, discovery.Config{
		Store: st, Cache: cfg.Cache, Loader: declarations,
		Work: tree.work.Scope(), Projections: cfg.projections,
		Declarations: cfg.Supply, Selection: request, Changes: tree.ExternalChanges,
		Path: tree.Path, RepoRoot: tree.RepoRoot, SelfURLs: tree.SelfURLs, Input: tree.Input,
		RemoteInputs: inputs, RemoteRetry: cfg.SourceRetry,
		StrictSubstitutions: cfg.StrictSubstitutions, SkipKinds: cfg.HelmOptions.SkipKinds,
	})
	if err != nil {
		return nil, err
	}
	defer session.Close()
	controller := sourcectrl.New(st, r.budget, cfg.Cache)
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindBucket, manifest.KindOCIRepository, manifest.KindHelmChart} {
		controller.Fetchers[kind] = nil
	}
	configureSourceFetchers(controller, cfg)
	planning := Orchestrator{
		store:     st,
		loader:    declarations,
		src:       controller,
		admission: session.Admission(),
		cfg:       treeConfig{Config: *cfg},
	}
	planning.configureSources(true)
	for {
		if err := reconcileConstruction(ctx, session.PendingSources(), controller, r.budget); err != nil {
			return nil, err
		}
		entries, err := session.Plan(ctx)
		if entries != nil || err != nil {
			return entries, err
		}
	}
}
