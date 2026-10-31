package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
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

// Tree identifies an admitted repository side and its requested scan boundary.
type Tree struct {
	ID              manifest.TreeID
	RepoRoot        string
	Path            string
	SelfURLs        []string
	Input           *store.SourceArtifact
	BaseInput       *store.SourceArtifact
	ExternalChanges *change.Set
	entry           *discovery.Entry
}

// Rendered retains one tree's completed output, warnings and attributed error.
// Absent denotes a missing comparison side and never exposes an orchestrator.
type Rendered struct {
	ID     manifest.TreeID
	Absent bool
	*Orchestrator
	Result *Result
	Err    error
}

// RenderTrees plans and renders isolated cluster trees using one invocation budget.
// Borrowed Cache, Network and HelmClient remain caller-owned. Returned documents
// and completed stores remain readable; later lazy source reads require a borrowed cache.
func RenderTrees(ctx context.Context, trees []Tree, cfg Config) (results []Rendered, err error) {
	ssrfguard.Restrict(cfg.RestrictEgress)
	seen := make(map[manifest.TreeID]bool, len(trees))
	for _, tree := range trees {
		if err := tree.ID.Validate(); err != nil {
			return nil, err
		}
		if seen[tree.ID] {
			return nil, manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("duplicate tree %s/%d", tree.ID.Cluster, tree.ID.Side))
		}
		seen[tree.ID] = true
	}
	if err := snapshotSubstitutions(&cfg); err != nil {
		return nil, err
	}
	if cfg.Network == nil {
		cfg.Network = new(source.Network)
		defer cfg.Network.Close()
	}
	if cfg.Cache == nil {
		cfg.Cache, err = sourcecache.New(ctx, cfg.CacheDir)
		if err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, cfg.Cache.Close()) }()
	}
	if cfg.HelmClient == nil {
		cfg.HelmClient = helm.NewClient(cfg.Cache)
	}
	cfg.chartFetcher = helmchart.New(&oci.Fetcher{Cache: cfg.Cache, Network: cfg.Network, RegistryConfig: cfg.RegistryConfig}, helmchart.Config{
		Cache: cfg.Cache, Network: cfg.Network, Concurrency: cfg.Concurrency, DisableChartDigestTracking: cfg.HelmOptions.DisableChartDigestTracking})
	budget := task.NewBounded(cfg.Concurrency)
	trees, err = prepareTrees(ctx, trees, cfg, budget)
	if err != nil {
		for _, tree := range trees {
			result := Rendered{ID: tree.ID, Result: &Result{}}
			if tree.Input != nil {
				result.Result.Warnings = tree.Input.Warnings
			}
			if failure, ok := errors.AsType[*treeError](err); !ok || failure.id == tree.ID {
				result.Err = err
				if failure, ok := errors.AsType[*treeError](err); ok {
					failure.attributed = len(trees) > 1
				}
			}
			results = append(results, result)
		}
		slices.SortFunc(results, func(a, b Rendered) int { return a.ID.Compare(b.ID) })
		return results, err
	}
	results = make([]Rendered, len(trees))
	stores := make([]*store.Store, len(trees))
	byID := make(map[manifest.TreeID]int, len(trees))
	var wg sync.WaitGroup
	for i, tree := range trees {
		byID[tree.ID] = i
		results[i].ID = tree.ID
		if tree.Input == nil {
			results[i].Absent = true
			results[i].Result = &Result{}
			continue
		}
		wg.Go(func() {
			o := newOrchestrator(cfg, tree, budget.Fork())
			stores[i] = o.store
			if failure := o.discover(ctx); failure != nil {
				results[i].Result = &Result{Warnings: o.store.Warnings()}
				results[i].Err = &treeError{id: tree.ID, err: failure, attributed: len(results) > 1}
				o.stop()
				return
			}
			results[i].Orchestrator = o
		})
	}
	wg.Wait()
	constructionRound := 0
	requalify := map[manifest.ClusterID]bool{}
	for {
		for i := range results {
			current := results[i].Orchestrator
			if current == nil {
				continue
			}
			opposite := results[i].ID
			opposite.Side = manifest.TreeHead - opposite.Side
			j, found := byID[opposite]
			var other *Orchestrator
			if found {
				other = results[j].Orchestrator
			}
			if other != nil && j < i {
				continue
			}
			before, after := current.index, (*manifest.DiscoveryIndex)(nil)
			if other != nil {
				after = other.index
			}
			if results[i].ID.Side == manifest.TreeHead {
				before, after = after, before
			}
			if constructionRound != 0 && !requalify[current.cfg.ID.Cluster] {
				continue
			}
			changes := current.cfg.ExternalChanges
			if other != nil && (changes != nil || current.cfg.Input != other.cfg.Input) {
				base, head := current, other
				if current.cfg.ID.Side == manifest.TreeHead {
					base, head = other, current
				}
				var failure error
				changes, failure = change.Qualify(ctx, changes, base.store, head.store)
				if failure != nil {
					for _, side := range []int{i, j} {
						results[side].Err = &treeError{id: results[side].ID, err: failure, attributed: len(results) > 1}
						results[side].Result = &Result{Warnings: results[side].store.Warnings()}
						results[side].stop()
						results[side].Orchestrator = nil
					}
					continue
				}
			}
			changes = changes.WithReaders(before, after)
			current.cfg.ExternalChanges = changes
			if other != nil {
				other.cfg.ExternalChanges = changes
			}
		}
		clear(requalify)
		for i := range results {
			o := results[i].Orchestrator
			if o == nil || o.session == nil {
				continue
			}
			wg.Go(func() {
				o.preparation = nil
				o.loader.Options.Changes = o.cfg.ExternalChanges
				proposal, failure := o.loader.Prepare(ctx, store.Child{}, nil, nil)
				advanced := false
				if failure == nil {
					advanced, failure = o.session.Advance(ctx, proposal)
				}
				if failure != nil {
					results[i].Err = &treeError{id: results[i].ID, err: failure, attributed: len(results) > 1}
					results[i].Result = &Result{Warnings: o.store.Warnings()}
					o.stop()
					results[i].Orchestrator = nil
				} else if !advanced {
					o.preparation = proposal
				}
			})
		}
		wg.Wait()
		for i := range results {
			o := results[i].Orchestrator
			if o != nil && o.session != nil && o.preparation == nil {
				requalify[results[i].ID.Cluster] = true
			}
		}
		if len(requalify) == 0 {
			break
		}
		constructionRound++
	}
	for i := range results {
		o := results[i].Orchestrator
		if o == nil || o.session == nil {
			continue
		}
		wg.Go(func() {
			if failure := o.discover(ctx); failure != nil {
				results[i].Err = &treeError{id: results[i].ID, err: failure, attributed: len(results) > 1}
				results[i].Result = &Result{Warnings: o.store.Warnings()}
				o.stop()
				results[i].Orchestrator = nil
			}
		})
	}
	wg.Wait()

	for i := range results {
		o := results[i].Orchestrator
		if o == nil || o.loader == nil {
			continue
		}
		opposite := o.cfg.ID
		opposite.Side = manifest.TreeHead - opposite.Side
		j, found := byID[opposite]
		var other *Orchestrator
		if found {
			other = results[j].Orchestrator
		}
		if other != nil && j < i {
			continue
		}
		base, head := o.store, (*store.Store)(nil)
		if other != nil {
			head = other.store
		}
		if o.cfg.ID.Side == manifest.TreeHead {
			base, head = head, base
		}
		var prepared [2]map[manifest.NamedResource]store.Child
		for side, st := range [2]*store.Store{base, head} {
			if st == nil {
				continue
			}
			owner := o
			if owner.store != st {
				owner = other
			}
			for id, record := range owner.index.Records {
				if record.Builder == "" {
					continue
				}
				child := owner.loader.IndexedObject(id, record)
				if child.Obj == nil {
					continue
				}
				if prepared[side] == nil {
					prepared[side] = map[manifest.NamedResource]store.Child{}
				}
				prepared[side][id] = child
			}
		}
		changes, failure := o.cfg.ExternalChanges.WithDeclarations(ctx, base, head, prepared)
		if failure != nil {
			for _, index := range []int{i, j} {
				if index != i && !found {
					continue
				}
				side := results[index].Orchestrator
				if side == nil {
					continue
				}
				results[index].Result = &Result{Warnings: side.store.Warnings()}
				results[index].Err = &treeError{id: results[index].ID, err: failure, attributed: len(results) > 1}
				side.stop()
				results[index].Orchestrator = nil
			}
			continue
		}
		if other == nil {
			before, after := o.index, (*manifest.DiscoveryIndex)(nil)
			if o.cfg.ID.Side == manifest.TreeHead {
				before, after = after, before
			}
			o.cfg.ExternalChanges = changes.WithReaders(before, after)
		} else {
			projectedOther := projectEntryReaders(other.index, other.entryBinding, o.entryBinding)
			projectedCurrent := projectEntryReaders(o.index, o.entryBinding, other.entryBinding)
			if o.cfg.ID.Side == manifest.TreeBase {
				o.cfg.ExternalChanges = changes.WithReaders(o.index, projectedOther)
				other.cfg.ExternalChanges = changes.WithReaders(projectedCurrent, other.index)
			} else {
				o.cfg.ExternalChanges = changes.WithReaders(projectedOther, o.index)
				other.cfg.ExternalChanges = changes.WithReaders(other.index, projectedCurrent)
			}
		}
	}
	for i := range results {
		o := results[i].Orchestrator
		if o == nil || o.loader == nil {
			continue
		}
		wg.Go(func() {
			o.completeBootstrap()
			if failure := o.buildChangeFilter(ctx); failure != nil {
				results[i].Result = &Result{Warnings: o.store.Warnings()}
				results[i].Err = &treeError{id: results[i].ID, err: failure, attributed: len(results) > 1}
				o.stop()
				results[i].Orchestrator = nil
			}
		})
	}
	wg.Wait()
	for i := range results {
		o := results[i].Orchestrator
		if o == nil || o.loader == nil {
			continue
		}
		if failure := o.installAdmission(ctx); failure != nil {
			results[i].Result = &Result{Warnings: o.store.Warnings()}
			results[i].Err = &treeError{id: results[i].ID, err: failure, attributed: len(results) > 1}
			o.stop()
			results[i].Orchestrator = nil
		}
	}
	for i := range results {
		o := results[i].Orchestrator
		if o == nil {
			continue
		}
		if cfg.HeadOnly && results[i].ID.Side == manifest.TreeBase {
			o.stop()
			results[i].Orchestrator = nil
			continue
		}
		wg.Go(func() {
			defer o.stop()
			if o.loader == nil {
				o.completeBootstrap()
				if failure := o.buildChangeFilter(ctx); failure != nil {
					results[i].Orchestrator = nil
					results[i].Result = &Result{Warnings: o.store.Warnings()}
					results[i].Err = &treeError{id: results[i].ID, err: failure, attributed: len(results) > 1}
					return
				}
			}
			result, failure := o.collectResult(o.run(ctx))
			results[i].Result = result
			if failure != nil {
				results[i].Err = &treeError{id: results[i].ID, err: failure, attributed: len(results) > 1}
			}
		})
	}
	wg.Wait()
	clusterAdvisories(results, stores)
	for _, result := range results {
		err = errors.Join(err, result.Err)
	}

	return results, err
}

func prepareTrees(ctx context.Context, trees []Tree, cfg Config, budget *task.Service) ([]Tree, error) {
	trees = slices.Clone(trees)
	inputs := source.NewHTTPInputs(cfg.Cache, cfg.Network, budget)
	defer inputs.Close()
	admitted := make(map[string]*store.SourceArtifact)
	for i := range trees {
		tree := &trees[i]
		if tree.Input == nil {
			scan := cmp.Or(tree.Path, tree.RepoRoot)
			if scan == "" {
				return trees, &treeError{id: tree.ID, err: manifest.NewError(manifest.ErrInput, nil, "tree path is required")}
			}
			input, path, err := baseline.AdmitDirectory(ctx, scan, tree.RepoRoot)
			if err != nil {
				return trees, &treeError{id: tree.ID, err: err}
			}
			tree.Input, tree.Path = input, path
			if previous := admitted[input.LocalPath]; previous != nil {
				tree.Input = previous
			} else {
				admitted[input.LocalPath] = input
			}
		}
		if tree.Input.FS == nil || tree.Input.LocalPath == "" {
			return trees, &treeError{id: tree.ID, err: manifest.NewError(manifest.ErrInput, nil, "tree input content unavailable")}
		}
		tree.RepoRoot = tree.Input.LocalPath
		var err error
		tree.Path, err = filepath.Abs(cmp.Or(tree.Path, tree.RepoRoot))
		if err != nil {
			return trees, &treeError{id: tree.ID, err: err}
		}
	}
	changes := make(map[[2]*store.SourceArtifact]*change.Set)
	var planned []Tree
	sides := make(map[manifest.TreeSide]bool)
	for _, tree := range trees {
		sides[tree.ID.Side] = true
		for _, opposite := range trees {
			if opposite.ID.Cluster == tree.ID.Cluster && opposite.ID.Side != tree.ID.Side {
				tree.BaseInput = opposite.Input
			}
		}
		if tree.ExternalChanges == nil && tree.BaseInput != nil && tree.BaseInput != tree.Input {
			pair := [2]*store.SourceArtifact{tree.BaseInput, tree.Input}
			if tree.ID.Side == manifest.TreeBase {
				pair[0], pair[1] = pair[1], pair[0]
			}
			if set, ok := changes[pair]; ok {
				tree.ExternalChanges = set
			} else {
				set, err := change.Detect(ctx, pair[0], pair[1])
				if err != nil {
					return trees, &treeError{id: tree.ID, err: err}
				}
				tree.ExternalChanges, changes[pair] = set, set
			}
		}
		selection := cfg.Selection
		selection.Changed = selection.Changed || tree.ExternalChanges != nil
		entries, err := discovery.Plan(ctx, discovery.Config{Selection: selection, Input: tree.Input, Path: tree.Path, RepoRoot: tree.RepoRoot, SelfURLs: tree.SelfURLs, WipeSecrets: cfg.WipeSecrets, RemoteInputs: &inputs, RemoteRetry: cfg.SourceRetry})
		if err != nil {
			return trees, &treeError{id: tree.ID, err: err}
		}
		for _, entry := range entries {
			side := tree
			side.Input, side.Path, side.entry = entry.Input, entry.Path, &entry
			planned = append(planned, side)
		}
	}
	byID := make(map[manifest.TreeID]Tree, len(planned))
	for _, tree := range planned {
		if _, exists := byID[tree.ID]; exists {
			return nil, manifest.NewError(manifest.ErrInput, nil, fmt.Sprintf("duplicate planned tree %s/%d", tree.ID.Cluster, tree.ID.Side))
		}
		byID[tree.ID] = tree
	}
	if len(sides) == 2 {
		for _, tree := range planned {
			other := tree.ID
			other.Side = manifest.TreeHead - other.Side
			if _, present := byID[other]; !present {
				byID[other] = Tree{ID: other}
				tree.BaseInput, tree.ExternalChanges = nil, nil
				byID[tree.ID] = tree
			}
		}
	}
	planned = planned[:0]
	for _, tree := range byID {
		planned = append(planned, tree)
	}
	slices.SortFunc(planned, func(a, b Tree) int { return a.ID.Compare(b.ID) })
	return planned, nil
}

type treeError struct {
	id         manifest.TreeID
	err        error
	attributed bool
}

func (e *treeError) Error() string {
	if !e.attributed {
		return e.err.Error()
	}
	side := "orig"
	if e.id.Side == manifest.TreeHead {
		side = "current"
	}
	return fmt.Sprintf("cluster %s %s snapshot: %s", e.id.Cluster, side, e.err)
}

func (e *treeError) Unwrap() error { return e.err }

func projectEntryReaders(index *manifest.DiscoveryIndex, from, to discovery.Binding) *manifest.DiscoveryIndex {
	if index == nil || from.Producer == to.Producer || from.Producer == (manifest.NamedResource{}) || to.Producer == (manifest.NamedResource{}) {
		return index
	}
	projected := *index
	projected.Readers = maps.Clone(index.Readers)
	for file, readers := range index.Readers {
		if !slices.ContainsFunc(readers, func(reader manifest.FileReader) bool { return reader.Reader == from.Producer }) {
			continue
		}
		readers = slices.Clone(readers)
		for i := range readers {
			if readers[i].Reader == from.Producer {
				readers[i].Reader = to.Producer
			}
		}
		projected.Readers[file] = readers
	}
	return &projected
}
