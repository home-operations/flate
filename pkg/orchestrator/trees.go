package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Tree identifies an admitted repository side and its requested scan boundary.
type Tree struct {
	work            *manifest.WorkRecorder
	entry           *discovery.Entry
	ID              manifest.TreeID
	RepoRoot        string
	Path            string
	SelfURLs        []string
	Input           *store.SourceArtifact
	BaseInput       *store.SourceArtifact
	ExternalChanges *change.Set
}

// Rendered retains one tree's completed output, warnings and attributed error.
// Absent denotes a missing comparison side and never exposes an orchestrator.
type Rendered struct {
	*Orchestrator

	ID     manifest.TreeID
	Absent bool
	Result *Result
	Err    error
}

// RenderTrees plans and renders isolated cluster trees using one invocation budget.
// Borrowed Cache, Network and HelmClient remain caller-owned. Returned documents
// and completed stores remain readable; later lazy source reads require a borrowed cache.
func RenderTrees(
	ctx context.Context,
	trees []Tree,
	cfg Config, //nolint:gocritic // SDK Config must remain a value snapshot; invocation setup mutates only its private copy.
) (results []Rendered, err error) { //nolint:nonamedreturns // Defers read returned results and join close errors into err.
	ssrfguard.Restrict(cfg.RestrictEgress)
	if innerLocalErr := validateTrees(trees); innerLocalErr != nil {
		return nil, innerLocalErr
	}
	if localErr := snapshotSubstitutions(&cfg); localErr != nil {
		return nil, localErr
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
	cfg.chartFetcher = helmchart.New(
		&oci.Fetcher{Cache: cfg.Cache, Network: cfg.Network, RegistryConfig: cfg.RegistryConfig},
		helmchart.Config{
			Cache:                      cfg.Cache,
			Network:                    cfg.Network,
			Concurrency:                cfg.Concurrency,
			DisableChartDigestTracking: cfg.HelmOptions.DisableChartDigestTracking,
		},
	)
	budget := task.NewBounded(cfg.Concurrency)
	inputs := source.NewHTTPInputs(cfg.Cache, cfg.Network, budget)
	defer inputs.Close()
	cfg.projections = new(manifest.ProducerProjections)
	r := renderInvocation{cfg: &cfg, budget: budget}
	trees, err = r.prepareTrees(ctx, trees, &inputs)
	if err != nil {
		inputs.Close()
		return failedTrees(trees, err), err
	}
	results = make([]Rendered, len(trees))
	defer func() {
		if err == nil {
			return
		}
		for _, result := range results {
			if result.Orchestrator != nil {
				result.stop()
			}
		}
	}()
	r.trees, r.results, r.stores, r.byID = trees, results,
		make([]*store.Store, len(trees)), make(map[manifest.TreeID]int, len(trees))
	r.discoverTrees(ctx)
	cfg.projections.Clear()
	if err = r.stopFailedDiscovery(); err != nil {
		r.stopRemaining()
		return results, err
	}
	if failure := r.constructTrees(ctx); failure != nil {
		return results, failure
	}
	r.completeDiscovery(ctx)
	if failure := r.qualifyDeclarations(ctx); failure != nil {
		return results, failure
	}
	r.installAdmission(ctx)
	r.renderTrees(ctx)
	clusterAdvisories(results, r.stores)
	for _, result := range results {
		err = errors.Join(err, result.Err)
	}
	return results, err
}

type renderInvocation struct {
	cfg     *Config
	trees   []Tree
	results []Rendered
	stores  []*store.Store
	byID    map[manifest.TreeID]int
	budget  *task.Service
	wg      sync.WaitGroup
}

func validateTrees(trees []Tree) error {
	seen := make(map[manifest.TreeID]bool, len(trees))
	for _, tree := range trees {
		if localErr := tree.ID.Validate(); localErr != nil {
			return localErr
		}
		if seen[tree.ID] {
			return manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf("duplicate tree %s/%d", tree.ID.Cluster, tree.ID.Side),
			)
		}
		seen[tree.ID] = true
	}
	return nil
}

func failedTrees(trees []Tree, err error) []Rendered {
	var results []Rendered
	if len(trees) > 0 {
		results = make([]Rendered, 0, len(trees))
	}
	failures := []error{err}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		failures = joined.Unwrap()
	}
	for _, tree := range trees {
		result := Rendered{ID: tree.ID, Result: &Result{Work: tree.work.Scope().Snapshot(0)}}
		if tree.Input != nil {
			result.Result.Warnings = tree.Input.Warnings
		}
		if _, ok := errors.AsType[*treeError](err); !ok {
			result.Err = err
		} else {
			for _, cause := range failures {
				if failure, ok := errors.AsType[*treeError](cause); ok && failure.id == tree.ID {
					failure.attributed = len(trees) > 1
					result.Err = cause
				}
			}
		}
		results = append(results, result)
	}
	slices.SortFunc(results, func(a, b Rendered) int { return a.ID.Compare(b.ID) })
	return results
}

func (r *renderInvocation) discoverTrees(ctx context.Context) {
	for i, tree := range r.trees {
		r.trees[i].entry = nil
		r.byID[tree.ID] = i
		r.results[i].ID = tree.ID
		if tree.Input == nil {
			r.results[i].Absent = true
			r.results[i].Result = &Result{}
			continue
		}
		r.wg.Go(func() {
			o, failure := newOrchestrator(ctx, new(*r.cfg), new(tree), r.budget.Fork())
			if failure != nil {
				r.results[i].Result = &Result{Warnings: tree.Input.Warnings}
				r.results[i].Err = &treeError{id: tree.ID, err: failure, attributed: len(r.results) > 1}
				return
			}
			r.stores[i] = o.store
			r.results[i].Orchestrator = o
			if failure := o.discover(ctx); failure != nil {
				result := o.failureResult()
				if (o.selection != nil) && o.session == nil {
					result.Warnings = tree.Input.Warnings
				}
				r.results[i].Result = result
				r.results[i].Err = &treeError{id: tree.ID, err: failure, attributed: len(r.results) > 1}
			}
		})
	}
	r.wg.Wait()
}

func (r *renderInvocation) stopFailedDiscovery() error {
	var err error
	for i, result := range r.results {
		if result.Err == nil {
			continue
		}
		o := result.Orchestrator
		if o == nil {
			err = errors.Join(err, result.Err)
			continue
		}
		if (o.selection != nil) && o.session == nil {
			err = errors.Join(err, result.Err)
		}
		o.stop()
		r.results[i].Orchestrator = nil
	}
	return err
}

func (r *renderInvocation) stopRemaining() {
	for i, result := range r.results {
		if result.Orchestrator != nil {
			r.results[i].Result = result.failureResult()
			result.stop()
			r.results[i].Orchestrator = nil
		}
	}
}

func (r *renderInvocation) opposite(i int) (*Orchestrator, int, bool) {
	id := r.results[i].ID
	id.Side = manifest.TreeHead - id.Side
	j, found := r.byID[id]
	if !found {
		return nil, j, false
	}
	return r.results[j].Orchestrator, j, true
}

func (r *renderInvocation) recordFailure(i int, err error) {
	r.results[i].Result = r.results[i].failureResult()
	r.results[i].Err = &treeError{id: r.results[i].ID, err: err, attributed: len(r.results) > 1}
	r.results[i].Orchestrator = nil
}

func (r *renderInvocation) constructTrees(ctx context.Context) error {
	constructionRound := 0
	requalify := map[manifest.ClusterID]bool{}
	for {
		if err := r.qualifyConstructionReaders(ctx, constructionRound, requalify); err != nil {
			return err
		}
		clear(requalify)
		for i := range r.results {
			o := r.results[i].Orchestrator
			if o != nil && o.session != nil {
				r.wg.Go(func() { r.prepareSession(ctx, i) })
			}
		}
		r.wg.Wait()
		for i := range r.results {
			o := r.results[i].Orchestrator
			if o != nil && o.session != nil && o.preparation == nil {
				requalify[r.results[i].ID.Cluster] = true
			}
		}
		if len(requalify) == 0 {
			return nil
		}
		constructionRound++
	}
}

func (r *renderInvocation) qualifyConstructionReaders(
	ctx context.Context,
	constructionRound int,
	requalify map[manifest.ClusterID]bool,
) error {
	for i := range r.results {
		current := r.results[i].Orchestrator
		if current == nil || constructionRound != 0 && !requalify[current.cfg.ID.Cluster] {
			continue
		}
		if err := r.qualifyReaders(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

func (r *renderInvocation) qualifyReaders(ctx context.Context, i int) error {
	current := r.results[i].Orchestrator
	other, j, _ := r.opposite(i)
	if other != nil && j < i {
		return nil
	}
	before, after := current.index, (*manifest.DiscoveryIndex)(nil)
	inputs := [2]map[manifest.NamedResource]inputprojection.Projection{}
	if current.loader != nil {
		inputs[0] = current.loader.InputContent
	}
	if other != nil {
		after = other.index
		if other.loader != nil {
			inputs[1] = other.loader.InputContent
		}
	}
	if r.results[i].ID.Side == manifest.TreeHead {
		before, after = after, before
		inputs[0], inputs[1] = inputs[1], inputs[0]
	}
	changes := current.cfg.ExternalChanges
	if other != nil && (changes != nil || current.cfg.Input != other.cfg.Input) {
		base, head := current, other
		if current.cfg.ID.Side == manifest.TreeHead {
			base, head = other, current
		}
		qualified, err := change.Qualify(
			ctx,
			changes,
			[2]*store.Store{base.store, head.store},
			current.cfg.work.Scope(),
		)
		if err != nil {
			for _, side := range []int{i, j} {
				o := r.results[side].Orchestrator
				r.recordFailure(side, err)
				o.stop()
			}
			return nil
		}
		changes = qualified
	}
	resolved, err := changes.WithReaders(
		ctx,
		[2]*manifest.DiscoveryIndex{before, after},
		inputs,
		current.cfg.work.Scope(),
	)
	if err != nil {
		return err
	}
	current.cfg.ExternalChanges = resolved
	if other != nil {
		other.cfg.ExternalChanges = resolved
	}
	return nil
}

func (r *renderInvocation) prepareSession(ctx context.Context, i int) {
	o := r.results[i].Orchestrator
	o.preparation = nil
	o.loader.Options.Changes = o.cfg.ExternalChanges
	failure := o.lookupHistory(ctx)
	if failure == nil {
		o.configureControllers()
	}
	var result selection.AdmissionResult
	var accepted bool
	if failure == nil {
		result, accepted, failure = o.prepareAdmission(
			ctx,
			selection.AdmissionOptions{
				Request: selection.PrepareRequest{Invocation: true, Preparation: &o.admissionPreparation},
				Stage:   selection.Construction,
			},
		)
	}
	advanced := false
	if failure == nil && accepted {
		advanced, failure = o.session.Advance(ctx, &result)
		if failure == nil {
			advanced = advanced || len(o.session.PendingSources()) != 0
			failure = reconcileConstruction(ctx, o.session.PendingSources(), o.src, o.tasks)
		}
	}
	if failure != nil {
		r.recordFailure(i, failure)
		o.stop()
	} else if accepted && !advanced {
		o.preparation = &result
	}
	for range o.store.Sweep() {
	}
}

func (r *renderInvocation) completeDiscovery(ctx context.Context) {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil || o.session == nil {
			continue
		}
		r.wg.Go(func() {
			if failure := o.discover(ctx); failure != nil {
				r.results[i].Err = &treeError{id: r.results[i].ID, err: failure, attributed: len(r.results) > 1}
				r.results[i].Result = o.failureResult()
				o.stop()
				r.results[i].Orchestrator = nil
			}
		})
	}
	r.wg.Wait()
}

func (r *renderInvocation) qualifyDeclarations(ctx context.Context) error {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil || o.selection == nil {
			continue
		}
		other, j, found := r.opposite(i)
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
		prepared := o.declarationChildren(other, base, head)
		changes, err := o.cfg.ExternalChanges.WithDeclarations(
			ctx,
			change.DeclarationInputs{Stores: [2]*store.Store{base, head}, Prepared: prepared, Work: o.cfg.work.Scope()},
		)
		if err != nil {
			r.stopDeclarationFailures(i, j, found, err)
			continue
		}
		if err := o.bindDeclaredReaders(ctx, other, changes); err != nil {
			return err
		}
	}
	return nil
}

func (r *renderInvocation) stopDeclarationFailures(i, j int, found bool, err error) {
	for _, index := range []int{i, j} {
		if index != i && !found {
			continue
		}
		current := r.results[index].Orchestrator
		if current == nil {
			continue
		}
		r.recordFailure(index, err)
		current.stop()
	}
}

func (o *Orchestrator) declarationChildren(
	other *Orchestrator,
	base, head *store.Store,
) [2]map[manifest.NamedResource]store.Child {
	var prepared [2]map[manifest.NamedResource]store.Child
	for side, st := range [2]*store.Store{base, head} {
		if st == nil {
			continue
		}
		owner := o
		if owner.store != st {
			owner = other
		}
		prepared[side] = owner.promotionChildren()
	}
	return prepared
}

func (o *Orchestrator) promotionChildren() map[manifest.NamedResource]store.Child {
	var prepared map[manifest.NamedResource]store.Child
	builders := map[string]bool{}
	for _, id := range slices.SortedFunc(maps.Keys(o.index.Records), manifest.NamedResource.Compare) {
		record := o.index.Records[id]
		if record.Builder == "" || builders[record.Builder] {
			continue
		}
		builders[record.Builder] = true
		childValues := o.loader.PromotionDocuments(id)
		for childIndex := range childValues {
			child := childValues[childIndex]
			fact, _ := o.index.Lookup(child.Obj.Named())
			if fact.Builder != record.Builder || fact.BuilderOwner != record.BuilderOwner {
				continue
			}
			if prepared == nil {
				prepared = map[manifest.NamedResource]store.Child{}
			}
			prepared[child.Obj.Named()] = child
		}
	}
	return prepared
}

func (o *Orchestrator) bindDeclaredReaders(ctx context.Context, other *Orchestrator, changes *change.Set) error {
	if other == nil {
		before, after := o.index, (*manifest.DiscoveryIndex)(nil)
		inputs := [2]map[manifest.NamedResource]inputprojection.Projection{o.loader.InputContent, nil}
		if o.cfg.ID.Side == manifest.TreeHead {
			before, after = after, before
			inputs[0], inputs[1] = inputs[1], inputs[0]
		}
		resolved, err := changes.WithReaders(
			ctx,
			[2]*manifest.DiscoveryIndex{before, after},
			inputs,
			o.cfg.work.Scope(),
		)
		if err != nil {
			return err
		}
		o.cfg.ExternalChanges = resolved
		return nil
	}
	projectedOther := projectEntryReaders(other.index, other.entryBinding.Producer, o.entryBinding.Producer)
	projectedCurrent := projectEntryReaders(o.index, o.entryBinding.Producer, other.entryBinding.Producer)
	inputs := [2]map[manifest.NamedResource]inputprojection.Projection{o.loader.InputContent, other.loader.InputContent}
	before, after := o.index, projectedOther
	if o.cfg.ID.Side == manifest.TreeHead {
		before, after = after, before
		inputs[0], inputs[1] = inputs[1], inputs[0]
	}
	resolved, err := changes.WithReaders(ctx, [2]*manifest.DiscoveryIndex{before, after}, inputs, o.cfg.work.Scope())
	if err != nil {
		return err
	}
	o.cfg.ExternalChanges = resolved
	before, after = projectedCurrent, other.index
	if o.cfg.ID.Side == manifest.TreeHead {
		before, after = after, before
	}
	resolved, err = changes.WithReaders(ctx, [2]*manifest.DiscoveryIndex{before, after}, inputs, other.cfg.work.Scope())
	if err != nil {
		return err
	}
	other.cfg.ExternalChanges = resolved
	return nil
}

func (r *renderInvocation) installAdmission(ctx context.Context) {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil || o.selection == nil {
			continue
		}
		r.wg.Go(func() {
			o.completeBootstrap()
			failure := o.prepareInvocation(ctx)
			if failure == nil {
				failure = o.finishInitialAdmission(ctx)
			}
			if failure != nil {
				r.results[i].Result = o.failureResult()
				r.results[i].Err = &treeError{id: r.results[i].ID, err: failure, attributed: len(r.results) > 1}
				o.stop()
				r.results[i].Orchestrator = nil
				return
			}
			// Initial admission MUST release retired captures before scheduler startup.
			for range o.store.Sweep() {
			}
		})
	}
	r.wg.Wait()
}

func (r *renderInvocation) renderTrees(ctx context.Context) {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil {
			continue
		}
		if r.cfg.HeadOnly && r.results[i].ID.Side == manifest.TreeBase {
			r.results[i].Result = &Result{Work: o.freezeWork()}
			o.stop()
			r.results[i].Orchestrator = nil
			continue
		}
		r.wg.Go(func() {
			defer o.stop()
			if o.selection == nil {
				o.completeBootstrap()
				if failure := o.prepareInvocation(ctx); failure != nil {
					r.results[i].Orchestrator = nil
					r.results[i].Result = o.failureResult()
					r.results[i].Err = &treeError{id: r.results[i].ID, err: failure, attributed: len(r.results) > 1}
					return
				}
			}
			result, failure := o.collectResult(o.run(ctx))
			r.results[i].Result = result
			if failure != nil {
				r.results[i].Err = &treeError{id: r.results[i].ID, err: failure, attributed: len(r.results) > 1}
			}
		})
	}
	r.wg.Wait()
}

func prepareTreeChanges(ctx context.Context, tree *Tree, changes map[[2]*store.SourceArtifact]*change.Set) error {
	if tree.ExternalChanges != nil || tree.BaseInput == nil || tree.BaseInput == tree.Input {
		return nil
	}
	pair := [2]*store.SourceArtifact{tree.BaseInput, tree.Input}
	if tree.ID.Side == manifest.TreeBase {
		pair[0], pair[1] = pair[1], pair[0]
	}
	if set, ok := changes[pair]; ok {
		tree.ExternalChanges = set
	} else {
		set, err := change.Detect(ctx, pair[0], pair[1], change.DetectConfig{Work: tree.work.Scope()})
		if err != nil {
			return &treeError{id: tree.ID, err: err}
		}
		tree.ExternalChanges, changes[pair] = set, set
	}
	return nil
}

func prepareTreeInputs(
	ctx context.Context,
	trees []Tree,
	admitted map[string]*store.SourceArtifact,
	cfg *Config,
) error {
	if cfg.RecordWork || slog.Default().Enabled(ctx, slog.LevelDebug) {
		for i := range trees {
			tree := &trees[i]
			tree.work = new(manifest.WorkRecorder)
			tree.work.Init(&manifest.WorkKey{Tree: tree.ID, Source: manifest.BootstrapSourceID}, trees[0].work.Scope())
		}
	}
	for i := range trees {
		if err := trees[i].prepareInput(ctx, admitted); err != nil {
			return err
		}
	}
	excludeTreeInputs(trees, cfg.Cache.Directory())
	return nil
}

func excludeTreeInputs(trees []Tree, directory string) {
	if directory == "" {
		return
	}
	selected := make(map[*store.SourceArtifact]*store.SourceArtifact)
	for i := range trees {
		for _, artifact := range []**store.SourceArtifact{&trees[i].Input, &trees[i].BaseInput} {
			input := *artifact
			if input == nil {
				continue
			}
			if previous := selected[input]; previous != nil {
				*artifact = previous
				continue
			}
			filtered := gittree.ExcludeDirectory(input, directory)
			if filtered == input {
				continue
			}
			*artifact = filtered
			selected[input] = *artifact
		}
	}
}

func (tr *Tree) prepareInput(ctx context.Context, admitted map[string]*store.SourceArtifact) error {
	if tr.Input == nil {
		scan := cmp.Or(tr.Path, tr.RepoRoot)
		if scan == "" {
			return &treeError{
				id:  tr.ID,
				err: manifest.NewError(manifest.ErrInput, nil, "tree path is required"),
			}
		}
		input, path, err := baseline.AdmitDirectory(ctx, scan, tr.RepoRoot, tr.work.Scope())
		if err != nil {
			return &treeError{id: tr.ID, err: err}
		}
		tr.Input, tr.Path = input, path
		if previous := admitted[input.LocalPath]; previous != nil {
			tr.Input = previous
		} else {
			admitted[input.LocalPath] = input
		}
	}
	if tr.Input.FS == nil || tr.Input.LocalPath == "" {
		return &treeError{
			id:  tr.ID,
			err: manifest.NewError(manifest.ErrInput, nil, "tree input content unavailable"),
		}
	}
	tr.RepoRoot = tr.Input.LocalPath
	var err error
	tr.Path, err = filepath.Abs(cmp.Or(tr.Path, tr.RepoRoot))
	if err != nil {
		return &treeError{id: tr.ID, err: err}
	}
	return nil
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
