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

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/baseline"
	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/discovery"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/engine/source"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	sourcegit "github.com/home-operations/flate/internal/engine/source/git"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/source/helmchart"
	"github.com/home-operations/flate/internal/engine/source/oci"
	"github.com/home-operations/flate/internal/engine/source/ssrfguard"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/pkg/manifest"
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

// RenderTrees requires Close for every non-nil invocation, including on failure.
func RenderTrees(
	ctx context.Context,
	trees []Tree,
	cfg Config, //nolint:gocritic // Invocation setup owns its configuration snapshot.
) (*Invocation, error) {
	if err := validateTrees(trees); err != nil {
		return nil, err
	}
	r, err := NewInvocation(ctx, cfg)
	if err != nil {
		return r, err
	}
	err = r.Render(ctx, trees)
	return r, err
}

// Invocation retains completed trees and their backing resources until Close.
type Invocation struct {
	Config  *Config
	Results []Rendered

	trees          []Tree
	results        []Rendered
	stores         []*store.Store
	byID           map[manifest.TreeID]int
	budget         *task.Service
	wg             sync.WaitGroup
	err            error
	closeErr       error
	releaseCache   func() error
	releaseNetwork func()
	closed         bool
}

// NewInvocation requires Close for every non-nil return, including initialization failure.
func NewInvocation(
	ctx context.Context,
	cfg Config, //nolint:gocritic // Invocation setup owns its configuration snapshot.
) (*Invocation, error) {
	ssrfguard.Restrict(cfg.RestrictEgress)
	if err := snapshotSubstitutions(&cfg); err != nil {
		return nil, err
	}
	r := &Invocation{Config: &cfg, budget: task.NewBounded(cfg.Concurrency)}
	if cfg.Network == nil {
		cfg.Network = new(source.Network)
		r.releaseNetwork = cfg.Network.Close
	}
	cfg.GitTransports = maps.Clone(cfg.GitTransports)
	if _, err := sourcegit.NewClient(
		sourcegit.ClientConfig{Transports: cfg.GitTransports, Network: cfg.Network},
	); err != nil {
		r.err = err
		return r, err
	}
	if cfg.Cache == nil {
		var err error
		cfg.Cache, err = sourcecache.New(ctx, cfg.CacheDir)
		if err != nil {
			r.err = err
			return r, err
		}
		r.releaseCache = cfg.Cache.Close
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
	if cfg.HelmClient == nil {
		cfg.HelmClient = helm.NewClient(cfg.Cache, cfg.chartFetcher)
	}
	cfg.projections = new(manifest.ProducerProjections)
	return r, nil
}

// Render MUST drain tasks and callbacks before return while retaining data until Close.
func (r *Invocation) Render(ctx context.Context, trees []Tree) (err error) {
	inputs := source.NewHTTPInputs(r.Config.Cache, r.Config.Network, r.budget)
	defer func() {
		for _, result := range r.results {
			if err != nil && result.Orchestrator != nil {
				result.stop()
			}
		}
		inputs.Close()
		r.budget.BlockTillDone()
		if r.byID == nil {
			r.byID = make(map[manifest.TreeID]int, len(r.results))
		}
		for i, result := range r.results {
			r.byID[result.ID] = i
		}
		r.Results = slices.Clone(r.results)
		r.err = err
	}()
	if validationErr := validateTrees(trees); validationErr != nil {
		return validationErr
	}
	trees, err = r.prepareTrees(ctx, trees, &inputs)
	if err != nil {
		r.results = failedTrees(trees, err)
		return err
	}
	r.trees, r.results, r.stores, r.byID = trees, make([]Rendered, len(trees)),
		make([]*store.Store, len(trees)), make(map[manifest.TreeID]int, len(trees))
	r.discoverTrees(ctx)
	r.Config.projections.Clear()
	if discoveryErr := r.stopFailedDiscovery(); discoveryErr != nil {
		r.stopRemaining()
		return discoveryErr
	}
	if constructionErr := r.constructTrees(ctx); constructionErr != nil {
		return constructionErr
	}
	r.completeDiscovery(ctx)
	if qualificationErr := r.qualifyDeclarations(ctx); qualificationErr != nil {
		return qualificationErr
	}
	r.installAdmission(ctx)
	r.renderTrees(ctx)
	clusterAdvisories(r.results, r.stores)
	for _, result := range r.results {
		err = errors.Join(err, result.Err)
	}
	return err
}

// Close MUST follow rendering, callbacks, selection, and every borrowed-data consumer.
func (r *Invocation) Close() error {
	if r == nil {
		return nil
	}
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	for _, result := range r.results {
		result.Result.Close()
		if result.Orchestrator != nil {
			*result.Orchestrator = Orchestrator{}
		}
	}
	for _, st := range r.stores {
		if st != nil {
			for range st.Sweep() {
			}
		}
	}
	r.Results, r.results, r.stores, r.trees, r.byID, r.budget = nil, nil, nil, nil, nil, nil
	if r.releaseCache != nil {
		r.closeErr = r.releaseCache()
	}
	if r.releaseNetwork != nil {
		r.releaseNetwork()
	}
	r.releaseCache, r.releaseNetwork = nil, nil
	r.Config = nil
	return r.closeErr
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
	for _, tree := range trees {
		result := Rendered{ID: tree.ID, Result: &Result{Work: tree.work.Scope().Snapshot(0)}}
		if tree.Input != nil {
			result.Result.Warnings = tree.Input.Warnings
		}
		_, result.Err = ownedErrors(err, []manifest.TreeID{tree.ID})
		if failure, ok := errors.AsType[*treeError](result.Err); ok {
			failure.attributed = len(trees) > 1
		}
		results = append(results, result)
	}
	slices.SortFunc(results, func(a, b Rendered) int { return a.ID.Compare(b.ID) })
	return results
}

func (r *Invocation) discoverTrees(ctx context.Context) {
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
			o, failure := newOrchestrator(ctx, new(*r.Config), new(tree), r.budget.Fork())
			if failure != nil {
				r.results[i].Result = &Result{Warnings: tree.Input.Warnings}
				r.results[i].Err = &treeError{id: tree.ID, err: failure, attributed: len(r.results) > 1}
				return
			}
			r.stores[i] = o.store
			r.results[i].Orchestrator = o
			if failure := o.discover(ctx); failure != nil {
				result := o.failureResult()
				if !o.selection.Snapshot().Output(manifest.NamedResource{}) && o.session == nil {
					result.Warnings = tree.Input.Warnings
				}
				r.results[i].Result = result
				r.results[i].Err = &treeError{id: tree.ID, err: failure, attributed: len(r.results) > 1}
			}
		})
	}
	r.wg.Wait()
}

func (r *Invocation) stopFailedDiscovery() error {
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
		if !o.selection.Snapshot().Output(manifest.NamedResource{}) && o.session == nil {
			err = errors.Join(err, result.Err)
		}
		o.stop()
		r.results[i].Orchestrator = nil
	}
	return err
}

func (r *Invocation) stopRemaining() {
	for i, result := range r.results {
		if result.Orchestrator != nil {
			r.results[i].Result = result.failureResult()
			result.stop()
			r.results[i].Orchestrator = nil
		}
	}
}

func (r *Invocation) opposite(i int) (*Orchestrator, int, bool) {
	id := r.results[i].ID
	id.Side = manifest.TreeHead - id.Side
	j, found := r.byID[id]
	if !found {
		return nil, j, false
	}
	return r.results[j].Orchestrator, j, true
}

func (r *Invocation) recordFailure(i int, err error) {
	r.results[i].Result = r.results[i].failureResult()
	r.results[i].Err = &treeError{id: r.results[i].ID, err: err, attributed: len(r.results) > 1}
	r.results[i].Orchestrator = nil
}

func (r *Invocation) constructTrees(ctx context.Context) error {
	constructionRound := 0
	requalify := map[manifest.ClusterID]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
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

func (r *Invocation) qualifyConstructionReaders(
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

func (r *Invocation) qualifyReaders(ctx context.Context, i int) error {
	current := r.results[i].Orchestrator
	other, j, found := r.opposite(i)
	if other != nil && j < i {
		return nil
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
	if err := current.completeDeclarations(ctx, other, changes); err != nil {
		r.stopDeclarationFailures(i, j, found, err)
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

func (r *Invocation) prepareSession(ctx context.Context, i int) {
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

func (r *Invocation) completeDiscovery(ctx context.Context) {
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

func (r *Invocation) qualifyDeclarations(ctx context.Context) error {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil || o.selection.Snapshot().Output(manifest.NamedResource{}) {
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
		if err == nil {
			err = o.bindDeclaredReaders(ctx, other, changes)
		}
		if err != nil {
			r.stopDeclarationFailures(i, j, found, err)
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Invocation) stopDeclarationFailures(i, j int, found bool, err error) {
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

func (o *Orchestrator) completeDeclarations(ctx context.Context, other *Orchestrator, changes *change.Set) error {
	for _, current := range []*Orchestrator{o, other} {
		if current == nil || current.loader == nil {
			continue
		}
		demand := selection.DemandDeclarations(current.cfg.ID.Side, changes, current.loader.Index)
		index, err := current.loader.CompleteDeclarations(ctx, demand)
		if err != nil {
			return err
		}
		current.index = index
	}
	return nil
}

func (o *Orchestrator) bindDeclaredReaders(ctx context.Context, other *Orchestrator, changes *change.Set) error {
	if err := o.completeDeclarations(ctx, other, changes); err != nil {
		return err
	}
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

func (r *Invocation) installAdmission(ctx context.Context) {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil || o.selection.Snapshot().Output(manifest.NamedResource{}) {
			continue
		}
		r.wg.Go(func() {
			failure := o.completeBootstrap(ctx)
			if failure == nil {
				failure = o.prepareInvocation(ctx)
			}
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

func (r *Invocation) renderTrees(ctx context.Context) {
	for i := range r.results {
		o := r.results[i].Orchestrator
		if o == nil {
			continue
		}
		if r.Config.HeadOnly && r.results[i].ID.Side == manifest.TreeBase {
			r.results[i].Result = &Result{Work: o.freezeWork()}
			o.stop()
			r.results[i].Orchestrator = nil
			continue
		}
		r.wg.Go(func() {
			defer o.stop()
			if o.selection.Snapshot().Output(manifest.NamedResource{}) {
				failure := o.completeBootstrap(ctx)
				if failure == nil {
					failure = o.prepareInvocation(ctx)
				}
				if failure != nil {
					r.results[i].Orchestrator = nil
					r.results[i].Result = o.failureResult()
					r.results[i].Err = &treeError{id: r.results[i].ID, err: failure, attributed: len(r.results) > 1}
					return
				}
			}
			result, failure := o.collectResult(ctx, o.run(ctx))
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
