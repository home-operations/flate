// Package source reconciles Flux source CRs (GitRepository,
// OCIRepository, Bucket, ExternalArtifact, HelmRepository) into on-disk
// artifacts via per-kind Fetcher implementations from pkg/source, then
// publishes the result to the Store. Mirrors
// pkg/controllers/{kustomization,helmrelease}.
//
// The controller does not know about individual source kinds — it
// dispatches via the Fetchers map keyed by id.Kind, so adding a new
// kind is a one-line registration at orchestrator-construction time.
package source

import (
	"cmp"
	"context"
	"log/slog"
	"time"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type localSources interface {
	Resolve(
		context.Context,
		manifest.NamedResource,
		manifest.BaseManifest,
		*store.ReadSet,
	) (store.Child, *store.SourceArtifact, error)
}

type sourceFacts interface {
	ExistenceOnly() bool
	SourceInterval() time.Duration
	Composition() manifest.ArtifactComposition
}

// Controller watches the Store for source-kind objects, fetches each
// into an on-disk artifact via the matching Fetcher, and updates the
// Store with the result.
type Controller struct {
	*base.Controller

	cache *cache.Cache
	retry cache.RetryConfig

	// Fetchers maps source kinds to their fetchers. Kinds without an entry
	// are ignored. Register fetchers before StartLifecycle.
	Fetchers map[string]src.Fetcher

	// allowMissingSecrets converts ErrMissingSecret fetch errors into a
	// skip (StatusReady with ReasonSkipped). Wired from
	// FetchOptions.AllowMissingSecrets via Configure.
	allowMissingSecrets bool
	admission           localSources
}

// FetchOptions carries the post-bootstrap state the orchestrator wires
// onto the controller. Filter narrows fetches to sources referenced by
// changed resources in changed-only mode.
type FetchOptions struct {
	// Loader borrows the invocation's declaration admission boundary.
	Loader              *loader.Loader
	Session             *selection.Session
	AllowMissingSecrets bool
	// Admission resolves runtime sources against immutable discovery policy.
	Admission localSources
}

type execution struct {
	*Controller

	run         base.Execution
	acquired    *store.SourceArtifact
	acquisition *store.ReadSet
}

// New requires fetcher registration before StartLifecycle.
func New(s *store.Store, t *task.Service, owner *cache.Cache) *Controller {
	return &Controller{
		Controller: base.New(s, t),
		cache:      owner,
		Fetchers:   map[string]src.Fetcher{},
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// StartLifecycle.
func (c *Controller) Configure(opts FetchOptions, retry cache.RetryConfig) {
	common := base.Options{Session: opts.Session, Loader: opts.Loader}
	if opts.Loader != nil {
		common.Index = opts.Loader.Index
	}
	c.Controller.Configure(common)
	c.retry = retry
	c.allowMissingSecrets = opts.AllowMissingSecrets
	c.admission = opts.Admission
}

// Owns reports whether id is a node this controller reconciles — the dag
// Dispatcher uses it to route source nodes to ReconcileNode. A registered
// kind is not enough: kind names are not unique across API groups, and a
// foreign CRD reusing one (seaweed.seaweedfs.com Bucket) parses to a
// RawObject that the kind's Fetcher would reject as an unexpected payload.
// An id absent from the store still counts as ours — reporting the
// vanished object is the controller's job.
func (c *Controller) Owns(id manifest.NamedResource) bool {
	if _, ok := c.Fetchers[id.Kind]; !ok {
		return false
	}
	obj := c.Store.GetObject(id)
	return obj == nil && id.KindInfo().IsSource() || manifest.KindOf(obj).Runnable
}

// ReconcileNode returns the captured assessment of a source acquisition.
func (c *Controller) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
	previous schedule.Result,
) *base.Execution {
	ex := &execution{Controller: c}
	defer func() { ex.acquisition.Retire() }()
	ex.run.Init(c.Controller, id)
	ex.run.Take(previous)
	for {
		ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
		if artifact, _ := c.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); c.admission != nil &&
			artifact != nil && !artifact.Reads.Valid() {
			if err := c.construct(ctx, id, false); err != nil {
				c.Store.UpdateFailure(id, err)
				return &ex.run
			}
		}
		if ex.acquired == nil || ex.run.Reads.Valid() {
			return &ex.run
		}
		next := &execution{Controller: c, acquired: ex.acquired, acquisition: ex.acquisition}
		next.run.Init(c.Controller, id)
		evidence := &ex.run.Reads
		if ex.acquisition != nil {
			evidence = ex.acquisition
		}
		if ex.acquisition != nil && !evidence.Valid() || !next.run.Reads.Matches(evidence) {
			next.run.Reads.Retire()
			return &ex.run
		}
		next.run.Reads.CarryAdvisories(&ex.run.Reads)
		next.run.Take(schedule.Result{Assessment: &ex.run})
		ex.run.Retire()
		ex = next
	}
}

func (c *execution) reconcile(ctx context.Context, obj manifest.BaseManifest, level manifest.DrainLevel) error {
	id := obj.Named()
	facts := obj.(sourceFacts) //nolint:errcheck // The source controller accepts only source manifests.
	if facts.ExistenceOnly() {
		_, err := c.run.RequireRefresh(ctx, obj, manifest.GateParents, level)
		return err
	}
	fetcher := c.Fetchers[id.Kind]
	var selected *store.SourceArtifact
	return c.run.Acquire(ctx, obj, base.Acquisition{
		AllowMissingSecrets: c.allowMissingSecrets,
		Fetch: func(ctx context.Context) (src.PreparedFetch, error) {
			return fetcher.Fetch(ctx, obj, &c.run.Reads, &cache.Policy{MaxAge: facts.SourceInterval(), Retry: c.retry})
		},
		Select: func(ctx context.Context) (bool, error) {
			if c.admission == nil {
				return false, nil
			}
			var err error
			_, selected, err = c.admission.Resolve(ctx, id, obj, &c.run.Reads)
			return selected != nil, err
		},
		Resolve: func(ctx context.Context, level manifest.DrainLevel) (*store.SourceArtifact, error) {
			return c.resolve(ctx, obj, selected, level)
		},
		Complete: func(fetchCtx context.Context, artifact *store.SourceArtifact, unavailable error, level manifest.DrainLevel) error {
			return c.completeFetch(
				fetchCtx,
				obj,
				fetchCompletion{artifact: artifact, unavailable: unavailable, level: level},
			)
		},
	}, level)
}

//nolint:nilnil // Acquisition requires a nil artifact and nil error to continue to fetching.
func (c *execution) resolve(
	ctx context.Context,
	obj manifest.BaseManifest,
	resolved *store.SourceArtifact,
	level manifest.DrainLevel,
) (*store.SourceArtifact, error) {
	id := obj.Named()
	facts := obj.(sourceFacts) //nolint:errcheck // The source controller accepts only source manifests.
	composed := len(facts.Composition().Includes) != 0
	if composed && c.acquisition == nil {
		c.acquisition = new(store.ReadSet)
		c.acquisition.Init(c.Store, id, nil)
		if !c.acquisition.Matches(&c.run.Reads) {
			return nil, c.run.Checkpoint()
		}
	}
	if composed {
		if _, err := c.run.RequireRefresh(ctx, obj, manifest.GateArtifacts, level); err != nil {
			return nil, err
		}
	}
	artifact, _ := c.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if artifact != nil && artifact.FS != nil &&
		(artifact.Reads.Valid() || c.run.Reads.Matches(artifact.Reads)) {
		c.run.Reads.CarryAdvisories(artifact.Reads)
		c.run.Disposition = store.AssessmentReused
		return artifact, nil
	}
	if resolved == nil && composed {
		resolved, _ = c.run.Reads.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	}
	if resolved != nil {
		return resolved, nil
	}
	// Declaration and input gates MUST follow credential settlement.
	if _, err := c.run.RequireRefresh(ctx, obj, manifest.GateParents, level); err != nil {
		return nil, err
	}
	if level == manifest.DrainNone {
		if blocked := c.run.Reads.UnsettledProducers(id); len(blocked) != 0 {
			return nil, &depwait.ErrBlocked{Deps: blocked}
		}
	}
	if c.acquired != nil {
		artifact := c.acquired
		c.acquired = nil
		return artifact, nil
	}
	if _, err := c.run.RequireRefresh(ctx, obj, manifest.GateInputs, level); err != nil {
		return nil, err
	}
	c.SetPendingUnlessReady(id, "fetching")
	return nil, nil
}

type fetchCompletion struct {
	artifact    *store.SourceArtifact
	unavailable error
	level       manifest.DrainLevel
}

func (c *execution) completeFetch(
	ctx context.Context,
	obj manifest.BaseManifest,
	completion fetchCompletion,
) error {
	artifact := completion.artifact
	if artifact != nil {
		for i := range artifact.Warnings {
			c.run.Reads.AddWarning(artifact.Warnings[i])
		}
	}
	if completion.unavailable != nil {
		return manifest.NewError(manifest.ErrSourceSkipped, completion.unavailable, "")
	}
	if c.run.Disposition == store.AssessmentReused {
		if artifact != nil && !artifact.Reads.Valid() {
			c.publish(artifact)
		}
		return nil
	}
	if artifact != nil {
		if !c.run.Reads.Valid() {
			c.acquired = artifact
			return nil
		}
		completed, err := c.compose(ctx, obj, artifact, completion.level)
		if err != nil {
			return err
		}
		if !c.run.Reads.Valid() {
			c.acquired = artifact
			return nil
		}
		c.publish(completed)
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(ctx, "source: fetch complete", "id", manifest.DiagnosticFor(obj.Named(), obj).String())
	}
	return nil
}

func (c *execution) publish(artifact *store.SourceArtifact) {
	for range c.run.Reads.Inputs() {
		artifact = artifact.WithReads(&c.run.Reads)
		break
	}
	c.Store.SetArtifact(c.run.Reads.ID(), artifact)
}

func (c *execution) compose(
	ctx context.Context,
	obj manifest.BaseManifest,
	artifact *store.SourceArtifact,
	level manifest.DrainLevel,
) (*store.SourceArtifact, error) {
	facts := obj.(sourceFacts) //nolint:errcheck // The source controller accepts only source manifests.
	composition := facts.Composition()
	if len(composition.Includes) == 0 {
		return artifact, nil
	}
	if _, err := c.run.RequireRefresh(ctx, obj, manifest.GateArtifacts, level); err != nil {
		return nil, err
	}
	request := cache.Composition{
		Base: cache.CompositionInput{
			FS:      artifact.FS,
			Content: artifact.Content,
		},
		Policy: composition.Policy,
		Ignore: composition.Ignore,
		Inputs: make(
			[]cache.CompositionInput,
			len(composition.Includes),
		),
		Copies: make([]cache.Copy, len(composition.Includes)),
	}
	for i := range composition.Includes {
		include := &composition.Includes[i]
		input, _ := c.run.Reads.GetArtifact(include.Reference.NamedResource, store.ArtifactContent).(*store.SourceArtifact)
		if input == nil || input.FS == nil {
			return nil, &depwait.ErrBlocked{Deps: []manifest.NamedResource{include.Reference.NamedResource}}
		}
		request.Inputs[i] = cache.CompositionInput{FS: input.FS, Content: input.Content}
		request.Copies[i] = cache.Copy{Input: i, From: include.FromPath, To: include.ToPath, Mode: cache.PlaceTree}
	}
	completed := *artifact
	var err error
	c.Tasks.YieldSlot(func() {
		input, descriptor, key, composeErr := c.cache.Compose(
			ctx,
			request,
			&cache.Policy{MaxAge: facts.SourceInterval(), Retry: c.retry},
		)
		err = composeErr
		completed.FS, completed.Key, completed.Content = input, key, cache.AcquisitionIdentity(input)
		completed.Digest, completed.Size = descriptor.Digest.String(), descriptor.Size
	})
	if err != nil {
		return nil, err
	}
	if artifact.LocalPath != "" {
		root := cmp.Or(artifact.LocalRoot, artifact.LocalPath)
		completed.LocalPath, _ = gittree.Label(
			gittree.LabelOptions{
				Root:    root,
				Mode:    gittree.Source,
				Source:  obj.Named(),
				Content: struct{ Kind, URL, Root string }{artifact.Kind, artifact.URL, root},
			},
		)
		completed.LocalRoot = completed.LocalPath
	}
	completed.InputAlias, completed.ContentFingerprint = false, nil
	return &completed, nil
}
