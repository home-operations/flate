package orchestrator

import (
	"context"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"sync"

	"github.com/home-operations/flate/internal/controllers/artifactgenerator"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/controllers/helmrelease"
	"github.com/home-operations/flate/internal/controllers/kustomization"
	resourcesetctrl "github.com/home-operations/flate/internal/controllers/resourceset"
	sourcectrl "github.com/home-operations/flate/internal/controllers/source"
	"github.com/home-operations/flate/internal/discovery"
	"github.com/home-operations/flate/internal/helm"
	"github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/bucket"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/external"
	"github.com/home-operations/flate/internal/source/git"
	"github.com/home-operations/flate/internal/source/helmchart"
	"github.com/home-operations/flate/internal/source/oci"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/values"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:gochecknoglobals // The immutable render-kind registry is computed once before parallel rendering.
var reconcilableKinds = func() []manifest.KindInfo {
	var kinds []manifest.KindInfo
	for info := range manifest.Kinds() {
		if info.Render {
			kinds = append(kinds, info)
		}
	}
	return kinds
}()

// Config contains invocation policy and shared rendering resources.
type Config struct {
	RecordWork bool

	// Selection narrows declarations before rendering; zero selection is universal.
	Selection selection.Request
	// HelmOptions tunes templating and kind filtering through SkipKinds.
	HelmOptions helm.Options

	// AllowMissingKinds tolerates missing source Secret material, Helm valuesFrom kinds, and CRDs.
	// Required postBuild references and malformed data remain errors.
	AllowMissingKinds []string
	// Supply contains authoritative reference-only documents, snapshotted before tree preparation.
	// Identity and payload come from Doc; later duplicates replace whole declarations.
	Supply []store.Declaration
	// StrictSubstitutions fails undefined postBuild variables without defaults.
	StrictSubstitutions bool

	// RestrictEgress is the untrusted-render switch. It (1) enables the SSRF
	// egress guard for ALL source fetches (kustomize remote resources/bases +
	// Git/OCI/Helm/Bucket sources) — outbound dials to loopback/RFC1918/link-
	// local/cloud-metadata addresses are refused — and (2) rejects GitRepository
	// URLs whose transport is not https/ssh (file://, git://, http://, bare
	// paths), so a fork-PR file:// source can't disclose an arbitrary in-pod
	// repo (#743). Off by default — flate normally renders TRUSTED repos that
	// legitimately reach private/LAN hosts and use local file:// self-sources;
	// a consumer rendering UNTRUSTED input (e.g. konflate fork mode) sets this.
	// The egress half is process-level (see internal/source/ssrfguard): go-git
	// installs its HTTPS transport process-wide, so a per-render git egress
	// policy is impossible.
	RestrictEgress bool

	// RegistryConfig is the docker config.json used for OCI auth.
	RegistryConfig string

	// CacheDir selects the source and render cache root. Empty selects memory
	// backing.
	CacheDir string
	// Cache, when injected, remains caller-owned and can be shared across runs.
	Cache *sourcecache.Cache
	// Network shares compatible HTTP transports and connection-timeout facts for this invocation.
	Network      *source.Network
	chartFetcher *helmchart.Fetcher
	projections  *manifest.ProducerProjections
	// HelmClient must borrow Cache; both remain caller-owned when supplied.
	HelmClient *helm.Client
	// StatusListener receives concurrent tree-qualified store events and must not block.
	StatusListener func(manifest.TreeID, manifest.DiagnosticIdentity, store.StatusInfo)
	// Concurrency caps the number of active reconcile bodies running
	// in parallel. <= 0 means unbounded (every Kustomization / HR
	// reconciles on its own goroutine). Background watch loops are
	// unaffected. Sensible default for I/O-bound work is
	// runtime.NumCPU() * 4.
	Concurrency int
	// HeadOnly prepares both sides and reconciles only the head.
	HeadOnly bool

	// SourceRetry tunes the bounded, classified retry applied to every
	// source fetch (Git/OCI/Bucket) on transient network failures —
	// connection resets, refused connections, dial/IO timeouts. Permanent
	// errors (bad path, missing secret, not-found) fail fast. Attempts
	// <= 1 disables retry; the CLI defaults it to 3. See
	// cache.RetryConfig.
	SourceRetry sourcecache.RetryConfig

	// GitDepth caps the shallow-clone history depth for GitRepository
	// sources. 0 clones full
	// history; the CLI defaults it to 1 (opt-out via --git-depth=0).
	// Commit-pinned refs always full-clone regardless. See
	// git.Fetcher.Depth.
	GitDepth int
}

// Orchestrator exposes a completed execution tree's store.
type Orchestrator struct {
	work       manifest.WorkSummary
	workFrozen bool

	changeNamespaces     []string
	cfg                  treeConfig
	store                *store.Store
	tasks                *task.Service
	src                  *sourcectrl.Controller
	ksc                  *kustomization.Controller
	hrc                  *helmrelease.Controller
	rsc                  *resourcesetctrl.Controller
	agc                  *artifactgenerator.Controller
	selection            *selection.Session
	loader               *loader.Loader
	session              *discovery.Session
	preparation          *selection.Admission
	admissionPreparation *selection.Preparation
	discoveryResult      *discovery.Result
	probes               *base.Execution
	history              map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}
	historyOwners           map[manifest.NamedResource][]manifest.SourceEvidence
	cache                   *sourcecache.Cache
	inputs                  source.HTTPInputs
	unsubStatus             store.Unsubscribe
	dependencySubstitutions values.DependencySubstitutions

	// repoRoot anchors fresh claims for post-render orphan reporting, including
	// Kustomizations admitted after static discovery is published.
	repoRoot string

	// index is immutable after discovery.
	index        *manifest.DiscoveryIndex
	entryBinding discovery.Binding
	entryFailure error
	admission    *discovery.Admission

	// orphans retain skipped failures for the completed result.
	orphans map[manifest.NamedResource]string

	// Store publication MUST follow cycle replacement after releasing preflightMu.
	preflightMu       sync.RWMutex
	preflightFailures map[manifest.NamedResource]string

	controllers base.Controllers
	depGraph    *dependencyGraph
}

// Result contains rendered document batches, failures and discovery diagnostics.
// All borrowed payloads MUST remain immutable.
type Result struct {
	emissions []selection.EmissionFacts
	catalog   *loader.Catalog

	Work manifest.WorkSummary

	Manifests []manifest.DocumentBatch
	Failed    map[manifest.NamedResource]store.StatusInfo
	Orphans   map[manifest.NamedResource]string
	// Blocked maps a failed resource to the immediate dependencies that blocked
	// it — present only for failures that are purely derived (the resource's own
	// reconcile never ran because a dependency failed or was missing), absent for
	// primary failures (a real render/template/build error). A consumer groups
	// derived failures under their root cause by walking these edges to a primary
	// failure or a missing id, instead of surfacing every cascaded failure.
	Blocked map[manifest.NamedResource][]manifest.NamedResource
	// Warnings contains ordinary side-specific diagnostics in deterministic order.
	Warnings []manifest.Warning
	// ClusterWarnings contains finalized cluster advisories on one RenderTrees result:
	// the non-absent head, otherwise the non-absent base, without snapshot attribution.
	ClusterWarnings []manifest.Warning
}

type treeConfig struct {
	Config
	Tree
}

func newOrchestrator(ctx context.Context, cfg *Config, treeInput *Tree, ts *task.Service) (*Orchestrator, error) {
	tree := *treeInput
	st := store.New()
	for _, child := range cfg.Supply {
		st.AddDeclaration(child)
	}
	o := &Orchestrator{
		cfg:    treeConfig{Config: *cfg, Tree: tree},
		inputs: source.NewHTTPInputs(cfg.Cache, cfg.Network, ts),
		store:  st,
		cache:  cfg.Cache,
		tasks:  ts,
	}
	recordWork := o.cfg.work != nil || cfg.RecordWork || slog.Default().Enabled(ctx, slog.LevelDebug)
	if recordWork && (o.cfg.work == nil || o.cfg.work.Key.Tree != tree.ID) {
		parent := o.cfg.work.Scope()
		o.cfg.work = new(manifest.WorkRecorder)
		o.cfg.work.Init(&manifest.WorkKey{Tree: tree.ID, Source: manifest.BootstrapSourceID}, parent)
	}
	st.SetWorkTree(tree.ID, o.cfg.work.Scope())
	o.loader = loader.New(st)
	o.loader.Options.AllowMissingKinds = cfg.AllowMissingKinds
	o.cfg.Selection.Changed = cfg.Selection.Changed || tree.ExternalChanges != nil
	var err error
	o.selection, err = selection.New(st, o.loader, o.cfg.Selection)
	if err != nil {
		o.inputs.Close()
		o.loader.Close()
		return nil, err
	}
	o.loader.Options.Session, o.loader.Options.Changes = o.selection, tree.ExternalChanges
	if cfg.StatusListener != nil {
		o.unsubStatus = st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, value any) {
			if st.ReportHidden(id) {
				return
			}
			cfg.StatusListener(
				tree.ID,
				manifest.DiagnosticFor(id, st.GetObject(id)),
				value.(store.StatusInfo), //nolint:errcheck // Status events always carry store.StatusInfo.
			)
		}, false)
	}
	trees := kustomize.NewTreeCache(cfg.Cache, &o.inputs, ts)
	o.src = sourcectrl.New(st, ts)
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindBucket, manifest.KindHelmRepository, manifest.KindOCIRepository, manifest.KindHelmChart} {
		o.src.Fetchers[kind] = nil
	}
	o.src.Fetchers[manifest.KindExternalArtifact] = source.Wrap(manifest.KindExternalArtifact, &external.Fetcher{})
	o.ksc = kustomization.New(st, ts, trees)
	o.hrc = helmrelease.New(base.New(st, ts), cfg.HelmClient, cfg.HelmOptions)
	o.rsc = resourcesetctrl.New(st, ts, cfg.Cache)
	o.agc = artifactgenerator.New(st, ts, cfg.Cache)
	o.controllers = base.Controllers{
		Kustomization:     o.ksc,
		HelmRelease:       o.hrc,
		ResourceSet:       o.rsc,
		Source:            o.src,
		ArtifactGenerator: o.agc,
	}
	o.depGraph = newDependencyGraph()
	return o, nil
}

// Store returns the underlying object store.
func (o *Orchestrator) Store() *store.Store { return o.store }

// parentOf returns the structural parent, falling back to rendered ownership.
// Structural ownership is immutable after discovery.
func (o *Orchestrator) parentOf(id manifest.NamedResource) (manifest.NamedResource, bool) {
	if record, _ := o.index.Lookup(id); record.Parent != (manifest.NamedResource{}) {
		return record.Parent, true
	}
	return o.store.RenderedParent(id, o.index)
}

func (o *Orchestrator) discover(ctx context.Context) error {
	gitFetcher := &git.Fetcher{
		Cache: o.cache, Depth: o.cfg.GitDepth,
		RestrictSchemes: o.cfg.RestrictEgress,
	}
	o.ksc.Trees.SetGitBaseFetcher(
		func(ctx context.Context, request kustomize.GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			artifact, err := gitFetcher.FetchRemoteBase(
				ctx,
				&git.RemoteBaseRequest{
					URL:        request.URL,
					Ref:        request.Ref,
					Timeout:    request.Timeout,
					Submodules: request.Submodules,
					Work:       request.Work,
				},
				&sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry},
			)
			if artifact != nil {
				for itemIndex := range artifact.Warnings {
					warning := artifact.Warnings[itemIndex]

					o.store.AddWarning(warning)
				}
			}
			return artifact, err
		},
	)
	if current, exists := o.src.Fetchers[manifest.KindGitRepository]; exists && current == nil {
		o.src.Fetchers[manifest.KindGitRepository] = source.Wrap(manifest.KindGitRepository, gitFetcher)
	}
	config := discovery.Config{
		Cache:        o.cfg.Cache,
		Work:         o.cfg.work.Scope(),
		Declarations: o.cfg.Supply,
		Selection:    o.cfg.Selection, Loader: o.loader,
		Path: o.cfg.Path, RepoRoot: o.cfg.RepoRoot, SelfURLs: o.cfg.SelfURLs, Input: o.cfg.Input, Entry: o.cfg.entry,

		Store: o.store,
		Trees: o.ksc.Trees, RemoteInputs: &o.inputs, RemoteRetry: o.cfg.SourceRetry,
		StrictSubstitutions: o.cfg.StrictSubstitutions, SkipKinds: o.cfg.HelmOptions.SkipResourceKinds(),
	}
	o.cfg.entry = nil
	var res *discovery.Result
	var err error
	if o.selection != nil {
		if o.session == nil {
			return o.beginDiscovery(ctx, &config)
		}
		res, err = o.session.Freeze(ctx)
	} else {
		res, err = discovery.Run(ctx, config)
	}

	if err != nil {
		return err
	}
	o.cfg.Input = o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact) //nolint:errcheck // Bootstrap admission publishes a source artifact.
	o.cfg.RepoRoot = res.RepoRoot
	o.repoRoot = res.RepoRoot
	o.discoveryResult = res
	o.index = res.Index
	o.entryBinding = res.Binding
	o.admission = res.Admission
	o.dependencySubstitutions = res.DependencySubstitutions
	if o.entryBinding.Failure != nil && slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.DebugContext(
			ctx,
			"orchestrator: entry failed",
			"id",
			o.diagnostic(o.entryBinding.Producer).String(),
			"err",
			manifest.ErrorDetails(o.entryBinding.Failure),
		)
	}

	return nil
}

func (o *Orchestrator) beginDiscovery(ctx context.Context, config *discovery.Config) error {
	var err error
	o.session, err = discovery.Begin(ctx, *config)
	if err != nil {
		return err
	}
	o.admission = o.session.Admission()
	o.cfg.Input = o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact) //nolint:errcheck // Bootstrap admission publishes a source artifact.
	o.cfg.RepoRoot, o.repoRoot, o.index = o.cfg.Input.LocalPath, o.cfg.Input.LocalPath, o.loader.Index
	return nil
}

func (o *Orchestrator) completeBootstrap() {
	o.failDependsOnCycles()
	if o.loader != nil && o.entryBinding.Failure != nil {
		o.store.UpdateStatus(
			o.entryBinding.Producer,
			store.StatusInfo{Status: store.StatusFailed, Message: o.entryBinding.Failure.Error()},
		)
	}
	ociFetcher := &oci.Fetcher{Cache: o.cache, Network: o.cfg.Network, RegistryConfig: o.cfg.RegistryConfig}
	o.rsc.Inputs = ociFetcher
	o.rsc.Retry = o.cfg.SourceRetry
	o.rsc.AllowMissingSecrets = slices.Contains(o.cfg.AllowMissingKinds, manifest.KindSecret)
	for kind, f := range map[string]source.Fetcher{
		manifest.KindBucket: source.Wrap(manifest.KindBucket,
			&bucket.Fetcher{Cache: o.cache, Network: o.cfg.Network}),
		manifest.KindOCIRepository: source.Wrap(manifest.KindOCIRepository, ociFetcher),
		manifest.KindHelmChart:     source.Wrap(manifest.KindHelmChart, o.cfg.chartFetcher),
	} {
		if current, exists := o.src.Fetchers[kind]; exists && current == nil {
			o.src.Fetchers[kind] = f
		}
	}
}

// replacePreflightFailures replaces the current preflight-failure map
// with failures and returns the IDs that were previously failing but
// are no longer present in the new set (cleared entries). Must be
// called while holding preflightMu for writing.
func (o *Orchestrator) replacePreflightFailures(failures map[manifest.NamedResource]string) []manifest.NamedResource {
	cleared := make([]manifest.NamedResource, 0, len(o.preflightFailures))
	for id := range o.preflightFailures {
		if _, stillFailed := failures[id]; !stillFailed {
			cleared = append(cleared, id)
		}
	}
	if len(failures) == 0 {
		o.preflightFailures = nil
		return cleared
	}
	o.preflightFailures = maps.Clone(failures)
	return cleared
}

func (o *Orchestrator) preflightFailure(id manifest.NamedResource) (string, bool) {
	o.preflightMu.RLock()
	defer o.preflightMu.RUnlock()
	msg, ok := o.preflightFailures[id]
	return msg, ok
}
