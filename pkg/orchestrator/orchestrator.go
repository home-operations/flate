package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/artifactgenerator"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	resourcesetctrl "github.com/home-operations/flate/pkg/controllers/resourceset"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

var reconcilableKinds = func() []manifest.KindInfo {
	var kinds []manifest.KindInfo
	for info := range manifest.Kinds() {
		if info.Render {
			kinds = append(kinds, info)
		}
	}
	return kinds
}()

// SubstitutionSource supplies a ConfigMap or Secret only for postBuild references.
// Path identifies its input file in collision diagnostics; empty denotes SDK input.
type SubstitutionSource struct {
	Object manifest.BaseManifest
	Path   string
}

// Config carries everything the orchestrator needs.
type Config struct {
	// Input carries this side's admitted direct reader and source coordinates.
	Input *store.SourceArtifact
	// BaseInput carries the opposite side's admitted direct reader.
	BaseInput *store.SourceArtifact

	// Path is the directory to scan for Flux objects (the scan entry
	// point — a cluster's Flux entry, which may be a subdir of RepoRoot).
	Path string
	// RepoRoot is the source root that Kustomization spec.path values
	// resolve against. Supplied explicitly by SDK consumers rendering
	// extracted trees that have no .git; the CLI defaults it to the .git
	// ancestor of Path. Empty ⇒ repository lookup runs,
	// preserving local behavior.
	RepoRoot string
	// SelfURLs are the remote URL(s) this tree represents, for
	// self-referential GitRepository aliasing (a cluster pulling itself).
	// Supplied explicitly by SDK consumers rendering extracted trees with
	// no .git/config; empty ⇒ the working tree's .git remotes are read.
	SelfURLs []string
	// PathOrig, when non-empty, switches every command into
	// changed-only mode: only resources whose source files differ
	// (plus the sources they reference) get reconciled.
	PathOrig string

	// HelmOptions tunes templating and kind filtering through SkipKinds.
	HelmOptions helm.Options
	// DisableChartDigestTracking selects chart digest tracking for the orchestrator.
	// Bootstrap assigns HelmOptions.DisableChartDigestTracking and ignores any value
	// supplied there. Nil selects automatic detection from file-loaded HelmRelease
	// values; false forces tracking on. New copies the requested value.
	DisableChartDigestTracking *bool
	// WipeSecrets controls Secret cleartext placeholders.
	WipeSecrets bool
	// AllowMissingSecrets converts source auth-secret-not-found errors
	// into skips and omits HelmRelease valuesFrom Secret/ConfigMap refs
	// that cannot materialize in the offline tree. Skipped source
	// resources mark Ready with ReasonSkipped; omitted valuesFrom
	// refs let HelmReleases render with the remaining values.
	AllowMissingSecrets bool
	// SubstituteFrom contains CI-side postBuild sources, never stored or emitted.
	SubstituteFrom []SubstitutionSource
	// Substitute overrides referenced and inline values when postBuild is present.
	Substitute map[string]string
	// StrictSubstitutions fails undefined postBuild variables without defaults.
	StrictSubstitutions bool

	// AllowMissingCRDs accepts CRD dependencies absent from the offline inputs
	// because the cluster installs them at runtime, and reports each missing CRD
	// with its dependent ResourceSets.
	AllowMissingCRDs bool

	// ForceGenericProvider sends non-generic spec.provider sources
	// (GitRepository, OCIRepository, Bucket) down the generic SecretRef path
	// instead of rejecting them up front. That path works offline when static
	// credentials are supplied; otherwise it fails on the missing secret.
	ForceGenericProvider bool

	// RestrictEgress is the untrusted-render switch. It (1) enables the SSRF
	// egress guard for ALL source fetches (kustomize remote resources/bases +
	// Git/OCI/Helm/Bucket sources) — outbound dials to loopback/RFC1918/link-
	// local/cloud-metadata addresses are refused — and (2) rejects GitRepository
	// URLs whose transport is not https/ssh (file://, git://, http://, bare
	// paths), so a fork-PR file:// source can't disclose an arbitrary in-pod
	// repo (#743). Off by default — flate normally renders TRUSTED repos that
	// legitimately reach private/LAN hosts and use local file:// self-sources;
	// a consumer rendering UNTRUSTED input (e.g. konflate fork mode) sets this.
	// The egress half is process-level (see pkg/source/ssrfguard): go-git
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
	Network *source.Network
	// ExternalChanges, when non-nil, supplies the file-level diff so
	// the orchestrator skips its built-in change.Detect step. The
	// filter is still built from this set and the discovery index
	// during Bootstrap.
	ExternalChanges *change.Set

	// Concurrency caps the number of active reconcile bodies running
	// in parallel. <= 0 means unbounded (every Kustomization / HR
	// reconciles on its own goroutine). Background watch loops are
	// unaffected. Sensible default for I/O-bound work is
	// runtime.NumCPU() * 4.
	Concurrency int

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

// Orchestrator wires controllers and drives reconciliation.
type Orchestrator struct {
	cfg    Config
	store  *store.Store
	tasks  *task.Service
	src    *sourcectrl.Controller
	ksc    *kustomization.Controller
	hrc    *helmrelease.Controller
	rsc    *resourcesetctrl.Controller
	agc    *artifactgenerator.Controller
	filter *change.Filter
	cache  *sourcecache.Cache
	inputs source.HTTPInputs
	// Accepted identities are immutable after bootstrap, including for late consumers.
	substitutionSources     map[manifest.NamedResource]manifest.BaseManifest
	dependencySubstitutions values.DependencySubstitutions

	// repoRoot anchors fresh claims for post-render orphan reporting, including
	// Kustomizations admitted after static discovery is published.
	repoRoot string

	// index is published once by Bootstrap and remains immutable during rendering.
	index     *manifest.DiscoveryIndex
	documents []map[string]any

	// orphans records resources Run demoted from Failed → Ready because
	// they aren't referenced by any parent KS. Populated during Run,
	// surfaced via Render() so embedders can distinguish orphan-skips
	// from genuine successes. Keyed by id, value is the original
	// failure message.
	orphans map[manifest.NamedResource]string

	// Store publication MUST follow cycle replacement after releasing preflightMu.
	preflightMu       sync.RWMutex
	preflightFailures map[manifest.NamedResource]string

	// depGraph maintains an incremental copy of the same-kind
	// dependsOn graph so cycle detection on EventObjectAdded touches
	// only the edges that changed instead of rerunning the full tri-
	// color DFS over every Kustomization / HelmRelease. Lives for the
	// life of the orchestrator; populated lazily as objects are added.
	depGraph *dependencyGraph

	// bootstrapped records successful Bootstrap completion. Read by
	// WithFetcher to refuse late fetcher swaps that would silently
	// miss any source CR discovery already reconciled.
	bootstrapped bool
	bootstrapErr error

	// renderOnce serializes the Run + Result-collection phase so a
	// second Render returns the cached Result/err pair instead of
	// re-driving the controllers. The underlying controllers' Configure
	// hooks panic if invoked after Start (the "reconcile-shaping config
	// is frozen once dispatch begins" invariant), so a naive re-Run
	// would panic even with Bootstrap idempotent. Caching at the Render
	// boundary is the embed-friendly answer: embedders that retry
	// Render get back the original artifacts without paying the
	// controller-restart cost.
	renderOnce   sync.Once
	renderResult *Result
	renderErr    error

	// stopOnce guards Stop so a New+Bootstrap-then-abort caller's
	// manual Stop and Run's deferred Stop don't double-close the
	// staging cache / controller unsubs.
	stopOnce   sync.Once
	stopErr    error
	ownNetwork bool
}

// Result contains rendered document batches, failures and discovery diagnostics.
// Ownerless documents have no producer; all borrowed payloads MUST remain immutable.
type Result struct {
	Manifests []manifest.DocumentBatch
	Failed    map[manifest.NamedResource]store.StatusInfo
	Orphans   map[manifest.NamedResource]string
	// DependsOn is the declared spec.dependsOn graph: each Kustomization or
	// HelmRelease that names dependencies maps to the sorted ids it depends on.
	// Same-kind only (KS→KS, HR→HR per the Flux spec); implicit coupling
	// (healthChecks, shared CRDs, service DNS) is not represented. Nodes with no
	// declared dependencies are omitted, and the map is nil (not empty) when
	// nothing declares a dependsOn. Consumers invert this into a dependents
	// adjacency list for impact / "blast radius" analysis.
	//
	// It is the declared graph verbatim: cycle members and self-edges (A → A) are
	// retained, not pruned (cycles also surface as render Failures) — a consumer
	// that walks it must tolerate them. It is also independent of the --skip-*
	// render filters, so an id here may have no entry in Manifests when its kind
	// was skipped.
	DependsOn map[manifest.NamedResource][]manifest.NamedResource
	// Blocked maps a failed resource to the immediate dependencies that blocked
	// it — present only for failures that are purely derived (the resource's own
	// reconcile never ran because a dependency failed or was missing), absent for
	// primary failures (a real render/template/build error). A consumer groups
	// derived failures under their root cause by walking these edges to a primary
	// failure or a missing id, instead of surfacing every cascaded failure.
	Blocked map[manifest.NamedResource][]manifest.NamedResource
	// Warnings are non-fatal render advisories — the render succeeded, but the
	// operator probably wants to look at these (e.g. a HelmRelease pinning
	// values the chart's schema no longer defines, an empty --path scan). Each
	// carries a stable Category code a consumer filters on rather than parsing
	// the message, an optional Resource (zero = render-global), and optional
	// structured Detail. Sorted deterministically; nil (not empty) when none.
	// Distinct from Failed (which blocks a resource) and from operational logs
	// (submodule skips, cache resets) which stay on stderr. A consumer like
	// konflate surfaces these alongside the diff.
	Warnings []manifest.Warning
}

// New constructs an Orchestrator with one cache owner and both renderers.
// Reconciliation starts with Bootstrap then Run.
// Substitution inputs are copied; callers MUST NOT mutate them during New.
func New(ctx context.Context, cfg Config) (result *Orchestrator, err error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("%w: orchestrator: path is required", manifest.ErrInput)
	}

	ownNetwork := cfg.Network == nil
	if ownNetwork {
		cfg.Network = new(source.Network)
	}
	var owner *sourcecache.Cache
	defer func() {
		if err != nil {
			if ownNetwork {
				cfg.Network.Close()
			}
			if cfg.Cache == nil && owner != nil {
				err = errors.Join(err, owner.Close())
			}
		}
	}()
	owner = cfg.Cache
	if owner == nil {
		owner, err = sourcecache.New(ctx, cfg.CacheDir)
		if err != nil {
			return nil, err
		}
	}
	if err := snapshotSubstitutions(&cfg); err != nil {
		return nil, err
	}

	// Arm (or disarm) the process-global SSRF egress guard before any fetcher
	// transport dials. Inert by default; see Config.RestrictEgress.
	ssrfguard.Restrict(cfg.RestrictEgress)

	st := store.New()
	client := helm.NewClient(owner)
	ts := task.NewBounded(cfg.Concurrency)
	o := &Orchestrator{inputs: source.NewHTTPInputs(owner, cfg.Network, ts)}
	trees := kustomize.NewTreeCache(owner, &o.inputs, ts)
	defer func() {
		if err != nil {
			trees.Close()
			o.inputs.Close()
		}
	}()
	srcCtrl := sourcectrl.New(st, ts)
	// Every kind-specific fetcher is wired through source.Wrap so the
	// concrete Fetch signature stays typed (no per-impl type
	// assertion). The wrap label is the kind string; a mismatched
	// payload at dispatch surfaces "<kind> fetcher: unexpected
	// payload <T>" from the single adapter site rather than from
	// four nearly-identical type assertions.
	srcCtrl.Fetchers[manifest.KindGitRepository] = nil
	srcCtrl.Fetchers[manifest.KindExternalArtifact] = source.Wrap(
		manifest.KindExternalArtifact, &external.Fetcher{})
	// Nil entries reserve default fetchers until discovery fixes their policy.
	srcCtrl.Fetchers[manifest.KindBucket] = nil
	srcCtrl.Fetchers[manifest.KindHelmRepository] = nil
	srcCtrl.Fetchers[manifest.KindOCIRepository] = nil
	srcCtrl.Fetchers[manifest.KindHelmChart] = nil
	o.cfg = cfg
	o.ownNetwork = ownNetwork
	o.cache = owner
	o.store = st
	o.tasks = ts
	o.src = srcCtrl
	o.ksc = kustomization.New(st, ts, trees, cfg.WipeSecrets)
	o.hrc = helmrelease.New(base.New(st, ts), client, cfg.HelmOptions, cfg.WipeSecrets)
	o.rsc = resourcesetctrl.New(st, ts, cfg.WipeSecrets)
	o.agc = artifactgenerator.New(st, ts, owner)
	o.depGraph = newDependencyGraph()
	if cfg.DisableChartDigestTracking != nil {
		o.cfg.DisableChartDigestTracking = new(*cfg.DisableChartDigestTracking)
	}
	return o, nil
}

// Store returns the underlying object store.
func (o *Orchestrator) Store() *store.Store { return o.store }

// WithFetcher installs (or replaces) a per-kind source.Fetcher on the
// internal source controller. Call BEFORE Bootstrap. Returns the
// orchestrator for chaining. Use this to embed flate as a library with
// a custom fetcher (in-memory test fixtures, additional source kinds,
// alternate verification logic) without forking the New() construction.
//
// Passing a nil fetcher unregisters the kind — useful for stripping a
// default registration in tests.
//
// Panics if called after Bootstrap: by then discovery has already run
// and any source CR discovered between New() and Bootstrap was
// reconciled by whichever fetcher was registered at that moment. A
// later swap silently misses those reconciles and the embedder gets
// inconsistent behavior across source CRs of the same kind. Bootstrap
// is the natural commit point for the controller wiring.
func (o *Orchestrator) WithFetcher(kind string, f source.Fetcher) *Orchestrator {
	if o.bootstrapped {
		panic("orchestrator: WithFetcher called after Bootstrap; install fetchers BEFORE Bootstrap")
	}
	if f == nil {
		delete(o.src.Fetchers, kind)
		return o
	}
	o.src.Fetchers[kind] = f
	return o
}

// parentOf returns the structural parent, falling back to rendered ownership.
// Bootstrap must have completed; structural ownership is immutable thereafter.
func (o *Orchestrator) parentOf(id manifest.NamedResource) (manifest.NamedResource, bool) {
	if record, _ := o.index.Lookup(id); record.Parent != (manifest.NamedResource{}) {
		return record.Parent, true
	}
	return o.store.RenderedParent(id, o.index)
}

// childrenByParent returns structural and rendered ownership, including children
// whose structural parents failed before emission. Bootstrap must have completed
// and Render must have returned; do not call concurrently with either. Returned
// maps and sorted, deduplicated slices are independent of orchestrator state.
func (o *Orchestrator) childrenByParent() map[manifest.NamedResource][]manifest.NamedResource {
	children := map[manifest.NamedResource][]manifest.NamedResource{}
	for _, obj := range o.store.ListObjects("", "") {
		for _, p := range o.store.Productions(obj.Named(), o.index) {
			if p.Owner != (manifest.NamedResource{}) && p.Owner != obj.Named() && p.Authored == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) {
				children[p.Owner] = append(children[p.Owner], obj.Named())
			}
		}
	}
	if o.index != nil {
		for child, record := range o.index.Records {
			if parent := record.Parent; parent != (manifest.NamedResource{}) {
				children[parent] = append(children[parent], child)
			}
		}
	}
	for parent, ids := range children {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
		children[parent] = slices.Compact(ids)
	}
	return children
}

// Bootstrap discovers manifests, applies namespace inheritance, primes
// existence-only sources Ready, and prepares the change filter.
// Delegates the load / expand / alias phase to pkg/discovery; the
// remainder is dependency validation + change-filter construction.
//
// Successful calls are idempotent. Failure is terminal and includes teardown;
// recovery requires a fresh New instance. Bootstrap MUST NOT race Run or Stop.
func (o *Orchestrator) Bootstrap(ctx context.Context) (err error) {
	if o.bootstrapErr != nil {
		return o.bootstrapErr
	}
	if o.bootstrapped {
		return nil
	}
	defer func() {
		if err != nil {
			o.bootstrapErr = errors.Join(err, o.Stop())
			err = o.bootstrapErr
		}
	}()
	gitFetcher := &git.Fetcher{
		Cache: o.cache, Depth: o.cfg.GitDepth,
		RestrictSchemes: o.cfg.RestrictEgress, ForceGeneric: o.cfg.ForceGenericProvider,
	}
	o.ksc.Trees.SetGitBaseFetcher(func(ctx context.Context, request kustomize.GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
		artifact, err := gitFetcher.FetchRemoteBase(ctx, git.RemoteBaseRequest{URL: request.URL, Ref: request.Ref}, sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry})
		if artifact != nil {
			for _, warning := range artifact.Warnings {
				o.store.AddWarning(manifest.NamedResource{}, warning)
			}
		}
		return artifact, err
	})
	admitFromDisk := o.cfg.Input == nil && o.store.GetArtifact(manifest.BootstrapSourceID) == nil
	res, err := discovery.Run(ctx, discovery.Config{
		Path: o.cfg.Path, RepoRoot: o.cfg.RepoRoot, SelfURLs: o.cfg.SelfURLs, Input: o.cfg.Input,
		Store: o.store, WipeSecrets: o.cfg.WipeSecrets,
		Trees: o.ksc.Trees, RemoteInputs: &o.inputs, RemoteRetry: o.cfg.SourceRetry,
	})
	if err != nil {
		return err
	}
	o.cfg.Input = o.store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	o.cfg.RepoRoot = res.RepoRoot
	if admitFromDisk && o.cfg.BaseInput == nil && o.cfg.PathOrig != "" && o.cfg.ExternalChanges == nil {
		baseInput, _, err := baseline.AdmitDirectory(ctx, o.cfg.PathOrig, o.cfg.PathOrig)
		if err != nil {
			return err
		}
		if baseInput.LocalPath == o.cfg.Input.LocalPath {
			baseInput = o.cfg.Input
		}
		o.cfg.BaseInput = baseInput
	}
	o.repoRoot = res.RepoRoot
	o.index = res.Index
	var entryPolicy manifest.EmissionPolicy
	skipKinds := o.cfg.HelmOptions.SkipResourceKinds()
	for _, doc := range res.Documents {
		if output, _ := entryPolicy.SelectDocument(doc, skipKinds); output {
			o.documents = append(o.documents, doc)
		}
	}
	o.dependencySubstitutions = res.DependencySubstitutions

	o.freezeSubstitutions()
	o.failDependsOnCycles()
	if err := o.buildChangeFilter(ctx, res.RepoRoot); err != nil {
		return err
	}
	o.hrc.Options.DisableChartDigestTracking = res.DisableChartDigestTracking
	if requested := o.cfg.DisableChartDigestTracking; requested != nil {
		o.hrc.Options.DisableChartDigestTracking = *requested
	}
	ociFetcher := &oci.Fetcher{Cache: o.cache, Network: o.cfg.Network, RegistryConfig: o.cfg.RegistryConfig, ForceGeneric: o.cfg.ForceGenericProvider}
	chartFetcher := helmchart.New(ociFetcher, helmchart.Config{
		Cache: o.cache, Network: o.cfg.Network, DisableChartDigestTracking: o.hrc.Options.DisableChartDigestTracking})
	for kind, f := range map[string]source.Fetcher{
		manifest.KindGitRepository: source.Wrap(manifest.KindGitRepository, gitFetcher),
		manifest.KindBucket: source.Wrap(manifest.KindBucket,
			&bucket.Fetcher{Cache: o.cache, Network: o.cfg.Network, ForceGeneric: o.cfg.ForceGenericProvider}),
		manifest.KindOCIRepository: source.Wrap(manifest.KindOCIRepository, ociFetcher),
		manifest.KindHelmChart:     source.Wrap(manifest.KindHelmChart, chartFetcher),
	} {
		if current, exists := o.src.Fetchers[kind]; exists && current == nil {
			o.src.Fetchers[kind] = f
		}
	}
	o.bootstrapped = true
	return nil
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
