package orchestrator

import (
	"context"
	"maps"
	"slices"
	"sync"

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

// Config contains invocation policy and shared rendering resources.
type Config struct {
	// HelmOptions tunes templating and kind filtering through SkipKinds.
	HelmOptions helm.Options
	// WipeSecrets controls Secret cleartext placeholders.
	WipeSecrets bool
	// AllowMissingKinds tolerates missing source Secret material, Helm valuesFrom kinds, and CRDs.
	// Required postBuild references and malformed data remain errors.
	AllowMissingKinds []string
	// Supply contains invocation data, never stored as canonical objects or emitted.
	Supply   []store.SuppliedObject
	supplied *store.Supplied
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
	Network      *source.Network
	chartFetcher *helmchart.Fetcher
	// HelmClient must borrow Cache; both remain caller-owned when supplied.
	HelmClient *helm.Client
	// StatusListener receives concurrent tree-qualified store events and must not block.
	StatusListener func(manifest.TreeID, manifest.NamedResource, store.StatusInfo)
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
	cfg                     treeConfig
	store                   *store.Store
	tasks                   *task.Service
	src                     *sourcectrl.Controller
	ksc                     *kustomization.Controller
	hrc                     *helmrelease.Controller
	rsc                     *resourcesetctrl.Controller
	agc                     *artifactgenerator.Controller
	filter                  *change.Filter
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

func newOrchestrator(cfg Config, tree Tree, ts *task.Service) *Orchestrator {
	st := store.New()
	st.SetSupplied(cfg.supplied)
	o := &Orchestrator{cfg: treeConfig{Config: cfg, Tree: tree}, inputs: source.NewHTTPInputs(cfg.Cache, cfg.Network, ts), store: st, cache: cfg.Cache, tasks: ts}
	if cfg.StatusListener != nil {
		o.unsubStatus = st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, value any) {
			if st.ReportHidden(id) {
				return
			}
			cfg.StatusListener(tree.ID, id, value.(store.StatusInfo))
		}, false)
	}
	trees := kustomize.NewTreeCache(cfg.Cache, &o.inputs, ts)
	o.src = sourcectrl.New(st, ts)
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindBucket, manifest.KindHelmRepository, manifest.KindOCIRepository, manifest.KindHelmChart} {
		o.src.Fetchers[kind] = nil
	}
	o.src.Fetchers[manifest.KindExternalArtifact] = source.Wrap(manifest.KindExternalArtifact, &external.Fetcher{})
	o.ksc = kustomization.New(st, ts, trees, cfg.WipeSecrets)
	o.hrc = helmrelease.New(base.New(st, ts), cfg.HelmClient, cfg.HelmOptions, cfg.WipeSecrets)
	o.rsc = resourcesetctrl.New(st, ts, cfg.WipeSecrets)
	o.agc = artifactgenerator.New(st, ts, cfg.Cache)
	o.controllers = base.Controllers{Kustomization: o.ksc, HelmRelease: o.hrc, ResourceSet: o.rsc, Source: o.src, ArtifactGenerator: o.agc}
	o.depGraph = newDependencyGraph()
	return o
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

// Ownership is read only after all tree writers have joined.
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

func (o *Orchestrator) discover(ctx context.Context) error {
	gitFetcher := &git.Fetcher{
		Cache: o.cache, Depth: o.cfg.GitDepth,
		RestrictSchemes: o.cfg.RestrictEgress,
	}
	o.ksc.Trees.SetGitBaseFetcher(func(ctx context.Context, request kustomize.GitBaseRequest, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
		artifact, err := gitFetcher.FetchRemoteBase(ctx, git.RemoteBaseRequest{URL: request.URL, Ref: request.Ref, Timeout: request.Timeout, Submodules: request.Submodules}, sourcecache.Policy{MaxAge: request.MaxAge, Retry: retry})
		if artifact != nil {
			for _, warning := range artifact.Warnings {
				o.store.AddWarning(warning)
			}
		}
		return artifact, err
	})
	if current, exists := o.src.Fetchers[manifest.KindGitRepository]; exists && current == nil {
		o.src.Fetchers[manifest.KindGitRepository] = source.Wrap(manifest.KindGitRepository, gitFetcher)
	}
	res, err := discovery.Run(ctx, discovery.Config{
		Path: o.cfg.Path, RepoRoot: o.cfg.RepoRoot, SelfURLs: o.cfg.SelfURLs, Input: o.cfg.Input,
		Store: o.store, WipeSecrets: o.cfg.WipeSecrets, Entry: o.cfg.entry,
		Trees: o.ksc.Trees, RemoteInputs: &o.inputs, RemoteRetry: o.cfg.SourceRetry,
		StrictSubstitutions: o.cfg.StrictSubstitutions, SkipKinds: o.cfg.HelmOptions.SkipResourceKinds(),
	})
	o.cfg.entry = nil
	if err != nil {
		return err
	}
	o.cfg.Input = o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	o.cfg.RepoRoot = res.RepoRoot
	o.repoRoot = res.RepoRoot
	o.index = res.Index
	o.entryBinding = res.Binding
	o.admission = res.Admission
	o.dependencySubstitutions = res.DependencySubstitutions

	return nil
}

func (o *Orchestrator) completeBootstrap() {
	o.failDependsOnCycles()
	ociFetcher := &oci.Fetcher{Cache: o.cache, Network: o.cfg.Network, RegistryConfig: o.cfg.RegistryConfig}
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
