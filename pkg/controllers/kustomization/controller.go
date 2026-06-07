// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish a KustomizationArtifact. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates Kustomization reconciliation. Reconcile-
// shaping state (Filter, ParentOf) flows in via Configure exactly once
// before Start. The invariant — "config is read-only after Start" — is
// encoded in the embedded *base.Controller, not just in code review.
type Controller struct {
	*base.Controller

	// Staging is a process-wide cache that materializes source roots
	// into writable copies so Flux's Generator can write the merged
	// kustomization.yaml without touching the user's working tree.
	Staging *kustomize.StagingCache

	// WipeSecrets controls whether Secret cleartext is wiped when
	// parsing rendered manifests.
	WipeSecrets bool

	// selfProduces reports whether a Kustomization's own render emits a
	// given ConfigMap (see Options.SelfProduces). collectDeps consults it
	// to drop a self-produced substituteFrom CM from the dep set. Nil when
	// unset (tests / no repoRoot) → edge always added.
	selfProduces func(cm, consumer manifest.NamedResource) bool

	// workingTreeFPs single-flights workingTreeFingerprint per source
	// LocalPath for the life of the controller. resolveSourceRootAnd-
	// Fingerprint runs once per Kustomization, and dozens of KSes share
	// the bootstrap working-tree source — they all reconcile at once
	// under --concurrency, so a plain Load/Store memo lets them all miss
	// before any Store lands and re-walk the entire repo in parallel.
	// Storing a sync.OnceValue closure collapses that herd to one walk.
	workingTreeFPs sync.Map // path string -> func() string (sync.OnceValue)

	// walks counts actual workingTreeFingerprint executions (one per
	// unique LocalPath, regardless of concurrent callers). Single-flight
	// observability — asserted by TestCachedWorkingTreeFingerprint_SingleFlight;
	// mirrors StagingCache.sweepKicks.
	walks atomic.Int64
}

// Options carries the post-bootstrap state the orchestrator wires onto
// the controller before Start. Filter narrows reconciliation to
// changed resources in changed-only mode. ParentOf maps each Flux
// Kustomization to its structural parent so reconcile waits for the
// parent's Ready before rendering (so any parent-render-time spec
// mutations — e.g. `replacements:` injecting spec.targetNamespace —
// are observable when the child renders). A nil ParentOf disables
// parent-enforcement, matching pre-#102 behavior. RenderTracker
// receives every reconcilable child this KS emits during render.
type Options struct {
	Filter        *change.Filter
	ParentOf      func(manifest.NamedResource) (manifest.NamedResource, bool)
	RenderTracker base.RenderTracker
	// Existence is the file-existence lookup the orchestrator wires
	// against the loader's ExistenceIndex. depwait uses it to lazy-
	// promote file-indexed deps and to distinguish render-only
	// deps from typo'd ones at the missing-dep grace boundary. See
	// depwait.ExistenceLookup for the decision matrix. Forwarded
	// to every Waiter built during reconcile.
	Existence depwait.ExistenceLookup
	// Renders is the quiescence signal the orchestrator wires
	// against the task service's active-task count. depwait's step-2
	// long wait short-circuits to "dependency not found" once Renders
	// reports no other reconcile is in flight — so a typo'd dependsOn
	// fails as soon as the orchestrator drains instead of burning
	// the full RenderProducingTimeout cap.
	Renders depwait.RenderInflight
	// PreflightFailure reports dependency-graph errors discovered by the
	// orchestrator before reconcile. When set for an id, the controller
	// marks the resource Failed and does not render it.
	PreflightFailure func(manifest.NamedResource) (string, bool)
	// SelfProduces reports whether consumer's OWN render emits cm. When
	// it does, collectDeps drops cm from the dependency set — a KS can't
	// hard-wait on a postBuild.substituteFrom ConfigMap that only its own
	// render produces (the bjw-s/onedr0p self-substitute). Available in
	// full mode (graph-aware index), unlike the changed-only producer
	// skip. Nil → the edge is always added (pre-index behavior).
	SelfProduces func(cm, consumer manifest.NamedResource) bool
}

// New constructs a Kustomization controller.
func New(s *store.Store, t *task.Service, staging *kustomize.StagingCache, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t),
		Staging:     staging,
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options) {
	c.SetFilter(opts.Filter)
	c.SetDepwait(opts.Existence, opts.Renders)
	c.SetPreflight(opts.PreflightFailure)
	c.SetParentOf(opts.ParentOf)
	c.SetRenderTracker(opts.RenderTracker)
	c.selfProduces = opts.SelfProduces
}

// Start registers the listener that drives reconciliation.
func (c *Controller) Start(ctx context.Context) {
	c.StartLifecycle("kustomization")
	c.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx))
}

func (c *Controller) onObjectAdded(ctx context.Context) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		if id.Kind != manifest.KindKustomization {
			return
		}
		ks, ok := payload.(*manifest.Kustomization)
		if !ok {
			return
		}
		if c.PreGate(id, ks.Suspend) {
			return
		}
		if msg, failed := c.PreflightFailure(id); failed {
			c.Store.UpdateStatus(id, store.StatusFailed, msg)
			return
		}
		c.Submit(ctx, id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "kustomization", c.reconcile)
		})
	}
}

func (c *Controller) reconcile(ctx context.Context, ks *manifest.Kustomization) error {
	id := ks.Named()
	if err := c.PreflightError(id); err != nil {
		return err
	}
	c.Store.UpdateStatus(id, store.StatusPending, "resolving dependencies")

	deps := c.collectDeps(ks)
	if len(deps) > 0 {
		err := c.Await(ctx, id, c.NewWaiter(id, ks.Timeout), deps,
			"", // status set above
			func(sum depwait.Summary) error {
				return &manifest.DependencyFailedError{
					Parent:  id,
					Failed:  sum.Failed,
					Reasons: sum.Messages,
				}
			})
		if err != nil {
			return err
		}
		// Refresh the KS — our structural parent may have re-emitted
		// us with mutated spec (e.g. `replacements:` injecting
		// spec.targetNamespace) while we were waiting. Without this
		// re-read the first render would use the stale-spec snapshot
		// captured by RunWithStatus, producing duplicate renders that
		// linger in the store with the wrong namespace. See #102.
		if fresh, ok := store.Get[*manifest.Kustomization](c.Store, id); ok {
			ks = fresh
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "resolving source artifact")
	sourceRoot, sourceFingerprint, err := c.resolveSourceRootAndFingerprint(ks)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "expanding substitutions")
	// kustomize.Prepare clones ks and expands postBuild.substituteFrom —
	// the same pre-render dance an embedder calling RenderFlux directly
	// would perform. Keeping one canonical implementation here means
	// changes to the contract only land in one place. Mirrors helm.Prepare.
	ks, err = kustomize.Prepare(ks, values.NewStoreProvider(c.Store))
	if err != nil {
		return err
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Fingerprint dedup: the same id may receive multiple AddObject
	// events with the same effective spec (e.g. when the structural
	// parent re-emits this KS after running its own render, stamping
	// kustomize.toolkit.fluxcd.io ownership labels). kustomize.RenderFlux
	// is the hot path; skip it and reuse the prior artifact when the
	// post-Prepare inputs are byte-identical. Same pattern as HR (#219).
	fp := kustomizationFingerprint(ks, sourceRoot)
	if existing, ok := c.Store.GetArtifact(id).(*store.KustomizationArtifact); ok && existing.Fingerprint != "" && existing.Fingerprint == fp {
		if err := c.PreflightError(id); err != nil {
			return err
		}
		slog.Debug("kustomization: skipped re-render (fingerprint unchanged)", "id", id.String())
		// Skip kustomize.RenderFlux (the expensive bit), but still
		// replay the cached artifact through emitRenderedChildren so
		// the per-emission side-effects (markRendered for parent
		// provenance + Filter.AddEmitted for runtime keep-set
		// extension, see #260/#308) fire on every reconcile pass.
		// Without this, a re-emit that hits dedup drops the child
		// from the parent index and the keep-set chain — a subsequent
		// changed-only run silently mis-attributes children to a
		// stale parent.
		c.emitRenderedChildren(id, existing.Manifests)
		return nil
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	data, err := kustomize.RenderFlux(ctx, c.Staging, sourceRoot, sourceFingerprint, ks.Path, ks.Contents)
	if err != nil {
		return err
	}
	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return err
	}
	docs = manifest.FlattenLists(docs)

	// Per-resource envsubst. Flux's kustomize-controller skips
	// substitution on any resource carrying the
	// "kustomize.toolkit.fluxcd.io/substitute: disabled" label or
	// annotation — used in real repos for ConfigMaps that embed
	// shell scripts with bash array expansions (${ARR[@]}) that
	// envsubst's parser cannot handle. Mirror that behavior here:
	// substitute per-doc, skip opted-out resources, so we match Flux
	// bit-for-bit.
	if vars := values.VarsMap(ks.PostBuildSubstitute); len(vars) > 0 {
		for i, doc := range docs {
			if manifest.HasSubstituteDisabled(doc) {
				continue
			}
			substituted, sErr := substituteDoc(doc, vars)
			if sErr != nil {
				return sErr
			}
			docs[i] = substituted
		}
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.PreflightError(id); err != nil {
		return err
	}
	c.emitRenderedChildren(id, docs)

	c.Store.SetArtifact(id, &store.KustomizationArtifact{
		Path:        filepath.Join(sourceRoot, ks.Path),
		Manifests:   docs,
		Fingerprint: fp,
	})
	return nil
}

// collectDeps assembles the dependency refs whose readiness must
// precede this Kustomization: explicit dependsOn entries (carrying any
// CEL ReadyExpr), the source ref, the implicit structural parent (the
// enclosing Flux KS that renders us — must finish first so any
// parent-render-time spec injections land before our reconcile), and
// every non-Optional postBuild.substituteFrom ConfigMap (or, in
// changed-only mode, the unchanged producer Kustomization that renders
// that ConfigMap).
//
// substituteFrom ConfigMap/producer edges fix the race where the
// referenced CM is emitted by another KS's render: without the edge,
// KS-A would race KS-B and Prepare would silently expand with empty
// values for vars that should have come from KS-B's CM. Flux's
// eventual-consistency reconcile loop self-heals; flate is one-shot,
// so a missed substitution shows up as broken rendered output.
//
// Secret refs are deliberately NOT added as depwait edges. In real
// repos substituteFrom Secrets are almost always SOPS-encrypted or
// ExternalSecret-managed — they live in cluster state that flate
// cannot materialize offline. values.ExpandPostBuildSubstituteReference
// already gracefully degrades on missing Secrets (logs DEBUG and
// continues); adding a hard edge here would regress every offline
// render against a Flux repo that uses secret-substitute patterns.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := append([]manifest.DependencyRef(nil), ks.DependsOn...)
	if ks.SourceKind != "" && ks.SourceName != "" {
		deps = append(deps, manifest.DependencyRef{
			NamedResource: manifest.NamedResource{
				Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
			},
		})
	}
	if parent, ok := c.LookupParent(ks.Named()); ok {
		deps = append(deps, manifest.DependencyRef{NamedResource: parent})
	}
	for _, ref := range ks.PostBuildSubstituteFrom {
		// Shared with change.transitiveDepsOf via manifest.IsHardConfigMapEdge:
		// only a non-Optional, named ConfigMap is a hard offline edge (Optional
		// is best-effort, Secrets are SOPS/ExternalSecret-managed). Keep-set and
		// reconcile ordering MUST agree on this — see the predicate's doc + #418.
		if !manifest.IsHardConfigMapEdge(ref) {
			continue
		}
		depID := manifest.NamedResource{Kind: ref.Kind, Namespace: ks.Namespace, Name: ref.Name}
		// Drop the hard CM edge ONLY when THIS KS's own render subtree
		// emits it (the bjw-s/onedr0p self-substitute: cluster-apps' bare-
		// dir render produces ConfigMap/flux-system/cluster-settings via a
		// component). Graph-aware and available in full mode, unlike the
		// changed-only ProducersFor skip below. A CM produced by a
		// DIFFERENT KS — or genuinely absent — keeps the edge and fails
		// loudly: a KS waiting on a CM only its own render can emit would
		// otherwise deadlock against itself.
		if c.selfProduces == nil || !c.selfProduces(depID, ks.Named()) {
			deps = append(deps, manifest.DependencyRef{NamedResource: depID})
		}
		// In changed-only mode, when the substituteFrom CM is rendered
		// by another Flux Kustomization (the producer), wait on that
		// producer too — the CM dep alone doesn't gate ordering when
		// the producer's reconcile is what materializes the CM. Only
		// KS producers form depwait edges (generator-produced CMs have
		// no controller to wait on); skip the self-producer case
		// (bjw-s self-substitute pattern).
		if f := c.Filter(); f != nil {
			for _, producer := range f.ProducersFor(depID) {
				if producer == ks.Named() {
					continue
				}
				if producer.Kind != manifest.KindKustomization {
					continue
				}
				deps = append(deps, manifest.DependencyRef{NamedResource: producer})
			}
		}
	}
	return deps
}

// resolveSourceRootAndFingerprint returns the source's on-disk root
// AND its content-addressed fingerprint. The fingerprint feeds the
// persistent kustomize staging cache: when non-empty, repeat renders
// against the same artifact skip the copyTree pass entirely. Empty
// is the right answer when the source has no stable content
// identifier (the bootstrap working-tree alias, local-path
// GitRepository overrides, sources soft-skipped via
// --allow-missing-secrets) — the caller falls back to per-process
// scratch staging and rebuilds from scratch each run.
func (c *Controller) resolveSourceRootAndFingerprint(ks *manifest.Kustomization) (string, string, error) {
	srcID := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	if ks.SourceKind == "" || ks.SourceName == "" {
		// Child Kustomizations that inherit sourceRef from a parent's
		// render patches show empty here until that parent reconciles.
		// Fall back to the seeded bootstrap GitRepository (the user's
		// working tree) so the first reconcile resolves to the repo
		// root instead of doubling ks.Path against itself (#105).
		if ks.Path == "" {
			return "", "", fmt.Errorf("%w: kustomization %s has no path and no source",
				manifest.ErrInput, ks.Named().NamespacedName())
		}
		srcID = manifest.BootstrapSourceID
	}
	art := c.Store.GetArtifact(srcID)
	if art == nil {
		// A source that soft-skipped (--allow-missing-secrets) reports
		// Ready with a "skipped:" reason but writes no artifact. Surface
		// that as a typed skip so the caller can mark the KS skipped too
		// rather than reporting a generic "artifact not found" failure.
		if info, ok := c.Store.GetStatus(srcID); ok && store.IsSkipped(info) {
			return "", "", fmt.Errorf("%w: source %s %s", manifest.ErrSourceSkipped, srcID.String(), info.Message)
		}
		return "", "", fmt.Errorf("%w: source %s artifact not found", manifest.ErrObjectNotFound, srcID.String())
	}
	if sa, ok := art.(*store.SourceArtifact); ok {
		// Prefer the content-addressed Digest when the fetcher
		// supplied one (OCI); fall back to Revision (the git commit
		// SHA) so git-backed sources also key the persistent stage
		// cache.
		//
		// The bootstrap source is the working-tree alias — fetchers
		// don't run for it and Revision/Digest are empty. We walk the
		// tree and hash (relpath, mtime, size) per regular file so
		// repeated invocations against an untouched tree still hit
		// the cache while any nested edit invalidates it. Same
		// treatment applies to any other artifact that landed
		// without a fetcher-supplied fingerprint
		// (`overrideSelfReferentialGitRepositories`) — they too
		// resolve to the user's working tree.
		fp := sa.Digest
		if fp == "" {
			fp = sourceFingerprintFromRevision(sa.Revision)
		}
		if fp == "" {
			fp = c.cachedWorkingTreeFingerprint(sa.LocalPath)
		}
		return sa.LocalPath, fp, nil
	}
	return "", "", fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}

// sourceFingerprintFromRevision normalizes a git-style Revision
// string into a stage-cache key. The git fetcher records the raw
// commit SHA, which is already a valid fingerprint. Future fetchers
// may follow Flux's `<branch>@sha1:<sha>` convention — strip the
// prefix so the cache key stays content-only.
func sourceFingerprintFromRevision(rev string) string {
	if rev == "" {
		return ""
	}
	// "main@sha1:abcd..." or "sha256:abcd..." — keep only the trailing
	// content-addressed token. Plain SHAs pass through unchanged.
	if i := strings.LastIndexAny(rev, ":@"); i >= 0 && i+1 < len(rev) {
		return rev[i+1:]
	}
	return rev
}

// cachedWorkingTreeFingerprint single-flights workingTreeFingerprint by
// LocalPath. resolveSourceRootAndFingerprint runs once per Kustomization
// and dozens of KSes share the bootstrap source, all reconciling
// concurrently — a plain Load/miss/Store memo lets them all miss before
// any value is stored and re-walk the entire repo in parallel (a
// thundering herd that dominates warm-run CPU). LoadOrStore keeps exactly
// one sync.OnceValue closure per path, so every concurrent caller invokes
// the same closure and the walk runs once. Mirrors StagingCache's copyOnce.
func (c *Controller) cachedWorkingTreeFingerprint(path string) string {
	once, _ := c.workingTreeFPs.LoadOrStore(path, sync.OnceValue(func() string {
		c.walks.Add(1)
		return workingTreeFingerprint(path)
	}))
	return once.(func() string)()
}

// workingTreeFingerprint hashes every regular file under path with
// (relpath, mtime, size). The directory skip set is shared with the
// staged copy via kustomize.SkipStageDir — the fingerprint and the
// staged tree MUST see the same files, or a cache hit can land a
// structurally broken stage (e.g. a kustomization.yaml that references
// a directory the stage omits). Cost is amortized against a cache miss,
// which would walk the same tree anyway to copy.
func workingTreeFingerprint(path string) string {
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write([]byte(abs))
	walkErr := filepath.WalkDir(abs, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Share the exclusion set with the staged copy; the root
			// (abs) is never skipped even if its base is dot-prefixed.
			if p != abs && kustomize.SkipStageDir(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		// Mirror copyTreeInto's symlink handling: dereference, then
		// only hash if the target is a regular file. Broken symlinks
		// are silently skipped — otherwise a dangling editor lockfile
		// would error out and empty the fingerprint, routing every
		// run to per-process scratch staging.
		var info fs.FileInfo
		if d.Type()&fs.ModeSymlink != 0 {
			info, err = os.Stat(p)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
		} else {
			if !d.Type().IsRegular() {
				return nil
			}
			info, err = d.Info()
			if err != nil {
				return nil
			}
		}
		rel, relErr := filepath.Rel(abs, p)
		if relErr != nil {
			return relErr
		}
		_, _ = h.Write([]byte(rel))
		_, _ = h.Write([]byte(info.ModTime().UTC().Format(time.RFC3339Nano)))
		_, _ = h.Write([]byte(strconv.FormatInt(info.Size(), 10)))
		return nil
	})
	if walkErr != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}
