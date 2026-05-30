package orchestrator

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
)

// buildChangeFilter computes the file-level change set (if changed-only
// mode is requested) and constructs the immutable change.Filter from
// (changes, sourceFiles, repoRoot, store), then wires it onto every
// controller. When changed-only mode is off the filter remains nil and
// controllers reconcile everything.
func (o *Orchestrator) buildChangeFilter(repoRoot string) error {
	changes := o.cfg.ExternalChanges
	if changes == nil && o.cfg.PathOrig != "" {
		cs, err := o.computeChangeSet(repoRoot)
		if err != nil {
			return err
		}
		changes = cs
	}
	if changes == nil {
		return nil
	}
	f := change.NewFilterWithCache(changes, o.sourceFiles, repoRoot, o.store, o.componentCache)
	// Wire OnAdd so a runtime keep-set extension (KS controller's
	// emitRenderedChildren → keepEmitted) refires any source whose
	// listener already short-circuited via PreGate before the
	// consuming KS joined keep. Without this hook, the source stays
	// Ready/"unchanged" with no artifact and the KS's
	// resolveSourceRoot surfaces "artifact not found" downstream.
	// Issue #260. Refire owns the status reset that closes the
	// depwait race — see Store.Refire.
	//
	// Kustomization is included for the symmetric substituteFrom
	// producer case (#418): when a primary parent emits a child KS
	// at render time and that child consumes a substituteFrom CM
	// produced by an unchanged producer KS, addRecursive pulls the
	// producer into keep dependency-only — but the producer's own
	// listener already PreGate-skipped during Bootstrap, leaving no
	// CM in the store. Refire re-runs the producer so the CM lands
	// before the consuming KS's depwait expires.
	//
	// Kinds limited to the source-controller-managed set (those wired
	// with a Fetcher in the constructor above) plus Kustomization.
	// HelmChart resources are read directly by helm.storeResolver
	// without a Fetcher, so Refire on a HelmChart id would write a
	// Pending status that no controller transitions back to Ready.
	f.OnAdd = func(id manifest.NamedResource) {
		switch id.Kind {
		case manifest.KindGitRepository,
			manifest.KindOCIRepository,
			manifest.KindHelmRepository,
			manifest.KindBucket,
			manifest.KindExternalArtifact,
			manifest.KindKustomization:
			o.store.Refire(id)
		}
	}
	o.filter = f
	slog.Debug("changed-only keep set", "size", o.filter.Size(), "items", o.filter.KeepNames())
	return nil
}

// computeChangeSet resolves --path / --path-orig, runs change.Detect,
// and reroots the result into repoRoot's coordinate system. Returns
// (nil, nil) when both paths resolve to the same directory (changed-
// only mode would diff a tree against itself); the caller skips the
// filter build in that case. Only reached when cfg.PathOrig != "".
func (o *Orchestrator) computeChangeSet(repoRoot string) (*change.Set, error) {
	origAbs, err := discovery.ResolveScanPath(o.cfg.PathOrig)
	if err != nil {
		return nil, fmt.Errorf("--path-orig: %w", err)
	}
	currAbs, err := discovery.ResolveScanPath(o.cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("--path: %w", err)
	}
	// Both paths resolved to the same directory (e.g. a symlink and
	// its target, or literally the same arg twice). Changed-only mode
	// would diff a tree against itself producing an empty change set.
	// Skip filter build so the user's `--path-orig` typo doesn't
	// silently render zero output.
	if origAbs == currAbs {
		slog.Warn("--path and --path-orig resolve to the same directory; ignoring --path-orig",
			"resolved_path", currAbs)
		return nil, nil
	}
	// Widen the diff scope to each side's .git root when the user
	// pointed at sibling subdirs of separate checkouts — the
	// canonical PR-vs-base-branch flow `flate diff ks --path
	// pr/cluster --path-orig base/cluster` where the actual edits
	// live in `apps/`, outside the cluster subdir. Diffing the
	// literal subdirs would report zero changes even though the
	// rendered output differs. We only widen when both sides resolve
	// to .git roots that are (a) not the same root (one checkout,
	// two subdirs is the deliberate subdir-vs-subdir case) and
	// (b) actually distinct from the path the user passed (no .git
	// ancestor → FindRepoRoot returns the path unchanged).
	diffOrig, diffCurr := origAbs, currAbs
	origRoot := discovery.FindRepoRoot(origAbs)
	currRoot := discovery.FindRepoRoot(currAbs)
	widened := origRoot != origAbs && currRoot != currAbs && origRoot != currRoot
	if widened {
		diffOrig, diffCurr = origRoot, currRoot
	}
	cs, err := change.Detect(diffOrig, diffCurr)
	if err != nil {
		return nil, fmt.Errorf("change detect: %w", err)
	}
	// Detect emits paths relative to diffCurr. When we widened, that
	// already equals repoRoot and SourceFiles keys line up. When we
	// didn't, lift the subdir-relative paths into repoRoot's
	// coordinate system so they match SourceFiles keys.
	if !widened {
		if rel, err := filepath.Rel(repoRoot, currAbs); err == nil && rel != "." {
			cs = cs.Reroot(rel)
		}
	}
	slog.Debug("changed-only mode",
		"baseline", diffOrig, "current", diffCurr, "changed_files", cs.Len(), "widened_to_repo_root", widened)
	if cs.Len() == 0 {
		slog.Warn("no changes detected between --path and --path-orig — output will be empty; verify both paths reference distinct snapshots")
	}
	return cs, nil
}
