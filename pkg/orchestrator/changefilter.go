package orchestrator

import (
	"fmt"
	"log/slog"

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
	f := change.NewFilterWithCache(changes, o.sourceFiles, repoRoot, o.store, o.componentCache, o.sourceRefs)
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

// computeChangeSet diffs the baseline source root (cfg.PathOrig) against
// this side's source root (repoRoot) and returns the file-level change
// set. Both are repo roots — the SAME coordinate system as repoRoot, so
// the emitted paths line up with SourceFiles keys directly (no rerooting).
// Returns (nil, nil) when the two roots resolve to the same directory
// (changed-only mode would diff a tree against itself); the caller skips
// the filter build then. Only reached when cfg.PathOrig != "".
//
// PathOrig carries the baseline's REPO ROOT (not a scan subdir): the CLI
// resolves --base into a materialized tree root, and a subdir --path-orig
// is lifted to its repo root before reaching here. That replaces the old
// .git-ancestor "widen" heuristic — the policy of picking each side's root
// now lives in the caller (internal/cli / RenderTrees), and this stays a
// straight root-to-root diff.
func (o *Orchestrator) computeChangeSet(repoRoot string) (*change.Set, error) {
	origRoot, err := discovery.ResolveScanPath(o.cfg.PathOrig)
	if err != nil {
		return nil, fmt.Errorf("--path-orig: %w", err)
	}
	// Both roots resolved to the same directory (e.g. a symlink and its
	// target, or the same arg twice). Changed-only mode would diff a tree
	// against itself producing an empty change set. Skip filter build so
	// the user's `--path-orig` typo doesn't silently render zero output.
	if origRoot == repoRoot {
		o.store.AddWarning(manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "--path and --path-orig resolve to the same root; ignoring --path-orig",
			Detail:   []string{repoRoot},
		})
		return nil, nil
	}
	cs, err := change.Detect(origRoot, repoRoot)
	if err != nil {
		return nil, fmt.Errorf("change detect: %w", err)
	}
	slog.Debug("changed-only mode", "baseline", origRoot, "current", repoRoot, "changed_files", cs.Len())
	if cs.Len() == 0 {
		o.store.AddWarning(manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "no changes detected between --path and --path-orig — output will be empty; verify both paths reference distinct snapshots",
		})
	}
	return cs, nil
}
