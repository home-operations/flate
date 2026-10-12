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
// (changes, discovery index, store), then wires it onto every
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
	f := change.NewFilter(changes, o.index, o.store)
	o.filter = f
	slog.Debug("changed-only keep set", "filter", o.filter)
	return nil
}

// computeChangeSet diffs the baseline source root (cfg.PathOrig) against
// this side's source root (repoRoot) and returns the file-level change
// set. Both are repo roots — the SAME coordinate system as repoRoot, so
// the emitted paths line up with discovery file coordinates directly (no rerooting).
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
			Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
		})
	}
	return cs, nil
}
