package orchestrator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

// buildChangeFilter computes the file-level change set (if changed-only
// mode is requested) and constructs the immutable change.Filter from
// (changes, discovery index, store), then wires it onto every
// controller. When changed-only mode is off the filter remains nil and
// controllers reconcile everything.
func (o *Orchestrator) buildChangeFilter(ctx context.Context, repoRoot string) error {
	changes := o.cfg.ExternalChanges
	if changes == nil && (o.cfg.PathOrig != "" || o.cfg.BaseInput != nil) {
		cs, err := o.computeChangeSet(ctx, repoRoot)
		if err != nil {
			return err
		}
		changes = cs
	}
	if changes == nil {
		return nil
	}
	if changes.Len() == 0 && (o.cfg.PathOrig != "" || o.cfg.BaseInput != nil) {
		o.store.AddWarning(manifest.NamedResource{}, manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
		})
	}
	f := change.NewFilter(changes, o.index, o.store)
	o.filter = f
	slog.Debug("changed-only keep set", "filter", o.filter)
	return nil
}

// computeChangeSet compares direct inputs in source-root coordinates.
// PathOrig is a source root; an admitted BaseInput may have a synthetic label.
func (o *Orchestrator) computeChangeSet(ctx context.Context, repoRoot string) (*change.Set, error) {
	if o.cfg.BaseInput == nil {
		var err error
		o.cfg.BaseInput, _, err = baseline.AdmitDirectory(ctx, o.cfg.PathOrig, o.cfg.PathOrig)
		if err != nil {
			return nil, fmt.Errorf("--path-orig: %w", err)
		}
	}
	origRoot := o.cfg.BaseInput.LocalPath
	// Directory admission reuses one artifact for the same-root exception.
	// Equal labels on distinct authoritative readers MUST still compare bytes.
	if o.cfg.BaseInput == o.cfg.Input {
		o.store.AddWarning(manifest.NamedResource{}, manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "--path and --path-orig resolve to the same root; ignoring --path-orig",
			Detail:   []string{repoRoot},
		})
		return nil, nil
	}
	cs, err := change.Detect(ctx, o.cfg.BaseInput, o.cfg.Input)
	if err != nil {
		return nil, fmt.Errorf("change detect: %w", err)
	}
	slog.Debug("changed-only mode", "baseline", origRoot, "current", repoRoot, "changed_files", cs.Len())
	return cs, nil
}
