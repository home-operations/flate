package orchestrator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) buildChangeFilter(repoRoot string, changes *change.Set) {
	if changes == nil {
		return
	}
	f := change.NewFilter(changes, o.index, o.store, repoRoot)
	if changes.Len() == 0 && len(f.KeepNames()) == 0 && (o.cfg.PathOrig != "" || o.cfg.BaseInput != nil) {
		o.store.AddWarning(manifest.NamedResource{}, manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
		})
	}
	o.filter = f
	slog.Debug("orchestrator: changed-only keep set", "filter", o.filter)
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
	if o.cfg.ExternalChanges == nil && o.cfg.BaseInput == o.cfg.Input {
		o.store.AddWarning(manifest.NamedResource{}, manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "--path and --path-orig resolve to the same root; ignoring --path-orig",
			Detail:   []string{repoRoot},
		})
		return nil, nil
	}
	cs := o.cfg.ExternalChanges
	if cs == nil {
		var err error
		cs, err = change.Detect(ctx, o.cfg.BaseInput, o.cfg.Input)
		if err != nil {
			return nil, fmt.Errorf("change detect: %w", err)
		}
	}
	slog.Debug("orchestrator: changed-only mode", "baseline", origRoot, "current", repoRoot, "changed_files", cs.Len())
	return cs, nil
}
