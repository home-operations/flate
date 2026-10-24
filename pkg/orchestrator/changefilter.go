package orchestrator

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) buildChangeFilter(ctx context.Context, repoRoot string, changes *change.Set) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if changes == nil {
		return nil
	}
	f := change.NewFilter(changes, o.index, o.store, repoRoot)
	history := map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]{}
	narrow := map[manifest.NamedResource]bool{}
	defer func() {
		for range o.store.Sweep() {
		}
	}()
	o.configureControllers()
	objects := o.store.Objects()
	slices.SortFunc(objects, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := obj.Named()
		if !manifest.KindOf(obj).Render {
			continue
		}
		reads := &store.ReadSet{}
		reads.Init(o.store, id, nil)
		func() {
			_, _, output := reads.GetEmission(id)
			defer output.Finish()
			defer reads.Retire()
			records, canNarrow := o.controllers.PreviousRender(ctx, reads)
			narrow[id] = canNarrow
			if records == nil {
				return
			}
			materialized := maps.Collect(records)
			if reads.Valid() {
				history[id] = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					for child, record := range materialized {
						if !yield(child, record) {
							return
						}
					}
				}
			}
		}()
	}
	f.RefineHistory(history, narrow)
	if err := ctx.Err(); err != nil {
		return err
	}
	if changes.Len() == 0 && len(f.KeepNames()) == 0 && (o.cfg.PathOrig != "" || o.cfg.BaseInput != nil) {
		o.store.AddWarning(manifest.NamedResource{}, manifest.Warning{
			Category: manifest.WarnPathConfig,
			Message:  "no changes detected between --path and --path-orig; output will be empty; Flux-excluded files are not counted; verify both paths reference distinct snapshots",
		})
	}
	o.filter = f
	slog.Debug("orchestrator: changed-only keep set", "filter", o.filter)
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
