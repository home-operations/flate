package discovery

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type sourceExpansion struct {
	ks     *manifest.Kustomization
	source *store.SourceArtifact
}

func (d *Session) loadEntryInput(ctx context.Context, l *loader.Loader, total *int) error {
	if d.loading {
		return nil
	}
	input := d.cfg.Input
	if input == nil {
		input, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		d.cfg.Input = input
	}
	name, err := filepath.Rel(input.LocalPath, d.cfg.Path)
	if err != nil {
		return fmt.Errorf("rel: %w", err)
	}
	if info, err := fs.Stat(input.FS, filepath.ToSlash(name)); err != nil {
		return fmt.Errorf("input path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("input path %q is not a directory", d.cfg.Path)
	}
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if _, err := d.loadAt(
		ctx,
		d.cfg.Path,
		total,
		&sourceLoad{work: d.cfg.Work.Record(manifest.WorkPreparation, d.cfg.Path)},
	); err != nil {
		return err
	}
	l.IgnoreFile = ""
	d.loading = true
	return nil
}

func (d *Session) enqueueExpansions(
	kustomizations []store.Child,
	pending []sourceExpansion,
) []sourceExpansion {
	for i := range kustomizations {
		child := &kustomizations[i]
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		art, _ := d.cfg.Store.GetArtifact(ks.EffectiveSourceID(), store.ArtifactContent).(*store.SourceArtifact)
		key := sourceExpansion{ks: ks, source: art}
		pending = append(pending, key)
	}
	return pending
}

func (d *Session) loadExpansions(
	ctx context.Context,
	repoRoot string,
	pending []sourceExpansion,
	total *int,
) ([]sourceExpansion, int, error) {
	added := 0
	next := pending[:0]
	for _, key := range pending {
		ks := key.ks
		if ks.IsFragment() {
			continue
		}
		sourceRoot := loader.SourceView(d.cfg.Store, ks.EffectiveSourceID(), repoRoot).Path
		if sourceRoot == "" {
			next = append(next, key)
			continue
		}
		target := filepath.Join(sourceRoot, manifest.NormalizeClaimBase(ks.Path))
		if !safepath.Contains(sourceRoot, target) {
			continue
		}
		loaded, loadErr := d.loadSourcePath(ctx, target, &key, total)
		if loadErr != nil {
			return next, added, loadErr
		}
		added += loaded
	}
	return next, added, nil
}
