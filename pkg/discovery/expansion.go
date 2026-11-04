package discovery

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type sourceExpansion struct {
	ks     *manifest.Kustomization
	source *store.SourceArtifact
	object manifest.BaseManifest
}

func (d *Session) loadEntryInput(ctx context.Context, l *loader.Loader, total *int) error {
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
		return fmt.Errorf("--path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("--path %q is not a directory", d.cfg.Path)
	}
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if err := d.loadAt(ctx, d.cfg.Path, total); err != nil {
		return err
	}
	l.IgnoreFile = ""
	return nil
}

func (d *Session) enqueueExpansions(
	kustomizations []store.Child,
	ksExpanded map[sourceExpansion]struct{},
	pending []sourceExpansion,
) []sourceExpansion {
	for _, child := range kustomizations {
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		art, _ := d.cfg.Store.GetArtifact(ks.SourceID(), store.ArtifactContent).(*store.SourceArtifact)
		key := sourceExpansion{ks: ks, source: art, object: d.cfg.Store.GetObject(ks.SourceID())}
		if _, seen := ksExpanded[key]; seen {
			continue
		}
		ksExpanded[key] = struct{}{}
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
		sourceRoot, _, external := loader.SourceView(d.cfg.Store, ks.SourceID(), repoRoot)
		if external && d.cfg.Store.GetObject(ks.SourceID()) == nil {
			next = append(next, key)
			continue
		}
		target := filepath.Join(sourceRoot, manifest.NormalizeClaimBase(ks.Path))
		if !safepath.Contains(sourceRoot, target) {
			continue
		}
		loaded, loadErr := d.loadSourcePath(ctx, target, key.source, total)
		if loadErr != nil {
			return next, added, loadErr
		}
		if !loaded {
			continue
		}
		added++
	}
	return next, added, nil
}
