package discovery

import (
	"context"
	"maps"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Entry identifies the literal input directory selected by the caller.
type Entry struct {
	Path     string
	Input    *store.SourceArtifact
	prepared *entryInputs
}

type entryInputs struct {
	input      store.SourceArtifact
	parsed     *loader.ParsedInputs
	aliasViews map[string]*store.SourceArtifact
}

// Plan establishes entries without publishing seeds into the caller's store.
//
//nolint:gocritic // Public entry planning preserves its configuration value snapshot.
func Plan(ctx context.Context, cfg Config) ([]Entry, error) {
	if cfg.Input == nil && cfg.Store != nil {
		cfg.Input, _ = cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	cfg.Store = store.New()
	cfg.Store.SetWorkTree(cfg.Work.Identity().Tree, cfg.Work)
	d := newDiscoverer(&cfg)
	l := d.loader
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return nil, err
	}
	input := cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact) //nolint:errcheck // Entry admission provides a source artifact.
	l.SourceRoot = input.LocalPath
	l.IgnoreFile = cfg.KRMIgnoreFile
	if _, err := l.Load(
		ctx,
		d.cfg.Path,
		manifest.NamedResource{},
		cfg.Work.Record(manifest.WorkPreparation, d.cfg.Path),
	); err != nil {
		return nil, err
	}
	l.IgnoreFile = ""
	l.PreferExisting = true
	if err := d.loadManifests(ctx, input.LocalPath, false); err != nil {
		return nil, err
	}
	prepared := &entryInputs{
		input:      *input,
		parsed:     l.ParsedInputs(),
		aliasViews: maps.Clone(d.aliasViews),
	}
	return []Entry{{Path: d.cfg.Path, Input: input, prepared: prepared}}, nil
}
