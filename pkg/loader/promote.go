package loader

import (
	"context"
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// PromoteOptions preserves the run's secret policy and per-document admission boundary.
type PromoteOptions struct {
	Record      manifest.Production
	Reads       *store.ReadSet
	WipeSecrets bool                             // SOPS ciphertext is replaced with offline placeholders.
	Admit       func(manifest.BaseManifest) bool // Admission applies to the requested object and every sibling.
}

// Promote materializes admitted objects from record.Path and reports whether id is available.
// Authored proves which raw identity may be projected; absent evidence requires an exact id.
// Existing objects take precedence over raw files.
func Promote(ctx context.Context, st *store.Store, id manifest.NamedResource, opts PromoteOptions) bool {
	record := opts.Record
	path := record.Path
	if st == nil || path == "" || ctx.Err() != nil {
		return false
	}
	if obj := st.GetObject(id); obj != nil {
		return opts.Admit(obj)
	}
	reader := documentReader{}
	source := record.Source
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	var sources sourceReader = st
	if opts.Reads != nil {
		sources = opts.Reads
	}
	mounted := map[string]*store.SourceArtifact{}
	mountSource(mounted, sources.GetArtifact(source))
	reader.setSources(mounted)
	objs, err := reader.parseFile(path, manifest.ParseDocOptions{WipeSecrets: opts.WipeSecrets, DefaultNamespace: ""})
	if err != nil {
		slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
		return false
	}
	authored := record.Authored
	if authored == (manifest.NamedResource{}) {
		authored = id
	}
	for _, obj := range objs {
		if obj.Named() == authored {
			if authored.Namespace != id.Namespace {
				obj = obj.WithNamespace(id.Namespace)
			}
		} else if obj.Named() == id {
			continue
		}
		if !opts.Admit(obj) || st.GetObject(obj.Named()) != nil {
			continue
		}
		if ctx.Err() != nil {
			return false
		}
		if opts.Reads != nil {
			opts.Reads.Admit(obj, source, record.Owner)
		} else {
			st.AddObject(obj)
		}
	}
	obj := st.GetObject(id)
	return obj != nil && opts.Admit(obj)
}
