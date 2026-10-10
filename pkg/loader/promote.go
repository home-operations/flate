package loader

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// PromoteOptions preserves the run's secret policy and per-document admission boundary.
type PromoteOptions struct {
	WipeSecrets bool                             // SOPS ciphertext is replaced with offline placeholders.
	Admit       func(manifest.BaseManifest) bool // Admission applies to the requested object and every sibling.
}

// Promote materializes admitted objects from path and reports whether id is available.
// Existing objects take precedence over raw files. The caller supplies an indexed raw path.
func Promote(st *store.Store, id manifest.NamedResource, path string, opts PromoteOptions) bool {
	if st == nil || path == "" {
		return false
	}
	if obj := st.GetObject(id); obj != nil {
		return opts.Admit(obj)
	}
	objs, err := parseFile(path, manifest.ParseDocOptions{WipeSecrets: opts.WipeSecrets})
	if err != nil {
		slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
		return false
	}
	for _, obj := range objs {
		if !opts.Admit(obj) || st.GetObject(obj.Named()) != nil {
			continue
		}
		st.AddObject(obj)
	}
	obj := st.GetObject(id)
	return obj != nil && opts.Admit(obj)
}
