package loader

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Promote materializes admitted objects from id's indexed file. Admission is
// checked separately for every document, including siblings. It returns whether
// id is available and admitted. Existing objects take precedence over raw files;
// wipeSecrets preserves discovery's secret-wiping policy on re-parse.
func (i *ExistenceIndex) Promote(st *store.Store, id manifest.NamedResource, wipeSecrets bool, admit func(manifest.BaseManifest) bool) bool {
	if i == nil || st == nil {
		return false
	}
	path, ok := i.Get(id)
	if !ok {
		return false
	}
	if obj := st.GetObject(id); obj != nil {
		return admit(obj)
	}
	objs, err := parseFile(path, manifest.ParseDocOptions{WipeSecrets: wipeSecrets})
	if err != nil {
		slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
		return false
	}
	for _, obj := range objs {
		if !admit(obj) || st.GetObject(obj.Named()) != nil {
			continue
		}
		st.AddObject(obj)
	}
	obj := st.GetObject(id)
	return obj != nil && admit(obj)
}
