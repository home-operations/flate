package loader

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Promote materializes id into st by re-parsing the file the
// ExistenceIndex recorded for it and AddObject'ing every Flux CR in
// the file. Returns true on success; false if the id is unknown to
// the index or its file can no longer be parsed.
//
// The whole file is parsed (not just id) because YAML multi-doc files
// often pack a CM + its Secret + a HelmRelease together — promoting
// one frequently means callers will need a sibling next. Parsing once
// and AddObject'ing every doc avoids re-opening the same file
// repeatedly when several lazy lookups land on it.
//
// PreferExisting semantics are honored: if an id already lives in
// the store (already promoted, or render-emitted by a KS), the
// existing object stays put. wipeSecrets matches the loader's:
// callers should pass through whatever DiscoveryOnly used at file-
// load time so SOPS Secrets stay wiped on promotion the same way
// they were skipped at load.
func (i *ExistenceIndex) Promote(st *store.Store, id manifest.NamedResource, wipeSecrets bool) bool {
	if i == nil || st == nil {
		return false
	}
	path, ok := i.Get(id)
	if !ok {
		return false
	}
	if st.GetObject(id) != nil {
		return true
	}
	objs, err := parseFile(path, manifest.ParseDocOptions{WipeSecrets: wipeSecrets})
	if err != nil {
		slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
		return false
	}
	for _, obj := range objs {
		if st.GetObject(obj.Named()) != nil {
			continue
		}
		st.AddObject(obj)
	}
	return st.GetObject(id) != nil
}
