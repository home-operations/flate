package loader

import (
	"fmt"
	"log/slog"
	"os"

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
// PreferExisting semantics are honored: if an id already lives in the
// store (already promoted, or render-emitted by a KS), the existing
// object stays put. wipeSecrets matches the loader's: callers should
// pass through whatever DiscoveryOnly used at file-load time so SOPS
// Secrets stay wiped on promotion the same way they were skipped at
// load.
func Promote(idx *ExistenceIndex, st *store.Store, id manifest.NamedResource, wipeSecrets bool) bool {
	if idx == nil || st == nil {
		return false
	}
	path, ok := idx.Get(id)
	if !ok {
		return false
	}
	if obj := st.GetObject(id); obj != nil {
		return true
	}
	objs, err := parseFileObjects(path, wipeSecrets)
	if err != nil {
		slog.Debug("loader: promote re-parse failed", "id", id.String(), "path", path, "err", err)
		return false
	}
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		if existing := st.GetObject(obj.Named()); existing != nil {
			continue
		}
		st.AddObject(obj)
	}
	return st.GetObject(id) != nil
}

// parseFileObjects re-reads path and returns the recognized Flux CRs
// in document order. Errors at the file level surface; per-doc parse
// failures are swallowed at Debug log level (consistent with
// Loader.loadFile, which also skips bad docs in mixed files).
func parseFileObjects(path string, wipeSecrets bool) ([]manifest.BaseManifest, error) {
	f, err := os.Open(path) //nolint:gosec // path came from ExistenceIndex, populated by tree walk
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	docs, err := manifest.DecodeDocs(f)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: wipeSecrets}
	out := make([]manifest.BaseManifest, 0, len(docs))
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("loader: promote doc skipped", "path", path, "err", err)
			continue
		}
		if _, ok := obj.(*manifest.RawObject); ok {
			continue
		}
		id := obj.Named()
		if manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}
