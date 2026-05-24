package loader

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/home-operations/flate/pkg/manifest"
)

// parseFile opens path, decodes every YAML/JSON doc, and returns the
// recognized Flux CRs in document order. Filters that match both the
// initial file-walk load and the lazy promotion path live here so
// "what counts as a loadable Flux doc" has one canonical definition:
//
//   - manifest.ParseDoc per-doc errors are swallowed at Debug log
//     level (matches flux-local — a malformed sibling doc in a
//     multi-doc file shouldn't poison the rest of the file).
//   - manifest.RawObject results are dropped — flate only persists
//     kinds it explicitly understands.
//   - Objects whose name/namespace contain `${VAR}` are dropped as
//     templates: a parent KS's postBuild.substitute(From) is supposed
//     to resolve them at render time; Kubernetes itself rejects `$`
//     in metadata.name, so the unsubstituted file copy would never
//     reach the API server in real Flux either.
//
// Per-doc parse errors include path in their log fields so the caller
// doesn't need to wrap them. File-level errors (open, decode) surface
// to the caller with the path baked into the wrap.
func parseFile(path string, opts manifest.ParseDocOptions) ([]manifest.BaseManifest, error) {
	f, err := os.Open(path) //nolint:gosec // path came from a tree-walk under the scan root or the ExistenceIndex
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	docs, err := manifest.DecodeDocs(f)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	out := make([]manifest.BaseManifest, 0, len(docs))
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("loader: doc skipped", "path", path, "err", err)
			continue
		}
		if _, ok := obj.(*manifest.RawObject); ok {
			continue
		}
		id := obj.Named()
		if manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
			slog.Debug("loader: skipped template doc (unresolved envsubst in name/namespace)",
				"path", path, "id", id.String())
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}
