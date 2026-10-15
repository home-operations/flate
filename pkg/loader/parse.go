package loader

import (
	"fmt"
	"io/fs"
	"log/slog"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

// parseFile opens path and returns the recognized Flux CRs in document order —
// a thin collector over decodeFileObjects with keepRaw=false, so unmodeled
// kinds (manifest.RawObject) are dropped. See decodeFileObjects for the shared
// decode + filter contract.
func (r documentReader) parseFile(path string, opts manifest.ParseDocOptions) ([]manifest.BaseManifest, error) {
	if cached, ok := r.cache[path]; ok && cached.objects != nil {
		return slices.DeleteFunc(slices.Clone(cached.objects), func(obj manifest.BaseManifest) bool {
			return !manifest.KindOf(obj).Tracked
		}), nil
	}
	var out []manifest.BaseManifest
	err := r.decodeFileObjects(path, opts, r.cache != nil, func(obj manifest.BaseManifest) {
		out = append(out, obj)
	})
	if err == nil && r.cache != nil {
		r.cache[path] = document{objects: out}
	}
	return slices.DeleteFunc(slices.Clone(out), func(obj manifest.BaseManifest) bool {
		return !manifest.KindOf(obj).Tracked
	}), err
}

// decodeFileObjects opens path, decodes every YAML/JSON doc, and yields each
// parsed object that survives the canonical loader filters — the one place
// "what counts as a loadable doc" is defined, shared by the file-walk load, the
// lazy promotion path, and the discovery-time producer scan:
//
//   - manifest.ParseDoc per-doc errors are swallowed at Debug level (matches
//     flux-local — a malformed sibling doc shouldn't poison the rest).
//   - Objects whose name/namespace still carry `${VAR}` are dropped as
//     templates: a parent KS's postBuild.substitute(From) resolves them at
//     render time, and Kubernetes rejects `$` in metadata.name, so the
//     unsubstituted copy would never reach the API server in real Flux either.
//   - manifest.RawObject results (kinds flate doesn't model) are dropped unless
//     keepRaw is set — parseFile only persists understood kinds, while the
//     producer scan keeps them to read ExternalSecret / SealedSecret targets.
//
// Per-doc parse errors carry path in their log fields; file-level errors (open,
// decode) surface to the caller with the path baked into the wrap.
func (r documentReader) decodeFileObjects(path string, opts manifest.ParseDocOptions, keepRaw bool, yield func(manifest.BaseManifest)) error {
	input, name := r.input(path)
	var f fs.File
	var err error
	if local, ok := input.(*gittree.FS); ok && r.regular {
		f, err = local.OpenRegular(name)
	} else {
		f, err = input.Open(name)
	}
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	docs, err := manifest.DecodeDocs(f)
	if err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	for _, doc := range docs {
		obj, _, _, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("loader: doc skipped", "path", path, "err", err)
			manifest.ReleaseDoc(doc)
			continue
		}
		if obj == nil || !manifest.KindOf(obj).Tracked && !keepRaw {
			manifest.ReleaseDoc(doc)
			continue
		}
		id := obj.Named()
		if manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
			slog.Debug("loader: skipped template doc (unresolved envsubst in name/namespace)",
				"path", path, "id", id.String())
			manifest.ReleaseIfNotRetained(doc, obj)
			continue
		}
		manifest.ReleaseIfNotRetained(doc, obj)
		yield(obj)
	}
	return nil
}
