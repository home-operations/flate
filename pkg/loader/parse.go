package loader

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"

	"k8s.io/apimachinery/pkg/util/validation/field"

	_ "github.com/home-operations/flate/internal/kustomize" // Loader-only SDK entrypoints MUST install the process scope owner at startup.
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func (r documentReader) parseFile(path string, opts manifest.ParseDocOptions) ([]store.Child, error) {
	out := r.cache[path].objects
	var err error
	if out == nil {
		cached := r.cache[path]
		err = r.decodeFileObjects(path, opts, r.cache != nil, func(child store.Child) {
			out = append(out, child)
		})
		if r.cache == nil {
			return out, err
		}
		if err == nil {
			cached.objects = out
			r.cache[path] = cached
		}
	}
	if out == nil {
		return nil, err
	}
	tracked := make([]store.Child, 0, len(out))
	for _, child := range out {
		if manifest.KindOf(child.Obj).Tracked {
			tracked = append(tracked, child)
		}
	}
	return tracked, err
}

// Raw objects must be retained for producer scans when keepRaw is set.
// API-version admission errors retain their source path for deferred ownership policy.
func (r documentReader) decodeFileObjects(path string, opts manifest.ParseDocOptions, keepRaw bool, yield func(store.Child)) error {
	input, name := r.input(path)
	path = r.localPath(path)
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
	for i, doc := range docs {
		obj, authoritative, _, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			if _, invalid := errors.AsType[*field.Error](err); !invalid {
				slog.Debug("loader: doc skipped", "path", path, "err", err)
				manifest.ReleaseDoc(doc)
				continue
			}
			for _, pending := range docs[i:] {
				manifest.ReleaseDoc(pending)
			}
			return fmt.Errorf("parse %s: %w", path, err)
		}
		tracked := obj != nil && manifest.KindOf(obj).Tracked
		if obj == nil || !tracked && !keepRaw {
			manifest.ReleaseDoc(doc)
			continue
		}
		id := obj.Named()
		if manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
			slog.Debug("loader: skipped template doc (unresolved envsubst in name/namespace)",
				"path", path, "id", id.String())
			manifest.ReleaseDoc(doc)
			continue
		}
		if !tracked {
			manifest.ReleaseDoc(doc)
			authoritative = nil
		}
		yield(store.Child{Obj: obj, Doc: authoritative})
	}
	return nil
}
