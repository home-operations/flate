package loader

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"

	_ "github.com/home-operations/flate/internal/kustomize" // Loader-only SDK entrypoints MUST install the process scope owner at startup.
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Prepare admits one authoritative document while retaining its sanitized outcome.
func Prepare(doc manifest.DocOccurrence, opts manifest.ParseDocOptions) (store.Child, manifest.DocOccurrence, bool) {
	if doc.Err != nil {
		return store.Child{}, doc, false
	}
	obj, authoritative, changed, err := manifest.ParseDoc(doc.Document, opts)
	doc.Document, doc.Err = authoritative, err
	if err != nil {
		obj = nil
	}
	return store.Child{Obj: obj, Doc: authoritative}, doc, changed
}

func (r documentReader) parseFile(path string, opts manifest.ParseDocOptions) ([]store.Child, error) {
	out, err := r.fileObjects(path, opts)
	if err != nil {
		return nil, err
	}
	var tracked []store.Child
	if len(out) != 0 {
		tracked = make([]store.Child, 0, len(out))
	}
	for _, child := range out {
		if child.Obj != nil && child.outcome.Err == nil && manifest.KindOf(child.Obj).Tracked {
			tracked = append(tracked, child.Child)
		}
	}
	return tracked, nil
}

func (r documentReader) fileObjects(path string, opts manifest.ParseDocOptions) ([]parsedObject, error) {
	r.restore(path)
	out := r.cache[path].objects
	if out == nil {
		out = []parsedObject{}
		err := r.decodeFileObjects(path, opts, r.cache != nil, func(child parsedObject) {
			out = append(out, child)
		})
		if err != nil {
			return nil, err
		}
		if r.cache != nil {
			cached := r.cache[path]
			cached.objects = out
			r.cache[path] = cached
		}
	}
	return out, nil
}

func (r documentReader) decodeFileObjects(path string, opts manifest.ParseDocOptions, keepRaw bool, yield func(parsedObject)) error {
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
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errSourceUnavailable) {
			return fmt.Errorf("open %s: %w", path, err)
		}
		yield(parsedObject{outcome: manifest.DocOccurrence{Err: fmt.Errorf("%w: open %s: %w", manifest.ErrInput, path, err)}})
		return nil
	}
	defer func() { _ = f.Close() }()
	docs, err := manifest.DecodeDocs(f, manifest.IncidentalDocuments)
	for _, occurrence := range docs {
		version := manifest.DocAPIVersion(occurrence.Document)
		child, outcome, _ := Prepare(occurrence, opts)
		if outcome.Err != nil {
			yield(parsedObject{Child: child, outcome: outcome, version: version})
			continue
		}
		obj := child.Obj
		tracked := obj != nil && manifest.KindOf(obj).Tracked
		if obj == nil || !tracked && !keepRaw {
			manifest.ReleaseDoc(occurrence.Document)
			continue
		}
		id := obj.Named()
		if manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
			slog.Debug("loader: skipped template doc (unresolved envsubst in name/namespace)", "path", path, "id", id.String())
			manifest.ReleaseDoc(occurrence.Document)
			continue
		}
		if !tracked {
			manifest.ReleaseDoc(occurrence.Document)
			child.Doc, outcome.Document = nil, nil
		}
		yield(parsedObject{Child: child, outcome: outcome, version: version})
	}
	if err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}
