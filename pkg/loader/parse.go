package loader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"

	"sigs.k8s.io/kustomize/api/resource"

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
	if r.compact && r.coordinate == nil {
		facts, err := r.censusFile(path, opts)
		var children []store.Child
		for _, fact := range facts {
			if fact.Obj != nil && fact.outcome.Err == nil && !fact.fact.facts.uncertain {
				children = append(children, fact.Child)
			}
		}
		return children, err
	}
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
		yield(parsedObject{outcome: manifest.DocOccurrence{Err: manifest.NewError(manifest.ErrInput, fmt.Errorf("open %s: %w", path, err), "")}})
		return nil
	}
	defer func() { _ = f.Close() }()
	docs, err := manifest.DecodeDocs(f, manifest.IncidentalDocuments)
	for _, occurrence := range docs {
		if r.coordinate != nil && occurrence.Ordinal != r.coordinate.Document {
			manifest.ReleaseDoc(occurrence.Document)
			continue
		}
		if occurrence.Err != nil {
			yield(parsedObject{outcome: occurrence})
			continue
		}
		leaves := []manifest.DocOccurrence{occurrence}
		collection := false
		if r.compact {
			var expandErr error
			leaves, collection, expandErr = expandDocuments(occurrence)
			if expandErr != nil {
				occurrence.Err = expandErr
				yield(parsedObject{outcome: occurrence})
				continue
			}
		}
		for item, leaf := range leaves {
			if r.coordinate != nil && (leaf.Ordinal != r.coordinate.Document || item != r.coordinate.item) {
				manifest.ReleaseDoc(leaf.Document)
				continue
			}
			version := manifest.DocAPIVersion(leaf.Document)
			child, outcome, _ := Prepare(leaf, opts)
			if outcome.Err != nil {
				yield(parsedObject{Child: child, outcome: outcome, item: item, version: version})
				continue
			}
			obj := child.Obj
			tracked := obj != nil && manifest.KindOf(obj).Tracked
			if obj == nil || !tracked && !keepRaw {
				manifest.ReleaseDoc(leaf.Document)
				continue
			}
			id := obj.Named()
			if (!r.compact || r.coordinate != nil) && (manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace)) {
				slog.Debug("loader: skipped template doc (unresolved envsubst in name/namespace)", "path", path, "id", id.String())
				manifest.ReleaseDoc(leaf.Document)
				continue
			}
			if !tracked {
				manifest.ReleaseDoc(leaf.Document)
				child.Doc, outcome.Document = nil, nil
			}
			yield(parsedObject{Child: child, outcome: outcome, item: item, version: version})
		}
		if collection {
			manifest.ReleaseDoc(occurrence.Document)
		}
	}
	if err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func (r documentReader) censusFile(path string, opts manifest.ParseDocOptions) ([]parsedObject, error) {
	r.restore(path)
	if cached := r.cache[path]; cached.objects != nil || cached.decodeErr != nil {
		return cached.objects, cached.decodeErr
	}
	facts := []parsedObject{}
	err := r.decodeFileObjects(path, opts, true, func(child parsedObject) {
		if child.outcome.Err != nil {
			facts = append(facts, child)
			return
		}
		o := compactOccurrence(child.Obj)
		origin := r.localPath(path)
		root := r.mountedRoot(origin)
		file, _ := filepath.Rel(root, origin)
		o.Declaration = OccurrenceID{Source: root, File: filepath.ToSlash(file), Document: child.outcome.Ordinal, item: child.item}
		var structural manifest.BaseManifest
		if o.facts.structural != nil {
			structural = o.facts.structural
		}
		manifest.ReleaseDoc(child.Doc)
		outcome := child.outcome
		outcome.Document = nil
		facts = append(facts, parsedObject{Obj: structural, fact: &o, outcome: outcome, version: child.version})
	})
	cached := r.cache[path]
	cached.objects, cached.decodeErr = facts, err
	r.cache[path] = cached
	return facts, err
}

func expandDocuments(o manifest.DocOccurrence) ([]manifest.DocOccurrence, bool, error) {
	kind, _ := o.Document["kind"].(string)
	if _, collection := o.Document["items"]; !strings.HasSuffix(kind, "List") || !collection {
		return []manifest.DocOccurrence{o}, false, nil
	}
	data, err := json.Marshal(o.Document)
	if err != nil {
		return nil, true, manifest.NewError(manifest.ErrInput, fmt.Errorf("encode collection: %w", err), "")
	}
	// A second empty document MUST prevent kio from silently unwrapping malformed List items.
	data = append(data, '\n', '-', '-', '-', '\n')
	nodes, err := resource.NewFactory(nil).RNodesFromBytes(data)
	if err != nil {
		return nil, true, manifest.NewError(manifest.ErrInput, fmt.Errorf("expand collection: %w", err), "")
	}
	out := make([]manifest.DocOccurrence, 0, len(nodes))
	for _, node := range nodes {
		doc, err := node.Map()
		if err != nil {
			return nil, true, manifest.NewError(manifest.ErrInput, fmt.Errorf("decode collection leaf: %w", err), "")
		}
		out = append(out, manifest.DocOccurrence{Document: doc, Ordinal: o.Ordinal})
	}
	return out, true, nil
}
