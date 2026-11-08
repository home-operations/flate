package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/resource"

	"github.com/home-operations/flate/internal/application"
	_ "github.com/home-operations/flate/internal/kustomize" // Loader-only SDK entrypoints MUST install the process scope owner at startup.
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Prepare admits one authoritative document while retaining its sanitized outcome.
func Prepare(doc manifest.DocOccurrence, opts manifest.ParseDocOptions) (store.Child, manifest.DocOccurrence, bool) {
	if doc.Err != nil {
		sanitized, changed := manifest.SanitizeDocument(doc)
		return store.Child{Doc: sanitized.Document, Warnings: sanitized.Warnings}, sanitized, changed
	}
	parsed, err := manifest.ParseDoc(doc.Document, opts)
	doc.Document, doc.Err = parsed.Document, err
	if len(parsed.Warnings) != 0 {
		doc.Warnings = append(slices.Clone(doc.Warnings), parsed.Warnings...)
	}
	for warningIndex := range doc.Warnings {
		warning := doc.Warnings[warningIndex]
		if warning.Resource != manifest.DocIdentity(parsed.Document) {
			doc.Warnings = slices.Clone(doc.Warnings)
			for i := range doc.Warnings {
				doc.Warnings[i].Resource = manifest.DocIdentity(parsed.Document)
			}
			break
		}
	}
	if err != nil {
		parsed.Object = nil
	}
	return store.Child{Obj: parsed.Object, Doc: parsed.Document, Warnings: doc.Warnings}, doc, parsed.Changed
}

//nolint:gocritic // Keep immutable value-receiver semantics.
func (r documentReader) parseFile(
	ctx context.Context,
	path string,
	opts manifest.ParseDocOptions,
) ([]store.Child, error) {
	if r.compact && r.coordinate == nil {
		facts, err := r.censusFile(ctx, path, opts)
		var children []store.Child
		{
			factValues := facts
			for factIndex := range factValues {
				fact := factValues[factIndex]
				if fact.Obj != nil && fact.outcome.Err == nil && !fact.fact.facts.uncertain {
					children = append(children, fact.Child)
				}
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
	{
		childValues := out
		for childIndex := range childValues {
			child := childValues[childIndex]
			if child.outcome.Err != nil && r.coordinate != nil {
				return nil, manifest.DocumentFailure([]manifest.DocOccurrence{child.outcome})
			}
			if child.Obj != nil && child.outcome.Err == nil && manifest.KindOf(child.Obj).Tracked {
				tracked = append(tracked, child.Child)
			}
		}
	}
	return tracked, nil
}

//nolint:gocritic // Immutable reader values retain their original snapshot semantics.
func (r documentReader) fileObjects(path string, opts manifest.ParseDocOptions) ([]parsedObject, error) {
	r.restore(path)
	if err := r.cache[path].decodeErr; err != nil {
		return nil, err
	}
	out := r.cache[path].objects
	if out == nil || !covers(r.cache[path].coverage, r.intent) {
		out = []parsedObject{}
		err := r.decodeFileObjects(path, opts, r.cache != nil, func(child parsedObject) {
			out = append(out, child)
		})
		if err != nil {
			return nil, err
		}
		if r.cache != nil {
			cached := r.cache[path]
			cached.sourceRoot = new(r).mountedRoot(path)
			cached.objects, cached.coverage = out, r.intent
			cached.parsed = nil
			r.cache[path] = cached
		}
	}
	if r.coordinate != nil {
		out = slices.DeleteFunc(slices.Clone(out), func(child parsedObject) bool {
			return child.outcome.Ordinal != r.coordinate.Document || child.item != r.coordinate.item
		})
	}
	return applicationView(out, r.intent), nil
}

//nolint:gocritic // Immutable reader values retain their original snapshot semantics.
func (r documentReader) decodeFileObjects(
	path string,
	opts manifest.ParseDocOptions,
	keepRaw bool,
	yield func(parsedObject),
) error {
	inputFile := new(r).input(path)
	path = new(r).localPath(path)
	var f fs.File
	var err error
	if local, ok := inputFile.fs.(*gittree.FS); ok && r.regular {
		f, err = local.OpenRegular(inputFile.name)
	} else {
		f, err = inputFile.fs.Open(inputFile.name)
	}
	if err != nil {
		return fileOpenFailure(path, err, yield)
	}
	defer func() { _ = f.Close() }()
	var reader io.Reader = f
	if r.intent == manifest.ApplicationDocuments {
		data, readErr := io.ReadAll(f)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", path, readErr)
		}
		cached := r.cache[path]
		cached.sourceRoot = new(r).mountedRoot(path)
		cached.possible = application.Contains(data)
		r.cache[path] = cached
		if !cached.possible {
			return nil
		}
		reader = bytes.NewReader(data)
	}

	err = r.prepareStream(reader, opts, func(child parsedObject) {
		if prepared, emit := new(r).prepareFileObject(path, keepRaw, &child); emit {
			yield(prepared)
		}
	})
	if err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

//nolint:gocritic // Immutable reader values retain their original snapshot semantics.
func (r documentReader) censusFile(
	ctx context.Context,
	path string,
	opts manifest.ParseDocOptions,
) ([]parsedObject, error) {
	r.restore(path)
	if cached := r.cache[path]; (cached.objects != nil || cached.decodeErr != nil) &&
		covers(cached.coverage, r.intent) {
		return applicationView(cached.objects, r.intent), cached.decodeErr
	}
	facts := []parsedObject{}
	var projectionErr error
	err := r.decodeFileObjects(path, opts, true, func(child parsedObject) {
		if child.outcome.Err != nil {
			facts = append(facts, child)
			return
		}
		if application.Contains([]byte(child.version)) {
			cached := r.cache[path]
			cached.sourceRoot = new(r).mountedRoot(path)
			cached.possible = true
			r.cache[path] = cached
		}
		var projection manifest.TemplateProjection
		if r.intent != manifest.ApplicationDocuments {
			var err error
			projection, err = r.projections.Project(ctx, child.Obj)
			if err != nil {
				projectionErr = err
				manifest.ReleaseDoc(child.Doc)
				return
			}
		}
		o := compactOccurrence(child.Obj, projection)
		o.facts.declared = child.declared
		origin := new(r).localPath(path)
		root := new(r).mountedRoot(origin)
		file, _ := filepath.Rel(root, origin)
		o.Declaration = occurrenceID{
			Source:   root,
			File:     filepath.ToSlash(file),
			Document: child.outcome.Ordinal,
			item:     child.item,
		}
		var structural manifest.BaseManifest
		if o.facts.structural != nil {
			structural = o.facts.structural
		}
		var payload *inputPayload
		if r.recordInputs && o.facts.retained && child.Doc != nil {
			payload = &inputPayload{}
			payload.data, _ = manifest.EncodeRender(
				[]manifest.DocOccurrence{{Document: child.Doc, Ordinal: child.outcome.Ordinal}},
			)
		}
		manifest.ReleaseDoc(child.Doc)
		outcome := child.outcome
		outcome.Document = nil
		facts = append(
			facts,
			parsedObject{
				Obj:      structural,
				Warnings: child.Warnings,
				item:     child.item,
				fact:     &o,
				outcome:  outcome,
				version:  child.version, payload: payload,
			},
		)
	})
	if projectionErr != nil {
		err = projectionErr
	}
	if r.intent != manifest.ApplicationDocuments && len(facts) > 1 {
		resolveDeclaredFingerprints(facts)
	}
	cached := r.cache[path]
	cached.sourceRoot = new(r).mountedRoot(path)
	cached.objects, cached.decodeErr, cached.coverage = facts, err, r.intent
	cached.parsed = nil
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

func resolveDeclaredFingerprints(facts []parsedObject) {
	declared := map[manifest.NamedResource]string{}
	{
		childValues2 := facts
		for childIndex2 := range childValues2 {
			child := childValues2[childIndex2]
			if child.fact == nil {
				continue
			}
			id, fingerprint := child.fact.Authored, child.fact.facts.declared
			if previous, known := declared[id]; known && previous != fingerprint {
				fingerprint = ""
			}
			declared[id] = fingerprint
		}
	}
	{
		childValues3 := facts
		for childIndex3 := range childValues3 {
			child := childValues3[childIndex3]
			if child.fact != nil {
				child.fact.facts.declared = declared[child.fact.Authored]
			}
		}
	}
}

func (r *documentReader) prepareFileObject(path string, keepRaw bool, childValue *parsedObject) (parsedObject, bool) {
	child := *childValue
	if child.outcome.Err != nil {
		return child, true
	}
	obj := child.Obj
	if r.intent == manifest.ApplicationDocuments {
		if _, applies := manifest.ApplicationFor(obj); !applies {
			manifest.ReleaseDoc(child.Doc)
			return parsedObject{}, false
		}
	}
	tracked := obj != nil && manifest.KindOf(obj).Tracked
	if obj == nil || !tracked && !keepRaw {
		manifest.ReleaseDoc(child.Doc)
		return parsedObject{}, false
	}
	id := obj.Named()
	if (!r.compact || r.coordinate != nil) &&
		(manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace)) {
		slog.Debug(
			"loader: skipped template doc (unresolved envsubst in name/namespace)",
			"path",
			path,
			"id",
			id.String(),
		)
		manifest.ReleaseDoc(child.Doc)
		return parsedObject{}, false
	}
	if !tracked {
		manifest.ReleaseDoc(child.Doc)
		child.Doc, child.outcome.Document = nil, nil
	}
	return child, true
}

func fileOpenFailure(path string, err error, yield func(parsedObject)) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errSourceUnavailable) {
		return fmt.Errorf("open %s: %w", path, err)
	}
	yield(
		parsedObject{
			outcome: manifest.DocOccurrence{
				Err: manifest.NewError(manifest.ErrInput, fmt.Errorf("open %s: %w", path, err), ""),
			},
		},
	)
	return nil
}

func (r *documentReader) skipFileOccurrence(occurrenceValue *manifest.DocOccurrence, yield func(parsedObject)) bool {
	occurrence := *occurrenceValue
	if r.coordinate != nil && occurrence.Ordinal != r.coordinate.Document {
		manifest.ReleaseDoc(occurrence.Document)
		return true
	}
	if occurrence.Err != nil {
		yield(parsedObject{outcome: occurrence})
		return true
	}
	return false
}

func (r *documentReader) expandFileOccurrence(
	occurrenceValue *manifest.DocOccurrence,
	leaves []manifest.DocOccurrence,
	yield func(parsedObject),
) ([]manifest.DocOccurrence, bool, bool) {
	occurrence := *occurrenceValue
	collection := false
	if r.compact {
		var expandErr error
		leaves, collection, expandErr = expandDocuments(occurrence)
		if expandErr != nil {
			occurrence.Err = expandErr
			yield(parsedObject{outcome: occurrence})
			return leaves, collection, true
		}
	}
	return leaves, collection, false
}

func (r *documentReader) skipFileLeaf(item int, leafValue *manifest.DocOccurrence) bool {
	leaf := *leafValue
	if r.coordinate != nil && (leaf.Ordinal != r.coordinate.Document || item != r.coordinate.item) {
		manifest.ReleaseDoc(leaf.Document)
		return true
	}
	return false
}

func (r *documentReader) prepareStream(input io.Reader, opts manifest.ParseDocOptions, yield func(parsedObject)) error {
	role := r.intent
	if opts.RequiredIdentity {
		role = manifest.RequiredDocuments
	}
	docs, err := manifest.DecodeDocs(input, role)
	for _, occurrence := range docs {
		if r.skipFileOccurrence(new(occurrence), yield) {
			continue
		}
		leaves, collection, skip := r.expandFileOccurrence(new(occurrence), []manifest.DocOccurrence{occurrence}, yield)
		if skip {
			continue
		}
		for item, leaf := range leaves {
			if r.skipFileLeaf(item, new(leaf)) {
				continue
			}
			child := r.prepareStreamObject(opts, new(leaf))
			child.item = item
			yield(child)
		}
		if collection {
			manifest.ReleaseDoc(occurrence.Document)
		}
	}
	return err
}

func (r *documentReader) prepareStreamObject(opts manifest.ParseDocOptions, leaf *manifest.DocOccurrence) parsedObject {
	version := manifest.DocAPIVersion(leaf.Document)
	declared := ""
	if r.compact && r.coordinate == nil && r.intent != manifest.ApplicationDocuments {
		declared = manifest.Fingerprint(manifest.WithoutOwnershipLabels(leaf.Document))
	}
	child, outcome, _ := Prepare(*leaf, opts)
	return parsedObject{Child: child, outcome: outcome, version: version, declared: declared}
}

func covers(coverage, intent manifest.DocumentRole) bool {
	return coverage == intent || intent == manifest.ApplicationDocuments &&
		(coverage == manifest.IncidentalDocuments || coverage == manifest.RequiredDocuments)
}

func applicationView(objects []parsedObject, intent manifest.DocumentRole) []parsedObject {
	if intent != manifest.ApplicationDocuments {
		return objects
	}
	var applications []parsedObject
	for i := range objects {
		if parsedApplication(&objects[i]) {
			applications = append(applications, objects[i])
		}
	}
	return applications
}

func parsedApplication(child *parsedObject) bool {
	object := child.Obj
	if child.fact != nil {
		object = child.fact.object
		if object == nil && child.fact.facts != nil {
			object = child.fact.facts.catalog
		}
	}
	_, valid := manifest.ApplicationFor(object)
	return child.outcome.Err != nil || valid
}
