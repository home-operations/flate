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

	"github.com/home-operations/flate/internal/core/application"
	_ "github.com/home-operations/flate/internal/engine/kustomize/krusty" // Loader-only SDK entrypoints MUST install the process scope owner at startup.
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Prepare MUST preserve the sanitized outcome even when typed admission fails.
//
//nolint:gocritic // DocOccurrence preserves an immutable value snapshot across this boundary.
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
	out, err := r.fileObjects(ctx, path, opts)
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
func (r documentReader) fileObjects(
	ctx context.Context,
	path string,
	opts manifest.ParseDocOptions,
) ([]parsedObject, error) {
	r.restore(path)
	cached := r.cache[path]
	if err := cached.decodeErr; err != nil {
		return nil, err
	}
	out := cached.objects
	if out == nil || !covers(cached.coverage, r.intent) || !r.declarationsOnly && cached.unprepared {
		out = []parsedObject{}
		err := r.decodeFileObjects(ctx, path, opts, func(child parsedObject) {
			out = append(out, child)
		})
		if err != nil {
			return nil, err
		}
		if r.cache != nil {
			cached = r.cache[path]
			cached.sourceRoot = new(r).mountedRoot(path)
			cached.objects, cached.coverage, cached.unprepared = out, r.intent, r.declarationsOnly
			cached.parsed = nil
			r.cache[path] = cached
		}
	}
	if r.coordinate != nil {
		out = slices.DeleteFunc(slices.Clone(out), func(child parsedObject) bool {
			return child.outcome.Ordinal != r.coordinate.Document || child.item != r.coordinate.item
		})
	}
	return applicationView(out, cached.coverage, r.intent), nil
}

//nolint:gocritic // Immutable reader values retain their original snapshot semantics.
func (r documentReader) decodeFileObjects(
	ctx context.Context,
	path string,
	opts manifest.ParseDocOptions,
	yield func(parsedObject),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cached := r.cache[path]; cached.unprepared &&
		covers(cached.coverage, new(r).documentRole(opts.RequiredIdentity)) &&
		!r.declarationsOnly {
		return new(r).prepareObserved(ctx, path, opts, yield)
	}
	file := path
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
		data, readFailure := io.ReadAll(&inputReader{context: ctx, input: f})
		if readFailure != nil {
			return manifest.NewError(manifest.ErrInput, fmt.Errorf("read %s: %w", path, readFailure), "")
		}
		cached := r.cache[file]
		cached.sourceRoot = new(r).mountedRoot(file)
		cached.possible = application.Contains(data)
		r.cache[file] = cached
		if !cached.possible {
			return nil
		}
		reader = bytes.NewReader(data)
	}

	declarations, err := r.prepareStream(ctx, reader, opts, func(child parsedObject) {
		if r.declarationsOnly {
			yield(child)
		} else if prepared, emit := new(r).prepareFileObject(path, r.cache != nil || r.compact, &child); emit {
			yield(prepared)
		}
	})
	if declarations.err != nil {
		declarations.err = manifest.NewError(manifest.ErrInput, fmt.Errorf("read %s: %w", path, declarations.err), "")
	}
	if err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if r.cache != nil && !r.skipDeclarations {
		cached := r.cache[file]
		cached.declarations, cached.declarationsData = declarations, nil
		cached.declarations.payload.observation = cached.declarations
		r.cache[file] = cached
	}
	return nil
}

func (r *documentReader) prepareObserved(
	ctx context.Context,
	path string,
	opts manifest.ParseDocOptions,
	yield func(parsedObject),
) error {
	cached := r.cache[path]
	objects := cached.objects
	cached.objects, cached.unprepared = nil, false
	r.cache[path] = cached
	for i := range objects {
		raw := objects[i]
		if err := ctx.Err(); err != nil {
			for j := i; j < len(objects); j++ {
				manifest.ReleaseDoc(objects[j].Doc)
			}
			return err
		}
		child := raw
		if raw.outcome.Err == nil {
			child = r.prepareStreamObject(opts, &raw.outcome)
			child.item, child.collection = raw.item, raw.collection
		}
		if prepared, emit := r.prepareFileObject(r.localPath(path), true, &child); emit {
			yield(prepared)
		}
	}
	return nil
}

//nolint:gocritic // The immutable census MUST transfer each document and its payload under one retention decision.
func (r documentReader) censusFile(
	ctx context.Context,
	path string,
	opts manifest.ParseDocOptions,
) ([]parsedObject, error) {
	r.restore(path)
	previous := r.cache[path]
	eager := !r.eager || !slices.ContainsFunc(previous.objects, func(child parsedObject) bool {
		return child.fact != nil && child.fact.object == nil
	})
	switch {
	case previous.objects == nil && previous.decodeErr == nil:
	case previous.unprepared:
	case !covers(previous.coverage, r.intent), !eager:
	default:
		return applicationView(previous.objects, previous.coverage, r.intent), previous.decodeErr
	}
	facts := []parsedObject{}
	var projectionErr error
	err := r.decodeFileObjects(ctx, path, opts, func(child parsedObject) {
		switch {
		case child.outcome.Err != nil:
			facts = append(facts, child)
			return
		case application.Contains([]byte(child.version)):
			cached := r.cache[path]
			cached.sourceRoot = new(r).mountedRoot(path)
			cached.possible = true
			r.cache[path] = cached
		}
		var projection manifest.TemplateProjection
		var err error
		if r.intent != manifest.ApplicationDocuments {
			projection, err = r.projections.Project(ctx, child.Obj)
		}
		if err != nil {
			projectionErr = err
			manifest.ReleaseDoc(child.Doc)
			return
		}
		o := new(occurrenceFacts).compactOccurrence(child.Obj, projection)
		o.requiresBuild = child.collection
		o.facts.collection = child.collection
		o.facts.substituteDisabled = child.substituteDisabled
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
		var retained map[string]any
		switch r.eager {
		case true:
			o.object, retained = child.Obj, child.Doc
		case false:
			manifest.ReleaseDoc(child.Doc)
		}
		outcome := child.outcome
		outcome.Document = nil
		facts = append(
			facts,
			parsedObject{
				Obj: structural, Doc: retained,
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
	cached := r.cache[path]
	cached.sourceRoot = new(r).mountedRoot(path)
	cached.objects, cached.decodeErr, cached.coverage, cached.unprepared = facts, err, r.intent, false
	cached.parsed = nil
	r.cache[path] = cached
	return facts, err
}

//nolint:gocritic // DocOccurrence preserves an immutable value snapshot across this boundary.
func expandDocuments(
	o manifest.DocOccurrence,
	documents []manifest.DocOccurrence,
) ([]manifest.DocOccurrence, bool, error) {
	kind, _ := o.Document["kind"].(string)
	if _, collection := o.Document["items"]; !strings.HasSuffix(kind, "List") || !collection {
		return append(documents, o), false, nil
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

func (r *documentReader) skipFileLeaf(item int, leafValue *manifest.DocOccurrence) bool {
	leaf := *leafValue
	if r.coordinate != nil && (leaf.Ordinal != r.coordinate.Document || item != r.coordinate.item) {
		manifest.ReleaseDoc(leaf.Document)
		return true
	}
	return false
}

func (r *documentReader) prepareStream(
	ctx context.Context,
	input io.Reader,
	opts manifest.ParseDocOptions,
	yield func(parsedObject),
) (*fileObservation, error) {
	role := r.documentRole(opts.RequiredIdentity)
	state := &struct {
		reader      inputReader
		observation fileObservation
	}{
		reader: inputReader{context: ctx, input: input},
		observation: fileObservation{
			Present:  true,
			Complete: !r.skipDeclarations && r.intent != manifest.ApplicationDocuments && r.coordinate == nil,
			limited:  r.skipDeclarations || r.intent == manifest.ApplicationDocuments || r.coordinate != nil,
		},
	}
	// Retained observations MUST NOT retain the reader or its context.
	defer func() { state.reader = inputReader{} }()
	declarations := &state.observation
	docs, err := decodeStream(ctx, &state.reader, role, declarations)
	if err != nil {
		return declarations, err
	}
	for _, occurrence := range docs {
		r.prepareOccurrence(opts, &occurrence, declarations, yield)
	}
	return declarations, ctx.Err()
}

func decodeStream(
	ctx context.Context,
	reader *inputReader,
	role manifest.DocumentRole,
	declarations *fileObservation,
) ([]manifest.DocOccurrence, error) {
	docs, err := manifest.DecodeDocs(reader, role)
	if (err != nil || manifest.DocumentFailure(docs) != nil) && reader.terminal == nil {
		_, _ = io.Copy(io.Discard, reader)
	}
	if cancelErr := ctx.Err(); cancelErr != nil {
		err = cancelErr
	}
	if reader.failure != nil {
		declarations.err = manifest.NewError(manifest.ErrInput, reader.failure, "")
		declarations.Complete = false
	}
	if err != nil {
		declarations.Complete = false
		for _, doc := range docs {
			manifest.ReleaseDoc(doc.Document)
		}
		return nil, err
	}
	return docs, nil
}

func (r *documentReader) prepareOccurrence(
	opts manifest.ParseDocOptions,
	occurrence *manifest.DocOccurrence,
	declarations *fileObservation,
	yield func(parsedObject),
) {
	if occurrence.Err != nil {
		declarations.Complete = false
	}
	if r.skipFileOccurrence(occurrence, yield) {
		return
	}
	var single [1]manifest.DocOccurrence
	leaves, collection, err := expandDocuments(*occurrence, single[:0])
	if err != nil {
		declarations.Complete = false
		occurrence.Err = err
		yield(parsedObject{outcome: *occurrence})
		return
	}
	observe := !r.skipDeclarations && r.intent != manifest.ApplicationDocuments
	if observe {
		for _, leaf := range leaves {
			observeDeclaration(&declarations.FileDeclarations, leaf.Document)
		}
	}
	if collection && !r.compact {
		for _, leaf := range leaves {
			manifest.ReleaseDoc(leaf.Document)
		}
		leaves = []manifest.DocOccurrence{*occurrence}
	}
	for item, leaf := range leaves {
		if r.skipFileLeaf(item, &leaf) {
			continue
		}
		child := r.prepareStreamObject(opts, &leaf)
		child.item, child.collection = item, collection
		yield(child)
	}
	if collection && r.compact {
		manifest.ReleaseDoc(occurrence.Document)
	}
}

func observeDeclaration(declarations *manifest.FileDeclarations, doc map[string]any) {
	id := manifest.DocIdentity(doc)
	concrete := id.Kind != "" && id.Name != "" && manifest.DocAPIVersion(doc) != ""
	if !concrete ||
		manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace) {
		declarations.Complete = false
		return
	}
	if declarations.Objects == nil {
		declarations.Objects = map[manifest.NamedResource]string{}
	}
	fingerprint := manifest.Fingerprint(manifest.WithoutOwnershipLabels(doc))
	if previous, known := declarations.Objects[id]; known && previous != fingerprint {
		fingerprint = ""
	}
	declarations.Objects[id] = fingerprint
}

//nolint:containedctx // io.Reader cannot receive cancellation through Read.
type inputReader struct {
	context  context.Context
	input    io.Reader
	terminal error
	failure  error
}

func (r *inputReader) Read(p []byte) (int, error) {
	if err := r.context.Err(); err != nil {
		r.terminal = err
		return 0, err
	}
	n, err := r.input.Read(p)
	r.terminal = err
	if err != nil && err != io.EOF {
		r.failure = err
	}
	if cancelErr := r.context.Err(); cancelErr != nil {
		r.terminal = cancelErr
		return 0, cancelErr
	}
	return n, err //nolint:wrapcheck // Reader errors and EOF retain their identity.
}

func (r *documentReader) documentRole(requiredIdentity bool) manifest.DocumentRole {
	if requiredIdentity {
		return manifest.RequiredDocuments
	}
	return r.intent
}

func (r *documentReader) prepareStreamObject(
	opts manifest.ParseDocOptions,
	leaf *manifest.DocOccurrence,
) parsedObject {
	if r.declarationsOnly {
		return parsedObject{Doc: leaf.Document, outcome: *leaf}
	}
	version := manifest.DocAPIVersion(leaf.Document)
	child, outcome, _ := Prepare(*leaf, opts)
	return parsedObject{
		Child: child, outcome: outcome, version: version,
		substituteDisabled: manifest.HasSubstituteDisabled(child.Doc),
	}
}

func covers(coverage, intent manifest.DocumentRole) bool {
	return coverage == intent || intent == manifest.ApplicationDocuments &&
		(coverage == manifest.IncidentalDocuments || coverage == manifest.RequiredDocuments)
}

func applicationView(objects []parsedObject, coverage, intent manifest.DocumentRole) []parsedObject {
	if intent != manifest.ApplicationDocuments || coverage == intent {
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

func parsedFactObject(child *parsedObject) manifest.BaseManifest {
	object := child.Obj
	if child.fact != nil {
		object = child.fact.object
		if object == nil && child.fact.facts != nil {
			object = child.fact.facts.catalog
		}
	}
	return object
}

func parsedApplication(child *parsedObject) bool {
	_, valid := manifest.ApplicationFor(parsedFactObject(child))
	return child.outcome.Err != nil || valid
}
