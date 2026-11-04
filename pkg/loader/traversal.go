package loader

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

type traversalPolicy uint8

const (
	traversalScope traversalPolicy = iota
	traversalAdmission
	traversalProof
)

type traversal struct {
	work        *manifest.WorkRecord
	files       *[]fileAdmission
	documents   documentReader
	policy      traversalPolicy
	visited     map[graphContext]struct{}
	visitBuf    [8]graphContext
	visitCount  int
	directories map[string]struct{}
	depth       int
	loader      *Loader
	ignore      *ignoreSet
	build       bool
	ignoreCache map[ignoreKey]bool
	count       int
	err         error
	canceled    bool
}

func (w *traversal) proceed(ctx context.Context) bool {
	if w.err == nil {
		w.err = ctx.Err()
		w.canceled = w.err != nil
	}
	return w.err == nil
}

//nolint:gocritic // Keep immutable value-receiver semantics.
func (r documentReader) references(
	dir string,
	body *types.Kustomization,
	resolve bool,
	yield func(kustomizeinput.Reference, string) bool,
) bool {
	doc := r.cache[dir]
	record := !doc.edgesKnown && doc.traversed && body != nil &&
		new(r).mountedRoot(dir) == new(r).mountedRoot(r.bootstrap)
	for ref := range kustomizeinput.Kustomization(body) {
		var target string
		if (record || resolve) && ref.Role != kustomizeinput.File && !ref.Inline {
			target, _ = kustpath.Resolve(dir, ref.Path, kustpath.Dependency)
			if record && target != "" {
				child := r.cache[target]
				child.incoming = true
				r.cache[target] = child
			}
		}
		if yield != nil && !yield(ref, target) {
			return false
		}
	}
	if record {
		doc = r.cache[dir]
		doc.edgesKnown = true
		r.cache[dir] = doc
	}
	return true
}

func (r *documentReader) replayEdges() {
	if r.edgesDirty {
		var pending []string
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for dir, doc := range r.cache {
			if !doc.edgesKnown && doc.traversed && doc.body != nil {
				pending = append(pending, dir)
			}
		}
		for _, dir := range pending {
			r.references(dir, r.cache[dir].body, false, nil)
		}
		r.edgesDirty = false
	}
}

func (w *traversal) admitFile(ctx context.Context, root, file string, component bool) bool {
	w.work.Add(&manifest.WorkCounts{FileEntries: 1})
	if !isManifestFile(file) || w.ignoreMatches(root, file, false) {
		return true
	}
	if !w.proceed(ctx) {
		return false
	}
	if component {
		if w.files != nil {
			*w.files = append(*w.files, fileAdmission{file: file, kind: admissionData})
		}
		if err := w.loader.recordDataFile(ctx, file); err != nil {
			w.loader.Store.AddWarning(manifest.Warning{
				Category: manifest.WarnInputParseFailed,
				Message:  "component data file failed to parse",
				Detail:   []string{file, err.Error()},
			})
		}
		return true
	}
	if w.files != nil {
		*w.files = append(*w.files, fileAdmission{file: file, kind: admissionResource})
	}
	n, err := w.loader.loadFile(ctx, file)
	w.count += n
	if err != nil {
		w.err = fmt.Errorf("loader: %w", err)
	}
	return w.err == nil
}

type traversalReference struct {
	kustomizeinput.Reference

	target string
}

type referenceVisit struct {
	parent    *graphVisit
	child     *graphVisit
	reference *traversalReference
}

func (w *traversal) reference(
	ctx context.Context,
	visitValue *graphVisit,
	refValue *traversalReference,
	yield func(graphVisit) bool,
) bool {
	v := *visitValue
	ref := *refValue
	if !w.proceed(ctx) {
		return false
	}
	child := v
	child.document, child.readable, child.incomplete = nil, false, ""
	visit := referenceVisit{parent: &v, child: &child, reference: &ref}
	switch w.policy {
	case traversalAdmission:
		if decision := w.admitReference(ctx, &visit); decision.done {
			return decision.more
		}
	case traversalProof:
		if ref.Role != kustomizeinput.Resource &&
			(ref.Role != kustomizeinput.Configuration || !strings.HasPrefix(ref.Field, ".transformers")) ||
			ref.Inline {
			return true
		}
		target, ok := kustomizeinput.Local(v.root, v.dir, ref.Path)
		if !ok {
			return true
		}
		if new(w.documents).fileHasNamespaceTransformer(v.root, target) {
			return false
		}
		child.dir = target
	case traversalScope:
		if decision := w.scopeReference(ctx, &visit, yield); decision.done {
			return decision.more
		}
	}
	count := w.count
	w.depth++
	more := w.walk(ctx, child, yield)
	w.depth--
	if !more && !w.canceled {
		w.count = count
	}
	return more
}

type traversalDecision struct{ done, more bool }

func (w *traversal) admitReference(ctx context.Context, visit *referenceVisit) traversalDecision {
	if visit.reference.Role != kustomizeinput.Resource && visit.reference.Role != kustomizeinput.Component {
		return traversalDecision{done: true, more: true}
	}
	base, resolved := visit.parent.file, visit.reference.target
	if resolved == "" || filepath.IsAbs(visit.reference.Path) {
		return traversalDecision{done: true, more: true}
	}
	inputFile := new(w.documents).input(resolved)
	info, err := fs.Stat(inputFile.fs, inputFile.name)
	new(w.documents).rememberStat(resolved, info, err)
	if err != nil {
		return traversalDecision{done: true, more: w.proceed(ctx)}
	}
	if !info.IsDir() {
		if _, ok := kustpath.Resolve(
			base,
			visit.reference.Path,
			kustpath.File,
		); !ok ||
			visit.reference.Role != kustomizeinput.Resource {
			return traversalDecision{done: true, more: true}
		}
		return traversalDecision{
			done: true,
			more: w.admitFile(ctx, visit.parent.root, resolved, visit.parent.role == kustomizeinput.Component),
		}
	}
	visit.child.role = kustomizeinput.Resource
	if visit.parent.role == kustomizeinput.Component {
		if visit.reference.Role != kustomizeinput.Component {
			return traversalDecision{done: true, more: true}
		}
		visit.child.role = kustomizeinput.Component
	}
	visit.child.file, visit.child.deferMaterialization = resolved, false
	return traversalDecision{}
}

func (w *traversal) scopeReference(
	ctx context.Context,
	visit *referenceVisit,
	yield func(graphVisit) bool,
) traversalDecision {
	switch {
	case visit.reference.Role == kustomizeinput.Configuration:
		visit.child.role, visit.child.pluginRoot = kustomizeinput.Configuration, visit.parent.dir
		visit.child.checkMetadata = visit.parent.role == kustomizeinput.Configuration &&
			strings.HasPrefix(visit.reference.Field, ".transformers")
		if visit.reference.Inline {
			return traversalDecision{done: true, more: w.inlineReference(ctx, visit, yield)}
		}
	case visit.reference.Role == kustomizeinput.File:
		visit.child.role = kustomizeinput.File
	case visit.parent.role != kustomizeinput.Configuration:
		visit.child.role = kustomizeinput.Resource
	}
	resolved, ok := kustpath.Resolve(
		filepath.Join(visit.parent.root, visit.parent.dir),
		visit.reference.Path,
		kustpath.Dependency,
	)
	if !ok || !safepath.Contains(visit.parent.root, resolved) ||
		new(w.documents).mountedRoot(resolved) != new(w.documents).mountedRoot(visit.parent.root) {
		if visit.child.role == kustomizeinput.Configuration {
			return traversalDecision{done: true, more: w.uncertain(
				new(*visit.parent),
				visit.reference.FieldPath()+": configuration reference cannot be inspected locally",
				yield,
			)}
		}
		return traversalDecision{done: true, more: true}
	}
	rel, _ := filepath.Rel(visit.parent.root, resolved)
	visit.child.dir = filepath.ToSlash(rel)
	if visit.child.role == kustomizeinput.Resource {
		_, admitted := kustpath.Resolve(
			filepath.Join(visit.parent.root, visit.parent.dir),
			visit.reference.Path,
			kustpath.File,
		)
		visit.child.deferDecode = !admitted || filepath.IsAbs(visit.reference.Path)
	}
	return traversalDecision{}
}

func (w *traversal) inlineReference(ctx context.Context, visit *referenceVisit, yield func(graphVisit) bool) bool {
	for _, resource := range visit.reference.Resources {
		refs, known := kustomizeinput.Plugin(resource.YNode())
		if !w.configuration(
			ctx,
			new(*visit.child),
			pluginInputs{
				references:   refs,
				known:        known,
				metadataOnly: kustomizeinput.MetadataOnly(resource.YNode()),
			},
			yield,
		) {
			return false
		}
	}
	return true
}

func (w *traversal) configuration(
	ctx context.Context,
	visitValue *graphVisit,
	config pluginInputs,
	yield func(graphVisit) bool,
) bool {
	v := *visitValue
	if !config.known {
		if !w.uncertain(new(v), "unknown plugin configuration may read additional files", yield) {
			return false
		}
	}
	if v.checkMetadata && !config.metadataOnly {
		if !w.uncertain(new(v), "configuration transformer may rewrite plugin file operands", yield) {
			return false
		}
	}
	for _, ref := range config.references {
		child := v
		child.dir = v.pluginRoot
		if !w.reference(ctx, new(child), new(traversalReference{Reference: ref}), yield) {
			return false
		}
	}
	return true
}

func (w *traversal) uncertain(visitValue *graphVisit, reason string, yield func(graphVisit) bool) bool {
	v := *visitValue
	v.document, v.readable, v.incomplete = nil, false, reason
	return yield(v)
}

type ignoreKey struct {
	path  string
	isDir bool
}

func (w *traversal) ignoreMatches(root, path string, isDir bool) bool {
	if w.ignore == nil || w.ignore.matcher == nil {
		return false
	}
	key := ignoreKey{path: path, isDir: isDir}
	if v, ok := w.ignoreCache[key]; ok {
		return v
	}
	v := w.ignore.matches(path, root, isDir)
	w.ignoreCache[key] = v
	if v && w.policy == traversalAdmission {
		doc := w.documents.cache[path]
		doc.scanExcluded = true
		w.documents.cache[path] = doc
	}
	return v
}

func (w *traversal) scanBootstrapFluxKS(ctx context.Context, root, dir, kpath string) int {
	if !w.proceed(ctx) {
		return 0
	}
	seen := map[string]struct{}{kpath: {}}
	spec := w.documents.read(dir).body
	includeBootstrapReferences(spec, dir, seen, w.work)
	if !w.proceed(ctx) {
		return 0
	}
	inputFile2 := new(w.documents).input(dir)
	entries, err := fs.ReadDir(inputFile2.fs, inputFile2.name)
	if err != nil {
		warning := manifest.Warning{
			Category: manifest.WarnInputReadFailed,
			Message:  "bootstrap-sibling readdir failed",
			Detail:   []string{dir, err.Error()},
		}
		if w.files != nil {
			*w.files = append(*w.files, fileAdmission{kind: admissionWarning, warning: warning})
		}
		w.loader.Store.AddWarning(warning)
		return 0
	}
	count := 0
	for _, e := range entries {
		w.work.Add(&manifest.WorkCounts{FileEntries: 1})
		if !w.proceed(ctx) {
			return count
		}
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		abs := filepath.Join(dir, e.Name())
		if !isManifestFile(abs) || w.ignoreMatches(root, abs, false) {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		reader := w.documents
		reader.regular = e.Type().IsRegular()
		count += w.scanBootstrapFile(ctx, abs, &reader)
	}
	return count
}

func includeBootstrapReferences(
	spec *types.Kustomization,
	dir string,
	seen map[string]struct{},
	work *manifest.WorkRecord,
) {
	for ref := range kustomizeinput.Kustomization(spec) {
		work.Add(&manifest.WorkCounts{ReaderEdges: 1})
		if ref.Inline {
			continue
		}
		if abs, ok := kustpath.Resolve(dir, ref.Path, kustpath.Dependency); ok {
			seen[abs] = struct{}{}
		}
	}
}

func (w *traversal) scanBootstrapFile(ctx context.Context, abs string, reader *documentReader) int {
	count := 0
	var objs []parsedObject
	var err error
	opts := manifest.ParseDocOptions{}
	if reader.compact {
		objs, err = reader.censusFile(ctx, abs, opts)
	} else {
		objs, err = reader.fileObjects(abs, opts)
	}
	if err != nil {
		warning := manifest.Warning{
			Category: manifest.WarnInputParseFailed,
			Message:  "bootstrap-sibling parse failed",
			Detail:   []string{abs, err.Error()},
		}
		if w.files != nil {
			*w.files = append(*w.files, fileAdmission{kind: admissionWarning, warning: warning})
		}
		w.loader.Store.AddWarning(warning)
		return 0
	}
	{
		childValues := objs
		for childIndex := range childValues {
			child := childValues[childIndex]
			if child.fact != nil && child.fact.facts.uncertain {
				continue
			}
			obj := child.Obj
			ks, ok := obj.(*manifest.Kustomization)
			if !ok {
				continue
			}
			// A Flux-GVK patch fragment with neither spec.path nor sourceRef
			// must never become a reconcilable Kustomization. A sourceRef alone
			// is sufficient for a bootstrap entry that renders the source root.
			if ks.IsFragment() {
				continue
			}
			if w.files != nil && child.fact != nil {
				*w.files = append(
					*w.files,
					fileAdmission{file: abs, kind: admissionBootstrap, coordinate: child.fact.Declaration},
				)
			}
			if !w.loader.acceptObject(obj.Named()) {
				continue
			}
			w.loader.recordObject(obj, abs)
			cached := w.documents.cache[abs]
			cached.bootstrap = true
			w.documents.cache[abs] = cached
			count++
		}
	}
	return count
}

// Explicit resources bypass scan pruning; negated descendants MUST remain reachable.
func (w *traversal) shouldSkipDir(root, name, full string) bool {
	if sourceignore.PruneDir(name, true) {
		return true
	}
	if w.ignore != nil && w.ignore.hasNegation {
		return false
	}
	return w.ignoreMatches(root, full, true)
}
