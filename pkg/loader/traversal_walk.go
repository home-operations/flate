package loader

import (
	"cmp"
	"context"
	"io/fs"
	"log/slog"
	"path"
	"path/filepath"
	"slices"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
)

//nolint:gocritic // Recursive visit snapshots MUST remain on the stack.
func (w *traversal) walk(ctx context.Context, v graphVisit, yield func(graphVisit) bool) bool {
	if !w.proceed(ctx) {
		return false
	}
	abs := v.file
	if w.policy != traversalAdmission {
		abs = filepath.Join(v.root, v.dir)
	}
	if done, more := w.prepareVisit(&v, abs, yield); done {
		return more
	}
	d, file := w.documents.read(abs)
	cached := w.documents.cache[abs]
	if !w.proceed(ctx) {
		return false
	}
	if done := w.acceptDocument(ctx, &v, &cached, walkDocument{abs: abs, file: file, body: d}); done {
		return true
	}
	if d == nil && (w.policy == traversalAdmission || cached.pathState == pathDirectory) {
		if w.prepareDirectory(&v, &cached, abs) {
			return true
		}
		return w.walkDirectory(ctx, &v, abs, yield)
	}
	if w.policy == traversalScope {
		if d == nil {
			return w.opaqueConfiguration(ctx, &v, &cached, yield)
		}
		v.deferDecode = false
		v.namespace = cmp.Or(v.namespace, d.Namespace)
		v.namePrefix += d.NamePrefix
		v.nameSuffix = d.NameSuffix + v.nameSuffix
		if v.namespace == "" {
			v.namespace = new(w.documents).transformerInjectedNamespace(v.root, v.dir).ns
		}
		if !w.publishScope(&v, &d, walkDocument{abs: abs, file: file}, yield) {
			return false
		}
	}
	if !w.documents.references(
		abs,
		d,
		w.policy == traversalAdmission,
		func(ref kustomizeinput.Reference, target string) bool {
			return w.reference(ctx, new(v), new(traversalReference{Reference: ref, target: target}), yield)
		},
	) {
		return false
	}
	if w.policy == traversalAdmission && v.role != kustomizeinput.Component && !w.build {
		w.count += w.scanBootstrapFluxKS(ctx, v.root, abs, file)
	}
	return w.proceed(ctx)
}

func (w *traversal) seenScopeVisit(visit *graphVisit) bool {
	if slices.Contains(w.visitBuf[:w.visitCount], visit.graphContext) {
		return true
	}
	if _, seen := w.visited[visit.graphContext]; seen {
		return true
	}
	if w.visitCount < len(w.visitBuf) {
		w.visitBuf[w.visitCount] = visit.graphContext
		w.visitCount++
	} else {
		if w.visited == nil {
			w.visited = map[graphContext]struct{}{}
		}
		w.visited[visit.graphContext] = struct{}{}
	}
	return false
}

type walkDocument struct {
	abs, file string
	body      *types.Kustomization
}

func (w *traversal) admitDocument(ctx context.Context, visit *graphVisit, cached *document, input walkDocument) bool {
	if visit.role == kustomizeinput.Component && !isKustomizeComponent(input.body) {
		return true
	}
	if input.file == "" {
		return false
	}
	if visit.deferMaterialization {
		if _, seen := w.directories[input.abs]; seen {
			return true
		}
		w.directories[input.abs] = struct{}{}
	}
	cached.traversed = true
	w.documents.cache[input.abs] = *cached
	if input.body == nil {
		return true
	}
	if isKustomizeComponent(input.body) {
		if visit.role != kustomizeinput.Component {
			slog.Debug("loader: skipping kustomize Component directory", "dir", input.abs)
		}
		visit.role = kustomizeinput.Component
	} else if !w.ignoreMatches(visit.root, input.file, false) {
		if w.files != nil {
			*w.files = append(*w.files, fileAdmission{file: input.file, kind: admissionPackage})
		}
		n, err := w.loader.loadFile(ctx, input.file)
		w.count += n
		if err != nil {
			w.loader.Store.AddWarning(manifest.Warning{
				Category: manifest.WarnInputParseFailed,
				Message:  "kustomization file failed to parse",
				Detail:   []string{input.file, err.Error()},
			})
		}
	}
	return false
}

type directoryEntry struct {
	abs   string
	entry fs.DirEntry
}

func (w *traversal) prepareVisit(visit *graphVisit, abs string, yield func(graphVisit) bool) (bool, bool) {
	switch w.policy {
	case traversalScope:
		if _, ok := kustpath.Resolve(
			"",
			visit.dir,
			kustpath.Root,
		); !ok || !safepath.Contains(visit.root, abs) ||
			new(w.documents).mountedRoot(abs) != new(w.documents).mountedRoot(visit.root) {
			return true, true
		}
		visit.file, visit.coordinate, visit.document = abs, path.Join(visit.prefix, visit.dir), nil
		if visit.role == kustomizeinput.File {
			return true, yield(*visit)
		}
	case traversalAdmission:
		if visit.deferMaterialization {
			if w.shouldSkipDir(visit.root, filepath.Base(abs), abs) {
				return true, true
			}
			cached := w.documents.cache[abs]
			cached.adHoc = true
			w.documents.cache[abs] = cached
		} else {
			if _, seen := w.directories[abs]; seen {
				return true, true
			}
			w.directories[abs] = struct{}{}
		}
	case traversalProof:
		if w.depth > transformerScanMaxDepth {
			return true, true
		}
		if _, seen := w.directories[visit.dir]; seen {
			return true, true
		}
		w.directories[visit.dir] = struct{}{}
	}
	return false, false
}

func (w *traversal) acceptDocument(ctx context.Context, visit *graphVisit, cached *document, input walkDocument) bool {
	switch w.policy {
	case traversalScope:
		if input.body != nil || cached.pathState == pathDirectory {
			if w.seenScopeVisit(visit) {
				return true
			}
		}
	case traversalAdmission:
		if done := w.admitDocument(ctx, visit, cached, input); done {
			return true
		}
	case traversalProof:
		if input.body == nil {
			return true
		}
	}
	return false
}

func (w *traversal) walkDirectory(
	ctx context.Context,
	visit *graphVisit,
	abs string,
	yield func(graphVisit) bool,
) bool {
	if !w.proceed(ctx) {
		return false
	}
	input, name := new(w.documents).input(abs)
	entries, err := fs.ReadDir(input, name)
	if err != nil && w.policy == traversalAdmission {
		w.err = err
		return false
	}
	for _, entry := range entries {
		if !w.proceed(ctx) {
			return false
		}
		if visit.entryResources && entry.IsDir() || !entry.IsDir() && !isManifestFile(entry.Name()) {
			continue
		}
		if !w.walkEntry(ctx, visit, directoryEntry{abs: abs, entry: entry}, yield) {
			return false
		}
	}
	return true
}

func (w *traversal) walkEntry(
	ctx context.Context,
	visit *graphVisit,
	input directoryEntry,
	yield func(graphVisit) bool,
) bool {
	child := *visit
	child.deferMaterialization = true
	if w.policy == traversalAdmission {
		child.file = filepath.Join(input.abs, input.entry.Name())
	} else {
		child.dir = path.Join(visit.dir, input.entry.Name())
	}
	if w.policy == traversalAdmission && !input.entry.IsDir() {
		return w.admitFile(ctx, visit.root, child.file, false)
	}
	count := w.count
	if !w.walk(ctx, child, yield) {
		if !w.canceled {
			w.count = count
		}
		return false
	}
	return true
}

func (w *traversal) opaqueConfiguration(
	ctx context.Context,
	visit *graphVisit,
	cached *document,
	yield func(graphVisit) bool,
) bool {
	visit.readable = cached.pathState != pathMissing && !visit.deferDecode
	if visit.role != kustomizeinput.Configuration {
		return yield(*visit)
	}
	if cached.plugins == nil {
		var plugins []pluginInputs
		input, name := new(w.documents).input(visit.file)
		body, readErr := fs.ReadFile(input, name)
		resources, parseErr := kustomizeinput.Resources(body)
		if readErr == nil && parseErr == nil {
			for _, resource := range resources {
				refs, known := kustomizeinput.Plugin(resource.YNode())
				plugins = append(
					plugins,
					pluginInputs{
						references:   refs,
						known:        known,
						metadataOnly: kustomizeinput.MetadataOnly(resource.YNode()),
					},
				)
			}
		}
		cached.plugins = &plugins
		w.documents.cache[visit.file] = *cached
	}
	if !yield(*visit) {
		return false
	}
	if len(*cached.plugins) == 0 {
		return w.uncertain(new(*visit), "configuration content is unavailable or opaque", yield)
	}
	for _, config := range *cached.plugins {
		if !w.configuration(ctx, new(*visit), config, yield) {
			return false
		}
	}
	return true
}

func (w *traversal) publishScope(
	visit *graphVisit,
	body **types.Kustomization,
	input walkDocument,
	yield func(graphVisit) bool,
) bool {
	visit.file, visit.document, visit.readable = input.file, *body, true
	visit.coordinate = path.Join(visit.prefix, visit.dir, filepath.Base(input.file))
	if !yield(*visit) {
		return false
	}
	*body = w.documents.cache[input.abs].body
	if visit.role == kustomizeinput.Configuration && !kustomizeinput.PreservesOperands(*body) {
		if !w.uncertain(new(*visit), "configuration transformations may rewrite plugin file operands", yield) {
			return false
		}
	}
	return true
}

func (w *traversal) prepareDirectory(visit *graphVisit, cached *document, abs string) bool {
	if w.policy == traversalAdmission {
		if w.shouldSkipDir(visit.root, filepath.Base(abs), abs) {
			return true
		}
		cached.adHoc = true
		w.documents.cache[abs] = *cached
	}
	return false
}
