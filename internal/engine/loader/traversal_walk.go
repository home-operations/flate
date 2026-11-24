package loader

import (
	"cmp"
	"context"
	"io/fs"
	"log/slog"
	"path"
	"path/filepath"
	"slices"

	"helm.sh/helm/v4/pkg/chart/loader/archive"
	chartloader "helm.sh/helm/v4/pkg/chart/v2/loader"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:gocritic // Recursive visit snapshots MUST remain on the stack.
func (w *traversal) walk(ctx context.Context, v graphVisit, yield func(graphVisit) bool) bool {
	if !w.proceed(ctx) {
		return false
	}
	w.work.Add(&manifest.WorkCounts{FileEntries: 1})
	abs := v.file
	if w.policy != traversalAdmission {
		abs = filepath.Join(v.root, v.dir)
	}
	if w.scopeInputs != nil {
		*w.scopeInputs = append(*w.scopeInputs, walkInput{path: abs})
	}
	if decision := w.prepareVisit(&v, abs, yield); decision.done {
		return decision.more
	}
	builder := w.documents.read(abs)
	cached := w.documents.cache[abs]
	input := walkDocument{abs: abs, builderDocument: builder, cached: &cached}
	if !w.proceed(ctx) {
		return false
	}
	if w.qualify && v.enclosing == "" && input.file != "" {
		if isKustomizeComponent(input.body) {
			return true
		}
		v.enclosing = v.dir
	}
	if w.acceptDocument(ctx, &v, input) {
		if w.qualify && v.enclosing != "" {
			v.requiresBuild = true
			return yield(v)
		}
		return true
	}
	return w.walkInputs(ctx, &v, input, yield)
}

func (w *traversal) walkInputs(
	ctx context.Context,
	visit *graphVisit,
	input walkDocument,
	yield func(graphVisit) bool,
) bool {
	enclosingPackage := visit.enclosing != "" && input.cached.pathState != pathFile
	unreadable := input.file != "" || enclosingPackage
	if w.qualify && input.body == nil && unreadable {
		visit.requiresBuild = true
		return yield(*visit)
	}
	if input.body == nil && (w.policy == traversalAdmission || input.cached.pathState == pathDirectory) {
		if w.prepareDirectory(visit, input.cached, input.abs) {
			return true
		}
		return w.walkDirectory(ctx, visit, input.abs, yield)
	}
	if w.policy == traversalScope {
		if input.body == nil {
			visit.requiresBuild = visit.requiresBuild || input.file != ""
			return w.opaqueConfiguration(ctx, visit, input.cached, yield)
		}
		switch {
		case !w.publishScope(visit, &input, yield):
			return false
		case w.qualify && visit.requiresBuild:
			return true
		}
	}
	if !w.documents.references(
		input.abs,
		input.body,
		w.policy == traversalAdmission,
		func(ref kustomizeinput.Reference, target string) bool {
			w.work.Add(&manifest.WorkCounts{ReaderEdges: 1})
			return w.reference(ctx, new(*visit), new(traversalReference{Reference: ref, target: target}), yield)
		},
	) {
		return false
	}
	if w.policy == traversalAdmission && visit.role != kustomizeinput.Component && !w.build {
		w.count += w.scanBootstrapFluxKS(ctx, visit.root, input.abs, input.file)
	}
	return w.proceed(ctx)
}

func (w *traversal) seenScopeVisit(visit *graphVisit) bool {
	if slices.Contains(w.visitBuf[:w.visitCount], visit.graphContext) {
		w.scopeSkipped++
		return true
	}
	if _, seen := w.visited[visit.graphContext]; seen {
		w.scopeSkipped++
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
	builderDocument

	abs    string
	cached *document
}

func (w *traversal) admitDocument(ctx context.Context, visit *graphVisit, input walkDocument) bool {
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
	input.cached.traversed = true
	w.documents.cache[input.abs] = *input.cached
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
		n, err := w.loader.loadFile(ctx, input.file, &w.documents)
		w.count += n
		if err != nil {
			w.loader.inputWarning(manifest.Warning{
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

func (w *traversal) prepareVisit(visit *graphVisit, abs string, yield func(graphVisit) bool) traversalDecision {
	switch w.policy {
	case traversalScope:
		_, validPath := kustpath.Resolve("", visit.dir, kustpath.Root)
		insideMount := validPath && safepath.Contains(visit.root, abs) &&
			new(w.documents).mountedRoot(abs) == new(w.documents).mountedRoot(visit.root)
		excluded := w.qualify && visit.enclosing == "" && safepath.Contains(w.qualificationEntry, abs)
		if !insideMount || excluded {
			return traversalDecision{done: true, more: true}
		}
		visit.file, visit.coordinate, visit.document = abs, path.Join(visit.prefix, visit.dir), nil
		if visit.role == kustomizeinput.File {
			return traversalDecision{done: true, more: yield(*visit)}
		}
	case traversalAdmission:
		switch {
		case visit.deferMaterialization:
			if w.shouldSkipDir(visit.root, abs) {
				return traversalDecision{done: true, more: true}
			}
			cached := w.documents.cache[abs]
			cached.adHoc = true
			w.documents.cache[abs] = cached
		default:
			if _, seen := w.directories[abs]; seen {
				return traversalDecision{done: true, more: true}
			}
			w.directories[abs] = struct{}{}
		}
	case traversalProof:
		if w.depth > transformerScanMaxDepth {
			return traversalDecision{done: true, more: true}
		}
		if _, seen := w.directories[visit.dir]; seen {
			return traversalDecision{done: true, more: true}
		}
		w.directories[visit.dir] = struct{}{}
	}
	return traversalDecision{}
}

func (w *traversal) acceptDocument(ctx context.Context, visit *graphVisit, input walkDocument) bool {
	switch w.policy {
	case traversalScope:
		if input.body != nil || input.cached.pathState == pathDirectory {
			if w.seenScopeVisit(visit) {
				return true
			}
		}
	case traversalAdmission:
		if done := w.admitDocument(ctx, visit, input); done {
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
	inputFile := new(w.documents).input(abs)
	if w.scopeInputs != nil {
		*w.scopeInputs = append(*w.scopeInputs, walkInput{path: abs, listed: true})
	}
	entries, err := fs.ReadDir(inputFile.fs, inputFile.name)
	if err != nil && w.policy == traversalAdmission {
		w.err = err
		return false
	}
	if w.scopeVisits != nil && visit.role == kustomizeinput.Resource &&
		(err != nil || w.documents.cache[abs].file != "") {
		w.unknownRecipes++
	}
	chart := w.policy == traversalAdmission && hasChartMetadata(inputFile.fs, inputFile.name, entries)
	for _, entry := range entries {
		w.work.Add(&manifest.WorkCounts{FileEntries: 1})
		if !w.proceed(ctx) {
			return false
		}
		isDir, name := entry.IsDir(), entry.Name()
		skipDir := isDir && (visit.entryResources || chartScanSkips(chart, name))
		if skipDir || !isDir && !isManifestFile(name) {
			continue
		}
		if !w.walkEntry(ctx, visit, directoryEntry{abs: abs, entry: entry}, yield) {
			return false
		}
	}
	return true
}

func hasChartMetadata(input fs.FS, dir string, entries []fs.DirEntry) bool {
	const name = "Chart.yaml"
	found := slices.ContainsFunc(entries, func(entry fs.DirEntry) bool {
		return entry.Name() == name && entry.Type().IsRegular()
	})
	if !found {
		return false
	}
	body, err := fs.ReadFile(input, path.Join(dir, name))
	if err != nil {
		return false
	}
	_, err = chartloader.LoadFiles([]*archive.BufferedFile{{Name: name, Data: body}})
	return err == nil
}

// Helm owns chart templates and CRDs, so the Flux manifest scan leaves them to chart rendering.
func chartScanSkips(chart bool, name string) bool {
	return chart && (name == "templates" || name == "crds")
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
		if w.scopeVisits != nil && (cached.pathState == pathMissing || cached.pathState == pathUnavailable) {
			w.unknownRecipes++
		}
		return yield(*visit)
	}
	if cached.plugins == nil {
		var plugins []pluginInputs
		inputFile2 := new(w.documents).input(visit.file)
		body, readErr := fs.ReadFile(inputFile2.fs, inputFile2.name)
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
	input *walkDocument,
	yield func(graphVisit) bool,
) bool {
	d := input.body
	visit.deferDecode = false
	visit.namespace = cmp.Or(visit.namespace, d.Namespace)
	visit.namePrefix += d.NamePrefix
	visit.nameSuffix = d.NameSuffix + visit.nameSuffix
	if w.qualify {
		visit.requiresBuild = visit.requiresBuild || input.recipe&recipeResourceOnly == 0
	} else {
		// Authored scope must retain projected declarations and reader edges before construction.
		visit.requiresBuild = visit.requiresBuild ||
			input.recipe&(recipeOpaquePluginInputs|recipeConfigurations|recipeTransformers) != 0
	}
	if !w.qualify && visit.namespace == "" {
		visit.namespace = new(w.documents).transformerInjectedNamespace(visit.root, visit.dir, w.scopeInputs).ns
	}
	visit.file, visit.document, visit.readable = input.file, input.body, true
	visit.coordinate = path.Join(visit.prefix, visit.dir, filepath.Base(input.file))
	if !yield(*visit) {
		return false
	}
	input.body = w.documents.cache[input.abs].body
	if visit.role == kustomizeinput.Configuration && input.recipe&(recipeOpaquePluginInputs|recipeConfigurations) != 0 {
		if !w.uncertain(new(*visit), "configuration transformations may rewrite plugin file operands", yield) {
			return false
		}
	}
	return true
}

func (w *traversal) prepareDirectory(visit *graphVisit, cached *document, abs string) bool {
	if w.policy == traversalAdmission {
		if w.shouldSkipDir(visit.root, abs) {
			return true
		}
		cached.adHoc = true
		w.documents.cache[abs] = *cached
	}
	return false
}
