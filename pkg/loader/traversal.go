package loader

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
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
	files       *[]fileAdmission
	documents   documentReader
	policy      traversalPolicy
	visited     map[graphContext]struct{}
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

func (w *traversal) walk(ctx context.Context, v graphVisit, yield func(graphVisit) bool) bool {
	if !w.proceed(ctx) {
		return false
	}
	abs := v.file
	if w.policy != traversalAdmission {
		abs = filepath.Join(v.root, v.dir)
	}
	switch w.policy {
	case traversalScope:
		if _, ok := kustpath.Resolve("", v.dir, kustpath.Root); !ok || !safepath.Contains(v.root, abs) || w.documents.mountedRoot(abs) != w.documents.mountedRoot(v.root) {
			return true
		}
		v.file, v.coordinate, v.document = abs, path.Join(v.prefix, v.dir), nil
		if v.role == kustomizeinput.File {
			return yield(v)
		}
	case traversalAdmission:
		if v.deferMaterialization {
			if w.shouldSkipDir(v.root, filepath.Base(abs), abs) {
				return true
			}
			cached := w.documents.cache[abs]
			cached.adHoc = true
			w.documents.cache[abs] = cached
		} else {
			if _, seen := w.directories[abs]; seen {
				return true
			}
			w.directories[abs] = struct{}{}
		}
	case traversalProof:
		if w.depth > transformerScanMaxDepth {
			return true
		}
		if _, seen := w.directories[v.dir]; seen {
			return true
		}
		w.directories[v.dir] = struct{}{}
	}
	d, file := w.documents.read(abs)
	cached := w.documents.cache[abs]
	if !w.proceed(ctx) {
		return false
	}
	switch w.policy {
	case traversalScope:
		if d != nil || cached.pathState == pathDirectory {
			if _, seen := w.visited[v.graphContext]; seen {
				return true
			}
			w.visited[v.graphContext] = struct{}{}
		}
	case traversalAdmission:
		if v.role == kustomizeinput.Component && !isKustomizeComponent(d) {
			return true
		}
		if file != "" {
			if v.deferMaterialization {
				if _, seen := w.directories[abs]; seen {
					return true
				}
				w.directories[abs] = struct{}{}
			}
			cached.traversed = true
			w.documents.cache[abs] = cached
			if d == nil {
				return true
			}
			if isKustomizeComponent(d) {
				if v.role != kustomizeinput.Component {
					slog.Debug("loader: skipping kustomize Component directory", "dir", abs)
				}
				v.role = kustomizeinput.Component
			} else if !w.ignoreMatches(v.root, file, false) {
				if w.files != nil {
					*w.files = append(*w.files, fileAdmission{file: file, kind: admissionPackage})
				}
				n, err := w.loader.loadFile(ctx, file)
				w.count += n
				if err != nil {
					w.loader.Store.AddWarning(manifest.Warning{
						Category: manifest.WarnInputParseFailed, Message: "kustomization file failed to parse", Detail: []string{file, err.Error()},
					})
				}
			}
		}
	case traversalProof:
		if d == nil {
			return true
		}
	}
	if d == nil && (w.policy == traversalAdmission || cached.pathState == pathDirectory) {
		if w.policy == traversalAdmission {
			if w.shouldSkipDir(v.root, filepath.Base(abs), abs) {
				return true
			}
			cached.adHoc = true
			w.documents.cache[abs] = cached
		}
		if !w.proceed(ctx) {
			return false
		}
		input, name := w.documents.input(abs)
		entries, err := fs.ReadDir(input, name)
		if err != nil && w.policy == traversalAdmission {
			w.err = err
			return false
		}
		for _, entry := range entries {
			if !w.proceed(ctx) {
				return false
			}
			if v.entryResources && entry.IsDir() || !entry.IsDir() && !isManifestFile(entry.Name()) {
				continue
			}
			child := v
			child.deferMaterialization = true
			if w.policy == traversalAdmission {
				child.file = filepath.Join(abs, entry.Name())
			} else {
				child.dir = path.Join(v.dir, entry.Name())
			}
			if w.policy == traversalAdmission && !entry.IsDir() {
				if !w.admitFile(ctx, v.root, child.file, false) {
					return false
				}
			} else {
				count := w.count
				if !w.walk(ctx, child, yield) {
					if !w.canceled {
						w.count = count
					}
					return false
				}
			}
		}
		return true
	}
	if w.policy == traversalScope {
		if d == nil {
			v.readable = cached.pathState != pathMissing && !v.deferDecode
			if v.role != kustomizeinput.Configuration {
				return yield(v)
			}
			if cached.plugins == nil {
				var plugins []pluginInputs
				input, name := w.documents.input(v.file)
				body, readErr := fs.ReadFile(input, name)
				resources, parseErr := kustomizeinput.Resources(body)
				if readErr == nil && parseErr == nil {
					for _, resource := range resources {
						refs, known := kustomizeinput.Plugin(resource.YNode())
						plugins = append(plugins, pluginInputs{references: refs, known: known, metadataOnly: kustomizeinput.MetadataOnly(resource.YNode())})
					}
				}
				cached.plugins = &plugins
				w.documents.cache[v.file] = cached
			}
			if !yield(v) {
				return false
			}
			if len(*cached.plugins) == 0 {
				return w.uncertain(v, "configuration content is unavailable or opaque", yield)
			}
			for _, config := range *cached.plugins {
				if !w.configuration(ctx, v, config, yield) {
					return false
				}
			}
			return true
		}
		v.deferDecode = false
		v.namespace = cmp.Or(v.namespace, d.Namespace)
		v.namePrefix += d.NamePrefix
		v.nameSuffix = d.NameSuffix + v.nameSuffix
		if v.namespace == "" {
			v.namespace = w.documents.transformerInjectedNamespace(v.root, v.dir).ns
		}
		v.file, v.document, v.readable = file, d, true
		v.coordinate = path.Join(v.prefix, v.dir, filepath.Base(file))
		if !yield(v) {
			return false
		}
		d = w.documents.cache[abs].body
		if v.role == kustomizeinput.Configuration && !kustomizeinput.PreservesOperands(d) {
			if !w.uncertain(v, "configuration transformations may rewrite plugin file operands", yield) {
				return false
			}
		}
	}
	if !w.documents.references(abs, d, w.policy == traversalAdmission, func(ref kustomizeinput.Reference, target string) bool {
		return w.reference(ctx, v, traversalReference{Reference: ref, target: target}, yield)
	}) {
		return false
	}
	if w.policy == traversalAdmission && v.role != kustomizeinput.Component && !w.build {
		w.count += w.scanBootstrapFluxKS(ctx, v.root, abs, file)
	}
	return w.proceed(ctx)
}

func (r documentReader) references(dir string, body *types.Kustomization, resolve bool, yield func(kustomizeinput.Reference, string) bool) bool {
	doc := r.cache[dir]
	record := !doc.edgesKnown && doc.traversed && body != nil && r.mountedRoot(dir) == r.mountedRoot(r.bootstrap)
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
				Category: manifest.WarnInputParseFailed, Message: "component data file failed to parse", Detail: []string{file, err.Error()},
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

func (w *traversal) reference(ctx context.Context, v graphVisit, ref traversalReference, yield func(graphVisit) bool) bool {
	if !w.proceed(ctx) {
		return false
	}
	child := v
	child.document, child.readable, child.incomplete = nil, false, ""
	switch w.policy {
	case traversalAdmission:
		if ref.Role != kustomizeinput.Resource && ref.Role != kustomizeinput.Component {
			return true
		}
		base, resolved := v.file, ref.target
		if resolved == "" || filepath.IsAbs(ref.Path) {
			return true
		}
		input, name := w.documents.input(resolved)
		info, err := fs.Stat(input, name)
		w.documents.rememberStat(resolved, info, err)
		if err != nil {
			return w.proceed(ctx)
		}
		if !info.IsDir() {
			if _, ok := kustpath.Resolve(base, ref.Path, kustpath.File); !ok || ref.Role != kustomizeinput.Resource {
				return true
			}
			return w.admitFile(ctx, v.root, resolved, v.role == kustomizeinput.Component)
		}
		child.role = kustomizeinput.Resource
		if v.role == kustomizeinput.Component {
			if ref.Role != kustomizeinput.Component {
				return true
			}
			child.role = kustomizeinput.Component
		}
		child.file, child.deferMaterialization = resolved, false
	case traversalProof:
		if ref.Role != kustomizeinput.Resource && (ref.Role != kustomizeinput.Configuration || !strings.HasPrefix(ref.Field, ".transformers")) || ref.Inline {
			return true
		}
		target, ok := kustomizeinput.Local(v.root, v.dir, ref.Path)
		if !ok {
			return true
		}
		if w.documents.fileHasNamespaceTransformer(v.root, target) {
			return false
		}
		child.dir = target
	case traversalScope:
		if ref.Role == kustomizeinput.Configuration {
			child.role, child.pluginRoot = kustomizeinput.Configuration, v.dir
			child.checkMetadata = v.role == kustomizeinput.Configuration && strings.HasPrefix(ref.Field, ".transformers")
			if ref.Inline {
				for _, resource := range ref.Resources {
					refs, known := kustomizeinput.Plugin(resource.YNode())
					if !w.configuration(ctx, child, pluginInputs{references: refs, known: known, metadataOnly: kustomizeinput.MetadataOnly(resource.YNode())}, yield) {
						return false
					}
				}
				return true
			}
		} else if ref.Role == kustomizeinput.File {
			child.role = kustomizeinput.File
		} else if v.role != kustomizeinput.Configuration {
			child.role = kustomizeinput.Resource
		}
		resolved, ok := kustpath.Resolve(filepath.Join(v.root, v.dir), ref.Path, kustpath.Dependency)
		if !ok || !safepath.Contains(v.root, resolved) || w.documents.mountedRoot(resolved) != w.documents.mountedRoot(v.root) {
			if child.role == kustomizeinput.Configuration {
				return w.uncertain(v, ref.FieldPath()+": configuration reference cannot be inspected locally", yield)
			}
			return true
		}
		rel, _ := filepath.Rel(v.root, resolved)
		child.dir = filepath.ToSlash(rel)
		if child.role == kustomizeinput.Resource {
			_, admitted := kustpath.Resolve(filepath.Join(v.root, v.dir), ref.Path, kustpath.File)
			child.deferDecode = !admitted || filepath.IsAbs(ref.Path)
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

func (w *traversal) configuration(ctx context.Context, v graphVisit, config pluginInputs, yield func(graphVisit) bool) bool {
	if !config.known {
		if !w.uncertain(v, "unknown plugin configuration may read additional files", yield) {
			return false
		}
	}
	if v.checkMetadata && !config.metadataOnly {
		if !w.uncertain(v, "configuration transformer may rewrite plugin file operands", yield) {
			return false
		}
	}
	for _, ref := range config.references {
		child := v
		child.dir = v.pluginRoot
		if !w.reference(ctx, child, traversalReference{Reference: ref}, yield) {
			return false
		}
	}
	return true
}

func (w *traversal) uncertain(v graphVisit, reason string, yield func(graphVisit) bool) bool {
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
	spec, _ := w.documents.read(dir)
	for ref := range kustomizeinput.Kustomization(spec) {
		if ref.Inline {
			continue
		}
		if abs, ok := kustpath.Resolve(dir, ref.Path, kustpath.Dependency); ok {
			seen[abs] = struct{}{}
		}
	}
	if !w.proceed(ctx) {
		return 0
	}
	input, name := w.documents.input(dir)
	entries, err := fs.ReadDir(input, name)
	if err != nil {
		warning := manifest.Warning{Category: manifest.WarnInputReadFailed, Message: "bootstrap-sibling readdir failed", Detail: []string{dir, err.Error()}}
		if w.files != nil {
			*w.files = append(*w.files, fileAdmission{kind: admissionWarning, warning: warning})
		}
		w.loader.Store.AddWarning(warning)
		return 0
	}
	count := 0
	for _, e := range entries {
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
		objs, err := reader.parseFile(ctx, abs, manifest.ParseDocOptions{WipeSecrets: w.loader.Options.WipeSecrets, DefaultNamespace: ""})
		if err != nil {
			warning := manifest.Warning{Category: manifest.WarnInputParseFailed, Message: "bootstrap-sibling parse failed", Detail: []string{abs, err.Error()}}
			if w.files != nil {
				*w.files = append(*w.files, fileAdmission{kind: admissionWarning, warning: warning})
			}
			w.loader.Store.AddWarning(warning)
			continue
		}
		for _, child := range objs {
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
			if w.files != nil {
				for _, fact := range w.documents.cache[abs].objects {
					if fact.fact != nil && fact.Obj == obj {
						*w.files = append(*w.files, fileAdmission{file: abs, kind: admissionBootstrap, coordinate: fact.fact.Declaration})
					}
				}
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
