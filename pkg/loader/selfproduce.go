package loader

import (
	"cmp"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) buildProductions(repoRoot string, emittedFiles map[string][]manifest.NamedResource) {
	if repoRoot == "" {
		return
	}
	b := &selfProduceBuilder{idx: l.Index,
		emittedFiles: emittedFiles,
		store:        l.Store, wipeSecrets: l.Options.WipeSecrets, reader: l.documents,
		secrets: map[manifest.NamedResource]*manifest.Secret{}}
	var generators []generatorRecord
	incomplete := map[manifest.NamedResource][]string{}
	remaining := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if doc.body != nil && len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) != 0 {
			remaining[dir] = struct{}{}
		}
	}
	record := func(v graphVisit) {
		if v.owner != (manifest.NamedResource{}) && v.coordinate != "" {
			l.recordReader(v.coordinate, v.localCoordinate(), v.owner)
		}
		if v.incomplete != "" {
			if v.owner != (manifest.NamedResource{}) {
				incomplete[v.owner] = append(incomplete[v.owner], v.source.String()+":"+v.coordinate+": "+v.incomplete)
			}
			return
		}
		if v.document != nil {
			delete(remaining, filepath.Dir(v.file))
		}
		if v.role != kustomizeinput.Resource {
			return
		}
		if v.document != nil {
			for _, g := range v.document.ConfigMapGenerator {
				generators = append(generators, generatorRecord{root: v.root, file: v.file, namespace: cmp.Or(v.namespace, g.Namespace, v.fallback), isConfigMap: true, args: g.GeneratorArgs, owner: v.owner, reader: l.documents})
			}
			for _, g := range v.document.SecretGenerator {
				generators = append(generators, generatorRecord{root: v.root, file: v.file, namespace: cmp.Or(v.namespace, g.Namespace, v.fallback), args: g.GeneratorArgs, owner: v.owner, reader: l.documents})
			}
			return
		}
		if v.readable {
			b.recordProduced(v, l.documents.cache[v.file].objects)
		}
	}
	for _, contribution := range l.scopes.contributions {
		for _, v := range contribution.visits {
			v.owner, v.source = contribution.owner, contribution.source
			record(v)
		}
	}
	if len(incomplete) != 0 {
		for _, owner := range slices.SortedFunc(maps.Keys(incomplete), manifest.NamedResource.Compare) {
			details := incomplete[owner]
			slices.Sort(details)
			details = slices.Compact(details)
			l.Store.AddWarning(owner, manifest.Warning{Resource: owner, Category: manifest.WarnIncompleteInputCoverage,
				Message: "changed-only input coverage is incomplete; run flate build all without --base or --path-orig to validate the full tree", Detail: details})
		}
	}

	if len(remaining) != 0 {
		for _, dir := range l.unreachedGeneratorRoots(repoRoot, remaining) {
			root := graphVisit{root: repoRoot, dir: dir}
			for _, v := range l.visitScope(root) {
				v.owner = manifest.NamedResource{}
				record(v)
			}
		}
	}
	l.addGenerated(generators)
	for _, id := range slices.SortedFunc(maps.Keys(b.secrets), manifest.NamedResource.Compare) {
		l.Store.AddObject(b.secrets[id])
	}
}

func (l *Loader) unreachedGeneratorRoots(repoRoot string, remaining map[string]struct{}) []string {
	var roots []string
	for _, dir := range slices.Sorted(maps.Keys(remaining)) {
		rel, err := filepath.Rel(repoRoot, l.documents.cache[dir].file)
		if err != nil {
			continue
		}
		owners, _ := l.Index.OwnersOfFile(filepath.ToSlash(rel))
		if slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool {
			ks, ok := l.Store.Get[*manifest.Kustomization](owner)
			if !ok {
				return false
			}
			_, pinned, _ := SourceView(l.Store, ks.SourceID(), repoRoot)
			return pinned
		}) {
			continue
		}
		roots = append(roots, filepath.ToSlash(filepath.Dir(rel)))
	}
	return roots
}

func (l *Loader) addGenerated(generators []generatorRecord) {
	for _, r := range generators {
		// Generator names are indexed before suffix hashing, including failed inputs.
		id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: r.namespace, Name: r.args.Name}
		if r.isConfigMap {
			id.Kind = manifest.KindConfigMap
		}
		record := l.Index.Records[id]
		if record.File == "" {
			record.File = l.fileCoordinate(r.file)
		}
		if r.owner != (manifest.NamedResource{}) {
			p := manifest.Production{Owner: r.owner, File: record.File}
			if ks, ok := l.Store.Get[*manifest.Kustomization](r.owner); ok {
				p.Source = ks.SourceID()
				if p.Source.Name == "" || p.Source.Kind == "" {
					p.Source = manifest.BootstrapSourceID
				}
			}
			if !slices.Contains(record.Productions, p) {
				record.Productions = append(record.Productions, p)
			}
		}
		l.Index.Records[id] = record
		obj, err := r.materialize(l.Options.WipeSecrets)
		if err != nil {
			slog.Debug("loader: skipping generator input", "file", r.file, "generator", r.args.Name, "error", err)
			continue
		}
		if l.Store.GetObject(id) == nil {
			l.Store.AddObject(obj)
		}
	}
}

type graphContext struct {
	root, dir, namespace, fallback string
	pluginRoot                     string
	role                           kustomizeinput.Role
	deferMaterialization           bool
	checkMetadata                  bool
	deferDecode                    bool
}

type graphVisit struct {
	graphContext
	file               string
	prefix, coordinate string
	owner              manifest.NamedResource
	source             manifest.NamedResource
	localBase          string
	incomplete         string
	document           *types.Kustomization
	readable           bool
}

func (v graphVisit) localCoordinate() string {
	if v.localBase == "" {
		return ""
	}
	local := strings.TrimPrefix(v.coordinate, v.prefix+"/")
	if v.localBase == "." {
		return local
	}
	return path.Join(v.localBase, local)
}

func (l *Loader) standaloneRoots(repoRoot string, kss []*manifest.Kustomization) []string {
	roots := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if doc.body != nil {
			roots[dir] = struct{}{}
		}
	}
	for _, ks := range kss {
		root, pinned, _ := SourceView(l.Store, ks.SourceID(), repoRoot)
		base := l.documents.claimBase(root, ks.Path)
		delete(roots, filepath.Join(root, base))
		if pinned {
			local := filepath.Join(repoRoot, base)
			prefix := strings.TrimSuffix(local, string(filepath.Separator)) + string(filepath.Separator)
			for dir := range roots {
				if dir == local || strings.HasPrefix(dir, prefix) {
					delete(roots, dir)
				}
			}
		}
	}
	for dir, doc := range l.documents.cache {
		if doc.body == nil {
			continue
		}
		for ref := range kustomizeinput.Kustomization(doc.body) {
			if ref.Role == kustomizeinput.File || ref.Inline {
				continue
			}
			if child, ok := kustpath.Resolve(dir, ref.Path, kustpath.Dependency); ok {
				delete(roots, child)
			}
		}
	}
	var dirs []string
	for dir := range roots {
		rel, err := filepath.Rel(repoRoot, dir)
		if err == nil && !safepath.Escaped(rel) {
			dirs = append(dirs, filepath.ToSlash(rel))
		}
	}
	slices.Sort(dirs)
	return dirs
}

type graphWalker struct {
	documents documentReader
	visited   map[graphContext]struct{}
}

// An outer namespace stays locked across nested bases and components.
func (w *graphWalker) walk(v graphVisit, yield func(graphVisit) bool) bool {
	if _, ok := kustpath.Resolve("", v.dir, kustpath.Root); !ok {
		return true
	}
	abs := filepath.Join(v.root, v.dir)
	if !safepath.Contains(v.root, abs) || w.documents.mountedRoot(abs) != w.documents.mountedRoot(v.root) {
		return true
	}
	v.file, v.coordinate, v.document = abs, path.Join(v.prefix, v.dir), nil
	if v.role == kustomizeinput.File {
		return yield(v)
	}
	d, file := w.documents.read(abs)
	cached := w.documents.cache[abs]
	if d != nil || cached.pathState == pathDirectory {
		if _, seen := w.visited[v.graphContext]; seen {
			return true
		}
		w.visited[v.graphContext] = struct{}{}
	}
	if d == nil {
		if cached.pathState == pathDirectory {
			input, name := w.documents.input(abs)
			entries, _ := fs.ReadDir(input, name)
			for _, entry := range entries {
				if !entry.IsDir() && !isManifestFile(entry.Name()) {
					continue
				}
				child := v
				child.deferMaterialization = true
				child.dir = path.Join(v.dir, entry.Name())
				if !w.walk(child, yield) {
					return false
				}
			}
			return true
		}
		v.readable = cached.pathState != pathMissing && !v.deferDecode
		if v.role != kustomizeinput.Configuration {
			return yield(v)
		}
		if cached.plugins == nil {
			var plugins []pluginInputs
			input, name := w.documents.input(abs)
			body, readErr := fs.ReadFile(input, name)
			resources, parseErr := kustomizeinput.Resources(body)
			if readErr == nil && parseErr == nil {
				for _, resource := range resources {
					refs, known := kustomizeinput.Plugin(resource.YNode())
					plugins = append(plugins, pluginInputs{references: refs, known: known, metadataOnly: kustomizeinput.MetadataOnly(resource.YNode())})
				}
			}
			cached.plugins = &plugins
			w.documents.cache[abs] = cached
		}
		if !yield(v) {
			return false
		}
		if len(*cached.plugins) == 0 {
			return w.uncertain(v, "configuration content is unavailable or opaque", yield)
		}
		for _, config := range *cached.plugins {
			if !w.configuration(v, config, yield) {
				return false
			}
		}
		return true
	}
	v.deferDecode = false
	v.namespace = cmp.Or(v.namespace, d.Namespace)
	if v.namespace == "" {
		v.namespace = w.documents.transformerInjectedNamespace(v.root, v.dir).ns
	}
	v.file, v.document, v.readable = file, d, true
	v.coordinate = path.Join(v.prefix, v.dir, filepath.Base(file))
	if !yield(v) {
		return false
	}
	doc := w.documents.cache[abs]
	if v.role == kustomizeinput.Configuration && !kustomizeinput.PreservesOperands(doc.body) {
		if !w.uncertain(v, "configuration transformations may rewrite plugin file operands", yield) {
			return false
		}
	}
	for ref := range kustomizeinput.Kustomization(doc.body) {
		if !w.reference(v, ref, yield) {
			return false
		}
	}
	return true
}

func (w *graphWalker) reference(v graphVisit, ref kustomizeinput.Reference, yield func(graphVisit) bool) bool {
	child := v
	child.document, child.readable, child.incomplete = nil, false, ""
	base := v.dir
	if ref.Role == kustomizeinput.Configuration {
		child.role, child.pluginRoot = kustomizeinput.Configuration, v.dir
		child.checkMetadata = v.role == kustomizeinput.Configuration && strings.HasPrefix(ref.Field, ".transformers")
		if ref.Inline {
			for _, resource := range ref.Resources {
				refs, known := kustomizeinput.Plugin(resource.YNode())
				if !w.configuration(child, pluginInputs{references: refs, known: known, metadataOnly: kustomizeinput.MetadataOnly(resource.YNode())}, yield) {
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
	resolved, ok := kustpath.Resolve(filepath.Join(v.root, base), ref.Path, kustpath.Dependency)
	if !ok || !safepath.Contains(v.root, resolved) || w.documents.mountedRoot(resolved) != w.documents.mountedRoot(v.root) {
		if child.role == kustomizeinput.Configuration {
			return w.uncertain(v, ref.FieldPath()+": configuration reference cannot be inspected locally", yield)
		}
		return true
	}
	rel, _ := filepath.Rel(v.root, resolved)
	child.dir = filepath.ToSlash(rel)
	// Reader coordinates survive the stricter object admission boundary.
	if child.role == kustomizeinput.Resource {
		_, admitted := kustpath.Resolve(filepath.Join(v.root, base), ref.Path, kustpath.File)
		child.deferDecode = !admitted || filepath.IsAbs(ref.Path)
	}
	return w.walk(child, yield)
}

func (w *graphWalker) configuration(v graphVisit, config pluginInputs, yield func(graphVisit) bool) bool {
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
		if !w.reference(child, ref, yield) {
			return false
		}
	}
	return true
}

func (w *graphWalker) uncertain(v graphVisit, reason string, yield func(graphVisit) bool) bool {
	v.document, v.readable, v.incomplete = nil, false, reason
	return yield(v)
}

type selfProduceBuilder struct {
	reader       documentReader
	idx          *manifest.DiscoveryIndex
	emittedFiles map[string][]manifest.NamedResource
	store        *store.Store
	secrets      map[manifest.NamedResource]*manifest.Secret
	wipeSecrets  bool
}

func (b *selfProduceBuilder) recordProduced(v graphVisit, objects []manifest.BaseManifest) {
	coordinate := cmp.Or(v.localCoordinate(), v.coordinate)
	record := func(obj manifest.BaseManifest) {
		id := manifest.Resolve(obj.Named(), v.namespace, v.fallback)
		if owner, _ := b.idx.Lookup(v.owner); v.owner == obj.Named() || owner.Authored == obj.Named() {
			id = v.owner
		}
		r := b.idx.Records[id]
		if r.Authored == (manifest.NamedResource{}) {
			r.Authored, r.Path, r.File = obj.Named(), v.file, v.coordinate
		}
		p := manifest.Production{Owner: v.owner, Authored: obj.Named(), Path: v.file, File: v.coordinate, Source: v.source}
		if ks, ok := b.store.Get[*manifest.Kustomization](v.owner); ok {
			p.Source = ks.SourceID()
			if p.Source.Name == "" || p.Source.Kind == "" {
				p.Source = manifest.BootstrapSourceID
			}
		}
		if !slices.Contains(r.Productions, p) {
			r.Productions = append(r.Productions, p)
		}
		b.idx.Records[id] = r
		if !v.deferMaterialization {
			b.recordProducer(obj, v.namespace, v.fallback, v.owner)
		}
		info := manifest.KindOf(obj)
		switch {
		case info.Data:
			if s, ok := obj.(*manifest.Secret); ok && !v.deferMaterialization {
				b.materializeSecret(s, v.namespace, v.fallback)
			}
		case info.Structural:
			b.emittedFiles[coordinate] = appendUniqueProducer(b.emittedFiles[coordinate], v.owner)
		}
	}
	if objects != nil {
		for _, obj := range objects {
			record(obj)
		}
		return
	}
	_ = b.reader.decodeFileObjects(v.file, manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets, DefaultNamespace: ""}, true, record)
}

// materializeSecret retains SOPS stand-ins under the rendered namespace so
// self-produced substituteFrom inputs are available before reconciliation.
// Real Store objects win; the last SOPS Secret replaces earlier stand-ins.
func (b *selfProduceBuilder) materializeSecret(s *manifest.Secret, baseNS, rootNS string) {
	ns := manifest.Resolve(s.Named(), baseNS, rootNS).Namespace
	if !b.wipeSecrets || b.store == nil || ns == "" {
		return
	}
	if !s.HasPlaceholder() {
		return
	}
	c := *s
	c.Namespace = ns
	if b.store.GetObject(c.Named()) == nil {
		b.secrets[c.Named()] = &c
	}
}

func (b *selfProduceBuilder) recordProducer(raw manifest.BaseManifest, baseNS, rootNS string, ks manifest.NamedResource) {
	for _, target := range manifest.ProducerTargets(raw) {
		target.NamedResource = manifest.Resolve(target.NamedResource, baseNS, rootNS)
		if target.Namespace == "" {
			continue
		}
		producer := raw.Named()
		projected := manifest.Resolve(producer, baseNS, rootNS)
		r := b.idx.Records[target.NamedResource]
		p := manifest.Production{Owner: ks, Declaration: projected, Authored: producer, File: b.idx.Records[projected].File}
		if owner, ok := b.store.Get[*manifest.Kustomization](ks); ok {
			p.Source = owner.SourceID()
			if p.Source.Name == "" || p.Source.Kind == "" {
				p.Source = manifest.BootstrapSourceID
			}
		}
		if !slices.Contains(r.Productions, p) {
			r.Productions = append(r.Productions, p)
		}
		b.idx.Records[target.NamedResource] = r
		for _, producer := range []manifest.NamedResource{producer, projected} {
			if ks == (manifest.NamedResource{}) {
				continue
			}
			r := b.idx.Records[producer]
			p := manifest.Production{Owner: ks, Declaration: projected}
			if owner, ok := b.store.Get[*manifest.Kustomization](ks); ok {
				p.Source = owner.SourceID()
				if p.Source.Name == "" || p.Source.Kind == "" {
					p.Source = manifest.BootstrapSourceID
				}
			}
			if !slices.Contains(r.Productions, p) {
				r.Productions = append(r.Productions, p)
			}
			b.idx.Records[producer] = r
		}
		if len(target.DeclaredKeys) > 0 && target.Kind == manifest.KindSecret {
			b.synthesizePlaceholderSecret(target.NamedResource, target.DeclaredKeys)
		}
	}
}

// synthesizePlaceholderSecret retains declared keys for offline substitution.
// Existing Store objects and collected stand-ins take precedence.
func (b *selfProduceBuilder) synthesizePlaceholderSecret(target manifest.NamedResource, keys []string) {
	if !b.wipeSecrets || b.store == nil || b.store.GetObject(target) != nil || b.secrets[target] != nil {
		return
	}
	stringData := make(map[string]any, len(keys))
	for _, k := range keys {
		stringData[k] = fmt.Sprintf(manifest.ValuePlaceholderTemplate, k)
	}
	b.secrets[target] = &manifest.Secret{Name: target.Name, Namespace: target.Namespace, StringData: stringData}
}

func appendUniqueProducer(dst []manifest.NamedResource, v manifest.NamedResource) []manifest.NamedResource {
	if slices.Contains(dst, v) {
		return dst
	}
	return append(dst, v)
}
