package loader

import (
	"cmp"
	"context"
	"errors"
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

func (l *Loader) buildProductions(ctx context.Context, repoRoot string, emittedFiles map[string][]manifest.NamedResource) error {
	if repoRoot == "" {
		return nil
	}
	b := &selfProduceBuilder{idx: l.Index, store: l.Store, wipeSecrets: l.Options.WipeSecrets, reader: l.documents,
		secrets: map[manifest.NamedResource]*manifest.Secret{}, emittedFiles: emittedFiles}
	var generators []generatorRecord
	incomplete := map[manifest.NamedResource][]string{}
	var declarations []producerDeclaration
	remaining := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if doc.body != nil && !doc.selected && len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) != 0 {
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
		if v.role != kustomizeinput.Resource || l.documents.cache[v.file].selected {
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
			declarations = b.recordProduced(v, l.documents.cache[v.file].objects, declarations)
		}
	}
	var builtDirs []string
	for dir, doc := range l.documents.cache {
		if doc.built != nil {
			builtDirs = append(builtDirs, dir)
		}
	}
	slices.Sort(builtDirs)
	for _, dir := range builtDirs {
		doc := l.documents.cache[dir]
		rel, _ := filepath.Rel(repoRoot, dir)
		root := graphVisit{root: repoRoot, dir: filepath.ToSlash(rel)}
		var candidates []manifest.NamedResource
		for _, child := range doc.built {
			obj := child.Obj
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
		slices.SortFunc(candidates, manifest.NamedResource.Compare)
		candidates = slices.Compact(candidates)
		root.source = manifest.BootstrapSourceID
		_, builder := l.documents.read(dir)
		root.coordinate = l.fileCoordinate(builder)
		for _, child := range doc.built {
			obj := child.Obj
			if record, ok := l.Index.Lookup(obj.Named()); ok && record.Builder == root.coordinate {
				b.recordProducer(producerDeclaration{obj: obj, authored: obj.Named(), visit: root}, candidates)
			}
		}
	}
	for _, contribution := range l.scopes.contributions {
		if !contribution.active || contribution.membershipOnly {
			continue
		}
		for _, v := range contribution.visits {
			v.owner, v.source = contribution.owner, contribution.source
			record(v)
		}
		b.joinProductions(declarations, contribution.visits)
		declarations = nil
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
			walks := l.visitScope(root)
			for _, v := range walks {
				v.owner = manifest.NamedResource{}
				v.source = manifest.BootstrapSourceID
				record(v)
			}
			b.joinProductions(declarations, walks)
			declarations = nil
		}
	}
	var candidates map[string][]manifest.NamedResource
	for id, r := range l.Index.Files() {
		obj := l.Store.GetObject(id)
		if obj == nil || !manifest.KindOf(obj).Runnable || r.Path == "" {
			continue
		}
		if slices.ContainsFunc(r.Productions, func(p manifest.Production) bool { return p.Owner != (manifest.NamedResource{}) }) {
			continue
		}
		owners, ancestors := l.Index.OwnersOfFile(r.File)
		if slices.ContainsFunc(append(owners, ancestors...), func(owner manifest.NamedResource) bool { return owner != id }) {
			continue
		}
		if manifest.ProducerNeedsCandidates(obj) && candidates == nil {
			candidates = map[string][]manifest.NamedResource{}
			for _, input := range l.Index.Files() {
				root := l.documents.mountedRoot(input.Path)
				candidates[root] = append(candidates[root], input.Refs...)
			}
		}
		v := graphVisit{owner: id, source: manifest.BootstrapSourceID, coordinate: r.File}
		b.recordProducer(producerDeclaration{obj: obj, authored: r.Authored, visit: v}, candidates[l.documents.mountedRoot(r.Path)])
	}
	if err := l.addGenerated(ctx, generators); err != nil {
		return err
	}
	for _, id := range slices.SortedFunc(maps.Keys(b.secrets), manifest.NamedResource.Compare) {
		l.Store.AddObject(b.secrets[id])
	}
	return nil
}

func (l *Loader) unreachedGeneratorRoots(repoRoot string, remaining map[string]struct{}) []string {
	pinned := map[manifest.NamedResource]bool{}
	for _, child := range l.Kustomizations() {
		ks := child.Obj.(*manifest.Kustomization)
		_, pinned[ks.Named()], _ = SourceView(l.Store, ks.SourceID(), repoRoot)
	}
	var roots []string
	for _, dir := range slices.Sorted(maps.Keys(remaining)) {
		if !l.documents.cache[dir].traversed || l.documents.cache[l.documents.cache[dir].file].eligible < fileEligible {
			continue
		}
		rel, err := filepath.Rel(repoRoot, l.documents.cache[dir].file)
		if err != nil {
			continue
		}
		owners, _ := l.Index.OwnersOfFile(filepath.ToSlash(rel))
		if slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool { return pinned[owner] }) {
			continue
		}
		roots = append(roots, filepath.ToSlash(filepath.Dir(rel)))
	}
	return roots
}

func (l *Loader) addGenerated(ctx context.Context, generators []generatorRecord) error {
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
		obj, err := r.materialize(ctx, l.Options)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, errRemoteInputs) {
				return err
			}
			slog.Debug("loader: skipping generator input", "file", r.file, "generator", r.args.Name, "error", err)
			continue
		}
		if l.Store.GetObject(id) == nil {
			l.Store.AddObject(obj)
		}
	}
	return nil
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
	file, origin       string
	localFile          string
	prefix, coordinate string
	owner              manifest.NamedResource
	source             manifest.NamedResource
	input              int
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

// StandaloneRoots returns sorted, unbuilt bootstrap packages outside Flux ownership.
// Coordinates are borrowed until the next loader admission.
func (l *Loader) StandaloneRoots(repoRoot string) []string {
	l.Admit(repoRoot)
	l.scopes.roots = slices.DeleteFunc(l.scopes.roots, func(dir string) bool {
		root := filepath.Join(repoRoot, filepath.FromSlash(dir))
		doc := l.documents.cache[root]
		if doc.buildExcluded || doc.selected || doc.built != nil || isKustomizeComponent(doc.body) && root != l.entryPath {
			return true
		}
		if root == l.entryPath {
			for _, claim := range l.scopes.claims {
				if strings.HasPrefix(dir+"/", claim.Prefix) {
					return true
				}
			}
			return false
		}
		for _, visit := range l.scopeWalk(graphVisit{root: repoRoot, dir: dir}) {
			if visit.role == kustomizeinput.Resource && len(l.rootAdmission(visit.file, visit.coordinate, false).owners) != 0 {
				return true
			}
		}
		return false
	})
	return l.scopes.roots
}

func (l *Loader) packageRoots(repoRoot string, kss []store.Child) []string {
	if len(l.documents.cache) == 0 {
		return nil
	}
	l.ensureScopes()
	roots := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if (doc.traversed || dir == l.entryPath) && doc.file != "" {
			roots[dir] = struct{}{}
			if doc.buildExcluded {
				doc.buildExcluded = false
				l.documents.cache[dir] = doc
			}
		}
	}
	for _, child := range kss {
		ks := child.Obj.(*manifest.Kustomization)
		root, pinned, external := SourceView(l.Store, ks.SourceID(), repoRoot)
		if ks.IsFragment() {
			continue
		}
		base := l.documents.claimBase(root, ks.Path)
		local := filepath.Join(root, base)
		if pinned {
			local = filepath.Join(repoRoot, base)
		} else if !external {
			physical := l.documents.localPath(local)
			if doc, exists := l.documents.cache[physical]; exists && physical != local {
				doc.buildExcluded = true
				l.documents.cache[physical] = doc
			}
		}
		prefix := ""
		if external || pinned {
			prefix = strings.TrimSuffix(local, string(filepath.Separator)) + string(filepath.Separator)
		}
		if external {
			for dir := range roots {
				doc := l.documents.cache[dir]
				if !doc.buildExcluded && dir != l.entryPath && (dir == local || strings.HasPrefix(dir, prefix)) {
					doc.buildExcluded = true
					l.documents.cache[dir] = doc
				}
			}
			if local != l.entryPath {
				delete(roots, local)
			}
			continue
		}
		record, _ := l.Index.Lookup(ks.Named())
		if filepath.Dir(record.Path) != local {
			delete(roots, local)
		}
		if pinned {
			for dir := range roots {
				if strings.HasPrefix(dir, prefix) {
					delete(roots, dir)
				}
			}
		}
	}
	if len(roots) == 0 {
		return nil
	}
	for dir, doc := range l.documents.cache {
		if !doc.traversed || doc.body == nil || l.documents.mountedRoot(dir) != l.documents.mountedRoot(repoRoot) {
			continue
		}
		for ref := range kustomizeinput.Kustomization(doc.body) {
			if ref.Role == kustomizeinput.File || ref.Inline {
				continue
			}
			if child, ok := kustpath.Resolve(dir, ref.Path, kustpath.Dependency); ok {
				if _, candidate := roots[child]; candidate {
					base, _ := filepath.Rel(repoRoot, child)
					if !l.anchorRoot(repoRoot, filepath.ToSlash(base)) {
						delete(roots, child)
					}
				}
			}
		}
	}
	var dirs []string
	for dir := range roots {
		if rel, err := filepath.Rel(repoRoot, dir); err == nil && !safepath.Escaped(rel) && l.documents.mountedRoot(dir) == l.documents.mountedRoot(repoRoot) {
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

type producerDeclaration struct {
	obj      manifest.BaseManifest
	authored manifest.NamedResource
	visit    graphVisit
}

type selfProduceBuilder struct {
	reader       documentReader
	idx          *manifest.DiscoveryIndex
	emittedFiles map[string][]manifest.NamedResource
	store        *store.Store
	secrets      map[manifest.NamedResource]*manifest.Secret
	wipeSecrets  bool
}

func (b *selfProduceBuilder) recordProduced(v graphVisit, objects []store.Child, declarations []producerDeclaration) []producerDeclaration {
	coordinate := cmp.Or(v.localCoordinate(), v.coordinate)
	record := func(child store.Child) {
		obj, info := child.Obj, manifest.KindOf(child.Obj)
		resolved := manifest.Resolve(obj.Named(), v.namespace, v.fallback)
		id := resolved
		if owner, _ := b.idx.Lookup(v.owner); v.owner == obj.Named() || owner.Authored == obj.Named() {
			id = v.owner
		}
		r := b.idx.Records[id]
		projected := obj
		needsCandidates := manifest.ProducerNeedsCandidates(obj)
		if id.Namespace != obj.Named().Namespace &&
			(r.Authored == (manifest.NamedResource{}) || needsCandidates || id.Namespace != resolved.Namespace) {
			projected = obj.WithNamespace(id.Namespace)
		}
		if r.Authored == (manifest.NamedResource{}) {
			r.Authored, r.Path, r.File = obj.Named(), v.file, coordinate
			r.Refs = discoveryReferences(projected)
		}
		p := manifest.Production{Owner: v.owner, Authored: obj.Named(), Path: v.file, File: coordinate, Source: v.source}
		if v.owner != (manifest.NamedResource{}) && info.Runnable && b.store.GetObject(id) == nil {
			p.Path = ""
		}
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
		if !v.deferMaterialization || info.Runnable && b.store.GetObject(id) != nil {
			declaration := producerDeclaration{obj: projected, authored: obj.Named(), visit: v}
			if needsCandidates {
				declarations = append(declarations, declaration)
			} else {
				b.recordProducer(declaration, nil)
			}
		}
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
		return declarations
	}
	_ = b.reader.decodeFileObjects(v.file, manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets, DefaultNamespace: ""}, true, record)
	return declarations
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

func (b *selfProduceBuilder) joinProductions(declarations []producerDeclaration, scope []graphVisit) {
	if len(declarations) == 0 {
		return
	}
	var candidates []manifest.NamedResource
	for _, v := range scope {
		if v.role != kustomizeinput.Resource || v.document != nil || !v.readable {
			continue
		}
		record := func(child store.Child) {
			obj := child.Obj
			ns := manifest.Resolve(obj.Named(), v.namespace, v.fallback).Namespace
			if ns != obj.Named().Namespace {
				obj = obj.WithNamespace(ns)
			}
			if v.deferMaterialization && b.store.GetObject(obj.Named()) == nil {
				return
			}
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
		if objects := b.reader.cache[v.file].objects; objects != nil {
			for _, obj := range objects {
				record(obj)
			}
		} else {
			_ = b.reader.decodeFileObjects(v.file, manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets, DefaultNamespace: ""}, true, record)
		}
	}
	slices.SortFunc(candidates, manifest.NamedResource.Compare)
	candidates = slices.Compact(candidates)
	for _, declaration := range declarations {
		b.recordProducer(declaration, candidates)
	}
}

func (b *selfProduceBuilder) recordProducer(declaration producerDeclaration, candidates []manifest.NamedResource) {
	obj, authored, v := declaration.obj, declaration.authored, declaration.visit
	projected := manifest.Resolve(obj.Named(), v.namespace, v.fallback)
	if v.owner == (manifest.NamedResource{}) && manifest.KindOf(obj).Runnable {
		v.owner = projected
	}
	for _, target := range manifest.ProducerTargets(obj, candidates...) {
		target.NamedResource = manifest.Resolve(target.NamedResource, v.namespace, v.fallback)
		if target.Namespace == "" {
			continue
		}
		p := manifest.Production{Owner: v.owner, Declaration: projected, Authored: authored, Source: v.source, File: cmp.Or(v.localCoordinate(), v.coordinate)}
		r := b.idx.Records[target.NamedResource]
		if !slices.Contains(r.Productions, p) {
			r.Productions = append(r.Productions, p)
		}
		b.idx.Records[target.NamedResource] = r
		if v.owner != (manifest.NamedResource{}) {
			for _, producer := range []manifest.NamedResource{authored, projected} {
				r := b.idx.Records[producer]
				if !slices.Contains(r.Productions, p) {
					r.Productions = append(r.Productions, p)
				}
				b.idx.Records[producer] = r
			}
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
		stringData[k] = manifest.ValuePlaceholder(k)
	}
	b.secrets[target] = &manifest.Secret{Name: target.Name, Namespace: target.Namespace, StringData: stringData}
}

func appendUniqueProducer(dst []manifest.NamedResource, v manifest.NamedResource) []manifest.NamedResource {
	if slices.Contains(dst, v) {
		return dst
	}
	return append(dst, v)
}
