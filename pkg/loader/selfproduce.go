package loader

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) buildProductions(ctx context.Context, repoRoot string, emittedFiles map[string][]manifest.NamedResource) error {
	if repoRoot == "" {
		return nil
	}
	b := &selfProduceBuilder{idx: l.Index, store: l.Store, wipeSecrets: l.Options.WipeSecrets, reader: l.documents, emittedFiles: emittedFiles}
	var generators []generatorRecord
	incomplete := map[manifest.NamedResource][]string{}
	var declarations []producerDeclaration
	prepared := map[string][]store.Child{}
	preparedOwners := map[manifest.NamedResource]manifest.NamedResource{}
	var pinned map[manifest.NamedResource]bool
	for _, input := range l.scopes.inputs {
		if l.scopes.sources[input.source].pinned {
			if pinned == nil {
				pinned = map[manifest.NamedResource]bool{}
			}
			pinned[input.owner.Named()] = true
		}
	}
	remaining := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		b.scanExclusions = b.scanExclusions || doc.scanExcluded
		if doc.body != nil && !doc.selected && len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) != 0 {
			remaining[dir] = struct{}{}
		}
	}
	record := func(v graphVisit) error {
		if b.scanExclusions && l.documents.scanExcluded(v.file) {
			return nil
		}
		if file := cmp.Or(v.origin, v.file); len(pinned) != 0 && !pinned[v.owner] && safepath.Contains(repoRoot, file) {
			owners, _ := l.Index.OwnersOfFile(l.fileCoordinate(file))
			if slices.ContainsFunc(owners, func(owner manifest.NamedResource) bool { return pinned[owner] }) {
				return nil
			}
		}
		if v.owner != (manifest.NamedResource{}) && v.coordinate != "" {
			l.recordReader(v.coordinate, v.localCoordinate(), manifest.FileReader{Reader: v.owner, Source: v.source})
		}
		if v.incomplete != "" {
			if v.owner != (manifest.NamedResource{}) {
				incomplete[v.owner] = append(incomplete[v.owner], v.source.String()+":"+v.coordinate+": "+v.incomplete)
			}
			return nil
		}
		if v.document != nil {
			delete(remaining, filepath.Dir(v.file))
		}
		if v.role != kustomizeinput.Resource {
			return nil
		}
		if l.documents.cache[v.file].selected {
			for _, child := range prepared[v.coordinate] {
				id := child.Obj.Named()
				r := l.Index.Records[id]
				if v.owner == (manifest.NamedResource{}) || v.owner == id || v.owner == r.Parent {
					continue
				}
				p := manifest.Production{Owner: v.owner, Source: v.source, File: r.File}
				if !slices.Contains(r.Productions, p) {
					r.Productions = append(r.Productions, p)
				}
				if manifest.KindOf(child.Obj).Structural && (r.Parent == preparedOwners[id] || r.Parent == (manifest.NamedResource{})) {
					r.Parent = manifest.NamedResource{}
					b.emittedFiles[r.File] = appendUniqueProducer(b.emittedFiles[r.File], v.owner)
				}
				l.Index.Records[id] = r
			}
			return nil
		}
		if v.document != nil {
			for _, g := range v.document.ConfigMapGenerator {
				generators = append(generators, generatorRecord{root: v.root, file: v.file, namespace: cmp.Or(v.namespace, g.Namespace, v.fallback), isConfigMap: true, args: g.GeneratorArgs, owner: v.owner, source: v.source, reader: l.documents})
			}
			for _, g := range v.document.SecretGenerator {
				generators = append(generators, generatorRecord{root: v.root, file: v.file, namespace: cmp.Or(v.namespace, g.Namespace, v.fallback), args: g.GeneratorArgs, owner: v.owner, source: v.source, reader: l.documents})
			}
			return nil
		}
		if v.readable {
			var err error
			declarations, err = b.recordProduced(v, l.documents.cache[v.file].objects, declarations, l)
			return err
		}
		return nil
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
		root := graphVisit{root: repoRoot, dir: filepath.ToSlash(rel), owner: doc.builtInput.Owner, source: doc.builtInput.Source}
		if record, ok := l.Index.Lookup(root.owner); ok {
			self := manifest.Production{Owner: root.owner, Authored: record.Authored, Path: record.Path, File: record.File, Source: root.source}
			if !slices.Contains(record.Productions, self) {
				record.Productions = append(record.Productions, self)
			}
			l.Index.Records[root.owner] = record
		}
		var candidates []manifest.NamedResource
		for _, child := range doc.built {
			obj := child.Obj
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
		slices.SortFunc(candidates, manifest.NamedResource.Compare)
		candidates = slices.Compact(candidates)
		_, builder := l.documents.read(dir)
		root.coordinate = l.fileCoordinate(cmp.Or(builder, dir))
		sourceRoot := l.documents.mountedRoot(dir)
		for name, exact := range doc.builtInput.Reads {
			file := filepath.Join(sourceRoot, filepath.FromSlash(name))
			coordinate := l.fileCoordinate(file)
			local := l.fileCoordinate(l.documents.localPath(file))
			l.recordReader(coordinate, local, manifest.FileReader{Reader: root.owner, Source: root.source, Exact: exact})
		}
		for _, child := range doc.built {
			obj := child.Obj
			if record, ok := l.Index.Lookup(obj.Named()); ok && record.Builder == root.coordinate {
				prepared[record.File] = append(prepared[record.File], child)
				preparedOwners[obj.Named()] = doc.builtInput.Owner
				b.recordProducer(producerDeclaration{obj: obj, identity: obj.Named(), authored: obj.Named(), visit: root}, candidates)
			}
		}
	}
	for _, contribution := range l.scopes.contributions {
		if !contribution.active || contribution.membershipOnly {
			continue
		}
		for _, v := range contribution.visits {
			v.owner, v.source = contribution.owner, contribution.source
			if err := record(v); err != nil {
				return err
			}
		}
		b.joinProductions(declarations, contribution.visits)
		declarations = nil
	}
	if len(incomplete) != 0 {
		for _, owner := range slices.SortedFunc(maps.Keys(incomplete), manifest.NamedResource.Compare) {
			details := incomplete[owner]
			slices.Sort(details)
			details = slices.Compact(details)
			l.Store.AddWarning(manifest.Warning{Resource: owner, Category: manifest.WarnIncompleteInputCoverage,
				Message: "changed-only input coverage is incomplete; run flate build all without --base or --path-orig to validate the full tree", Detail: details})
		}
	}

	if len(remaining) != 0 {
		for _, dir := range l.unreachedGeneratorRoots(repoRoot, remaining) {
			root := graphVisit{root: repoRoot, dir: dir}
			walks := l.visitScope(root, false)
			for _, v := range walks {
				v.owner = manifest.NamedResource{}
				v.source = manifest.BootstrapSourceID
				if err := record(v); err != nil {
					return err
				}
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
		b.recordProducer(producerDeclaration{obj: obj, identity: id, authored: r.Authored, visit: v}, candidates[l.documents.mountedRoot(r.Path)])
	}
	if b.err != nil {
		return b.err
	}
	if err := l.addGenerated(ctx, generators); err != nil {
		return err
	}
	return nil
}

func (l *Loader) unreachedGeneratorRoots(repoRoot string, remaining map[string]struct{}) []string {
	var roots []string
	for _, dir := range slices.Sorted(maps.Keys(remaining)) {
		if !l.documents.cache[dir].traversed || l.documents.cache[l.documents.cache[dir].file].eligible < fileEligible {
			continue
		}
		rel, err := filepath.Rel(repoRoot, l.documents.cache[dir].file)
		if err != nil {
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
		p := manifest.Production{Owner: r.owner, Source: r.source, File: l.fileCoordinate(r.file)}
		if ks, ok := l.Store.Get[*manifest.Kustomization](r.owner); ok {
			p.Source = ks.SourceID()
		}
		if p.Source.Name == "" || p.Source.Kind == "" {
			p.Source = manifest.BootstrapSourceID
		}
		if !slices.Contains(record.Productions, p) {
			record.Productions = append(record.Productions, p)
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
		record.ReferenceData = append(record.ReferenceData, manifest.ReferenceData{Production: p, OfflineData: obj.(manifest.Data)})
		l.Index.Records[id] = record
	}
	return nil
}

type graphContext struct {
	root, dir, namespace, fallback string
	namePrefix, nameSuffix         string
	pluginRoot                     string
	role                           kustomizeinput.Role
	deferMaterialization           bool
	checkMetadata                  bool
	deferDecode                    bool
	entryResources                 bool
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
	membershipOnly     bool
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

func (l *Loader) packageRoots(repoRoot string, kss []store.Child) []string {
	if len(l.documents.cache) == 0 {
		return nil
	}
	l.ensureScopes()
	l.documents.replayEdges()
	sourceRoot := l.documents.mountedRoot(repoRoot)
	roots := map[string]bool{}
	for dir, doc := range l.documents.cache {
		if (doc.traversed || dir == l.entryPath) && doc.file != "" {
			roots[dir] = false
			if l.documents.mountedRoot(dir) == sourceRoot {
				rel, err := filepath.Rel(repoRoot, dir)
				roots[dir] = err == nil && !safepath.Escaped(rel) && l.anchorRoot(repoRoot, filepath.ToSlash(rel))
			}
			if doc.buildExcluded {
				doc.buildExcluded = false
				l.documents.cache[dir] = doc
			}
		}
	}
	for i, child := range kss {
		ks := child.Obj.(*manifest.Kustomization)
		view := l.scopes.sources[l.scopes.inputs[i].source]
		root, pinned, external := view.root, view.pinned, view.external
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
			for dir, anchored := range roots {
				if anchored || dir == l.entryPath || dir != local && !strings.HasPrefix(dir, prefix) {
					continue
				}
				doc := l.documents.cache[dir]
				doc.buildExcluded = true
				l.documents.cache[dir] = doc
				delete(roots, dir)
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
	var dirs []string
	for dir := range roots {
		if l.documents.cache[dir].incoming && !roots[dir] {
			continue
		}
		if rel, err := filepath.Rel(repoRoot, dir); err == nil && !safepath.Escaped(rel) && l.documents.mountedRoot(dir) == sourceRoot {
			dirs = append(dirs, filepath.ToSlash(rel))
		}
	}
	slices.Sort(dirs)
	return dirs
}

type producerDeclaration struct {
	obj      manifest.BaseManifest
	identity manifest.NamedResource
	authored manifest.NamedResource
	visit    graphVisit
}

type selfProduceBuilder struct {
	reader         documentReader
	idx            *manifest.DiscoveryIndex
	emittedFiles   map[string][]manifest.NamedResource
	store          *store.Store
	wipeSecrets    bool
	scanExclusions bool
	err            error
}

func (b *selfProduceBuilder) recordProduced(v graphVisit, objects []parsedObject, declarations []producerDeclaration, l *Loader) ([]producerDeclaration, error) {
	coordinate := cmp.Or(v.localCoordinate(), v.coordinate)
	var projectionErr error
	record := func(child store.Child, version string) {
		if projectionErr != nil || child.Obj == nil {
			return
		}
		obj, info := child.Obj, manifest.KindOf(child.Obj)
		if !info.Tracked && b.reader.cache[v.file].loaded && len(manifest.ProducerTargets(obj)) == 0 {
			return
		}
		resolved := manifest.Resolve(obj.Named(), v.namespace, v.fallback)
		id := resolved
		if owner, _ := b.idx.Lookup(v.owner); v.owner == obj.Named() || owner.Authored == obj.Named() {
			id = v.owner
		} else if info.Runnable && (v.namePrefix != "" || v.nameSuffix != "") {
			id, projectionErr = l.projectIdentity(obj, v, version)
			if projectionErr != nil {
				return
			}
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
			if id.Name != obj.Named().Name {
				r.Path = ""
			}
			r.Refs = discoveryReferences(nil, projected)
		}
		p := manifest.Production{Owner: v.owner, Authored: obj.Named(), Path: v.file, File: coordinate, Source: v.source}
		if p.Materializer(id) != (manifest.NamedResource{}) && info.Runnable && b.store.GetObject(id) == nil {
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
		if !v.deferMaterialization || info.Runnable && (b.reader.cache[v.file].loaded || b.store.GetObject(id) != nil) {
			declaration := producerDeclaration{obj: projected, identity: id, authored: obj.Named(), visit: v}
			if needsCandidates {
				declarations = append(declarations, declaration)
			} else {
				b.recordProducer(declaration, nil)
			}
		}
		switch {
		case info.Data:
			if s, ok := obj.(*manifest.Secret); ok && !v.deferMaterialization {
				b.recordSecret(s, id, p)
			}
		case info.Structural:
			b.emittedFiles[coordinate] = appendUniqueProducer(b.emittedFiles[coordinate], v.owner)
		}
	}
	if objects == nil {
		var err error
		objects, err = b.reader.fileObjects(v.file, manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets})
		if err != nil {
			return declarations, err
		}
	}
	for _, obj := range objects {
		if obj.outcome.Err == nil {
			record(obj.Child, obj.version)
		}
	}
	return declarations, projectionErr
}

func (b *selfProduceBuilder) recordSecret(secret *manifest.Secret, id manifest.NamedResource, production manifest.Production) {
	if !b.wipeSecrets || id.Namespace == "" || !secret.HasPlaceholder() {
		return
	}
	record := b.idx.Records[id]
	data := manifest.NewSecret(id, secret.Data, secret.StringData)
	// The last traversed SOPS declaration wins before unhashed generator candidates.
	record.ReferenceData = append([]manifest.ReferenceData{{Production: production, OfflineData: data}}, record.ReferenceData...)
	b.idx.Records[id] = record
}

func (b *selfProduceBuilder) joinProductions(declarations []producerDeclaration, scope []graphVisit) {
	if len(declarations) == 0 {
		return
	}
	var candidates []manifest.NamedResource
	for _, v := range scope {
		if v.role != kustomizeinput.Resource || v.document != nil || !v.readable || b.scanExclusions && b.reader.scanExcluded(v.file) {
			continue
		}
		record := func(child store.Child) {
			obj := child.Obj
			if obj == nil {
				return
			}
			ns := manifest.Resolve(obj.Named(), v.namespace, v.fallback).Namespace
			if ns != obj.Named().Namespace {
				obj = obj.WithNamespace(ns)
			}
			if v.deferMaterialization && !b.reader.cache[v.file].loaded && b.store.GetObject(obj.Named()) == nil {
				return
			}
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
		objects, err := b.reader.fileObjects(v.file, manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets})
		if err != nil {
			if b.err == nil {
				b.err = err
			}
			continue
		}
		for _, obj := range objects {
			if obj.outcome.Err == nil {
				record(obj.Child)
			}
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
	projected := declaration.identity
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
		if len(target.DeclaredKeys) != 0 && !slices.ContainsFunc(r.ReferenceData, func(candidate manifest.ReferenceData) bool { return candidate.Production == p }) {
			r.ReferenceData = append(r.ReferenceData, manifest.ReferenceData{Production: p, DeclaredKeys: target.DeclaredKeys})
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
	}
}

func appendUniqueProducer[T comparable](dst []T, v T) []T {
	if slices.Contains(dst, v) {
		return dst
	}
	return append(dst, v)
}
