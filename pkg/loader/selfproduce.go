package loader

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"

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
	remaining := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if doc.body != nil && len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) != 0 {
			remaining[dir] = struct{}{}
		}
	}
	record := func(v graphVisit) {
		if v.owner != (manifest.NamedResource{}) && !v.generated {
			l.recordReader(v.coordinate, v.localCoordinate(), v.owner)
		}
		if v.document != nil {
			delete(remaining, filepath.Dir(v.file))
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
	for _, root := range l.scopes.roots {
		for _, v := range l.scopes.walks[root.graphContext] {
			v.owner, v.source = root.owner, root.source
			record(v)
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
}

type graphVisit struct {
	graphContext
	file               string
	prefix, coordinate string
	owner              manifest.NamedResource
	source             manifest.NamedResource
	document           *types.Kustomization
	readable           bool
	localBase          string
	generated          bool
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
		for _, refs := range [][]string{doc.body.Resources, doc.body.Components} {
			for _, ref := range refs {
				if child, ok := kustpath.Resolve(dir, ref, kustpath.Directory); ok {
					delete(roots, child)
				}
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
	d, file := w.documents.read(filepath.Join(v.root, v.dir))
	if d == nil {
		input, name := w.documents.input(filepath.Join(v.root, v.dir))
		entries, _ := fs.ReadDir(input, name)
		for _, entry := range entries {
			child := v
			child.generated = true
			child.dir = path.Join(v.dir, entry.Name())
			if entry.IsDir() {
				if !w.walk(child, yield) {
					return false
				}
			} else if ext := path.Ext(entry.Name()); ext == ".yaml" || ext == ".yml" {
				child.file, child.coordinate, child.document = filepath.Join(v.root, child.dir), path.Join(v.prefix, child.dir), nil
				child.readable = true
				if !yield(child) {
					return false
				}
			}
		}
		return true
	}
	v.namespace = cmp.Or(v.namespace, d.Namespace)
	if v.namespace == "" {
		v.namespace = w.documents.transformerInjectedNamespace(v.root, v.dir).ns
	}
	if _, seen := w.visited[v.graphContext]; seen {
		return true
	}
	w.visited[v.graphContext] = struct{}{}
	v.file, v.document, v.readable = file, d, true
	v.coordinate = path.Join(v.prefix, v.dir, filepath.Base(file))
	if !yield(v) {
		return false
	}
	for _, refs := range [][]string{d.Resources, d.Components} {
		for _, ref := range refs {
			resolved, ok := kustpath.Resolve(v.dir, ref, kustpath.Directory)
			if !ok {
				continue
			}
			child := v
			child.dir = filepath.ToSlash(resolved)
			abs := filepath.Join(v.root, resolved)
			childDoc, known := w.documents.cache[abs]
			directory := childDoc.body != nil
			readable := true
			if !known {
				input, name := w.documents.input(abs)
				info, err := fs.Stat(input, name)
				directory = err == nil && info.IsDir()
				readable = !errors.Is(err, fs.ErrNotExist)
			}
			if directory {
				if !w.walk(child, yield) {
					return false
				}
				continue
			}
			child.file, child.document = abs, nil
			child.coordinate = path.Join(v.prefix, filepath.ToSlash(resolved))
			child.readable = readable
			if child.readable {
				_, child.readable = kustpath.Resolve(filepath.Join(v.root, v.dir), ref, kustpath.File)
			}
			if !yield(child) {
				return false
			}
		}
	}
	return true
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
		if !v.generated {
			b.recordProducer(obj, v.namespace, v.fallback, v.owner)
		}
		info := manifest.KindOf(obj)
		switch {
		case info.Data:
			if s, ok := obj.(*manifest.Secret); ok && !v.generated {
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
