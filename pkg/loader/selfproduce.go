package loader

import (
	"cmp"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) buildRenderers(repoRoot string, producers *manifest.ProducerIndex) {
	if repoRoot == "" {
		return
	}
	b := &selfProduceBuilder{idx: l.Index,
		emittedFiles: map[string][]manifest.NamedResource{}, producers: producers,
		store: l.Store, wipeSecrets: l.Options.WipeSecrets, reader: l.documents,
		secrets: map[manifest.NamedResource]*manifest.Secret{}}
	var generators []generatorRecord
	kss := l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization)
	remaining := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if doc.body != nil && len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) != 0 {
			remaining[dir] = struct{}{}
		}
	}
	var standalone []string
	if len(remaining) != 0 {
		standalone = l.standaloneRoots(repoRoot, kss)
	}
	record := func(v graphVisit) {
		if v.owner != (manifest.NamedResource{}) {
			b.idx.Readers[v.coordinate] = appendUniqueProducer(b.idx.Readers[v.coordinate], v.owner)
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
		if v.owner != (manifest.NamedResource{}) && v.readable {
			b.recordProduced(v, l.documents.cache[v.file].objects)
		}
	}
	for v := range l.graphVisits(repoRoot, kss, standalone) {
		record(v)
	}
	if len(remaining) != 0 {
		for v := range l.graphVisits(repoRoot, nil, l.unreachedGeneratorRoots(repoRoot, remaining)) {
			record(v)
		}
	}
	l.addGenerated(generators)
	for _, id := range slices.SortedFunc(maps.Keys(b.secrets), manifest.NamedResource.Compare) {
		l.Store.AddObject(b.secrets[id])
	}
	for id, r := range l.Index.Files() {
		if id.Kind == manifest.KindKustomization && r.Parent == (manifest.NamedResource{}) {
			if parents := b.emittedFiles[r.File]; len(parents) == 1 && parents[0] != id {
				r.Parent = parents[0]
				l.Index.Records[id] = r
			}
		}
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
			_, pinned, _ := SourceView(l.Store, ks, repoRoot)
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
			record.Renderers = appendUniqueProducer(record.Renderers, r.owner)
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
	peers              []manifest.NamedResource
	document           *kustomizationDocument
	readable           bool
}

// graphVisits projects each source and namespace separately. Visits borrow cached
// documents; consumers must never mutate them or alter the traversal's sources.
func (l *Loader) graphVisits(repoRoot string, kss []*manifest.Kustomization, standalone []string) iter.Seq[graphVisit] {
	return func(yield func(graphVisit) bool) {
		w := graphWalker{documents: l.documents, visited: map[graphContext]struct{}{}}
		var scopes map[string][]namespaceContext
		for _, ks := range kss {
			if ks.Namespace == "" {
				scopes = map[string][]namespaceContext{}
				break
			}
		}
		emit := func(v graphVisit) bool {
			if scopes != nil {
				scope := namespaceContext{namespace: v.namespace, fallback: v.fallback}
				if !slices.Contains(scopes[v.file], scope) {
					scopes[v.file] = append(scopes[v.file], scope)
				}
			}
			peers := v.peers
			v.peers = nil
			if !yield(v) {
				return false
			}
			for _, owner := range peers {
				v.owner = owner
				if !yield(v) {
					return false
				}
			}
			return true
		}
		for _, dir := range standalone {
			if !w.walk(graphVisit{root: repoRoot, dir: dir}, emit) {
				return
			}
		}
		for _, v := range l.graphRoots(repoRoot, kss, scopes) {
			clear(w.visited)
			if d, _ := l.documents.read(filepath.Join(v.root, v.dir)); d != nil {
				if !w.walk(v, emit) {
					return
				}
				continue
			}
			input, name := l.documents.input(filepath.Join(v.root, v.dir))
			entries, _ := fs.ReadDir(input, name)
			for _, e := range entries {
				if e.IsDir() {
					child := v
					child.dir = path.Join(v.dir, e.Name())
					if !w.walk(child, emit) {
						return
					}
				}
			}
		}
	}
}

func (l *Loader) graphRoots(repoRoot string, kss []*manifest.Kustomization, scopes map[string][]namespaceContext) []graphVisit {
	var roots []graphVisit
	grouped := map[graphContext]int{}
	for _, ks := range kss {
		if ks.Path == "" && ks.SourceName == "" {
			continue
		}
		root, _, external := SourceView(l.Store, ks, repoRoot)
		if external {
			continue
		}
		ns := ks.Namespace
		if ns == "" {
			if contexts := scopes[l.Index.Records[ks.Named()].Path]; len(contexts) != 0 {
				ns = cmp.Or(contexts[0].namespace, contexts[0].fallback)
			}
		}
		v := graphVisit{root: root, dir: l.documents.claimBase(root, ks.Path), namespace: ks.TargetNamespace, fallback: cmp.Or(ks.TargetNamespace, ns), owner: ks.Named()}
		if root != repoRoot {
			prefix, err := filepath.Rel(repoRoot, root)
			if err != nil {
				continue
			}
			v.prefix = filepath.ToSlash(prefix)
		}
		if i, found := grouped[v.graphContext]; found {
			roots[i].peers = append(roots[i].peers, v.owner)
		} else {
			grouped[v.graphContext] = len(roots)
			roots = append(roots, v)
		}
	}
	return roots
}

func (l *Loader) standaloneRoots(repoRoot string, kss []*manifest.Kustomization) []string {
	roots := map[string]struct{}{}
	for dir, doc := range l.documents.cache {
		if doc.body != nil {
			roots[dir] = struct{}{}
		}
	}
	for _, ks := range kss {
		root, pinned, _ := SourceView(l.Store, ks, repoRoot)
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
				if child, ok := resolvePath(dir, ref); ok {
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

// Kustomize graph and transformer references use slash separators on every OS.
func escapedSlashPath(rel string) bool {
	rel = path.Clean(rel)
	return rel == ".." || strings.HasPrefix(rel, "../")
}

type graphWalker struct {
	documents documentReader
	visited   map[graphContext]struct{}
}

// An outer namespace stays locked across nested bases and components.
func (w *graphWalker) walk(v graphVisit, yield func(graphVisit) bool) bool {
	if escapedSlashPath(v.dir) {
		return true
	}
	d, file := w.documents.read(filepath.Join(v.root, v.dir))
	if d == nil {
		return true
	}
	v.namespace = cmp.Or(v.namespace, d.Namespace)
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
			resolved, ok := resolvePath(v.dir, ref)
			if !ok {
				continue
			}
			child := v
			child.dir = filepath.ToSlash(resolved)
			abs := filepath.Join(v.root, resolved)
			childDoc, known := w.documents.cache[abs]
			directory := childDoc.body != nil
			if !known {
				input, name := w.documents.input(abs)
				info, err := fs.Stat(input, name)
				directory = err == nil && info.IsDir()
			}
			if directory {
				if !w.walk(child, yield) {
					return false
				}
				continue
			}
			child.file, child.document = abs, nil
			child.coordinate = path.Join(v.prefix, filepath.ToSlash(resolved))
			_, child.readable = resolveDataPath(v.dir, ref)
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
	producers    *manifest.ProducerIndex
	store        *store.Store
	secrets      map[manifest.NamedResource]*manifest.Secret
	wipeSecrets  bool
}

func (b *selfProduceBuilder) recordProduced(v graphVisit, objects []manifest.BaseManifest) {
	record := func(obj manifest.BaseManifest) {
		switch o := obj.(type) {
		case *manifest.ConfigMap, *manifest.Secret:
			b.recordDataObject(o, v.namespace, v.fallback, v.owner)
			if s, ok := o.(*manifest.Secret); ok {
				b.materializeSecret(s, v.namespace, v.fallback)
			}
		case *manifest.Kustomization:
			b.emittedFiles[v.coordinate] = appendUniqueProducer(b.emittedFiles[v.coordinate], v.owner)
		case *manifest.RawObject:
			b.recordProducer(o, v.namespace, v.fallback, v.owner)
		}
	}
	if objects != nil {
		for _, obj := range objects {
			record(obj)
		}
		return
	}
	_ = b.reader.decodeFileObjects(v.file, manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets}, true, record)
}

// materializeSecret retains SOPS stand-ins under the rendered namespace so
// self-produced substituteFrom inputs are available before reconciliation.
// Real Store objects win; the last SOPS Secret replaces earlier stand-ins.
func (b *selfProduceBuilder) materializeSecret(s *manifest.Secret, baseNS, rootNS string) {
	ns := cmp.Or(baseNS, s.Namespace, rootNS)
	if !b.wipeSecrets || b.store == nil || ns == "" {
		return
	}
	if !manifest.ContainsValuePlaceholder(s.StringData) && !manifest.ContainsValuePlaceholder(s.Data) {
		return
	}
	c := *s
	c.Namespace = ns
	if b.store.GetObject(c.Named()) == nil {
		b.secrets[c.Named()] = &c
	}
}

// Rendered data uses graph namespace, then authored namespace, then consumer fallback.
func (b *selfProduceBuilder) recordDataObject(obj manifest.BaseManifest, baseNS, rootNS string, ks manifest.NamedResource) {
	id := obj.Named()
	ns := cmp.Or(baseNS, id.Namespace, rootNS)
	if ns == "" || ks == (manifest.NamedResource{}) {
		return
	}
	id.Namespace = ns
	r := b.idx.Records[id]
	r.Renderers = appendUniqueProducer(r.Renderers, ks)
	b.idx.Records[id] = r
}

// recordProducer records the target(s) a producer generates — the Secret an
// ExternalSecret / SealedSecret materializes, or the Secret + ConfigMap an
// ObjectBucketClaim's provisioner creates — each re-keyed under the producer's
// effective namespace so it matches a consumer's same-namespace lookup.
// Non-producer kinds are no-ops. Ownership is recorded before rendering.
func (b *selfProduceBuilder) recordProducer(raw *manifest.RawObject, baseNS, rootNS string, ks manifest.NamedResource) {
	for _, target := range manifest.ProducerTargets(raw) {
		ns := cmp.Or(baseNS, target.Namespace, rootNS)
		if ns == "" {
			continue
		}
		target.Namespace = ns
		b.producers.Record(target.NamedResource, raw.Named())
		producer := raw.Named()
		projected := producer
		projected.Namespace = ns
		for _, producer := range []manifest.NamedResource{producer, projected} {
			if ks == (manifest.NamedResource{}) {
				continue
			}
			r := b.idx.Records[producer]
			r.DeclarationOwners = appendUniqueProducer(r.DeclarationOwners, ks)
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
