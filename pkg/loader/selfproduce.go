package loader

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (l *Loader) buildRenderers(ctx context.Context, repoRoot string, producers *manifest.ProducerIndex) error {
	if repoRoot == "" {
		return nil
	}
	b := &selfProduceBuilder{repoRoot: repoRoot, documents: l.documents, idx: l.Index,
		emittedFiles: map[string]emissionFile{}, producers: producers,
		store: l.Store, wipeSecrets: l.Options.WipeSecrets,
		secrets: map[manifest.NamedResource]*manifest.Secret{}}
	b.walkGraph(l)
	for _, r := range b.generators {
		obj, err := r.materialize(ctx)
		if err != nil {
			return err
		}
		id := obj.Named()
		if l.Store.GetObject(id) == nil {
			l.Store.AddObject(obj)
		}
		record := l.Index.Records[id]
		if record.File == "" {
			record.File = l.fileCoordinate(r.file)
		}
		if r.owner != (manifest.NamedResource{}) {
			record.Renderers = appendUniqueProducer(record.Renderers, r.owner)
		}
		l.Index.Records[id] = record
	}
	for _, id := range slices.SortedFunc(maps.Keys(b.secrets), manifest.NamedResource.Compare) {
		l.Store.AddObject(b.secrets[id])
	}
	for id, r := range l.Index.Files() {
		if id.Kind == manifest.KindKustomization && r.Parent == (manifest.NamedResource{}) {
			if parents := b.emittedFiles[r.File].owners; len(parents) == 1 && parents[0] != id {
				r.Parent = parents[0]
				l.Index.Records[id] = r
			}
		}
	}
	return nil
}

// walkGraph evaluates shared documents separately for each enclosing Flux source and namespace.
func (b *selfProduceBuilder) walkGraph(l *Loader) {
	kss := l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization)
	if b.idx != nil && !l.incompleteFileKinds {
		for _, ks := range kss {
			if record, ok := b.idx.Lookup(ks.Named()); ok && record.File != "" && record.Parent != (manifest.NamedResource{}) {
				b.emittedFiles[record.File] = emissionFile{kustomizationsOnly: true}
			}
		}
		if len(b.emittedFiles) != 0 {
			for id, record := range b.idx.Files() {
				if id.Kind != manifest.KindKustomization {
					if _, tracked := b.emittedFiles[record.File]; tracked {
						b.emittedFiles[record.File] = emissionFile{}
					}
				}
			}
		}
	}
	visited := map[graphContext]struct{}{}
	if b.namespaces != nil {
		roots := map[string]struct{}{}
		for dir, doc := range b.documents {
			if doc.body != nil {
				roots[dir] = struct{}{}
			}
		}
		for _, ks := range kss {
			root, pinned, _ := SourceView(l.Store, ks, b.repoRoot)
			delete(roots, filepath.Join(root, manifest.NormalizeClaimBase(ks.Path)))
			if pinned {
				local := filepath.Join(b.repoRoot, manifest.NormalizeClaimBase(ks.Path))
				prefix := strings.TrimSuffix(local, string(filepath.Separator)) + string(filepath.Separator)
				for dir := range roots {
					if dir == local || strings.HasPrefix(dir, prefix) {
						delete(roots, dir)
					}
				}
			}
		}
		if len(roots) != 0 {
			for dir, doc := range b.documents {
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
		}
		for _, dir := range slices.Sorted(maps.Keys(roots)) {
			rel, err := filepath.Rel(b.repoRoot, dir)
			if err == nil {
				b.walkBase(filepath.ToSlash(rel), "", "", manifest.NamedResource{}, visited)
			}
		}
	}
	for _, ks := range kss {
		if ks.Path == "" && ks.SourceName == "" {
			continue
		}
		root, _, external := SourceView(l.Store, ks, b.repoRoot)
		if external {
			continue
		}
		walker := *b
		walker.repoRoot = root
		if b.namespaces != nil && ks.Namespace == "" {
			if record, ok := l.Index.Lookup(ks.Named()); ok {
				if scopes := b.namespaces[record.File]; len(scopes) == 1 {
					projected := *ks
					projected.Namespace = scopes[0].namespace
					ks = &projected
				}
			}
		}
		if b.namespaces == nil {
			clear(visited)
		}
		walker.walkRoot(ks, visited, b.repoRoot)
		b.generators = walker.generators
	}
	if b.namespaces == nil {
		var dirs []string
		var namespaces map[string][]namespaceContext
		for _, doc := range b.documents {
			if doc.body == nil || len(doc.body.ConfigMapGenerator)+len(doc.body.SecretGenerator) == 0 {
				continue
			}
			rel, err := filepath.Rel(b.repoRoot, doc.file)
			if err != nil || strings.HasPrefix(rel, "..") || len(b.idx.Readers[filepath.ToSlash(rel)]) != 0 {
				continue
			}
			owners, _ := b.idx.OwnersOfFile(filepath.ToSlash(rel))
			pinned := false
			for _, owner := range owners {
				if ks, ok := l.Store.Get[*manifest.Kustomization](owner); ok {
					_, pinned, _ = SourceView(l.Store, ks, b.repoRoot)
					if pinned {
						break
					}
				}
			}
			if pinned {
				continue
			}
			if namespaces == nil {
				namespaces = map[string][]namespaceContext{}
			}
			namespaces[filepath.ToSlash(rel)] = nil
			dirs = append(dirs, filepath.ToSlash(filepath.Dir(rel)))
		}
		if len(dirs) == 0 {
			return
		}
		walker := *b
		walker.idx, walker.namespaces = nil, namespaces
		walker.walkGraph(l)
		slices.Sort(dirs)
		for _, dir := range dirs {
			_, file, _ := b.directives(dir)
			scopes := namespaces[file]
			if len(scopes) == 0 {
				scopes = []namespaceContext{{}}
			}
			for _, scope := range scopes {
				b.walkBase(dir, scope.namespace, scope.fallback, manifest.NamedResource{}, map[graphContext]struct{}{})
			}
		}
	}
}

type emissionFile struct {
	owners             []manifest.NamedResource
	kustomizationsOnly bool
}

type graphContext struct {
	root, dir, namespace, fallback string
}

type selfProduceBuilder struct {
	repoRoot string
	// filePrefix projects artifact-relative files into discovery file coordinates.
	filePrefix string
	documents  documentReader
	namespaces map[string][]namespaceContext
	generators []generatorRecord
	idx        *manifest.DiscoveryIndex
	// Only uniquely emitted definition files supply a cross-tree parent gate.
	emittedFiles map[string]emissionFile
	// producers retains target-to-producer edges from the graph.
	producers *manifest.ProducerIndex
	store     *store.Store
	secrets   map[manifest.NamedResource]*manifest.Secret
	// Only wipe mode may synthesize unavailable Secret keys.
	wipeSecrets bool
}

func (b *selfProduceBuilder) graphContext(dir, namespace, fallback string) graphContext {
	if namespace != "" {
		fallback = ""
	}
	return graphContext{root: b.repoRoot, dir: dir, namespace: namespace, fallback: fallback}
}

func (b *selfProduceBuilder) directives(dir string) (*kustomizationDocument, string, bool) {
	d, file := b.documents.read(filepath.Join(b.repoRoot, dir))
	if d == nil {
		return nil, "", false
	}
	return d, path.Join(filepath.ToSlash(dir), filepath.Base(file)), true
}

// walkRoot follows spec.path or each immediate kustomize subdirectory of a bare path.
// targetNamespace overrides graph namespaces; rootNS is the fallback for unstamped data.
func (b *selfProduceBuilder) walkRoot(ks *manifest.Kustomization, visited map[graphContext]struct{}, repoRoot string) {
	id := ks.Named()
	rootNS := cmp.Or(ks.TargetNamespace, ks.Namespace)
	specDir := manifest.NormalizeClaimBase(ks.Path)

	d, _ := b.documents.read(filepath.Join(b.repoRoot, specDir))
	if d != nil && b.namespaces != nil {
		if _, seen := visited[b.graphContext(specDir, cmp.Or(ks.TargetNamespace, d.Namespace), rootNS)]; seen {
			return
		}
	}
	if b.repoRoot != repoRoot {
		prefix, err := filepath.Rel(repoRoot, b.repoRoot)
		if err != nil {
			return
		}
		b.filePrefix = filepath.ToSlash(prefix)
	}
	if d != nil {
		b.walkBase(specDir, ks.TargetNamespace, rootNS, id, visited)
		return
	}
	entries, err := os.ReadDir(filepath.Join(b.repoRoot, specDir))
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		sub := path.Join(specDir, e.Name())
		if d, _ := b.documents.read(filepath.Join(b.repoRoot, sub)); d != nil {
			b.walkBase(sub, ks.TargetNamespace, rootNS, id, visited)
		}
	}
}

// walkBase locks an outer namespace across nested bases and components.
func (b *selfProduceBuilder) walkBase(dir, parentNS, rootNS string, ks manifest.NamedResource, visited map[graphContext]struct{}) {
	if dir == "" || strings.HasPrefix(dir, "..") {
		return // outside the repo root — never read
	}
	d, file, ok := b.directives(dir)
	if !ok {
		return
	}
	baseNS := cmp.Or(parentNS, d.Namespace)
	key := b.graphContext(dir, baseNS, rootNS)
	if _, seen := visited[key]; seen {
		return
	}
	visited[key] = struct{}{}
	b.recordFile(file, ks)
	if b.namespaces != nil {
		coordinate := file
		if b.filePrefix != "" {
			coordinate = path.Join(b.filePrefix, file)
		}
		if _, needed := b.namespaces[coordinate]; needed {
			b.recordProduced(file, baseNS, rootNS, ks)
		}
	}

	if b.idx != nil {
		file := filepath.Join(b.repoRoot, filepath.FromSlash(file))
		for _, g := range d.ConfigMapGenerator {
			b.generators = append(b.generators, generatorRecord{file: file, namespace: cmp.Or(baseNS, g.Namespace, rootNS),
				isConfigMap: true, args: g.GeneratorArgs, owner: ks})
		}
		for _, g := range d.SecretGenerator {
			b.generators = append(b.generators, generatorRecord{file: file, namespace: cmp.Or(baseNS, g.Namespace, rootNS),
				args: g.GeneratorArgs, owner: ks})
		}
	}

	for _, r := range d.Resources {
		resolved, ok := resolvePath(dir, r)
		if !ok {
			continue
		}
		resolved = filepath.ToSlash(resolved)
		if b.namespaces != nil {
			coordinate := resolved
			if b.filePrefix != "" {
				coordinate = path.Join(b.filePrefix, resolved)
			}
			if _, needed := b.namespaces[coordinate]; needed {
				b.recordProduced(resolved, baseNS, rootNS, ks)
			} else if b.documents[filepath.Join(b.repoRoot, resolved)].body != nil {
				b.walkBase(resolved, baseNS, rootNS, ks, visited)
			}
			continue
		}
		info, err := os.Stat(filepath.Join(b.repoRoot, resolved))
		if err != nil {
			// Still attribute the path: a resource deleted from this tree
			// while its resources: entry remains must resolve to ks in
			// changed-only mode so the broken reference renders and fails
			// loud instead of being skipped.
			b.recordFile(resolved, ks)
			continue
		}
		if info.IsDir() {
			b.walkBase(resolved, baseNS, rootNS, ks, visited)
			continue
		}
		// File resource — re-impose the stricter, escape-rejecting policy
		// used for paths that get opened (resolvePath permits
		// escapes for the dir-descent case only).
		if _, ok := resolveDataPath(dir, r); !ok {
			// Escaping file: not opened for content, but still attributed so
			// changed-only mode keeps ks when it is the only edit (#833).
			b.recordFile(resolved, ks)
			continue
		}
		b.recordProduced(resolved, baseNS, rootNS, ks)
	}

	for _, c := range d.Components {
		resolved, ok := resolvePath(dir, c)
		if !ok {
			continue
		}
		// A component carries no namespace of its own that wins over the
		// including base — baseNS flows in as the parent and stays locked.
		b.walkBase(filepath.ToSlash(resolved), baseNS, rootNS, ks, visited)
	}
}

// recordProduced collects data renderers, producer declarations and wipe-mode
// Secret stand-ins. Store materialization follows completion of the graph walk.
func (b *selfProduceBuilder) recordProduced(relFile, baseNS, rootNS string, ks manifest.NamedResource) {
	if b.namespaces != nil {
		if b.filePrefix != "" {
			relFile = path.Join(b.filePrefix, relFile)
		}
		scope := namespaceContext{namespace: baseNS}
		if baseNS == "" {
			scope.fallback = rootNS
		}
		if !slices.Contains(b.namespaces[relFile], scope) {
			b.namespaces[relFile] = append(b.namespaces[relFile], scope)
		}
		return
	}

	b.recordFile(relFile, ks)
	coordinate := relFile
	if b.filePrefix != "" {
		coordinate = path.Join(b.filePrefix, relFile)
	}
	if b.emittedFiles[coordinate].kustomizationsOnly {
		b.recordEmittedKS(relFile, ks)
		return
	}
	_ = decodeFileObjects(filepath.Join(b.repoRoot, relFile),
		manifest.ParseDocOptions{WipeSecrets: b.wipeSecrets}, true,
		func(obj manifest.BaseManifest) {
			switch o := obj.(type) {
			case *manifest.ConfigMap:
				b.recordConfigMap(o, baseNS, rootNS, ks)
			case *manifest.Secret:
				b.materializeSecret(o, baseNS, rootNS)
			case *manifest.Kustomization:
				b.recordEmittedKS(relFile, ks)
			case *manifest.RawObject:
				b.recordProducer(o, baseNS, rootNS, ks)
			}
		})
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
	s.Namespace = ns
	if b.store.GetObject(s.Named()) == nil {
		b.secrets[s.Named()] = s
	}
}

// recordConfigMap uses graph namespace, then authored namespace, then consumer fallback.
func (b *selfProduceBuilder) recordConfigMap(cm *manifest.ConfigMap, baseNS, rootNS string, ks manifest.NamedResource) {
	ns := cmp.Or(baseNS, cm.Namespace, rootNS)
	if ns == "" || ks == (manifest.NamedResource{}) {
		return
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: cm.Name}
	r := b.idx.Records[id]
	r.Renderers = appendUniqueProducer(r.Renderers, ks)
	b.idx.Records[id] = r
}

func (b *selfProduceBuilder) recordFile(relFile string, ks manifest.NamedResource) {
	if relFile == "" || b.idx == nil || ks == (manifest.NamedResource{}) {
		return
	}
	if b.filePrefix != "" {
		relFile = path.Join(b.filePrefix, relFile)
	}
	b.idx.Readers[relFile] = appendUniqueProducer(b.idx.Readers[relFile], ks)
}

// recordEmittedKS retains distinct readers for cross-tree parent resolution.
func (b *selfProduceBuilder) recordEmittedKS(relFile string, ks manifest.NamedResource) {
	if ks == (manifest.NamedResource{}) {
		return
	}
	if b.filePrefix != "" {
		relFile = path.Join(b.filePrefix, relFile)
	}
	file := b.emittedFiles[relFile]
	file.owners = appendUniqueProducer(file.owners, ks)
	b.emittedFiles[relFile] = file
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
