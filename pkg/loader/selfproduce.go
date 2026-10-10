package loader

import (
	"cmp"
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

func (l *Loader) buildRenderers(repoRoot string, producers *manifest.ProducerIndex) {
	if repoRoot == "" {
		return
	}
	b := &selfProduceBuilder{repoRoot: repoRoot, documents: l.documents, idx: l.Index,
		emittedFiles: map[string][]manifest.NamedResource{}, producers: producers,
		store: l.Store, wipeSecrets: l.Options.WipeSecrets,
		secrets: map[manifest.NamedResource]*manifest.Secret{}}
	for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		if ks.Path == "" && ks.SourceName == "" {
			continue
		}
		root, pinned, external := SourceView(l.Store, ks, repoRoot)
		if external {
			continue
		}
		if pinned {
			prefix, err := filepath.Rel(repoRoot, root)
			if err != nil {
				continue
			}
			committed := *b
			committed.repoRoot, committed.filePrefix = root, filepath.ToSlash(prefix)
			committed.walkRoot(ks)
		} else {
			b.walkRoot(ks)
		}
	}
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

type selfProduceBuilder struct {
	repoRoot string
	// filePrefix projects artifact-relative files into discovery file coordinates.
	filePrefix string
	documents  documentReader
	idx        *manifest.DiscoveryIndex
	// emittedFiles accumulates each cross-tree-emitted ks.yaml path → the
	// distinct parents reaching it; resolved to single-parent
	// record Parent edges after the walk.
	emittedFiles map[string][]manifest.NamedResource
	// producers, when non-nil, receives target-Secret → ExternalSecret /
	// SealedSecret edges discovered during the same walk.
	producers *manifest.ProducerIndex
	// store receives a synthetic placeholder Secret for each producer whose
	// output keys are statically declared (see synthesizePlaceholderSecret).
	store   *store.Store
	secrets map[manifest.NamedResource]*manifest.Secret
	// wipeSecrets gates the placeholder synthesis — it is a wipe-mode behavior
	// (a stand-in for a secret flate can't read offline), matching the SOPS
	// ciphertext wipe; --no-wipe-secrets leaves the secret genuinely absent.
	wipeSecrets bool
}

func (b *selfProduceBuilder) directives(dir string) (*kustomizationDocument, string, bool) {
	d, file := b.documents.read(filepath.Join(b.repoRoot, dir))
	if d == nil {
		return nil, "", false
	}
	rel, err := filepath.Rel(b.repoRoot, file)
	return d, filepath.ToSlash(rel), err == nil
}

// walkRoot enumerates the base(s) of a Flux Kustomization's spec.path and
// walks each. A spec.path that holds a kustomization file is itself the
// single base; a bare spec.path (no kustomization file — Flux's generator
// synthesizes one listing the subdirs) contributes each immediate subdir
// that holds a kustomization file. The root imposes no namespace of its
// own (Flux's generated kustomization sets only `resources:`); rootNS is
// the consumer-side fallback for a ConfigMap that no layer ever stamps.
func (b *selfProduceBuilder) walkRoot(ks *manifest.Kustomization) {
	id := ks.Named()
	rootNS := cmp.Or(ks.TargetNamespace, ks.Namespace)
	specDir := manifest.NormalizeClaimBase(ks.Path)
	visited := map[string]struct{}{}

	if _, _, ok := b.directives(specDir); ok {
		b.walkBase(specDir, "", rootNS, id, visited)
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
		if _, _, ok := b.directives(sub); ok {
			b.walkBase(sub, "", rootNS, id, visited)
		}
	}
}

// walkBase walks one kustomize base (or component) at the repo-relative
// dir, recording the ConfigMaps it emits for ks. parentNS is the namespace
// inherited from the enclosing layer; baseNS applies the kustomize-faithful
// rule that the OUTER (ancestor) `namespace:` wins — once an ancestor set
// one it is locked, so an inner base's directive cannot override it.
func (b *selfProduceBuilder) walkBase(dir, parentNS, rootNS string, ks manifest.NamedResource, visited map[string]struct{}) {
	if dir == "" || strings.HasPrefix(dir, "..") {
		return // outside the repo root — never read
	}
	// Key the cycle guard by (dir, inherited namespace): a shared component
	// (e.g. ../../components/substitutions) is legitimately included by every
	// app group, each stamping its own `namespace:` — so the SAME dir under a
	// DIFFERENT inherited namespace produces a DIFFERENT ConfigMap id and must
	// be walked again. Keying by dir alone would attribute the CM to whichever
	// group walked the component first.
	key := dir + "\x00" + parentNS
	if _, seen := visited[key]; seen {
		return // cycle / repeated-under-same-namespace guard
	}
	visited[key] = struct{}{}

	d, file, ok := b.directives(dir)
	if !ok {
		return
	}
	b.recordFile(file, ks)
	baseNS := cmp.Or(parentNS, d.Namespace)

	for _, r := range d.Resources {
		resolved, ok := resolvePath(dir, r)
		if !ok {
			continue
		}
		resolved = filepath.ToSlash(resolved)
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
	b.recordFile(relFile, ks)
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

// recordConfigMap attributes cm to ks's self-produce set under its effective
// namespace: the active transformer (baseNS) wins, then the resource's own,
// then the consumer-side rootNS fallback (so a namespace-less CM still matches
// a same-namespace substituteFrom lookup).
func (b *selfProduceBuilder) recordConfigMap(cm *manifest.ConfigMap, baseNS, rootNS string, ks manifest.NamedResource) {
	ns := cmp.Or(baseNS, cm.Namespace, rootNS)
	if ns == "" {
		return
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: cm.Name}
	r := b.idx.Records[id]
	r.Renderers = appendUniqueProducer(r.Renderers, ks)
	b.idx.Records[id] = r
}

func (b *selfProduceBuilder) recordFile(relFile string, ks manifest.NamedResource) {
	if relFile == "" {
		return
	}
	if b.filePrefix != "" {
		relFile = path.Join(b.filePrefix, relFile)
	}
	b.idx.Readers[relFile] = appendUniqueProducer(b.idx.Readers[relFile], ks)
}

// recordEmittedKS notes that ks's render subtree reaches a Flux Kustomization
// defined at relFile — i.e. ks emits that cross-tree base. Accumulated per file;
// resolved to a single-parent gate after the walk.
func (b *selfProduceBuilder) recordEmittedKS(relFile string, ks manifest.NamedResource) {
	if b.filePrefix != "" {
		relFile = path.Join(b.filePrefix, relFile)
	}
	b.emittedFiles[relFile] = appendUniqueProducer(b.emittedFiles[relFile], ks)
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
