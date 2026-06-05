package loader

import (
	"cmp"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// SelfProduceIndex maps a ConfigMap id (kind + resolved namespace +
// name) to the Flux Kustomization(s) whose OWN render subtree emits it.
//
// It exists for one job: let collectDeps tell a self-substitute apart
// from a cross-KS substituteFrom. A Kustomization that renders the very
// ConfigMap it lists in postBuild.substituteFrom (the bjw-s/onedr0p
// `cluster-apps` → `cluster-settings` pattern: a bare-dir spec.path whose
// subdir bases pull in a component defining the CM) must NOT hard-wait on
// that CM — no other reconcile can produce it, so depwait would deadlock
// the consumer against its own render. A CM produced by a DIFFERENT KS,
// or genuinely absent, is left out of this index so its dependency edge
// stays and still fails loudly.
//
// Built once per Bootstrap (in discovery.Run) and read-only afterwards.
type SelfProduceIndex struct {
	byID map[manifest.NamedResource][]manifest.NamedResource
}

// ProducedBy returns the Kustomizations whose render subtree emits cm.
// Nil-safe: a nil index (no repoRoot, stripped-down tests) produces no
// matches, so collectDeps falls back to the always-add edge behavior.
func (i *SelfProduceIndex) ProducedBy(cm manifest.NamedResource) []manifest.NamedResource {
	if i == nil {
		return nil
	}
	return i.byID[cm]
}

// BuildSelfProduceIndex resolves, per Flux Kustomization with a spec.path,
// which ConfigMaps its own kustomize render emits and in which namespace —
// by walking the render graph the loader's discovery pass does not attribute
// to a producer: bare-dir base generation (each immediate subdir holding a
// kustomization file becomes a base, mirroring Flux's own generator),
// recursion through `resources:` (files + nested bases) and `components:`,
// and propagation of each layer's `namespace:` transformer. repoRoot anchors
// the on-disk reads; an empty repoRoot yields an empty (but usable) index.
func BuildSelfProduceIndex(s *store.Store, repoRoot string) *SelfProduceIndex {
	idx := &SelfProduceIndex{byID: map[manifest.NamedResource][]manifest.NamedResource{}}
	if repoRoot == "" {
		return idx
	}
	b := &selfProduceBuilder{
		repoRoot: repoRoot,
		dirs:     map[string]cachedDir{},
		idx:      idx,
	}
	for _, ks := range store.ListAs[*manifest.Kustomization](s, manifest.KindKustomization) {
		if ks.Path == "" {
			continue
		}
		b.walkRoot(ks)
	}
	return idx
}

type cachedDir struct {
	d  kustomizeDirectives
	ok bool
}

type selfProduceBuilder struct {
	repoRoot string
	// dirs memoizes whole-file kustomization reads (namespace / resources /
	// components) keyed by repo-relative dir, so subtrees shared across the
	// KS list — every app group's substitutions component, say — are read
	// once. Discovery is serial, so no lock is needed.
	dirs map[string]cachedDir
	idx  *SelfProduceIndex
}

// directives returns the (memoized) kustomization directives at the
// repo-relative dir; ok is false when no kustomization file is present.
func (b *selfProduceBuilder) directives(dir string) (kustomizeDirectives, bool) {
	if c, ok := b.dirs[dir]; ok {
		return c.d, c.ok
	}
	d, ok := readKustomizeDirectives(b.repoRoot, dir)
	b.dirs[dir] = cachedDir{d: d, ok: ok}
	return d, ok
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
	specDir := strings.TrimSuffix(NormalizePrefix(ks.Path), "/")
	visited := map[string]struct{}{}

	if _, ok := b.directives(specDir); ok {
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
		if _, ok := b.directives(sub); ok {
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

	d, ok := b.directives(dir)
	if !ok {
		return
	}
	baseNS := cmp.Or(parentNS, d.Namespace)

	for _, r := range d.Resources {
		resolved, ok := resolveResourcePath(dir, r)
		if !ok {
			continue
		}
		resolved = filepath.ToSlash(resolved)
		info, err := os.Stat(filepath.Join(b.repoRoot, resolved))
		if err != nil {
			continue
		}
		if info.IsDir() {
			b.walkBase(resolved, baseNS, rootNS, ks, visited)
			continue
		}
		// File resource — re-impose the stricter, escape-rejecting policy
		// used for paths that get opened (resolveResourcePath permits
		// escapes for the dir-descent case only).
		if _, ok := resolveDataPath(dir, r); !ok {
			continue
		}
		b.recordConfigMaps(resolved, baseNS, rootNS, ks)
	}

	for _, c := range d.Components {
		resolved, ok := resolveComponentPath(dir, c)
		if !ok {
			continue
		}
		// A component carries no namespace of its own that wins over the
		// including base — baseNS flows in as the parent and stays locked.
		b.walkBase(filepath.ToSlash(resolved), baseNS, rootNS, ks, visited)
	}
}

// recordConfigMaps parses a file `resources:` entry and records every
// ConfigMap it defines under its resolved namespace. The active namespace
// transformer (baseNS) overrides a resource's own metadata.namespace —
// matching kustomize; rootNS is the final fallback when no layer stamps a
// namespace (so a self-produced, namespace-less CM still matches the
// consumer's own-namespace substituteFrom lookup).
func (b *selfProduceBuilder) recordConfigMaps(relFile, baseNS, rootNS string, ks manifest.NamedResource) {
	objs, err := parseFile(filepath.Join(b.repoRoot, relFile), manifest.ParseDocOptions{WipeSecrets: true})
	if err != nil {
		return
	}
	for _, obj := range objs {
		cm, ok := obj.(*manifest.ConfigMap)
		if !ok {
			continue
		}
		ns := cmp.Or(baseNS, cm.Namespace, rootNS)
		if ns == "" {
			continue
		}
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: ns, Name: cm.Name}
		b.idx.byID[id] = appendUniqueProducer(b.idx.byID[id], ks)
	}
}

func appendUniqueProducer(dst []manifest.NamedResource, v manifest.NamedResource) []manifest.NamedResource {
	if slices.Contains(dst, v) {
		return dst
	}
	return append(dst, v)
}
