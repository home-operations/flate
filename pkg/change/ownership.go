package change

import (
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
)

// ksClaim records a single (Flux Kustomization, path) tuple. A KS may
// claim multiple paths — its own spec.path plus every spec.components
// entry — and several KSes may claim the same path (a shared
// component).
type ksClaim struct {
	id   manifest.NamedResource
	path string // repo-relative, slash-separated, with trailing "/"
}

// ownershipIndex maps changed files to the Flux Kustomizations that
// would re-render in response. Built once per filter resolution.
type ownershipIndex struct{ claims []ksClaim }

// buildOwnership records every Flux KS's spec.path, spec.components,
// and any `components:` referenced from the kustomization.yaml at
// spec.path. Sorted longest-prefix first so lookup returns the most
// specific claimant. Kustomization-file reads are memoized by
// directory so KSes sharing a spec.path don't re-stat the same disk.
func buildOwnership(objs ObjectLister, repoRoot string) ownershipIndex {
	var claims []ksClaim
	add := func(id manifest.NamedResource, base, ref string) {
		if ref == "" || strings.Contains(ref, "://") || filepath.IsAbs(ref) {
			return
		}
		resolved := path.Clean(path.Join(base, ref))
		if resolved == "." || strings.HasPrefix(resolved, "..") {
			return
		}
		claims = append(claims, ksClaim{id: id, path: resolved + "/"})
	}
	componentCache := make(map[string][]string)
	for _, obj := range objs.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.Path == "" {
			continue
		}
		id := ks.Named()
		base := normalizeKSPath(ks.Path)
		claims = append(claims, ksClaim{id: id, path: base + "/"})
		for _, comp := range ks.Components {
			add(id, base, comp)
		}
		comps, ok := componentCache[base]
		if !ok {
			comps = readKustomizeComponents(repoRoot, base)
			componentCache[base] = comps
		}
		for _, comp := range comps {
			add(id, base, comp)
		}
	}
	slices.SortStableFunc(claims, func(a, b ksClaim) int { return len(b.path) - len(a.path) })
	return ownershipIndex{claims: claims}
}

// readKustomizeComponents returns the top-level `components:` field
// of the kustomization file at base (resolved relative to repoRoot).
func readKustomizeComponents(repoRoot, base string) []string {
	for _, name := range []string{"kustomization.yaml", "kustomization.yml", "Kustomization"} {
		data, err := os.ReadFile(filepath.Join(repoRoot, base, name)) //nolint:gosec // path composed from known cluster layout
		if err != nil {
			continue
		}
		var doc struct {
			Components []string `yaml:"components"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			continue
		}
		return doc.Components
	}
	return nil
}

// ownersOf returns every KS that claims the longest-matching prefix
// of file. Multiple KSes are possible when a shared component is in
// play.
func (idx ownershipIndex) ownersOf(file string) []manifest.NamedResource {
	if file == "" {
		return nil
	}
	file = file + "/" // so prefix matching catches whole-segment boundaries
	var bestLen int
	var owners []manifest.NamedResource
	for _, c := range idx.claims {
		if len(c.path) < bestLen {
			break // sorted longest-first; nothing shorter can beat
		}
		if !strings.HasPrefix(file, c.path) {
			continue
		}
		if len(c.path) > bestLen {
			bestLen = len(c.path)
			owners = owners[:0]
		}
		owners = append(owners, c.id)
	}
	return owners
}

// ancestorsOf returns every Kustomization whose claim is a strict
// prefix of file but shorter than the longest match (so excluding
// what ownersOf would return). Used by Filter.resolve to keep
// parent/meta Kustomizations in scope under changed-only mode —
// their render injects spec.patches and postBuild.substituteFrom
// onto children, and skipping them produces undefined-${VAR}
// failures when the leaf renders. See #58.
func (idx ownershipIndex) ancestorsOf(file string) []manifest.NamedResource {
	if file == "" {
		return nil
	}
	file = file + "/"
	var bestLen int
	var ancestors []manifest.NamedResource
	for _, c := range idx.claims {
		if !strings.HasPrefix(file, c.path) {
			continue
		}
		if bestLen == 0 {
			bestLen = len(c.path) // first (longest) match — skip
			continue
		}
		if len(c.path) < bestLen {
			ancestors = append(ancestors, c.id)
		}
	}
	return ancestors
}

// normalizeKSPath strips the conventional `./` prefix and any trailing
// slash so KS paths can be compared as plain string prefixes.
func normalizeKSPath(p string) string {
	p = strings.TrimPrefix(p, "./")
	return strings.TrimSuffix(p, "/")
}
