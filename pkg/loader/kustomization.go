package loader

import (
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/yaml"
)

// kustomization is the subset of sigs.k8s.io/kustomize/api/types.Kustomization
// the loader inspects to decide what each tree-walk file is — a manifest,
// a Component fragment, or a generator data source. Adding fields here
// is the supported way to widen the loader's understanding of kustomize
// declarations; we deliberately do not import the upstream type to
// avoid pulling its full transitive dep tree into the load path.
type kustomization struct {
	APIVersion         string            `json:"apiVersion"                   yaml:"apiVersion"`
	Kind               string            `json:"kind"                         yaml:"kind"`
	Namespace          string            `json:"namespace,omitempty"          yaml:"namespace,omitempty"`
	Resources          []string          `json:"resources,omitempty"          yaml:"resources,omitempty"`
	Components         []string          `json:"components,omitempty"         yaml:"components,omitempty"`
	ConfigMapGenerator []kvPairGenerator `json:"configMapGenerator,omitempty" yaml:"configMapGenerator,omitempty"`
	SecretGenerator    []kvPairGenerator `json:"secretGenerator,omitempty"    yaml:"secretGenerator,omitempty"`
}

// kustomizeAPIPrefix is the kustomize.config.k8s.io API group; both
// regular Kustomizations and Components live here. A `kind: Component`
// outside this prefix is a different CR that happens to share the
// kind name (e.g. a custom operator's Component CR) and must NOT be
// silently skipped by descend.
const kustomizeAPIPrefix = "kustomize.config.k8s.io/"

// isKustomizeComponent reports whether k is a kustomize Component
// declaration (kind=Component AND kustomize.config.k8s.io apiVersion).
// Kind alone isn't enough: a YAML with `kind: Component` and a
// different apiVersion is a foreign CR and must not be treated as a
// Component fragment.
func (k *kustomization) isKustomizeComponent() bool {
	if k == nil || k.Kind != "Component" {
		return false
	}
	return strings.HasPrefix(k.APIVersion, kustomizeAPIPrefix)
}

// kvPairGenerator captures one configMap or secret generator entry.
// Name + Namespace + Literals participate in flate's generator
// discovery (see generators.go); Files / Envs are tracked so the
// outer kustomization can exclude them from resource scans.
type kvPairGenerator struct {
	Name      string   `json:"name,omitempty"      yaml:"name,omitempty"`
	Namespace string   `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Literals  []string `json:"literals,omitempty"  yaml:"literals,omitempty"`
	Files     []string `json:"files,omitempty"     yaml:"files,omitempty"`
	Envs      []string `json:"envs,omitempty"      yaml:"envs,omitempty"`
}

// kustomizationFileNames is the set of filenames the loader treats as a
// kustomization root during its filesystem walk. The first match in a
// directory wins.
//
// NOTE: this list intentionally differs from manifest.KustomizeBuilderFilenames:
//   - it includes "kustomization.json" so flate's generic YAML/JSON load path
//     can discover kustomization roots stored as JSON (kustomize also accepts
//     this format at build time even though it is rarely used in the wild).
//   - it omits the bare "Kustomization" name (no extension) which kustomize
//     build supports but which the loader never encounters in practice —
//     keeping the loader's list minimal avoids false-positive directory matches.
var kustomizationFileNames = [3]string{
	"kustomization.yaml",
	"kustomization.yml",
	"kustomization.json",
}

// readKustomization opens dir/kustomization.{yaml,yml,json} and decodes
// the subset of fields the loader uses. Returns nil when no
// kustomization file exists in dir or when decode fails — the latter
// is treated as "absent" because krusty's render path will surface the
// real error later with better context than the loader can.
func readKustomization(dir string) *kustomization {
	for _, name := range kustomizationFileNames {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path) //nolint:gosec // path joined under the user-supplied scan root
		if err != nil {
			continue
		}
		var k kustomization
		if err := yaml.Unmarshal(data, &k); err != nil {
			continue
		}
		return &k
	}
	return nil
}

// dataFilesFor returns the set of file paths declared as
// configMapGenerator/secretGenerator data in k, resolved against
// dir. The caller's walkKustomize uses this to exclude generator
// inputs from the resource-scan — kustomize reads them at render
// time and they aren't Flux manifests.
//
// When a path appears in both the generator list AND k.Resources,
// the resource interpretation wins (the data exclusion is dropped):
// kustomize doesn't forbid the overlap, and the resource
// declaration is the more authoritative "this IS a Flux manifest"
// signal. The overlap-resolve runs per-kustomization, scoped to one
// directory — there's no cross-tree state to manage anymore.
func dataFilesFor(dir string, k *kustomization) map[string]struct{} {
	if k == nil || (len(k.ConfigMapGenerator) == 0 && len(k.SecretGenerator) == 0) {
		return nil
	}
	resources := map[string]struct{}{}
	for _, r := range k.Resources {
		if abs, ok := resolveDataPath(dir, r); ok {
			resources[abs] = struct{}{}
		}
	}
	data := map[string]struct{}{}
	addEntries := func(entries []string, parseKey bool) {
		for _, e := range entries {
			p := e
			if parseKey {
				p = stripFileEntryKey(e)
			}
			abs, ok := resolveDataPath(dir, p)
			if !ok {
				continue
			}
			if _, isRes := resources[abs]; isRes {
				continue
			}
			data[abs] = struct{}{}
		}
	}
	for _, g := range k.ConfigMapGenerator {
		addEntries(g.Files, true)
		addEntries(g.Envs, false)
	}
	for _, g := range k.SecretGenerator {
		addEntries(g.Files, true)
		addEntries(g.Envs, false)
	}
	return data
}

// stripFileEntryKey returns the path portion of a kustomize
// configMapGenerator/secretGenerator `files:` entry. Per the spec
// (sigs.k8s.io/kustomize/api/types/kvsources.go), entries take the
// form "[KEY=]PATH"; the first `=` separates an optional ConfigMap
// key from the on-disk path. Entries with no `=` are pure paths.
func stripFileEntryKey(s string) string {
	if _, after, ok := strings.Cut(s, "="); ok {
		return after
	}
	return s
}

// resolveDataPath joins rel against base and cleans the result. An
// empty rel is rejected (kustomize would reject it too). Absolute
// paths pass through verbatim — kustomize doesn't forbid them and
// some generated manifests use them. Relative paths that escape
// base via `..` are rejected: the resolved key is only consulted
// against tree-walk paths today (no escape can match a walked path
// rooted at --path), but a future caller that opens the path would
// otherwise hit a TOCTOU + path-traversal surface for free.
func resolveDataPath(base, rel string) (string, bool) {
	if rel == "" {
		return "", false
	}
	if filepath.IsAbs(rel) {
		return filepath.Clean(rel), true
	}
	abs := filepath.Clean(filepath.Join(base, rel))
	cleanBase := filepath.Clean(base) + string(filepath.Separator)
	if abs != filepath.Clean(base) && !strings.HasPrefix(abs+string(filepath.Separator), cleanBase) {
		return "", false
	}
	return abs, true
}

// resolveComponentPath is the components: counterpart to
// resolveDataPath. kustomize's `components:` legitimately escapes the
// calling kustomization's directory (e.g. `../components/cluster-
// settings` is the canonical Flux pattern), so the in-base
// constraint resolveDataPath enforces is too strict for this field.
// We still reject URLs and absolute paths — flate's loader only
// follows local relative components.
func resolveComponentPath(base, rel string) (string, bool) {
	if rel == "" || filepath.IsAbs(rel) || strings.Contains(rel, "://") {
		return "", false
	}
	return filepath.Clean(filepath.Join(base, rel)), true
}

