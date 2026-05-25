package loader

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
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
	Kind               string            `json:"kind"                         yaml:"kind"`
	Resources          []string          `json:"resources,omitempty"          yaml:"resources,omitempty"`
	ConfigMapGenerator []kvPairGenerator `json:"configMapGenerator,omitempty" yaml:"configMapGenerator,omitempty"`
	SecretGenerator    []kvPairGenerator `json:"secretGenerator,omitempty"    yaml:"secretGenerator,omitempty"`
}

// kvPairGenerator captures the file/env entries of a configMap or
// secret generator. Literals are inline strings and stay out of scope.
type kvPairGenerator struct {
	Files []string `json:"files,omitempty" yaml:"files,omitempty"`
	Envs  []string `json:"envs,omitempty"  yaml:"envs,omitempty"`
}

// kustomizationFileNames is the ordered list kustomize checks; first
// match wins.
var kustomizationFileNames = []string{
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

// kustomizationScan is the result of walking the tree once to inspect
// every kustomization.yaml the loader cares about. The fields cover
// the three exclusion/inclusion rules the main walk applies:
//
//   - DataFiles: configMapGenerator / secretGenerator file paths.
//     Skipped because they're YAML/env data the chart loader handles
//     at render time, not Flux manifests.
//   - ClaimedResources: absolute paths declared in some
//     kustomization.yaml's resources list. Authoritative "this IS a
//     Flux manifest" inclusions — they survive the orphan-skip check
//     even if they happen to share a dir with other YAML.
//   - KustomizationDirs: directories that contain a kustomization.yaml
//     file. Used by the orphan check: a YAML file in a directory
//     governed by a kustomization.yaml must be referenced as a
//     resource to be loaded.
type kustomizationScan struct {
	DataFiles         map[string]struct{}
	ClaimedResources  map[string]struct{}
	KustomizationDirs map[string]struct{}
}

// collectKustomizationScan walks root and decodes every
// kustomization.yaml it finds, building the three sets the main
// loader uses to decide which YAML files to parse.
//
// Files declared as configMapGenerator/secretGenerator data are
// captured separately from files declared as resources. When a path
// appears in BOTH lists across the tree, the resource interpretation
// wins (the data exclusion is dropped) — kustomize doesn't forbid
// the duplication and the resource declaration is the more
// authoritative "this IS a Flux manifest" signal.
//
// Honors ctx cancellation and the same shouldSkipDir rules as the
// main walk so we don't descend into `.git`, `templates/`, Component
// dirs, or ignore-matched paths.
func collectKustomizationScan(ctx context.Context, root string, ignore *ignoreSet) (*kustomizationScan, error) {
	scan := &kustomizationScan{
		DataFiles:         map[string]struct{}{},
		ClaimedResources:  map[string]struct{}{},
		KustomizationDirs: map[string]struct{}{},
	}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if shouldSkipDir(d.Name(), path, root, ignore) {
				return filepath.SkipDir
			}
			return nil
		}
		if !slices.Contains(kustomizationFileNames, d.Name()) {
			return nil
		}
		k := readKustomization(filepath.Dir(path))
		if k == nil {
			return nil
		}
		base := filepath.Dir(path)
		scan.KustomizationDirs[base] = struct{}{}
		for _, r := range k.Resources {
			if abs, ok := resolveDataPath(base, r); ok {
				scan.ClaimedResources[abs] = struct{}{}
			}
		}
		addEntries := func(entries []string, parseKey bool) {
			for _, e := range entries {
				p := e
				if parseKey {
					p = stripFileEntryKey(e)
				}
				if abs, ok := resolveDataPath(base, p); ok {
					scan.DataFiles[abs] = struct{}{}
				}
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
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Resource interpretation wins over data interpretation if the same
	// path appears in both lists across any kustomization.yaml in the
	// tree.
	for r := range scan.ClaimedResources {
		delete(scan.DataFiles, r)
	}
	if len(scan.DataFiles) > 0 {
		slog.Debug("loader: data files excluded from manifest scan", "count", len(scan.DataFiles))
	}
	return scan, nil
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

