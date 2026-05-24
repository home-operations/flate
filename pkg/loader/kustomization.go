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

// collectGeneratorDataFiles walks root and decodes every kustomization
// file it finds, returning the set of absolute file paths declared as
// configMapGenerator/secretGenerator data sources (files + envs).
//
// Files that are ALSO declared in `resources:` are removed from the
// set so a resource-and-data conflict still loads as a manifest: the
// `resources:` entry is an authoritative "this IS a Flux manifest"
// declaration, and the kustomize spec doesn't actually forbid the
// same path from appearing in both lists (though it'd be unusual).
//
// Honors ctx cancellation and the same shouldSkipDir rules as the
// main walk so we don't descend into `.git`, `templates/`, Component
// dirs, or ignore-matched paths.
func collectGeneratorDataFiles(ctx context.Context, root string, ignore *ignoreSet) (map[string]struct{}, error) {
	data := map[string]struct{}{}
	resources := map[string]struct{}{}
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
		if !isKustomizationFileName(d.Name()) {
			return nil
		}
		k := readKustomization(filepath.Dir(path))
		if k == nil {
			return nil
		}
		base := filepath.Dir(path)
		for _, r := range k.Resources {
			if abs, ok := resolveDataPath(base, r); ok {
				resources[abs] = struct{}{}
			}
		}
		addEntries := func(entries []string, parseKey bool) {
			for _, e := range entries {
				p := e
				if parseKey {
					p = stripFileEntryKey(e)
				}
				if abs, ok := resolveDataPath(base, p); ok {
					data[abs] = struct{}{}
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
	for r := range resources {
		delete(data, r)
	}
	if len(data) > 0 {
		slog.Debug("loader: data files excluded from manifest scan", "count", len(data))
	}
	return data, nil
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

// isKustomizationFileName reports whether name matches one of the
// three kustomize-recognized filenames.
func isKustomizationFileName(name string) bool {
	return slices.Contains(kustomizationFileNames, name)
}
