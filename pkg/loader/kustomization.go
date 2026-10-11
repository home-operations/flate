package loader

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// kustomizationDocument reuses upstream fields while retaining permissive
// replacement injection detection for singular and plural field path variants.
type kustomizationDocument struct {
	types.Kustomization
	Replacements []replacementDirective `json:"replacements,omitempty"`
}

// document caches a directory document or parsed file objects by source-view path.
// Consumers MUST never mutate its objects.
type document struct {
	body    *kustomizationDocument
	file    string
	objects []manifest.BaseManifest
}

// documentReader caches positive and negative results by absolute source-view directory.
type documentReader struct {
	cache map[string]document
	views map[string]fs.FS
}

func (r *documentReader) mountSources(s *store.Store) {
	for _, obj := range s.ListObjects(manifest.KindGitRepository) {
		art, ok := s.GetArtifact(obj.Named()).(*store.SourceArtifact)
		if !ok || art.FS == nil || art.LocalPath == "" {
			continue
		}
		if r.views == nil {
			r.views = map[string]fs.FS{}
		}
		r.views[art.LocalPath] = art.FS
	}
}

var workingFiles = os.DirFS("/")

// input binds source coordinates to their filesystem; an absent mounted file
// MUST never fall through to the working tree.
func (r documentReader) input(file string) (fs.FS, string) {
	if len(r.views) != 0 {
		for root := file; root != filepath.Dir(root); root = filepath.Dir(root) {
			if input := r.views[root]; input != nil {
				rel, _ := filepath.Rel(root, file)
				return input, filepath.ToSlash(rel)
			}
		}
	}
	root := filepath.VolumeName(file) + string(filepath.Separator)
	name := filepath.ToSlash(strings.TrimPrefix(file, root))
	if name == "" {
		name = "."
	}
	return workingFiles, name
}

func (r *documentReader) read(dir string) (*kustomizationDocument, string) {
	if cached, ok := r.cache[dir]; ok {
		return cached.body, cached.file
	}
	for _, name := range manifest.KustomizeBuilderFilenames {
		file := filepath.Join(dir, name)
		input, name := r.input(file)
		data, err := fs.ReadFile(input, name)
		if err != nil {
			continue
		}
		var k kustomizationDocument
		if yaml.Unmarshal(data, &k) != nil {
			continue
		}
		if r.cache != nil {
			r.cache[dir] = document{body: &k, file: file}
		}
		return &k, file
	}
	if r.cache != nil {
		r.cache[dir] = document{}
	}
	return nil, ""
}

func isKustomizeComponent(k *kustomizationDocument) bool {
	return k != nil && k.Kind == types.ComponentKind && strings.HasPrefix(k.APIVersion, manifest.KustomizeDomain+"/")
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
func dataFilesFor(dir string, k *kustomizationDocument) map[string]struct{} {
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
		addEntries(g.FileSources, true)
		addEntries(g.EnvSources, false)
	}
	for _, g := range k.SecretGenerator {
		addEntries(g.FileSources, true)
		addEntries(g.EnvSources, false)
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

// resolveDataPath joins rel against base and cleans the result, for
// references that get *opened and read*: configMapGenerator /
// secretGenerator data files and file `resources:`. An empty rel is
// rejected (kustomize would reject it too). Absolute paths pass
// through verbatim — kustomize doesn't forbid them and some generated
// manifests use them. Relative paths that escape base via `..` are
// rejected: a path that gets opened would otherwise hand a TOCTOU +
// path-traversal surface for free. Directory `resources:` and
// `components:` are merely descended into, never opened, so they use
// the escape-permitting resolvePath.
func resolveDataPath(base, rel string) (string, bool) {
	if rel == "" {
		return "", false
	}
	if filepath.IsAbs(rel) {
		return filepath.Clean(rel), true
	}
	cleanBase := filepath.Clean(base)
	abs := filepath.Clean(filepath.Join(base, rel))
	if abs != cleanBase && !strings.HasPrefix(abs+string(filepath.Separator), cleanBase+string(filepath.Separator)) {
		return "", false
	}
	return abs, true
}

// resolvePath joins rel under base, rejecting empty, absolute, and URL
// refs (flate's loader only follows local relative paths). It permits
// parent-directory escaping — both `components:` and directory
// `resources:` legitimately reference `../...` and kustomize follows
// them.
func resolvePath(base, rel string) (string, bool) {
	if rel == "" || filepath.IsAbs(rel) || strings.Contains(rel, "://") {
		return "", false
	}
	return filepath.Clean(filepath.Join(base, rel)), true
}
