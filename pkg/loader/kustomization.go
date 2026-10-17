package loader

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// document caches a directory document or parsed file objects by source-view path.
// Consumers MUST never mutate its objects.
type document struct {
	body    *types.Kustomization
	file    string
	objects []manifest.BaseManifest
	loaded  bool
}

// documentReader caches positive and negative results by absolute source-view directory.
type documentReader struct {
	cache     map[string]document
	views     map[string]fs.FS
	artifacts map[string]*store.SourceArtifact
	regular   bool
}

func (r *documentReader) mountSources(s *store.Store) []string {
	mounted := map[string]*store.SourceArtifact{}
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket, manifest.KindExternalArtifact} {
		for _, obj := range s.ListObjects(manifest.SourceDomain, kind) {
			art, ok := s.GetArtifact(obj.Named()).(*store.SourceArtifact)
			if ok && art.LocalPath != "" {
				mounted[art.LocalPath] = art
			}
		}
	}
	if bootstrap, ok := s.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact); ok && bootstrap.LocalPath != "" {
		mounted[bootstrap.LocalPath] = bootstrap
	}
	var changed []string
	for root, art := range mounted {
		if r.artifacts[root] != art {
			changed = append(changed, root)
		}
	}
	for root := range r.artifacts {
		if mounted[root] == nil {
			changed = append(changed, root)
			delete(r.views, root)
		}
	}
	for _, root := range changed {
		for file := range r.cache {
			if file == root || strings.HasPrefix(file, root+string(filepath.Separator)) {
				delete(r.cache, file)
			}
		}
	}
	if len(mounted) != 0 && r.views == nil {
		r.views = map[string]fs.FS{}
	}
	for root, art := range mounted {
		if r.artifacts[root] == art {
			continue
		}
		r.views[root] = art.FS
		if art.FS == nil {
			r.views[root] = unavailableSource{}
		}
	}
	r.artifacts = mounted
	slices.Sort(changed)
	return changed
}

func (l *Loader) mountSources() {
	changed := l.documents.mountSources(l.Store)
	if len(changed) == 0 {
		return
	}
	if l.scopes != nil {
		ready := l.scopes.ready
		l.scopes = nil
		l.ensureScopes()
		for state := range ready {
			if state == l.sourceState(state.id) {
				l.scopes.ready[state] = struct{}{}
			}
		}
	}
	for _, root := range changed {
		if native, ok := l.documents.views[root].(*gittree.FS); ok {
			l.documents.views[root] = native.OperationView()
		}
		for id, r := range l.Index.Files() {
			if r.Path == root || strings.HasPrefix(r.Path, root+string(filepath.Separator)) {
				delete(l.Index.Records, id)
				l.Store.DeleteObject(id)
			}
		}
		for file := range l.Index.Errors {
			abs := filepath.Join(l.SourceRoot, filepath.FromSlash(file))
			if abs == root || strings.HasPrefix(abs, root+string(filepath.Separator)) {
				delete(l.Index.Errors, file)
			}
		}
	}
}

type unavailableSource struct{}

func (unavailableSource) Open(string) (fs.File, error) {
	return nil, fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)
}

// input binds source coordinates to their filesystem; an absent mounted file
// MUST never fall through to the working tree.
func (r documentReader) input(file string) (fs.FS, string) {
	if root := r.mountedRoot(file); root != "" {
		input := r.views[root]
		rel, _ := filepath.Rel(root, file)
		if input == nil {
			input = unavailableSource{}
		}
		return input, filepath.ToSlash(rel)
	}
	return unavailableSource{}, "."
}

func (r documentReader) mountedRoot(file string) string {
	for root := file; ; root = filepath.Dir(root) {
		if _, exists := r.views[root]; exists {
			return root
		}
		if root == filepath.Dir(root) {
			return ""
		}
	}
}

func (r *documentReader) read(dir string) (*types.Kustomization, string) {
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
		var k types.Kustomization
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

func isKustomizeComponent(k *types.Kustomization) bool {
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
func dataFilesFor(dir string, k *types.Kustomization) map[string]struct{} {
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
