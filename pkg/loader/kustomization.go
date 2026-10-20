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
	for info := range manifest.Kinds() {
		if !info.SourceArtifact {
			continue
		}
		for _, obj := range s.ListObjects(info.Group, info.Kind) {
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
