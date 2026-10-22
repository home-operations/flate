package loader

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// document caches a directory document or parsed file objects by source-view path.
// Consumers MUST never mutate its objects.
type document struct {
	body          *types.Kustomization
	file          string
	objects       []store.Child
	built         []store.Child
	selected      bool
	lookup        map[manifest.NamedResource]store.Child
	plugins       *[]pluginInputs
	loaded        bool
	bootstrap     bool
	traversed     bool
	buildExcluded bool
	pathState     pathState
	searched      bool
}

type pathState uint8

const (
	pathUnknown pathState = iota
	pathDirectory
	pathFile
	pathMissing
	pathUnavailable
)

// documentReader caches positive and negative results by absolute source-view directory.
type documentReader struct {
	cache     map[string]document
	views     map[string]fs.FS
	artifacts map[string]*store.SourceArtifact
	regular   bool
}

type sourceReader interface {
	GetArtifact(manifest.NamedResource) store.Artifact
	ListObjects(string, string) []manifest.BaseManifest
}

func mountSource(mounted map[string]*store.SourceArtifact, artifact store.Artifact) {
	if art, ok := artifact.(*store.SourceArtifact); ok && art.LocalPath != "" {
		mounted[art.LocalPath] = art
	}
}

func (r *documentReader) mountSources(s sourceReader, candidates ...*manifest.DiscoveryIndex) []string {
	mounted := map[string]*store.SourceArtifact{}
	for info := range manifest.Kinds() {
		if info.SourceArtifact {
			for _, obj := range s.ListObjects(info.Group, info.Kind) {
				mountSource(mounted, s.GetArtifact(obj.Named()))
			}
		}
	}
	for _, index := range candidates {
		for id, record := range index.Records {
			if id.KindInfo().SourceArtifact && record.File != "" {
				mountSource(mounted, s.GetArtifact(id))
			}
		}
	}
	mountSource(mounted, s.GetArtifact(manifest.BootstrapSourceID))
	return r.setSources(mounted)
}

func (r *documentReader) setSources(mounted map[string]*store.SourceArtifact) []string {
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
				cached := r.cache[file]
				if cached.selected || cached.built != nil {
					r.cache[file] = document{selected: cached.selected, built: cached.built}
				} else {
					delete(r.cache, file)
				}
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
	changed := l.documents.mountSources(l.Store, l.Index)
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
				delete(l.candidates, id)
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

var errSourceUnavailable = fmt.Errorf("%w: source artifact content unavailable", manifest.ErrInput)

type unavailableSource struct{}

func (unavailableSource) Open(string) (fs.File, error) {
	return nil, errSourceUnavailable
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
	cached, seen := r.cache[dir]
	if seen && (cached.body != nil || cached.searched || cached.pathState == pathFile || cached.pathState == pathMissing) {
		return cached.body, cached.file
	}
	if cached.objects != nil {
		cached.pathState = pathFile
		if r.cache != nil {
			r.cache[dir] = cached
		}
		return nil, ""
	}
	if cached.pathState == pathUnknown {
		input, name := r.input(dir)
		info, err := fs.Stat(input, name)
		cached = r.rememberStat(dir, info, err)
	}
	if cached.pathState == pathFile || cached.pathState == pathMissing {
		return nil, ""
	}
	cached.searched = true
	for _, name := range manifest.KustomizeBuilderFilenames {
		file := filepath.Join(dir, name)
		input, name := r.input(file)
		data, err := fs.ReadFile(input, name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		var k types.Kustomization
		if err != nil || yaml.Unmarshal(data, &k) != nil {
			cached.file, cached.pathState = file, pathDirectory
			if r.cache != nil {
				r.cache[dir] = cached
			}
			return nil, file
		}
		//nolint:staticcheck // Kustomize still accepts these deprecated input fields.
		fix := k.Kind == types.ComponentKind && k.APIVersion == "" || len(k.Bases)+len(k.ImageTags)+len(k.HelmChartInflationGenerator) != 0
		for _, g := range k.ConfigMapGenerator {
			fix = fix || g.EnvSource != ""
		}
		for _, g := range k.SecretGenerator {
			fix = fix || g.EnvSource != ""
		}
		if fix {
			k.FixKustomization()
		}
		cached.body, cached.file, cached.pathState = &k, file, pathDirectory
		if r.cache != nil {
			r.cache[dir] = cached
		}
		return &k, file
	}
	if r.cache != nil {
		r.cache[dir] = cached
	}
	return nil, cached.file
}

func (r documentReader) rememberStat(file string, info fs.FileInfo, err error) document {
	cached := r.cache[file]
	switch {
	case err == nil && info.IsDir():
		cached.pathState = pathDirectory
	case err == nil:
		cached.pathState = pathFile
	case errors.Is(err, fs.ErrNotExist):
		cached.pathState = pathMissing
	default:
		cached.pathState = pathUnavailable
	}
	if r.cache != nil {
		r.cache[file] = cached
	}
	return cached
}

func isKustomizeComponent(k *types.Kustomization) bool {
	return k != nil && k.Kind == types.ComponentKind && strings.HasPrefix(k.APIVersion, manifest.KustomizeDomain+"/")
}

type pluginInputs struct {
	references          []kustomizeinput.Reference
	known, metadataOnly bool
}
