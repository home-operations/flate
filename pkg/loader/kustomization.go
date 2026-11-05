package loader

import (
	"errors"
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
// Consumers MUST never mutate its objects. Pointer fields precede flags to keep map values inline.
type document struct {
	body          *types.Kustomization
	file          string
	coverage      manifest.DocumentRole
	possible      bool
	objects       []parsedObject
	decodeErr     error
	built         []store.Child
	builtInput    *BuiltInput
	lookup        map[manifest.NamedResource]store.Child
	plugins       *[]pluginInputs
	selected      bool
	eligible      fileEligibility
	rootKnown     bool
	rootAdmitted  bool
	terminal      bool
	loaded        bool
	scanExcluded  bool
	bootstrap     bool
	initial       bool
	traversed     bool
	adHoc         bool
	buildExcluded bool
	incoming      bool
	edgesKnown    bool
	pathState     pathState
	searched      bool
}

type parsedObject struct {
	store.Child

	outcome  manifest.DocOccurrence
	item     int
	fact     *Occurrence
	version  string
	declared string
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
//
//nolint:recvcheck // SecureJoin consumes Lstat and Readlink from values alongside pointer-only cache updates.
type documentReader struct {
	projections *manifest.ProducerProjections
	cache       map[string]document
	// Views MUST stay shared when discovery copies a reader.
	views      map[string]fs.FS
	artifacts  map[string]*store.SourceArtifact
	parsed     *ParsedInputs
	compact    bool
	intent     manifest.DocumentRole
	coordinate *OccurrenceID
	regular    bool
	changed    []string
	bootstrap  string
	edgesDirty bool
}

func (r *documentReader) scanExcluded(file string) bool {
	dr := *r
	for {
		doc := dr.cache[file]
		if doc.loaded || doc.selected {
			return false
		}
		if doc.scanExcluded {
			return true
		}
		parent := filepath.Dir(file)
		if parent == file {
			return false
		}
		file = parent
	}
}

type sourceReader interface {
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
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
				mountSource(mounted, s.GetArtifact(obj.Named(), store.ArtifactContent))
			}
		}
	}
	for _, index := range candidates {
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for id, record := range index.Records {
			if id.KindInfo().SourceArtifact && record.File != "" {
				mountSource(mounted, s.GetArtifact(id, store.ArtifactContent))
			}
		}
	}
	artifact := s.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	mountSource(mounted, artifact)
	if bootstrap, ok := artifact.(*store.SourceArtifact); ok {
		r.bootstrap = bootstrap.LocalPath
	}
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
	r.invalidateSourceDocuments(changed)
	if len(changed) != 0 {
		r.edgesDirty = true
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for file, doc := range r.cache {
			doc.incoming, doc.edgesKnown = false, false
			r.cache[file] = doc
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

func (r *documentReader) invalidateSourceDocuments(changed []string) {
	for _, root := range changed {
		for file := range r.cache {
			if file == root || strings.HasPrefix(file, root+string(filepath.Separator)) {
				cached := r.cache[file]
				if cached.selected || cached.built != nil {
					r.cache[file] = document{
						selected:   cached.selected,
						built:      cached.built,
						builtInput: cached.builtInput,
					}
				} else {
					delete(r.cache, file)
				}
			}
		}
	}
}

func (l *Loader) refreshSources() {
	l.documents.projections = l.Options.Projections
	if l.documents.projections == nil {
		l.documents.projections = &l.projections
	}
	l.documents.parsed = nil
	if inputs := l.Options.ParsedInputs; inputs != nil &&
		inputs.compact == l.documents.compact {
		l.documents.parsed = inputs
	}
	changed := l.documents.mountSources(l.Store, l.Index)
	if len(changed) == 0 {
		return
	}
	for _, root := range changed {
		if len(l.Index.Records) != 0 {
			l.documents.changed = appendUniqueProducer(l.documents.changed, root)
		}
		if native, ok := l.documents.views[root].(*gittree.FS); ok {
			l.documents.views[root] = native.OperationView()
		}
	}
	l.resetAdmissions()
	if l.scopes != nil {
		previous := l.scopes
		l.scopes = nil
		l.ensureScopes()
		{
			scopeValues := previous.sources
			for scopeIndex := range scopeValues {
				scope := scopeValues[scopeIndex]
				if scope.sourceState == l.sourceState(scope.id, scope.repoRoot) {
					l.scopes.sources = append(l.scopes.sources, scope)
				}
			}
		}
		*previous = scopeConstruction{}
	}
}

func (l *Loader) mountSources() {
	l.refreshSources()
	for _, root := range l.documents.changed {
		//nolint:gocritic // Filtered iterator values remain snapshots during record deletion.
		for id, r := range l.Index.Files() {
			if r.Path == root || strings.HasPrefix(r.Path, root+string(filepath.Separator)) {
				delete(l.Index.Records, id)
				delete(l.candidates, id)
				delete(l.locations, id)
				l.Store.DeleteObject(id)
			}
		}
	}
	l.documents.changed = l.documents.changed[:0]
}

var errSourceUnavailable = manifest.NewError(manifest.ErrInput, nil, "source artifact content unavailable")

type unavailableSource struct{}

func (unavailableSource) Open(string) (fs.File, error) {
	return nil, errSourceUnavailable
}

type fileInput struct {
	fs   fs.FS
	name string
}

// input binds source coordinates to their filesystem; an absent mounted file
// MUST never fall through to the working tree.
func (r *documentReader) input(file string) fileInput {
	dr := *r
	if root := new(dr).mountedRoot(file); root != "" {
		input := dr.views[root]
		rel, _ := filepath.Rel(root, file)
		if input == nil {
			input = unavailableSource{}
		}
		return fileInput{fs: input, name: filepath.ToSlash(rel)}
	}
	return fileInput{fs: unavailableSource{}, name: "."}
}

func (r *documentReader) mountedRoot(file string) string {
	dr := *r
	if len(dr.views) == 0 {
		return ""
	}
	for root := file; ; root = filepath.Dir(root) {
		if _, exists := dr.views[root]; exists {
			return root
		}
		if root == filepath.Dir(root) {
			return ""
		}
	}
}

type builderDocument struct {
	body *types.Kustomization
	file string
}

func (r *documentReader) read(dir string) builderDocument {
	r.restore(dir)
	cached, seen := r.cache[dir]
	if seen &&
		(cached.body != nil || cached.searched || cached.pathState == pathFile || cached.pathState == pathMissing) {
		return builderDocument{body: cached.body, file: cached.file}
	}
	if cached.objects != nil {
		cached.pathState = pathFile
		if r.cache != nil {
			r.cache[dir] = cached
		}
		return builderDocument{}
	}
	if cached.pathState == pathUnknown {
		inputFile := new(*r).input(dir)
		info, err := fs.Stat(inputFile.fs, inputFile.name)
		cached = new(*r).rememberStat(dir, info, err)
	}
	if cached.pathState == pathFile || cached.pathState == pathMissing {
		return builderDocument{}
	}
	return r.readBuilder(dir, new(cached))
}

func (r *documentReader) readBuilder(dir string, cachedValue *document) builderDocument {
	cached := *cachedValue
	cached.searched = true
	for _, name := range manifest.KustomizeBuilderFilenames {
		file := filepath.Join(dir, name)
		inputFile2 := new(*r).input(file)
		data, err := fs.ReadFile(inputFile2.fs, inputFile2.name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		var k types.Kustomization
		if err != nil || yaml.Unmarshal(data, &k) != nil {
			cached.file, cached.pathState = file, pathDirectory
			if r.cache != nil {
				r.cache[dir] = cached
			}
			return builderDocument{body: nil, file: file}
		}
		normalizeKustomization(&k)
		cached.body, cached.file, cached.pathState = &k, file, pathDirectory
		if r.cache != nil {
			r.cache[dir] = cached
		}
		return builderDocument{body: &k, file: file}
	}
	if r.cache != nil {
		r.cache[dir] = cached
	}
	return builderDocument{body: nil, file: cached.file}
}

func normalizeKustomization(k *types.Kustomization) {
	//nolint:staticcheck // Kustomize still accepts these deprecated input fields.
	fix := k.Kind == types.ComponentKind && k.APIVersion == "" ||
		len(k.Bases)+len(k.ImageTags)+len(k.HelmChartInflationGenerator) != 0
	{
		gValues := k.ConfigMapGenerator
		for gIndex := range gValues {
			g := gValues[gIndex]
			fix = fix || g.EnvSource != ""
		}
	}
	{
		gValues2 := k.SecretGenerator
		for gIndex2 := range gValues2 {
			g := gValues2[gIndex2]
			fix = fix || g.EnvSource != ""
		}
	}
	if fix {
		k.FixKustomization()
	}
}

func (r *documentReader) rememberStat(file string, info fs.FileInfo, err error) document {
	dr := *r
	cached := dr.cache[file]
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
	if dr.cache != nil {
		dr.cache[file] = cached
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
