package loader

import (
	"errors"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/core/kustpath"
	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/kustomize/kustomizeinput"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// document caches a directory document or parsed file objects by source-view path.
// Consumers MUST never mutate its objects. Pointer fields precede flags to keep map values inline.
type document struct {
	declarationsData []byte
	declarations     *fileObservation
	deferred         *cachedInput
	projection       *projectionInputs
	parsed           *parsedDocument
	body             *types.Kustomization
	file             string
	sourceRoot       string
	coverage         manifest.DocumentRole
	unprepared       bool
	possible         bool
	objects          []parsedObject
	decodeErr        error
	lookup           map[manifest.NamedResource]store.Child
	pathInputs       map[manifest.NamedResource]manifest.Data
	plugins          *[]pluginInputs
	rootApplications map[manifest.NamedResource]rootApplication
	rootOwners       []selection.RootOwnerFacts
	rootPeers        [][]manifest.NamedResource
	selected         bool
	eligible         selection.RootEligibility
	rootKnown        bool
	rootAdmitted     bool
	rootNative       bool
	terminal         bool
	loaded           bool
	scanExcluded     bool
	bootstrap        bool
	initial          bool
	traversed        bool
	adHoc            bool
	buildExcluded    bool
	incoming         bool
	edgesKnown       bool
	pathState        pathState
	recipe           recipeClass
	orderingConfig   orderingConfiguration
	searched         bool
}

type fileObservation struct {
	manifest.FileDeclarations

	payload inputPayload
	err     error
	limited bool
}

type parsedObject struct {
	store.Child

	payload *inputPayload

	outcome            manifest.DocOccurrence
	item               int
	collection         bool
	fact               *authoredOccurrence
	version            string
	substituteDisabled bool
}

type pathState uint8

const (
	pathUnknown pathState = iota
	pathDirectory
	pathFile
	pathMissing
	pathStatFailed
	pathSourceUnavailable
)

func (s pathState) excludesBuilder() bool {
	return s == pathFile || s == pathMissing || s == pathSourceUnavailable
}

// documentReader caches positive and negative results by absolute source-view directory.
//
//nolint:recvcheck // SecureJoin consumes Lstat and Readlink from values alongside pointer-only cache updates.
type documentReader struct {
	configured  *store.SourceArtifact
	projections *manifest.ProducerProjections
	cache       map[string]document
	// Views MUST stay shared when discovery copies a reader.
	views            map[string]fs.FS
	artifacts        map[string]*store.SourceArtifact
	workingTree      map[string]*store.SourceArtifact
	parsed           *ParsedInputs
	compact          bool
	eager            bool
	recordInputs     bool
	declarationsOnly bool
	skipDeclarations bool
	intent           manifest.DocumentRole
	coordinate       *occurrenceID
	regular          bool
	changed          []string
	bootstrap        string
	edgesDirty       bool
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

func mountSource(
	mounted map[string]*store.SourceArtifact, artifact store.Artifact,
) *store.SourceArtifact {
	art, _ := artifact.(*store.SourceArtifact)
	if mounted != nil && art != nil && art.LocalPath != "" {
		mounted[art.LocalPath] = art
	}
	return art
}

// AdmittedWorkingTreeMounts lets the last eligible ID in identity order win at each path.
// Callers can also supply the general source table to index all admitted artifacts in the same pass.
func AdmittedWorkingTreeMounts(
	getArtifact func(manifest.NamedResource, store.ArtifactRead) store.Artifact,
	mounted map[string]*store.SourceArtifact,
	admitted ...manifest.NamedResource,
) map[string]*store.SourceArtifact {
	var scratch [8]manifest.NamedResource
	ids := append(scratch[:0], admitted...)
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	var workingTree map[string]*store.SourceArtifact
	for _, id := range ids {
		art := mountSource(mounted, getArtifact(id, store.ArtifactContent))
		if !art.WorkingTree() || art.LocalPath == "" {
			continue
		}
		if workingTree == nil {
			workingTree = map[string]*store.SourceArtifact{}
		}
		workingTree[art.LocalPath] = art
	}
	return workingTree
}

func (r *documentReader) mountSources(
	s sourceReader,
	input *store.SourceArtifact,
	index *manifest.DiscoveryIndex,
	admitted ...manifest.NamedResource,
) []string {
	mounted := map[string]*store.SourceArtifact{}
	r.workingTree = AdmittedWorkingTreeMounts(s.GetArtifact, mounted, admitted...)
	for info := range manifest.Kinds() {
		if !info.SourceArtifact {
			continue
		}
		for _, obj := range s.ListObjects(info.Group, info.Kind) {
			mountSource(mounted, s.GetArtifact(obj.Named(), store.ArtifactContent))
		}
	}
	if index != nil {
		for id := range index.Records {
			if id.KindInfo().SourceArtifact && index.Records[id].File != "" {
				mountSource(mounted, s.GetArtifact(id, store.ArtifactContent))
			}
			for _, ref := range index.Records[id].Refs {
				mountSource(mounted, s.GetArtifact(ref, store.ArtifactContent))
			}
		}
	}
	bootstrap := mountSource(mounted, s.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent))
	if bootstrap != nil && bootstrap.Provider == store.ProviderInput {
		r.bootstrap = bootstrap.LocalPath
	}
	if r.configured != nil && mounted[r.configured.LocalPath] == nil {
		mountSource(mounted, r.configured)
		r.bootstrap = r.configured.LocalPath
	}
	mountSource(mounted, input)
	return r.setSources(mounted)
}

func (r *documentReader) setSources(mounted map[string]*store.SourceArtifact) []string {
	var changed []string
	for root, art := range mounted {
		if !r.artifacts[root].SameMaterial(art, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) {
			changed = append(changed, root)
		}
	}
	for root := range r.artifacts {
		if mounted[root] == nil {
			changed = append(changed, root)
		}
	}
	if len(changed) != 0 && r.declarationsOnly {
		// Failed completion MUST retain the documents of the previously mounted material.
		r.cache = maps.Clone(r.cache)
		r.views = maps.Clone(r.views)
	}
	r.invalidateSources(changed, mounted)
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
		if r.artifacts[root].SameMaterial(art, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) {
			continue
		}
		r.views[root] = art.FS
		if overlay, ok := art.FS.(*inputprojection.Overlay); ok {
			r.views[root] = overlay.Authored.FS
		}
		if art.FS == nil {
			r.views[root] = unavailableSource{}
		}
	}
	r.artifacts = mounted
	slices.Sort(changed)
	return changed
}

func (r *documentReader) invalidateSources(changed []string, mounted map[string]*store.SourceArtifact) {
	for _, root := range changed {
		if mounted[root] == nil {
			delete(r.views, root)
		}
		for file := range r.cache {
			if kustpath.RelateHost(root, file).Contains() {
				if r.cache[file].selected {
					r.cache[file] = document{selected: true}
				} else {
					delete(r.cache, file)
				}
			}
		}
	}
}

func (l *Loader) refreshSources(input *store.SourceArtifact, admitted ...manifest.NamedResource) {
	l.documents.projections = l.Options.Projections
	if l.documents.projections == nil {
		l.documents.projections = &l.projections
	}
	l.documents.parsed = nil
	if inputs := l.Options.ParsedInputs; inputs != nil &&
		inputs.compact == l.documents.compact {
		l.documents.parsed = inputs
	}
	l.documents.configured = l.Options.ConfiguredInput
	changed := l.documents.mountSources(l.Store, input, l.Index, admitted...)
	if len(changed) == 0 {
		return
	}
	if len(l.Index.Declarations) != 0 {
		//nolint:errcheck // Encoded document errors remain pending until FinalizeIndex can report them.
		defer l.indexDeclarations()
	}
	for _, root := range changed {
		l.roots.removeSource(root)
		if len(l.Index.Records) != 0 {
			l.documents.changed = appendUniqueProducer(l.documents.changed, root)
		}
		if native, ok := l.documents.views[root].(*gittree.FS); ok {
			l.documents.views[root] = native.OperationView()
		}
	}
	l.resetAdmissions()
	if l.scopes == nil {
		return
	}
	previous := l.scopes
	l.scopes = nil
	l.ensureScopes()
	{
		scopeValues := previous.sources
		for scopeIndex := range scopeValues {
			scope := scopeValues[scopeIndex]
			if scope.sourceState.matches(new(l.sourceState(scope.id, scope.repoRoot))) {
				l.scopes.sources = append(l.scopes.sources, scope)
			}
		}
	}
	*previous = scopeConstruction{}
}

func (l *Loader) mountSources(admitted ...manifest.NamedResource) {
	l.refreshSources(l.provisionalInput, admitted...)
	if l.provisionalInput != nil {
		return
	}
	for _, root := range l.documents.changed {
		//nolint:gocritic // Filtered iterator values remain snapshots during record deletion.
		for id, r := range l.Index.Files() {
			if kustpath.RelateHost(root, r.Path).Contains() {
				l.DiscardOccurrence(id)
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
	return selectMount(file, r.cache[file].sourceRoot, r.views)
}

func selectMount[T any](file, cachedRoot string, mounts map[string]T) string {
	if len(mounts) == 0 {
		return ""
	}
	if _, ok := mounts[cachedRoot]; cachedRoot != "" && ok {
		return cachedRoot
	}
	selected := ""
	for root := range mounts {
		if !kustpath.RelateHost(root, file).Contains() {
			continue
		}
		relation := kustpath.RelateHost(selected, root)
		if selected == "" || relation == kustpath.HostAncestor || relation == kustpath.HostEqual && root < selected {
			selected = root
		}
	}
	return selected
}

type builderDocument struct {
	body   *types.Kustomization
	file   string
	recipe recipeClass
}

func (r *documentReader) read(dir string) builderDocument {
	cached := r.cache[dir]
	complete := cached.body != nil || cached.searched ||
		cached.pathState.excludesBuilder()
	if !complete || cached.deferred != nil {
		r.restore(dir)
		cached = r.cache[dir]
		complete = cached.body != nil || cached.searched ||
			cached.pathState.excludesBuilder()
	}
	if complete {
		return builderDocument{body: cached.body, file: cached.file, recipe: cached.recipe}
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
	if cached.pathState.excludesBuilder() {
		return builderDocument{}
	}
	return r.readBuilder(dir, new(cached))
}

func (r *documentReader) readBuilder(dir string, cachedValue *document) builderDocument {
	cached := *cachedValue
	cached.searched = true
	var found bool
	var inspected gittree.InspectedFile
	for file := range kustomizeinput.Builders(dir, func(file string) bool {
		input := r.input(file)
		info, opened, err := gittree.InspectFile(input.fs, input.name)
		inspected = opened
		return kustomizeinput.BuilderFile(info, err)
	}) {
		if found {
			cached.recipe &^= recipeResourceOnly
			break
		}
		found = true
		builder, err := inspected.Open()
		var data []byte
		if err == nil {
			data, err = io.ReadAll(builder)
			err = errors.Join(err, builder.Close())
		}
		var k types.Kustomization
		if err != nil || yaml.Unmarshal(data, &k) != nil {
			cached.file, cached.pathState = file, pathDirectory
			if r.cache != nil {
				r.cache[dir] = cached
			}
			return builderDocument{body: nil, file: file}
		}
		cached.recipe = r.classifyRecipe(dir, data, &k)
		normalizeKustomization(&k)
		cached.body, cached.file, cached.pathState = &k, file, pathDirectory
		if cached.recipe&recipeResourceOnly == 0 {
			break
		}
	}
	if r.cache != nil {
		r.cache[dir] = cached
	}
	return builderDocument{body: cached.body, file: cached.file, recipe: cached.recipe}
}

type recipeClass uint8

const (
	recipeResourceOnly recipeClass = 1 << iota
	recipeOpaquePluginInputs
	recipeTransformers
	recipeDataGeneratorsOnly
	recipeConfigurations
	recipeGenerators
)

const (
	apiVersionKey = "apiVersion"
	kindKey       = "kind"
)

func (r *documentReader) classifyRecipe(dir string, data []byte, k *types.Kustomization) recipeClass {
	class := r.classifyRecipeFields(dir, data, k)
	if len(k.Transformers) != 0 {
		class |= recipeTransformers
	}
	if len(k.Configurations) != 0 {
		class |= recipeConfigurations
	}
	if len(k.Generators) != 0 {
		class |= recipeGenerators
	}
	if opaquePluginInputs(k) {
		class |= recipeOpaquePluginInputs
	}
	return class
}

func opaquePluginInputs(k *types.Kustomization) bool {
	if len(k.Patches)+len(k.PatchesJson6902)+len(k.PatchesStrategicMerge)+len(k.Replacements)+
		len(k.Crds)+len(k.Vars)+len(k.Images)+len(k.ImageTags)+len(k.Replicas) != 0 {
		return true
	}
	for _, label := range k.Labels {
		for _, spec := range label.FieldSpecs {
			if !strings.HasPrefix(spec.Path, "metadata/") {
				return true
			}
		}
	}
	return false
}

func (r *documentReader) classifyRecipeFields(dir string, data []byte, k *types.Kustomization) recipeClass {
	if k.Kind != "" && k.Kind != types.KustomizationKind ||
		k.APIVersion != "" && k.APIVersion != types.KustomizationVersion {
		return 0
	}
	var fields map[string]any
	if err := yaml.UnmarshalStrict(data, &fields); err != nil {
		return 0
	}
	class := recipeResourceOnly | recipeDataGeneratorsOnly
	for field := range fields {
		switch field {
		case apiVersionKey, kindKey:
		case "resources":
			class &^= recipeDataGeneratorsOnly
		case "configMapGenerator", "secretGenerator", "generatorOptions":
			class &^= recipeResourceOnly
		default:
			return 0
		}
	}
	if class&recipeResourceOnly == 0 {
		return class
	}
	root := r.mountedRoot(dir)
	relative, _ := filepath.Rel(root, dir)
	for _, ref := range k.Resources {
		local, ok := kustomizeinput.Local(root, relative, ref)
		if !ok || r.mountedRoot(filepath.Join(root, local)) != root {
			return 0
		}
	}
	return recipeResourceOnly
}

func normalizeKustomization(k *types.Kustomization) {
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
	cached.sourceRoot = new(dr).mountedRoot(file)
	switch {
	case err == nil && info.IsDir():
		cached.pathState = pathDirectory
	case err == nil:
		cached.pathState = pathFile
	case errors.Is(err, fs.ErrNotExist):
		cached.pathState = pathMissing
	case errors.Is(err, errSourceUnavailable):
		cached.pathState = pathSourceUnavailable
	default:
		cached.pathState = pathStatFailed
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

func (l *Loader) configureDocuments() {
	_, bootstrap, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
	qualified := bootstrap[0] != nil && bootstrap[1] != nil
	l.documents.skipDeclarations = qualified || l.Options.Changes == nil && !l.Options.Session.ComparesDeclarations()
	l.documents.compact = l.Options.Session != nil || l.Options.ParsedInputs != nil && l.Options.ParsedInputs.compact
	l.documents.eager = l.Options.Session.Snapshot().Output(manifest.NamedResource{})
}

func isManifestFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return false
}

// DiscardOccurrence retires a physical declaration and its published identity.
func (l *Loader) DiscardOccurrence(id manifest.NamedResource) {
	delete(l.Index.Records, id)
	delete(l.candidates, id)
	delete(l.locations, id)
	l.Store.DeleteObject(id)
}
