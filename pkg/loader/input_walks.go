package loader

import (
	"cmp"
	"io/fs"
	"iter"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

type cachedGraphContext struct {
	Root                 string              `json:"Root,omitempty"`
	Dir                  string              `json:"Dir,omitempty"`
	Namespace            string              `json:"Namespace,omitempty"`
	Fallback             string              `json:"Fallback,omitempty"`
	NamePrefix           string              `json:"NamePrefix,omitempty"`
	NameSuffix           string              `json:"NameSuffix,omitempty"`
	PluginRoot           string              `json:"PluginRoot,omitempty"`
	Role                 kustomizeinput.Role `json:"Role,omitempty"`
	DeferMaterialization bool                `json:"DeferMaterialization,omitempty"`
	CheckMetadata        bool                `json:"CheckMetadata,omitempty"`
	DeferDecode          bool                `json:"DeferDecode,omitempty"`
	EntryResources       bool                `json:"EntryResources,omitempty"`
	RequiresBuild        bool                `json:"RequiresBuild,omitempty"`
}

type cachedWalk struct {
	dictionary []cachedVisit
	Context    cachedGraphContext `json:"Context"`
	SourceRoot string             `json:"SourceRoot,omitempty"`
	Prefix     string             `json:"Prefix,omitempty"`
	LocalBase  string             `json:"LocalBase,omitempty"`
	Visits     []cachedVisit      `json:"-"`
	VisitIDs   []int              `json:"Visits,omitempty"`
	Inputs     []string           `json:"Inputs,omitempty"`
	Listed     []string           `json:"Listed,omitempty"`
}

type cachedVisit struct {
	Context    cachedGraphContext `json:"Context"`
	File       string             `json:"File,omitempty"`
	Prefix     string             `json:"Prefix,omitempty"`
	Coordinate string             `json:"Coordinate,omitempty"`
	LocalBase  string             `json:"LocalBase,omitempty"`
	Incomplete string             `json:"Incomplete,omitempty"`
	Document   bool               `json:"Document,omitempty"`
	Readable   bool               `json:"Readable,omitempty"`
}

func cacheGraphContext(root string, context *graphContext) (cachedGraphContext, bool) {
	base, valid := inputRelative(root, context.root)
	return cachedGraphContext{
		Root: base, Dir: context.dir, Namespace: context.namespace, Fallback: context.fallback,
		NamePrefix: context.namePrefix, NameSuffix: context.nameSuffix, PluginRoot: context.pluginRoot,
		Role: context.role, DeferMaterialization: context.deferMaterialization,
		CheckMetadata: context.checkMetadata, DeferDecode: context.deferDecode,
		EntryResources: context.entryResources, RequiresBuild: context.requiresBuild,
	}, valid
}

func (c *cachedGraphContext) restore(root string) graphContext {
	return graphContext{
		root: filepath.Join(root, filepath.FromSlash(c.Root)), dir: c.Dir, namespace: c.Namespace, fallback: c.Fallback,
		namePrefix: c.NamePrefix, nameSuffix: c.NameSuffix, pluginRoot: c.PluginRoot,
		role: c.Role, deferMaterialization: c.DeferMaterialization, checkMetadata: c.CheckMetadata,
		deferDecode: c.DeferDecode, entryResources: c.EntryResources, requiresBuild: c.RequiresBuild,
	}
}

func cacheWalk(state *inputCache, key *physicalContext, walk physicalWalk) (cachedWalk, bool) {
	if walk.cached != nil {
		return *walk.cached, true
	}
	root := state.input.LocalPath
	context, valid := cacheGraphContext(root, &key.graphContext)
	if !valid || len(walk.inputs) == 0 {
		return cachedWalk{}, false
	}
	saved := cachedWalk{Context: context, SourceRoot: key.sourceRoot, Prefix: key.prefix, LocalBase: key.localBase}
	if key.sourceRoot == root {
		saved.SourceRoot = ""
	}
	if !saved.captureInputs(state, walk.inputs) {
		return cachedWalk{}, false
	}

	for visitIndex := range walk.visits {
		visit := &walk.visits[visitIndex]
		context, valid := cacheGraphContext(root, &visit.graphContext)
		file, fileValid := inputRelative(root, visit.file)
		if !valid || !fileValid {
			return cachedWalk{}, false
		}
		saved.Visits = append(saved.Visits, cachedVisit{
			Context:    context,
			File:       file,
			Prefix:     visit.prefix,
			Coordinate: visit.coordinate,
			LocalBase:  visit.localBase,
			Incomplete: visit.incomplete,
			Document:   visit.document != nil,
			Readable:   visit.readable,
		})
	}
	return saved, true
}

func (w *cachedWalk) visits() iter.Seq[*cachedVisit] {
	return func(yield func(*cachedVisit) bool) {
		if w.dictionary != nil {
			for _, id := range w.VisitIDs {
				if !yield(&w.dictionary[id]) {
					return
				}
			}
			return
		}
		for i := range w.Visits {
			if !yield(&w.Visits[i]) {
				return
			}
		}
	}
}

func (w *cachedWalk) matches(changed inputChanges) bool {
	for _, name := range w.Inputs {
		if changed.paths[name] {
			return false
		}
	}
	for _, name := range w.Listed {
		if changed.directories[name] {
			return false
		}
	}
	return true
}

func (w *cachedWalk) key(root string) physicalContext {
	source := w.SourceRoot
	if source == "" {
		source = root
	}
	return physicalContext{
		graphContext: w.Context.restore(root),
		sourceRoot:   source,
		prefix:       w.Prefix,
		localBase:    w.LocalBase,
	}
}

func (l *Loader) restoreWalk(root string, saved *cachedWalk) (physicalWalk, bool) {
	walk := physicalWalk{imported: true, visits: make([]graphVisit, 0, max(len(saved.Visits), len(saved.VisitIDs)))}
	for _, name := range saved.Inputs {
		walk.inputs = append(walk.inputs, walkInput{path: filepath.Join(root, filepath.FromSlash(name))})
	}
	for _, name := range saved.Listed {
		walk.inputs = append(walk.inputs, walkInput{path: filepath.Join(root, filepath.FromSlash(name)), listed: true})
	}
	for v := range saved.visits() {
		visit := graphVisit{
			graphContext: v.Context.restore(root),
			file:         filepath.Join(root, filepath.FromSlash(v.File)),
			prefix:       v.Prefix,
			coordinate:   v.Coordinate,
			localBase:    v.LocalBase,
			incomplete:   v.Incomplete,
			readable:     v.Readable,
		}
		if v.Document {
			visit.document = l.documents.cache[filepath.Join(visit.root, visit.dir)].body
			if visit.document == nil {
				return physicalWalk{}, false
			}
		}
		if origin := new(l.documents).localPath(visit.file); origin != visit.file {
			visit.origin = origin
		}
		if local := visit.localCoordinate(); local != "" {
			visit.localFile = filepath.Join(saved.key(root).sourceRoot, filepath.FromSlash(local))
		}
		walk.visits = append(walk.visits, visit)
	}
	return walk, true
}

func (l *Loader) cacheWalks(state *inputCache, force bool) ([]cachedWalk, bool) {
	facts := l.sourceFacts(state.input.LocalPath)
	pending := force
	for key, entry := range facts.walks.entries() {
		_, known := state.walks[key]
		pending = pending || !known && (entry.value.cached != nil || len(entry.value.inputs) != 0)
	}
	if !pending {
		return nil, false
	}
	var walks []cachedWalk
	changed := false
	for key, entry := range facts.walks.entries() {
		saved, valid := cacheWalk(state, &key, entry.value)
		if !valid {
			continue
		}
		walks = append(walks, saved)
		_, known := state.walks[key]
		changed = changed || !known
	}
	slices.SortFunc(walks, compareCachedWalks)
	return walks, changed
}

func (w *cachedWalk) valid() bool {
	if !fs.ValidPath(w.Context.Root) || !slices.IsSorted(w.Inputs) || !slices.IsSorted(w.Listed) {
		return false
	}
	for _, name := range w.Inputs {
		if !fs.ValidPath(name) {
			return false
		}
	}
	for _, name := range w.Listed {
		if !fs.ValidPath(name) {
			return false
		}
	}
	for visit := range w.visits() {
		if !fs.ValidPath(visit.Context.Root) || !fs.ValidPath(visit.File) {
			return false
		}
	}
	return true
}

func (w *cachedWalk) captureInputs(state *inputCache, observations []walkInput) bool {
	root := state.input.LocalPath
	inputs, listed := map[string]bool{}, map[string]bool{}
	for _, input := range observations {
		name, valid := inputRelative(root, input.path)
		if !valid {
			return false
		}
		for parent := name; ; parent = path.Dir(parent) {
			mode, present := state.types[parent]
			if present && !mode.IsRegular() && !mode.IsDir() {
				return false
			}
			inputs[parent] = true
			if parent == "." {
				break
			}
		}
		if input.listed {
			listed[name] = true
		}
		if state.types[name].IsDir() {
			for _, builder := range manifest.KustomizeBuilderFilenames {
				inputs[path.Join(name, builder)] = true
			}
		}
	}
	w.Inputs, w.Listed = slices.Sorted(maps.Keys(inputs)), slices.Sorted(maps.Keys(listed))
	return true
}

//nolint:gocritic // slices.SortFunc supplies value snapshots to its comparator.
func compareCachedWalks(a, b cachedWalk) int {
	left := [...]string{
		a.Context.Root,
		a.Context.Dir,
		a.Context.Namespace,
		a.Context.Fallback,
		a.Context.NamePrefix,
		a.Context.NameSuffix,
		a.Context.PluginRoot,
		a.SourceRoot,
		a.Prefix,
		a.LocalBase,
	}
	right := [...]string{
		b.Context.Root,
		b.Context.Dir,
		b.Context.Namespace,
		b.Context.Fallback,
		b.Context.NamePrefix,
		b.Context.NameSuffix,
		b.Context.PluginRoot,
		b.SourceRoot,
		b.Prefix,
		b.LocalBase,
	}
	for i, value := range left {
		if order := strings.Compare(value, right[i]); order != 0 {
			return order
		}
	}
	if order := cmp.Compare(a.Context.Role, b.Context.Role); order != 0 {
		return order
	}
	leftFlags := [...]bool{
		a.Context.DeferMaterialization,
		a.Context.CheckMetadata,
		a.Context.DeferDecode,
		a.Context.EntryResources,
		a.Context.RequiresBuild,
	}
	rightFlags := [...]bool{
		b.Context.DeferMaterialization,
		b.Context.CheckMetadata,
		b.Context.DeferDecode,
		b.Context.EntryResources,
		b.Context.RequiresBuild,
	}
	for i, value := range leftFlags {
		if value != rightFlags[i] {
			if value {
				return 1
			}
			return -1
		}
	}
	return 0
}
