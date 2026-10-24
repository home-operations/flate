package change

import (
	"iter"
	"log/slog"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type objectReader interface {
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	IncomingProductions(store.Child, *manifest.DiscoveryIndex) []manifest.Production
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource) store.Artifact
	GetEmission(manifest.NamedResource) (manifest.BaseManifest, map[string]any, store.Emission)
}

type objects interface {
	objectReader
	Objects() []manifest.BaseManifest
}

// Filter selects affected output and retains its execution dependencies.
// The zero value disables filtering. Discovery and changes are immutable.
type Filter struct {
	hasInputs bool
	changes   *Set
	index     *manifest.DiscoveryIndex
	objs      objects
	root      string
	mu        sync.RWMutex
	keep      map[manifest.NamedResource]role
	revision  uint64
}

// Ancestor-only entries MUST stay kept for patches, never primary, to prevent unrelated file-loaded sibling cascades.
type role uint8

const (
	absent role = iota
	kept
	primary
)

type selectionState struct {
	consumers          []manifest.NamedResource
	role, walked       role
	declarationChecked bool
}

type selection struct {
	filter  *Filter
	objects objectReader
	child   store.Child
	emitter manifest.NamedResource
	states  map[manifest.NamedResource]selectionState
	queue   []manifest.NamedResource
}

// NewFilter resolves changed inputs through the discovery and production relation.
// root defaults to the admitted bootstrap coordinate; nil changes disables filtering.
func NewFilter(changes *Set, index *manifest.DiscoveryIndex, objs objects, root string) *Filter {
	if root == "" && objs != nil {
		if input, ok := objs.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact); ok {
			root = input.LocalPath
		}
	}
	f := &Filter{changes: changes, index: index, objs: objs, root: root}
	if changes != nil {
		f.hasInputs = len(changes.inputs) != 0
		changed := len(changes.paths) != 0
		for _, source := range changes.sources {
			f.hasInputs = f.hasInputs || len(source.changes.inputs) != 0
			changed = changed || len(source.changes.paths) != 0
		}
		f.keep = make(map[manifest.NamedResource]role)
		if changed || f.hasInputs {
			f.selectWork(manifest.NamedResource{}, store.Child{}, objs)
		}
	}
	return f
}

// Enabled reports whether change-based filtering is active.
func (f *Filter) Enabled() bool { return f != nil && f.changes != nil }

// ShouldReconcile tests exact selected identity without relation access.
func (f *Filter) ShouldReconcile(id manifest.NamedResource) bool {
	if !f.Enabled() {
		return true
	}
	f.mu.RLock()
	_, ok := f.keep[id]
	f.mu.RUnlock()
	return ok
}

// AddEmitted admits the incoming object and contribution before publication.
// Returned identities are uniquely newly kept; refire them after this call returns.
func (f *Filter) AddEmitted(emitter manifest.NamedResource, child store.Child, objs objectReader) []manifest.NamedResource {
	if !f.Enabled() || child.Obj == nil {
		return nil
	}
	return f.selectWork(emitter, child, objs)
}

func (f *Filter) selectWork(emitter manifest.NamedResource, child store.Child, objs objectReader) []manifest.NamedResource {
	for {
		f.mu.RLock()
		revision := f.revision
		f.mu.RUnlock()
		s := selection{filter: f, objects: objs, emitter: emitter, child: child, states: map[manifest.NamedResource]selectionState{}}
		if child.Obj == nil {
			index := manifest.DiscoveryIndex{}
			if f.index != nil {
				index = *f.index
			}
			bootstrap := s.evidence(manifest.BootstrapSourceID)
			if bootstrap.changes != nil {
				for id := range bootstrap.changes.inputs {
					if bootstrap.input(&s, manifest.BootstrapSourceID, id) {
						s.add(id, primary)
					}
				}
			}
			known := 0
			if f.objs != nil {
				for _, obj := range f.objs.Objects() {
					if s.link(obj.Named(), obj) {
						known++
					}
				}
			}
			if known < len(index.Records) {
				for id := range index.Records {
					if f.objs == nil || f.objs.GetObject(id) == nil {
						s.link(id, nil)
					}
				}
			}
			index.Readers = nil
			for file := range s.files() {
				for reader := range f.index.ReadersOfFile(file) {
					if s.changedClaim(reader, file) {
						s.add(reader.Reader, primary)
					}
				}
				owners, ancestors := index.OwnersOfFile(file)
				for _, id := range owners {
					ref := s.source(id)
					if ref != (manifest.NamedResource{}) && s.changedClaim(manifest.FileReader{Reader: id, Source: ref}, file) {
						s.add(id, primary)
					}
				}
				for _, id := range ancestors {
					ref := s.source(id)
					if ref != (manifest.NamedResource{}) && s.changedClaim(manifest.FileReader{Reader: id, Source: ref}, file) {
						s.add(id, kept)
					}
				}
			}
		}
		if child.Obj != nil {
			s.add(child.Obj.Named(), s.admit())
		}
		s.walk()
		f.mu.Lock()
		if revision != f.revision {
			f.mu.Unlock()
			continue
		}
		var added []manifest.NamedResource
		for id, state := range s.states {
			r := state.role
			if f.keep[id] >= r {
				continue
			}
			if f.keep[id] == absent {
				added = append(added, id)
			}
			f.keep[id] = r
			f.revision++
		}
		f.mu.Unlock()
		slices.SortFunc(added, manifest.NamedResource.Compare)
		return added
	}
}

func (s *selection) read(id manifest.NamedResource) role {
	return max(s.states[id].role, s.observed(id))
}

func (s *selection) observed(id manifest.NamedResource) role {
	s.filter.mu.RLock()
	r := s.filter.keep[id]
	s.filter.mu.RUnlock()
	return r
}

func (s *selection) add(id manifest.NamedResource, r role) {
	if id == (manifest.NamedResource{}) {
		return
	}
	r = max(r, s.observed(id))
	state := s.states[id]
	if state.role >= r {
		return
	}
	state.role = r
	s.states[id] = state
	s.queue = append(s.queue, id)
}

func (s *selection) object(id manifest.NamedResource) manifest.BaseManifest {
	if s.child.Obj != nil && s.child.Obj.Named() == id {
		return s.child.Obj
	}
	if s.objects == nil {
		return nil
	}
	return s.objects.GetObject(id)
}

func (s *selection) productions(id manifest.NamedResource) []manifest.Production {
	if s.objects != nil {
		if s.child.Obj != nil && s.child.Obj.Named() == id {
			return s.objects.IncomingProductions(s.child, s.filter.index)
		}
		return s.objects.Productions(id, s.filter.index)
	}
	r, _ := s.filter.index.Lookup(id)
	return r.Productions
}

// Both branches MUST share one iterator closure to avoid per-reference allocations.
func (s *selection) deps(obj manifest.BaseManifest, id manifest.NamedResource, reverse bool) iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		if obj != nil {
			manifest.ContentInputs(obj, reverse)(yield)
		} else {
			r, _ := s.filter.index.Lookup(id)
			slices.Values(r.Refs)(yield)
		}
	}
}

func (s *selection) link(id manifest.NamedResource, obj manifest.BaseManifest) bool {
	record, known := s.filter.index.Lookup(id)
	if s.filter.hasInputs && s.changedInput(id, id) {
		s.add(id, primary)
	}
	for dep := range s.deps(obj, id, true) {
		state := s.states[dep]
		state.consumers = append(state.consumers, id)
		checked := state.declarationChecked
		state.declarationChecked = true
		s.states[dep] = state
		if s.filter.hasInputs && s.changedInput(id, dep) {
			s.add(dep, primary)
		}
		if !checked {
			for _, p := range s.productions(dep) {
				if materializer := p.Materializer(dep); p.Declaration != (manifest.NamedResource{}) && (materializer != (manifest.NamedResource{}) || p.Owner == (manifest.NamedResource{})) {
					relative, indexed := s.coordinates(p)
					if s.changedProduction(p, relative, indexed) {
						s.add(p.Declaration, primary)
					}
				}
			}
		}
	}
	hasFile := false
	for _, p := range s.productions(id) {
		relative, indexed := s.coordinates(p)
		hasFile = hasFile || indexed == record.File
		owner := p.Materializer(id)
		if p.Declaration == (manifest.NamedResource{}) && (owner != (manifest.NamedResource{}) || p.Owner == (manifest.NamedResource{}) || manifest.Resolve(p.Authored, id.Namespace, "") == id) && s.changedProduction(p, relative, indexed) {
			s.add(id, primary)
		}
		if owner == (manifest.NamedResource{}) && p.Owner != (manifest.NamedResource{}) {
			continue
		}
		if p.Declaration != (manifest.NamedResource{}) {
			owner = p.Declaration
		}
		if owner != id {
			state := s.states[owner]
			state.consumers = append(state.consumers, id)
			s.states[owner] = state
		}
	}
	if !known {
		return false
	}
	if bootstrap := s.evidence(manifest.BootstrapSourceID); bootstrap.changes != nil && (!hasFile && bootstrap.changes.Contains(record.File) || slices.ContainsFunc(record.Inputs, bootstrap.changes.Contains)) {
		s.add(id, primary)
	}
	return true
}

func (s *selection) admit() role {
	id := s.child.Obj.Named()
	r := s.read(id)
	if r == primary || s.read(s.emitter) == primary || s.affectedRoot(s.child.Obj) || s.filter.hasInputs && s.changedInput(id, id) {
		return primary
	}
	for dep := range s.deps(s.child.Obj, id, true) {
		if s.read(dep) == primary || s.filter.hasInputs && s.changedInput(id, dep) {
			return primary
		}
	}
	for _, p := range s.productions(id) {
		if s.read(p.Materializer(id)) == primary || s.read(p.Declaration) == primary || p.Authored != (manifest.NamedResource{}) && s.read(p.Authored) == primary {
			return primary
		}
		relative, file := s.coordinates(p)
		if s.changedProduction(p, relative, file) {
			return primary
		}
	}
	return r
}

type sourceEvidence struct {
	changes       *Set
	artifact      *store.SourceArtifact
	paired, local bool
}

func (s *selection) evidence(ref manifest.NamedResource) sourceEvidence {
	f := s.filter
	if ref == (manifest.NamedResource{}) {
		ref = manifest.BootstrapSourceID
	}
	pair, known := f.changes.sources[ref]
	out := sourceEvidence{local: ref == manifest.BootstrapSourceID}
	if out.local && !known {
		out.changes = f.changes
		return out
	}
	if s.objects != nil && !out.local {
		_, pinned, external := loader.SourceView(s.objects, ref, f.root)
		out.local = !pinned && !external
	}
	if known {
		if s.objects != nil {
			out.artifact, _ = s.objects.GetArtifact(ref).(*store.SourceArtifact)
			declaration := s.objects.GetObject(ref)
			for side, artifact := range pair.artifacts {
				if artifact != nil && artifact == out.artifact && declaration == pair.declarations[side] {
					out.changes, out.paired = pair.changes, true
					return out
				}
			}
		}
		if ref == manifest.BootstrapSourceID {
			return out
		}
	}
	if out.local {
		out.changes = s.evidence(manifest.BootstrapSourceID).changes
	}
	return out
}

func (e sourceEvidence) input(s *selection, ref, id manifest.NamedResource) bool {
	if e.changes == nil {
		return false
	}
	file, known := e.changes.inputs[id]
	if !known {
		return false
	}
	if e.paired {
		return true
	}
	artifact := e.artifact
	if artifact == nil && s.objects != nil {
		artifact, _ = s.objects.GetArtifact(ref).(*store.SourceArtifact)
	}
	if artifact == nil {
		return false
	}
	root := s.filter.root
	if (e.changes.origins[0] != nil || e.changes.origins[1] != nil) && !slices.ContainsFunc(e.changes.origins[:], func(input *store.SourceArtifact) bool {
		return input != nil && (input.LocalPath == root || input.LocalRoot == root)
	}) {
		return false
	}
	origin := loader.OriginPath(artifact, artifact.LocalPath)
	base, err := filepath.Rel(root, origin)
	base = filepath.ToSlash(base)
	return err == nil && (base == "." || file == base || strings.HasPrefix(file, base+"/"))
}

func (s *selection) changedInput(consumer, dep manifest.NamedResource) bool {
	inputs := [2]manifest.NamedResource{dep, dep}
	inputs[1].Namespace = ""
	groups := s.filter.changes.groups
	if len(groups) == 0 {
		groups = []sourceGroup{{changes: s.filter.changes}}
	}
	for _, group := range groups {
		if _, exact := group.changes.inputs[inputs[0]]; !exact {
			if _, projected := group.changes.inputs[inputs[1]]; !projected {
				continue
			}
		}
		refs := group.refs
		if len(refs) == 0 {
			refs = []manifest.NamedResource{manifest.BootstrapSourceID}
		}
		for _, ref := range refs {
			evidence := s.evidence(ref)
			if evidence.input(s, ref, inputs[0]) {
				return true
			}
			if !evidence.input(s, ref, inputs[1]) {
				continue
			}
			consumerSource := s.source(consumer)
			if consumerSource == (manifest.NamedResource{}) {
				continue
			}
			if consumerSource == ref {
				return true
			}
			if ref == manifest.BootstrapSourceID {
				local := s.evidence(consumerSource)
				if !local.paired && local.input(s, consumerSource, inputs[1]) {
					return true
				}
			}
		}
	}
	return false
}

func (s *selection) coordinates(p manifest.Production) (relative, indexed string) {
	f := s.filter
	relative, indexed = p.File, p.File
	if filepath.IsAbs(p.Path) && s.objects != nil {
		art, _ := s.objects.GetArtifact(p.Source).(*store.SourceArtifact)
		root := f.root
		if art != nil {
			root = art.LocalPath
		}
		rel, err := filepath.Rel(root, p.Path)
		if err != nil {
			return "", ""
		}
		relative = filepath.ToSlash(rel)
		rel, err = filepath.Rel(f.root, loader.OriginPath(art, p.Path))
		if err != nil {
			return "", ""
		}
		indexed = filepath.ToSlash(rel)
	}
	return relative, indexed
}

func (s *selection) changedProduction(p manifest.Production, relative, indexed string) bool {
	if p.File == "" {
		return false
	}
	if p.Owner == (manifest.NamedResource{}) && p.Source == manifest.BootstrapSourceID {
		if record, _ := s.filter.index.Lookup(p.Authored); record.Path != "" && record.Path != p.Path {
			return false
		}
	}
	evidence := s.evidence(p.Source)
	if evidence.changes == nil {
		return false
	}
	if evidence.paired {
		return evidence.changes.Contains(relative)
	}
	return evidence.changes.Contains(indexed)
}

func (s *selection) source(id manifest.NamedResource) manifest.NamedResource {
	obj := s.object(id)
	if obj == nil {
		return manifest.NamedResource{}
	}
	for r := range manifest.References(obj, manifest.ReferenceRelations) {
		if r.Use == "source" {
			return r.NamedResource
		}
	}
	return manifest.BootstrapSourceID
}

func (s *selection) files() iter.Seq[string] {
	return func(yield func(string) bool) {
		groups := s.filter.changes.groups
		if len(groups) == 0 {
			groups = []sourceGroup{{changes: s.filter.changes}}
		}
		for _, group := range groups {
			refs := group.refs
			if len(refs) == 0 {
				refs = []manifest.NamedResource{manifest.BootstrapSourceID}
			}
			for _, ref := range refs {
				for file := range s.sourceFiles(ref) {
					if !yield(file) {
						return
					}
				}
			}
		}
	}
}

func (s *selection) sourceFiles(ref manifest.NamedResource) iter.Seq[string] {
	return func(yield func(string) bool) {
		evidence := s.evidence(ref)
		if evidence.changes == nil {
			return
		}
		for file := range evidence.changes.paths {
			if evidence.paired {
				_, file = s.coordinates(manifest.Production{Source: ref, File: file, Path: filepath.Join(evidence.artifact.LocalPath, filepath.FromSlash(file))})
			}
			if !yield(file) {
				return
			}
		}
	}
}

func (s *selection) changedClaim(reader manifest.FileReader, file string) bool {
	evidence := s.evidence(reader.Source)
	if evidence.changes == nil {
		return false
	}
	if evidence.paired {
		origin := loader.OriginPath(evidence.artifact, evidence.artifact.LocalPath)
		relative, err := filepath.Rel(origin, filepath.Join(s.filter.root, filepath.FromSlash(file)))
		return err == nil && evidence.changes.Contains(filepath.ToSlash(relative))
	}
	record, _ := s.filter.index.Lookup(reader.Reader)
	return !record.Pinned && evidence.changes.Contains(file)
}

func (s *selection) walk() {
	for len(s.queue) > 0 {
		id := s.queue[0]
		s.queue = s.queue[1:]
		state := s.states[id]
		if state.walked >= state.role || s.child.Obj != nil && id != s.child.Obj.Named() && s.observed(id) >= state.role {
			continue
		}
		state.walked = state.role
		s.states[id] = state
		record, _ := s.filter.index.Lookup(id)
		s.add(record.Parent, kept)
		for dep := range s.deps(s.object(id), id, false) {
			s.add(dep, kept)
		}
		for _, p := range s.productions(id) {
			owner := p.Materializer(id)
			if state.role == primary {
				owner = p.Owner
			}
			s.add(owner, kept)
			s.add(p.Declaration, kept)
			if owner != (manifest.NamedResource{}) && filepath.IsAbs(p.Path) {
				_, file := s.coordinates(p)
				for reader := range s.filter.index.ReadersOfFile(file) {
					if producer, ok := s.object(reader.Reader).(interface {
						EmissionPolicy() manifest.EmissionPolicy
					}); ok && !producer.EmissionPolicy().MatchesNamespace(id) {
						continue
					}
					if reader.Source != p.Source && !s.evidence(p.Source).local {
						continue
					}
					s.add(reader.Reader, kept)
				}
			}
		}
		if state.role != primary {
			continue
		}
		if s.objects != nil && id.KindInfo().Parent && s.observed(id) != primary {
			_, _, output := s.objects.GetEmission(id)
			for _, child := range output.Children {
				s.add(child.Obj.Named(), primary)
			}
			output.Finish()
		}
		for _, target := range state.consumers {
			s.add(target, primary)
		}
	}
}

func (s *selection) affectedRoot(obj manifest.BaseManifest) bool {
	if s.objects == nil {
		return false
	}
	for base := range manifest.RenderRoots(obj) {
		ref := s.source(obj.Named())
		evidence := s.evidence(ref)
		if evidence.changes == nil {
			continue
		}
		art := evidence.artifact
		if art == nil {
			art, _ = s.objects.GetArtifact(ref).(*store.SourceArtifact)
		}
		root := s.filter.root
		if ref != manifest.BootstrapSourceID && art != nil {
			root = art.LocalPath
		}
		if strings.Contains(base, "/../") || strings.HasSuffix(base, "/..") {
			// Frozen readers resolve known symlink roots; unknown footprints remain conservative.
			if record, known := s.filter.index.Lookup(obj.Named()); known && record.Path != "" {
				for file := range s.sourceFiles(ref) {
					for reader := range s.filter.index.ReadersOfFile(file) {
						if reader.Reader == obj.Named() && s.changedClaim(reader, file) {
							return true
						}
					}
				}
				continue
			}
			base = ""
		}
		physical := loader.OriginPath(art, filepath.Join(root, filepath.FromSlash(base)))
		rel, err := filepath.Rel(s.filter.root, physical)
		if err != nil {
			continue
		}
		prefix := filepath.ToSlash(rel)
		if prefix == "." {
			prefix = ""
		}
		for file := range s.sourceFiles(ref) {
			if file == prefix || strings.HasPrefix(file, prefix+"/") || prefix == "" && !path.IsAbs(file) {
				if !s.changedClaim(manifest.FileReader{Reader: obj.Named(), Source: ref}, file) {
					continue
				}
				claim := prefix + "/"
				if prefix == "" {
					claim = ""
				}
				if s.filter.index != nil && slices.Contains(s.filter.index.Claims[claim], obj.Named()) {
					owners, _ := s.filter.index.OwnersOfFile(file)
					if !slices.Contains(owners, obj.Named()) {
						continue
					}
				}
				return true
			}
		}
	}
	return false
}

// LogValue snapshots the keep set only when the log handler needs it.
func (f *Filter) LogValue() slog.Value {
	items := f.KeepNames()
	return slog.GroupValue(slog.Int("size", len(items)), slog.Any("items", items))
}

// KeepNames returns the resolved keep-set as sorted strings for logs.
func (f *Filter) KeepNames() []string {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.keep == nil {
		return nil
	}
	out := make([]string, 0, len(f.keep))
	for id := range f.keep {
		out = append(out, id.String())
	}
	slices.Sort(out)
	return out
}

// KeepNamespaces returns the namespaces represented in the keep-set,
// or nil when no scope can be derived (disabled, empty, or
// cluster-scoped only).
func (f *Filter) KeepNamespaces() map[string]struct{} {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.keep == nil {
		return nil
	}
	out := make(map[string]struct{})
	for id := range f.keep {
		if id.Namespace != "" {
			out[id.Namespace] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
