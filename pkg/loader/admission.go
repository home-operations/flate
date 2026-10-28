package loader

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"path"
	"path/filepath"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Admission is a temporary proposal computed outside execution-membership locks.
type Admission struct {
	Needed     []manifest.NamedResource
	Selected   []manifest.NamedResource
	Dirty      []manifest.NamedResource
	Objects    []store.Child
	Blocked    []string
	Revision   uint64
	Namespaces []string
}

type admissionReader interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	ReferenceFacts(manifest.NamedResource, *manifest.DiscoveryIndex) store.ReferenceFacts
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	GetStatus(manifest.NamedResource) (store.StatusInfo, bool)
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	IncomingProductions(store.Child, *manifest.DiscoveryIndex) []manifest.Production
	GetEmission(manifest.NamedResource) (manifest.BaseManifest, map[string]any, store.Emission)
	ListObjects(string, string) []manifest.BaseManifest
}

type admissionRole struct{ needed, selected, dirty bool }
type admissionState struct {
	productions                               []manifest.Production
	role, walked                              admissionRole
	consumer, generation, restoration, claim  int
	changed, seed, direct, rescan             bool
	walkedOnce, productionsKnown, listed      bool
	eligible, declarationChanged, materialize bool
}

type materializerDemand struct {
	target, consumer  manifest.NamedResource
	source            string
	ordering, readers bool
}
type admissionClosure struct {
	loader       *Loader
	objects      admissionReader
	incoming     store.Child
	proposal     *Admission
	states       map[manifest.NamedResource]admissionState
	result       Admission
	counts       [3]int
	collecting   bool
	children     map[manifest.NamedResource]store.Child
	queue        []manifest.NamedResource
	roots        []string
	searched     map[materializerDemand]bool
	inputScratch [1]manifest.NamedResource
	accepted     map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord
	replace      map[manifest.NamedResource]bool
	history      map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}
}

// Prepare computes the local declaration and input closure without publishing membership or objects.
// History is invocation-local borrowed evidence; complete inventories replace only their owner's support.
func (l *Loader) Prepare(ctx context.Context, incoming store.Child, reads *store.ReadSet, history map[manifest.NamedResource]struct {
	Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
	Complete bool
}) (*Admission, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if l.Options.Filter == nil {
		return &Admission{}, nil
	}
	closure := admissionClosure{loader: l, incoming: incoming, history: history}
	proposal := &closure.result
	closure.proposal = proposal
	_, _, _, proposal.Revision = l.Options.Filter.Membership(manifest.NamedResource{})
	var objects admissionReader = l.Store
	if reads != nil {
		objects = reads
	}
	if !l.frozen && !l.sideKnown && l.Options.Changes != nil {
		input := objects.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		_, snapshots, _ := l.Options.Changes.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range snapshots {
			if snapshot != nil && snapshot == input {
				l.side, l.sideKnown = manifest.TreeSide(side), true
				break
			}
		}
	}
	closure.objects = objects
	if !l.frozen && l.scopes != nil {
		for _, root := range l.StandaloneRoots(l.SourceRoot) {
			if filepath.Join(l.SourceRoot, filepath.FromSlash(root)) == l.entryPath {
				closure.roots = append(closure.roots, root)
			}
		}
	}
	proposal, err := closure.evaluate(ctx, reads)
	if err != nil {
		return nil, err
	}
	closure = admissionClosure{result: *proposal}
	return &closure.result, nil
}

func (c *admissionClosure) evaluate(ctx context.Context, reads *store.ReadSet) (*Admission, error) {
	if len(c.history) != 0 {
		c.readHistory()
	}
	var candidates [2][2][]Occurrence
	var candidatesKnown [2]bool
	materializers := func(demand materializerDemand, consumer manifest.NamedResource, open bool) bool {
		mode := 0
		if demand.ordering {
			mode = 1
		}
		if !candidatesKnown[mode] {
			candidates[mode] = c.materializerCandidates(demand.ordering)
			candidatesKnown[mode] = true
		}
		return c.materializers(demand, consumer, open, candidates[mode])
	}
	if c.incoming.Obj == nil && reads != nil {
		for id := range reads.Required() {
			role := c.role(id)
			role.needed = true
			c.add(id, role)
		}
	} else if c.incoming.Obj == nil {
		if err := c.start(ctx, materializers); err != nil {
			return nil, err
		}
	} else {
		id := c.incoming.Obj.Named()
		role := c.role(id)
		eligible := c.loader.Options.Selection.Matches(id)
		var affected, footprintRead bool
		if eligible && !c.loader.Options.Selection.Changed {
			role.needed, role.selected, role.dirty = true, true, true
		}
		for _, production := range c.productions(id) {
			parent := c.role(production.Owner)
			if parent.selected && parent.dirty && (!production.Owner.ReportHidden() || !c.loader.IsFileIndexed(id)) && !c.unchangedDeclaration(id, production.Owner, c.incoming.Obj) || c.changedProduction(production) && eligible {
				role.needed, role.selected, role.dirty = true, true, true
			}
		}
		if (!role.selected || !role.dirty) && (eligible || role.needed) {
			affected, footprintRead = c.affected(id, c.incoming.Obj), true
		}
		if (!role.selected || !role.dirty) && (eligible || role.needed) && (affected || c.changedInput(id, id)) {
			role.needed = true
			if eligible {
				role.selected, role.dirty = true, true
			}
		}
		if (!role.selected || !role.dirty) && (eligible || role.needed) {
			for ref := range c.references(id, c.incoming.Obj) {
				if !c.reverse(id, c.incoming.Obj, ref) {
					continue
				}
				inputs, err := c.matchingInputs(ref, true)
				if err != nil {
					continue
				}
				for _, input := range inputs {
					if !c.role(input).dirty && !c.affected(input, c.object(input)) && !c.changedInput(id, input) {
						continue
					}
					if changed, known := c.outputChanged(id, ref, c.incoming.Obj); known && !changed {
						continue
					}
					role.needed = true
					if eligible {
						role.selected, role.dirty = true, true
					}
				}
			}
		}
		if _, _, applies := c.incoming.Obj.SourcePath(); applies && !role.needed {
			if producer, ok := c.incoming.Obj.(interface {
				EmissionPolicy() manifest.EmissionPolicy
			}); ok && !c.loader.Options.Selection.Changed &&
				slices.ContainsFunc(c.loader.Options.Selection.Roots, func(root manifest.NamedResource) bool {
					return root.Name == "" && producer.EmissionPolicy().MatchesNamespace(root)
				}) &&
				slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
					return p.Owner != (manifest.NamedResource{}) && c.role(p.Owner).needed
				}) {
				role.needed = true
			}
			for target, record := range c.loader.Index.Files() {
				needed, _, _, _ := c.loader.Options.Filter.Membership(target)
				if needed && (record.Parent == (manifest.NamedResource{}) || manifest.Resolve(record.Parent, "", id.Namespace) == id) && record.BuilderOwner == (manifest.NamedResource{}) && c.coversDeclaration(c.incoming.Obj, target) {
					role.needed = true
					break
				}
			}
		}
		c.add(id, role)
		if !role.dirty && !footprintRead {
			affected = c.affected(id, c.incoming.Obj)
		}
		if role.dirty || affected {
			key := id
			key.Name = ""
			for _, readers := range [][]manifest.NamedResource{c.loader.inputReaders[id], c.loader.inputReaders[key]} {
				for _, reader := range readers {
					if !c.loader.Options.Selection.Matches(reader) {
						continue
					}
					object := c.object(reader)
					for ref := range c.references(reader, object) {
						if !c.reverse(reader, object, ref) || ref.Group != id.Group || ref.Kind != id.Kind || ref.Namespace != id.Namespace {
							continue
						}
						inputs, err := c.matchingInputs(ref, true)
						if err != nil {
							return nil, err
						}
						if !slices.Contains(inputs, id) {
							continue
						}
						if changed, known := c.outputChanged(reader, ref, object); known && !changed {
							continue
						}
						c.add(reader, admissionRole{needed: true, selected: true, dirty: true})
					}
				}
			}
		}
	}
	if err := c.walk(ctx, materializers); err != nil {
		return nil, err
	}
	if total := c.counts[0] + c.counts[1] + c.counts[2]; total != 0 {
		roles := make([]manifest.NamedResource, total)
		c.proposal.Needed = roles[:0:c.counts[0]]
		c.proposal.Selected = roles[c.counts[0] : c.counts[0] : c.counts[0]+c.counts[1]]
		c.proposal.Dirty = roles[c.counts[0]+c.counts[1] : c.counts[0]+c.counts[1]]
	}
	c.proposal.Objects = slices.Grow(c.proposal.Objects, len(c.children))
	for id, state := range c.states {
		if state.role.needed {
			c.proposal.Needed = append(c.proposal.Needed, id)
		}
		if state.role.selected {
			c.proposal.Selected = append(c.proposal.Selected, id)
		}
		if state.role.dirty {
			c.proposal.Dirty = append(c.proposal.Dirty, id)
		}
	}
	for _, child := range c.children {
		c.proposal.Objects = append(c.proposal.Objects, child)
	}
	slices.SortFunc(c.proposal.Needed, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Selected, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Dirty, manifest.NamedResource.Compare)
	slices.SortFunc(c.proposal.Objects, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	slices.Sort(c.proposal.Blocked)
	c.proposal.Blocked = slices.Compact(c.proposal.Blocked)
	return c.proposal, ctx.Err()
}

func (c *admissionClosure) role(id manifest.NamedResource) admissionRole {
	role := c.states[id].role
	needed, selected, dirty, _ := c.loader.Options.Filter.Membership(id)
	role.needed, role.selected, role.dirty = role.needed || needed, role.selected || selected, role.dirty || dirty
	return role
}

func (c *admissionClosure) add(id manifest.NamedResource, role admissionRole) {
	if id == (manifest.NamedResource{}) || !role.needed {
		return
	}
	previous := c.role(id)
	root := id
	if occurrence, ok := c.loader.occurrence(id); ok && occurrence.Authored.Namespace == "" {
		if _, exact := c.loader.Index.Lookup(id); !exact {
			root.Namespace = ""
		}
	}
	role.needed, role.selected, role.dirty = true, role.selected || previous.selected, role.dirty || previous.dirty
	if previous == role && (!c.loader.roots[root] || c.objects.GetObject(id) != nil) && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
		return
	}
	state := c.states[id]
	if state.role == role {
		return
	}
	c.assignRole(id, role)
}

func (c *admissionClosure) assignRole(id manifest.NamedResource, role admissionRole) {
	state := c.states[id]
	if c.states == nil {
		c.states = map[manifest.NamedResource]admissionState{}
	}
	for axis, present := range [3]bool{role.needed, role.selected, role.dirty} {
		previous := [3]bool{state.role.needed, state.role.selected, state.role.dirty}
		if present && !previous[axis] {
			c.counts[axis]++
		}
	}
	state.role = role
	c.states[id] = state
	if !c.collecting {
		c.queue = append(c.queue, id)
	}
}

func (c *admissionClosure) markChanged(id manifest.NamedResource) {
	state := c.states[id]
	state.changed = true
	c.states[id] = state
}

func (c *admissionClosure) object(id manifest.NamedResource) manifest.BaseManifest {
	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
		return c.incoming.Obj
	}
	if child := c.children[id]; child.Obj != nil {
		return child.Obj
	}
	if object := c.objects.GetObject(id); object != nil {
		return object
	}
	record, _ := c.loader.Index.Lookup(id)
	return record.Declaration
}

// Direct iterator calls keep range callbacks on the stack.
func (c *admissionClosure) references(id manifest.NamedResource, obj manifest.BaseManifest) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		if obj != nil {
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				if !yield(ref) {
					return
				}
			}
		} else if occurrence, ok := c.loader.occurrence(id); ok {
			for ref := range occurrence.references() {
				if !yield(ref) {
					return
				}
			}
		} else if child := c.loader.candidates[id]; child.Obj != nil && c.loader.Index.Records[id].Builder != "" {
			for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
				if !yield(ref) {
					return
				}
			}
		} else {
			for _, ref := range c.record(id).Refs {
				if !yield(manifest.Reference{NamedResource: ref, Content: true}) {
					return
				}
			}
		}
		if obj != nil && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) {
			for ref := range c.historicalReferences(id) {
				if !yield(ref) {
					return
				}
			}
		}
	}
}

func (c *admissionClosure) declarationSource(id manifest.NamedResource) string {
	if occurrence, ok := c.loader.occurrence(id); ok {
		return occurrence.Declaration.Source
	}
	if input, ok := c.objects.GetArtifact(c.source(id), store.ArtifactContent).(*store.SourceArtifact); ok {
		return input.OriginPath(input.LocalPath)
	}
	return ""
}

func (c *admissionClosure) source(id manifest.NamedResource) manifest.NamedResource {
	occurrence, located := c.loader.occurrence(id)
	for _, p := range c.productions(id) {
		if p.Source == (manifest.NamedResource{}) {
			continue
		}
		if located {
			input, _ := c.objects.GetArtifact(p.Source, store.ArtifactContent).(*store.SourceArtifact)
			if input == nil || input.LocalPath != occurrence.Declaration.Source && input.OriginPath(input.LocalPath) != occurrence.Declaration.Source {
				continue
			}
		}
		return p.Source
	}
	return manifest.BootstrapSourceID
}

func (c *admissionClosure) productions(id manifest.NamedResource) []manifest.Production {
	state := c.states[id]
	productions := state.productions
	if !state.productionsKnown {
		if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
			productions = c.objects.IncomingProductions(c.incoming, c.loader.Index)
		} else {
			productions = c.objects.Productions(id, c.loader.Index)
		}
		if c.states == nil {
			c.states = map[manifest.NamedResource]admissionState{}
		}
		state.productions, state.productionsKnown = productions, true
		c.states[id] = state
	}
	if len(c.accepted) == 0 {
		return productions
	}
	productions = slices.DeleteFunc(slices.Clone(productions), func(p manifest.Production) bool { return c.replace[p.Owner] })
	for _, records := range c.accepted {
		if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
			break
		}
		if record, ok := records[id]; ok {
			productions = append(productions, record.Productions...)
		}
	}
	return productions
}

func (c *admissionClosure) delta(source manifest.NamedResource) (*change.Set, *store.SourceArtifact, bool) {
	if source == (manifest.NamedResource{}) {
		source = manifest.BootstrapSourceID
	}
	delta, snapshots, declarations := c.loader.Options.Changes.ForSource(source)
	input, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	declaration := c.objects.GetObject(source)
	for side, snapshot := range snapshots {
		if snapshot == nil || input == nil || declaration != declarations[side] {
			continue
		}
		bound := snapshot == input
		if c.loader.frozen {
			bound = c.loader.sideKnown && manifest.TreeSide(side) == c.loader.side && c.loader.bindings[source] == input && snapshot.Input(store.ArtifactContent|store.ArtifactRevision) == input.Input(store.ArtifactContent|store.ArtifactRevision)
		}
		if bound && input.Reads.Valid() {
			return delta, input, true
		}
	}
	if source == manifest.BootstrapSourceID {
		return delta, input, false
	}
	_, pinned, external := SourceView(c.objects, source, c.loader.SourceRoot)
	if !pinned && !external {
		delta, _, _ = c.loader.Options.Changes.ForSource(manifest.BootstrapSourceID)
		return delta, input, false
	}
	return nil, input, false
}

func (c *admissionClosure) fileChanged(source manifest.NamedResource, file string) bool {
	delta, input, paired := c.delta(source)
	if delta == nil {
		return false
	}
	if paired && input != nil {
		relative, err := filepath.Rel(input.OriginPath(input.LocalPath), filepath.Join(c.loader.SourceRoot, filepath.FromSlash(file)))
		return err == nil && delta.Contains(filepath.ToSlash(relative))
	}
	return delta.Contains(file)
}

func (c *admissionClosure) inputsChanged(source manifest.NamedResource, inputs []string) bool {
	if len(inputs) == 0 {
		return false
	}
	for file := range c.sourceFiles(source) {
		if slices.Contains(inputs, file) {
			return true
		}
	}
	return false
}

func (c *admissionClosure) outputChanged(id manifest.NamedResource, ref manifest.Reference, obj manifest.BaseManifest) (bool, bool) {
	source := c.source(id)
	_, _, paired := c.delta(source)
	if !paired || !c.loader.sideKnown {
		return false, false
	}
	if prepared := c.loader.candidates[id]; prepared.Obj != nil && obj != prepared.Obj && c.loader.Index.Records[id].Builder != "" && manifest.Equal(obj, prepared.Obj) {
		var doc map[string]any
		if c.incoming.Obj != nil && c.incoming.Obj.Named() == id && c.incoming.Doc != nil {
			doc = c.incoming.Doc
		} else if c.objects.GetDeclaration(id) == obj {
			_, current, output := c.objects.GetEmission(id)
			defer output.Finish()
			doc = current
		}
		if manifest.ValueEqual(doc, prepared.Doc) {
			obj = prepared.Obj
		}
	}
	if ref.Use != "" && ref.Name != "" {
		input := c.object(ref.NamedResource)
		if input == nil {
			return false, false
		}
		if _, known := c.outputChanged(ref.NamedResource, manifest.Reference{NamedResource: ref.NamedResource}, input); !known {
			return false, false
		}
	}
	return c.loader.Options.Changes.OutputChanged(c.loader.side, source, ref, obj)
}

func (c *admissionClosure) affected(id manifest.NamedResource, obj manifest.BaseManifest) bool {
	if c.loader.Options.Changes == nil {
		return false
	}
	record := c.record(id)
	direct := false
	fileOwned := false
	for _, production := range c.productions(id) {
		_, indexed := c.coordinates(production)
		fileOwned = fileOwned || indexed == record.File
		if production.Declaration == (manifest.NamedResource{}) && (production.Materializer(id) != (manifest.NamedResource{}) || production.Owner == (manifest.NamedResource{}) || manifest.Resolve(production.Authored, id.Namespace, "") == id) && c.changedProduction(production) {
			direct = true
		}
	}
	if !fileOwned {
		delta, _, _ := c.delta(manifest.BootstrapSourceID)
		direct = direct || delta.Contains(record.File)
	}
	if direct {
		if changed, known := c.outputChanged(id, manifest.Reference{NamedResource: id}, obj); !known || changed {
			return true
		}
	}
	if c.inputsChanged(manifest.BootstrapSourceID, record.Inputs) {
		if changed, known := c.outputChanged(id, manifest.Reference{NamedResource: id}, obj); !known || changed {
			return true
		}
	}
	roots := record.ContentRoots
	if roots == nil && obj != nil {
		roots = map[manifest.NamedResource][]string{}
		var paths []string
		initialized := false
		for root := range manifest.RenderRoots(obj) {
			if !initialized {
				initialized = true
				for source := range c.sources(id) {
					if input, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact); input != nil && input.FS != nil {
						roots[source] = nil
					}
				}
			}
			if len(roots) != 0 {
				paths = append(paths, root)
			}
		}
		for source := range roots {
			roots[source] = paths
		}
	}

	if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
		source := manifest.BootstrapSourceID
		for ref := range manifest.References(obj, manifest.ReferenceRelations) {
			if ref.Use == "source" {
				source = ref.NamedResource
				break
			}
		}
		if source == manifest.BootstrapSourceID {
			for _, ref := range record.Refs {
				if ref.KindInfo().SourceArtifact {
					source = ref
					break
				}
			}
		}
		roots = map[manifest.NamedResource][]string{source: slices.Collect(manifest.RenderRoots(obj))}
	}
	for source, roots := range roots {
		delta, input, paired := c.delta(source)
		if delta == nil || len(roots) == 0 || record.Pinned && !paired {
			continue
		}
		files := delta.Paths()
		for _, root := range roots {
			if strings.Contains(root, "/../") || strings.HasSuffix(root, "/..") {
				if resolved, ok := c.resolveRoot(source, root); ok {
					root = resolved
				} else {
					root = "."
				}
			}
			for _, file := range files {
				base := cmp.Or(root, ".")
				if !paired && input != nil {
					relative, err := filepath.Rel(c.loader.SourceRoot, input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(root))))
					if err != nil {
						continue
					}
					base = filepath.ToSlash(relative)
				}
				if base != "." && file != base && !strings.HasPrefix(file, strings.TrimSuffix(base, "/")+"/") {
					continue
				}
				prefix, indexed := base, file
				if paired && input != nil {
					physical := input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(root)))
					relative, err := filepath.Rel(c.loader.SourceRoot, physical)
					if err != nil {
						continue
					}
					prefix = filepath.ToSlash(relative)
					physical = input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(file)))
					relative, err = filepath.Rel(c.loader.SourceRoot, physical)
					if err != nil {
						continue
					}
					indexed = filepath.ToSlash(relative)
				}
				if prefix == "." {
					prefix = ""
				}
				claim := prefix
				if claim != "" {
					claim += "/"
				}
				if slices.Contains(c.loader.Index.Claims[claim], id) {
					owners, _ := c.loader.Index.OwnersOfFile(indexed)
					if !slices.Contains(owners, id) {
						continue
					}
				}
				return true
			}
		}
	}
	return false
}

func (c *admissionClosure) start(ctx context.Context, materializers func(materializerDemand, manifest.NamedResource, bool) bool) error {
	c.collecting = true
	c.queue = c.queue[:0]
	for _, root := range c.roots {
		occurrences, err := c.loader.Occurrences(ctx, filepath.Join(c.loader.SourceRoot, filepath.FromSlash(root)), "")
		if err != nil {
			return err
		}
		matches := slices.ContainsFunc(occurrences, func(o Occurrence) bool { return c.loader.Options.Selection.Matches(o.Identity) })
		entry := filepath.Join(c.loader.SourceRoot, filepath.FromSlash(root)) == c.loader.entryPath && c.loader.Options.Selection.Matches(manifest.NamedResource{})
		if !matches && !entry {
			continue
		}
		changed := entry || !c.loader.Options.Selection.Changed || c.loader.Options.Changes == nil
		for _, visit := range c.loader.scopeWalk(graphVisit{root: c.loader.SourceRoot, dir: root}) {
			changed = changed || c.fileChanged(manifest.BootstrapSourceID, visit.coordinate)
		}
		if changed {
			c.proposal.Blocked = append(c.proposal.Blocked, root)
		}
	}
	objects := c.loader.Store.Objects()
	capacity := max(len(c.loader.Index.Records), len(objects))
	if c.states == nil {
		c.states = make(map[manifest.NamedResource]admissionState, capacity)
	}
	ids := make([]manifest.NamedResource, 0, capacity)
	index := func(id manifest.NamedResource) {
		state := c.states[id]
		if state.listed {
			return
		}
		state.listed = true
		c.states[id] = state
		ids = append(ids, id)
	}
	for id := range c.loader.Index.Records {
		index(id)
	}
	for _, records := range c.accepted {
		for id := range records {
			index(id)
		}
	}
	for _, obj := range objects {
		index(obj.Named())
	}
	delta, _, _ := c.delta(manifest.BootstrapSourceID)
	for id := range delta.Inputs() {
		if c.changedInput(id, id) {
			index(id)
		}
	}
	type edge struct {
		consumer         manifest.NamedResource
		next, generation int
	}
	edges := make([]edge, 0, len(ids))
	link := func(from, to manifest.NamedResource) {
		state := c.states[from]
		edges = append(edges, edge{consumer: to, next: state.consumer, generation: c.states[to].generation})
		state.consumer = len(edges)
		c.states[from] = state
	}
	type restoration struct {
		child manifest.NamedResource
		next  int
	}
	var restorations []restoration
	type seed struct {
		target, origin   manifest.NamedResource
		generation       int
		requiresConsumer bool
	}
	var seeds []seed
	type claim struct {
		file     string
		next     int
		ancestor bool
	}
	var claims []claim
	claimFile := func(id manifest.NamedResource, file string, ancestor bool) {
		state := c.states[id]
		claims = append(claims, claim{file: file, next: state.claim, ancestor: ancestor})
		state.claim = len(claims)
		c.states[id] = state
	}
	for source := range c.loader.Options.Changes.Sources() {
		for file := range c.sourceFiles(source) {
			for reader := range c.loader.Index.ReadersOfFile(file) {
				if c.changedClaim(reader, file) {
					state := c.states[reader.Reader]
					state.seed = true
					c.states[reader.Reader] = state
				}
			}
			index := *c.loader.Index
			index.Readers = nil
			owners, ancestors := index.OwnersOfFile(file)
			for _, owner := range owners {
				claimFile(owner, file, false)
			}
			for _, ancestor := range ancestors {
				claimFile(ancestor, file, true)
			}
		}
	}
	files := make([]string, 0, delta.Len())
	for file := range c.sourceFiles(manifest.BootstrapSourceID) {
		files = append(files, file)
	}
	known := make([]bool, len(c.loader.Options.Selection.Roots)+len(files))
	covered := known[len(c.loader.Options.Selection.Roots):]
	scan := func(id manifest.NamedResource) {
		state := c.states[id]
		state.generation++
		c.states[id] = state
		obj := c.object(id)
		record := c.record(id)
		changed := c.affected(id, obj) || c.changedInput(id, id)
		for ref := range c.references(id, obj) {
			if !c.reverse(id, obj, ref) {
				continue
			}
			if c.changedInput(id, ref.NamedResource) {
				seeds = append(seeds, seed{target: ref.NamedResource, origin: id, generation: c.states[id].generation})
			}
			inputs, err := c.matchingInputs(ref, true)
			if err != nil {
				continue
			}
			for _, input := range inputs {
				link(input, id)
			}
		}
		if parent := record.Parent; parent != (manifest.NamedResource{}) && parent != id && !parent.ReportHidden() && !c.unchangedDeclaration(id, parent, obj) {
			link(parent, id)
		}
		productions := c.productions(id)
		for _, p := range productions {
			owner := p.Materializer(id)
			if owner == (manifest.NamedResource{}) && p.Owner != (manifest.NamedResource{}) {
				continue
			}
			if p.Declaration != (manifest.NamedResource{}) {
				if p.Authored == id && p.Declaration != id {
					continue
				}
				owner = p.Declaration
				if c.changedProduction(p) {
					seeds = append(seeds, seed{target: owner, origin: id, generation: c.states[id].generation, requiresConsumer: true})
				}
			}
			if owner != id && owner != (manifest.NamedResource{}) && !owner.ReportHidden() && !c.unchangedDeclaration(id, p.Owner, obj) {
				link(owner, id)
			}
		}
		state = c.states[id]
		state.direct = changed
		state.eligible = c.loader.Options.Selection.Matches(id)
		state.declarationChanged = state.eligible && !changed && (c.fileChanged(c.source(id), record.File) || c.inputsChanged(c.source(id), record.Inputs))
		state.materialize = false
		if state.eligible {
			for source, roots := range record.ContentRoots {
				delta, _, _ := c.delta(source)
				state.materialize = state.materialize || delta != nil && delta.Len() != 0 && slices.ContainsFunc(roots, manifest.HasEnvsubstReference)
			}
		}
		for next := state.claim; next != 0; next = claims[next-1].next {
			claim := claims[next-1]
			if claim.ancestor {
				continue
			}
			for source := range c.sources(id) {
				state.direct = state.direct || c.changedClaim(manifest.FileReader{Reader: id, Source: source}, claim.file)
			}
		}
		c.states[id] = state
		for i, target := range c.loader.Options.Selection.Roots {
			if !known[i] && id.Group == target.Group && id.Kind == target.Kind && (target.Name == "" || id.Name == target.Name) && (target.Namespace == "" || id.Namespace == target.Namespace) && (c.loader.IsFileIndexed(id) || obj != nil || len(productions) != 0) {
				known[i] = true
			}
		}
		for i, file := range files {
			covered[i] = covered[i] || record.File == file || slices.Contains(record.Inputs, file)
		}
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		scan(id)
		for _, production := range c.states[id].productions {
			if !c.replace[production.Owner] {
				continue
			}
			state := c.states[production.Owner]
			restorations = append(restorations, restoration{child: id, next: state.restoration})
			state.restoration = len(restorations)
			c.states[production.Owner] = state
		}
	}
	c.queue = slices.Grow(c.queue, len(c.states))
	for {
		c.queue = c.queue[:0]
		for id, state := range c.states {
			state.changed = state.seed || state.direct
			c.states[id] = state
		}
		for _, seed := range seeds {
			state := c.states[seed.origin]
			if seed.generation != state.generation {
				continue
			}
			if !seed.requiresConsumer {
				c.markChanged(seed.target)
				continue
			}
			for next := state.consumer; next != 0; next = edges[next-1].next {
				edge := edges[next-1]
				if edge.generation == c.states[edge.consumer].generation {
					c.markChanged(seed.target)
					break
				}
			}
		}
		for id, state := range c.states {
			if state.changed {
				c.queue = append(c.queue, id)
			}
		}
		for cursor := 0; cursor < len(c.queue); cursor++ {
			id := c.queue[cursor]
			for next := c.states[id].consumer; next != 0; next = edges[next-1].next {
				edge := edges[next-1]
				consumer := edge.consumer
				if edge.generation != c.states[consumer].generation || c.states[consumer].changed {
					continue
				}
				obj := c.object(consumer)
				unchanged := false
				for ref := range c.references(consumer, obj) {
					if ref.NamedResource != id {
						continue
					}
					changed, known := c.outputChanged(consumer, ref, obj)
					unchanged = known && !changed
					if !unchanged {
						break
					}
				}
				if !unchanged {
					c.markChanged(consumer)
					c.queue = append(c.queue, consumer)
				}
			}
		}
		c.queue = c.queue[:0]
		rescan := func(id manifest.NamedResource) {
			state := c.states[id]
			if state.rescan {
				return
			}
			state.rescan = true
			c.states[id] = state
			c.queue = append(c.queue, id)
		}
		for owner, replacing := range c.replace {
			if !replacing || !c.states[owner].changed {
				continue
			}
			c.replace[owner] = false
			rescan(owner)
			for next := c.states[owner].restoration; next != 0; next = restorations[next-1].next {
				rescan(restorations[next-1].child)
			}
		}
		if len(c.queue) == 0 {
			break
		}
		//nolint:intrange // rescan MUST extend the worklist during traversal.
		for cursor := 0; cursor < len(c.queue); cursor++ {
			for next := c.states[c.queue[cursor]].consumer; next != 0; next = edges[next-1].next {
				edge := edges[next-1]
				if edge.generation == c.states[edge.consumer].generation {
					rescan(edge.consumer)
				}
			}
		}
		for _, id := range c.queue {
			if err := ctx.Err(); err != nil {
				return err
			}
			scan(id)
			state := c.states[id]
			state.rescan = false
			c.states[id] = state
		}
	}
	var entry manifest.NamedResource
	for id, state := range c.states {
		if id.ReportHidden() {
			if producer, ok := c.object(id).(interface {
				EmissionPolicy() manifest.EmissionPolicy
			}); ok {
				policy := producer.EmissionPolicy()
				if policy.DocumentScope {
					entry = id
				}
			}
		}
		if state.changed && !id.ReportHidden() && id.Namespace != "" {
			c.proposal.Namespaces = append(c.proposal.Namespaces, id.Namespace)
		}
		for next := state.claim; next != 0; next = claims[next-1].next {
			claim := claims[next-1]
			if !claim.ancestor {
				continue
			}
			for source := range c.sources(id) {
				if c.changedClaim(manifest.FileReader{Reader: id, Source: source}, claim.file) {
					c.add(id, admissionRole{needed: true})
				}
			}
		}
		if !state.listed || !state.eligible {
			continue
		}
		if state.materialize {
			for _, production := range c.productions(id) {
				c.add(production.Materializer(id), admissionRole{needed: true})
			}
		}
		if state.changed || !c.loader.Options.Selection.Changed || c.loader.Options.Changes == nil {
			c.add(id, admissionRole{needed: true, selected: !id.ReportHidden(), dirty: true})
		} else if state.declarationChanged {
			c.add(id, admissionRole{needed: true, selected: true})
		}
	}
	if entry != (manifest.NamedResource{}) {
		status, _ := c.objects.GetStatus(entry)
		if status.Outcome() == store.OutcomeFailed {
			c.add(entry, admissionRole{needed: true})
		}
	}
	slices.Sort(c.proposal.Namespaces)
	c.proposal.Namespaces = slices.Compact(c.proposal.Namespaces)
	for i, target := range c.loader.Options.Selection.Roots {
		if !known[i] {
			materializers(materializerDemand{target: target, source: c.loader.SourceRoot}, manifest.NamedResource{}, true)
		}
	}
	for i, file := range files {
		owners, _ := c.loader.Index.OwnersOfFile(file)
		covered[i] = covered[i] || len(owners) != 0
		for range c.loader.Index.ReadersOfFile(file) {
			covered[i] = true
		}
		if !covered[i] {
			target := manifest.NamedResource{}
			if len(c.loader.Options.Selection.Roots) != 0 {
				target.Namespace = c.loader.Options.Selection.Roots[0].Namespace
			}
			materializers(materializerDemand{target: target, source: c.loader.SourceRoot}, manifest.NamedResource{}, true)
			break
		}
	}
	c.queue = c.queue[:0]
	c.collecting = false
	for id, state := range c.states {
		if state.role.needed {
			c.queue = append(c.queue, id)
		}
	}

	return nil
}

func (c *admissionClosure) independent(id manifest.NamedResource, owners func() []manifest.BaseManifest) bool {
	record := c.record(id)
	root := id
	if occurrence, ok := c.loader.occurrence(id); ok && occurrence.Authored.Namespace == "" {
		if _, exact := c.loader.Index.Lookup(id); !exact {
			root.Namespace = ""
		}
	}
	if !c.loader.AdmitsRoot(c.loader.CatalogObject(root), record.Path) || record.Parent != (manifest.NamedResource{}) && record.Parent != id {
		return false
	}
	for _, owner := range owners() {
		if owner.Named() != id && c.coversDeclaration(owner, id) {
			return false
		}
	}
	return true
}

func (c *admissionClosure) coversDeclaration(owner manifest.BaseManifest, id manifest.NamedResource) bool {
	if owner == nil {
		return false
	}
	source, dir, applies := owner.SourcePath()
	producer, ok := owner.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	})
	if !applies || !ok || !producer.EmissionPolicy().MatchesNamespace(id) {
		return false
	}
	input, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
	file := c.record(id).Path
	if input == nil || file == "" || c.declarationSource(id) != input.OriginPath(input.LocalPath) {
		return false
	}
	root := input.OriginPath(filepath.Join(input.LocalPath, dir))
	return safepath.Contains(input.OriginPath(input.LocalPath), root) && safepath.Contains(root, file)
}

func (c *admissionClosure) hydrate(ctx context.Context, id manifest.NamedResource) (manifest.BaseManifest, error) {
	record := c.record(id)
	var obj manifest.BaseManifest
	blocked := false
	for _, root := range c.roots {
		relative := filepath.ToSlash(root)
		if record.File == path.Join(relative, "kustomization.yaml") || strings.HasPrefix(record.File, strings.TrimSuffix(relative, "/")+"/") || relative == "." {
			c.proposal.Blocked = append(c.proposal.Blocked, root)
			blocked = true
		}
	}
	if !blocked {
		if child := c.loader.candidates[id]; child.Obj != nil && record.Path == "" {
			if c.children == nil {
				c.children = map[manifest.NamedResource]store.Child{}
			}
			c.children[id] = child
			obj = child.Obj
		} else if occurrence, ok := c.loader.occurrence(id); ok {
			root := c.loader.documents.mountedRoot(record.Path)
			var input *store.SourceArtifact
			for _, source := range [...]manifest.NamedResource{c.source(id), manifest.BootstrapSourceID} {
				candidate, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
				if candidate != nil && candidate == c.loader.documents.artifacts[root] {
					input = candidate
					break
				}
			}
			if input != nil {
				file, _ := filepath.Rel(root, record.Path)
				occurrence.Declaration.Source, occurrence.Declaration.File = root, filepath.ToSlash(file)
				child, err := c.loader.exactObject(ctx, occurrence, input)
				if err != nil {
					return nil, err
				}
				if child.Obj != nil {
					child.Productions = slices.Clone(c.productions(id))
					if c.children == nil {
						c.children = map[manifest.NamedResource]store.Child{}
					}
					c.children[id] = child
					obj = child.Obj
				}
			}
		}
	}
	return obj, nil
}

func (c *admissionClosure) walk(ctx context.Context, materializers func(materializerDemand, manifest.NamedResource, bool) bool) error {
	if len(c.queue) == 0 {
		return nil
	}
	var owners []manifest.BaseManifest
	ownersKnown := false
	owned := func() []manifest.BaseManifest {
		if !ownersKnown {
			for _, owner := range c.objects.ListObjects("", "") {
				if slices.ContainsFunc(c.productions(owner.Named()), func(p manifest.Production) bool {
					return p.Owner != (manifest.NamedResource{}) && p.Owner != owner.Named()
				}) {
					owners = append(owners, owner)
				}
			}
			ownersKnown = true
		}
		return owners
	}
	//nolint:intrange // add MUST extend the worklist during traversal.
	for cursor := 0; cursor < len(c.queue); cursor++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := c.queue[cursor]
		role := c.role(id)
		state := c.states[id]
		if state.walkedOnce && state.walked == role {
			continue
		}
		state.walked, state.walkedOnce = role, true
		c.states[id] = state
		obj := c.object(id)
		record := c.record(id)

		independent := c.independent(id, owned)
		materialized := record.Parent != (manifest.NamedResource{}) && record.Parent != id || record.BuilderOwner != (manifest.NamedResource{}) && record.BuilderOwner != id
		if !independent || !record.Parent.ReportHidden() {
			c.add(record.Parent, admissionRole{needed: true, dirty: c.states[record.Parent].changed})
		}
		pathBearing := false
		if occurrence, ok := c.loader.occurrence(id); ok {
			_, _, pathBearing = occurrence.SourcePath()
			if c.loader.Options.WipeSecrets && occurrence.facts.placeholder {
				independent = true
			}
		}
		for _, p := range c.productions(id) {
			owner := p.Materializer(id)
			if independent && owner.ReportHidden() && owner == record.BuilderOwner {
				owner = manifest.NamedResource{}
			}
			if owner == (manifest.NamedResource{}) && !independent && p.Declaration == (manifest.NamedResource{}) && p.Authored != (manifest.NamedResource{}) && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) && c.objects.GetDeclaration(id) == nil {
				owner = p.Owner
			}
			materialized = materialized || p.Owner != (manifest.NamedResource{}) && p.Owner != id || p.Declaration != (manifest.NamedResource{}) && p.Declaration != id
			if independent && c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
				needed, _, _, _ := c.loader.Options.Filter.Membership(id)
				materialized = materialized || needed && p.Owner == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) &&
					p.Path == record.Path && p.File == record.File && p.Source == c.source(id) && manifest.Resolve(p.Authored, id.Namespace, "") == id
			}
			c.add(owner, admissionRole{needed: true, dirty: c.states[owner].changed})
			c.add(p.Declaration, admissionRole{needed: true, dirty: c.states[p.Declaration].changed})
			if owner != (manifest.NamedResource{}) && filepath.IsAbs(p.Path) {
				_, file := c.coordinates(p)
				for reader := range c.loader.Index.ReadersOfFile(file) {
					if producer, ok := c.object(reader.Reader).(interface {
						EmissionPolicy() manifest.EmissionPolicy
					}); ok {
						if !producer.EmissionPolicy().MatchesNamespace(id) {
							continue
						}
					} else if occurrence, ok := c.loader.occurrence(reader.Reader); ok && occurrence.facts.policy != nil && !occurrence.facts.policy.MatchesNamespace(id) {
						continue
					}
					_, pinned, external := SourceView(c.objects, p.Source, c.loader.SourceRoot)
					if reader.Source != p.Source && (pinned || external) {
						continue
					}
					c.add(reader.Reader, admissionRole{needed: true})
				}
			}
		}
		if c.incoming.Obj != nil && c.incoming.Obj.Named() == id {
			record, _ := c.loader.Index.Lookup(id)
			for _, production := range record.Productions {
				if owner := production.Materializer(id); c.object(owner) != nil {
					c.add(owner, admissionRole{needed: true, dirty: c.states[owner].changed})
				}
			}
		}

		if record.Path != "" && !materialized && (role.selected && (c.fileChanged(c.source(id), record.File) || id.Namespace == "" && id.KindInfo().Namespaced() && c.affected(id, obj)) || (role.selected || pathBearing) && !independent && c.objects.GetDeclaration(id) == nil) {
			target := id
			if independent && pathBearing {
				target = manifest.NamedResource{Namespace: id.Namespace}
			}
			if materializers(materializerDemand{target: target, source: c.declarationSource(id)}, id, !independent || pathBearing) {
				if occurrence, ok := c.loader.occurrence(id); ok {
					_, _, applies := occurrence.SourcePath()
					independent = independent && !applies
				}
			}
		}
		if independent && (c.incoming.Obj == nil || c.incoming.Obj.Named() != id) && c.children[id].Obj == nil && c.objects.GetObject(id) == nil {
			var err error
			obj, err = c.hydrate(ctx, id)
			if err != nil {
				return err
			}
		}
		if role.selected && role.dirty && c.loader.Store.HasInventory(id) {
			_, _, output := c.objects.GetEmission(id)
			for _, child := range output.Children {
				if id.ReportHidden() && c.loader.IsFileIndexed(child.Obj.Named()) {
					continue
				}
				if c.unchangedDeclaration(child.Obj.Named(), id, child.Obj) {
					continue
				}
				c.add(child.Obj.Named(), admissionRole{needed: true, selected: true, dirty: true})
			}
			output.Finish()
		}

		if state.listed && role.selected && role.dirty && id.KindInfo().Data && c.loader.Options.Selection.Changed && c.loader.Options.Selection.Matches(manifest.NamedResource{Namespace: id.Namespace}) {
			materializers(materializerDemand{target: id, source: c.declarationSource(id), readers: true}, id, true)
		}

		generated, _ := manifest.GeneratedObjects(obj)
		var generatedTargets []manifest.ProducerTarget
		if obj == nil {
			if occurrence, ok := c.loader.occurrence(id); ok {
				generatedTargets = occurrence.facts.targets[len(occurrence.facts.authoredTargets):]
			}
		}
		for ref := range c.references(id, obj) {
			if slices.ContainsFunc(generated, func(child manifest.BaseManifest) bool { return child.Named() == ref.NamedResource }) || slices.ContainsFunc(generatedTargets, func(target manifest.ProducerTarget) bool {
				return manifest.Resolve(target.NamedResource, "", id.Namespace) == ref.NamedResource
			}) {
				continue
			}
			inputRecord, _ := c.loader.Index.Lookup(ref.NamedResource)
			if slices.ContainsFunc(inputRecord.Productions, func(p manifest.Production) bool { return p.Owner == id && p.Declaration == id }) {
				continue
			}
			if ref.Ordering {
				indexed := c.loader.IsFileIndexed(ref.NamedResource)
				object := c.object(ref.NamedResource)
				if !c.role(ref.NamedResource).needed && (indexed || object != nil) {
					continue
				}
				if !indexed && object == nil {
					materializers(materializerDemand{target: ref.NamedResource, source: c.declarationSource(id), ordering: true}, id, false)
				}
			} else if !ref.ContentInput() && ref.Gate == 0 && ref.Use != "materializer" {
				continue
			}
			if ref.Selector != nil && ref.Name == "" {
				inputs, err := c.matchingInputs(ref, false)
				if err != nil {
					return err
				}
				for _, input := range inputs {
					c.add(input, admissionRole{needed: true, dirty: c.states[input].changed})
				}
				if len(inputs) == 0 {
					materializers(materializerDemand{target: ref.NamedResource, source: c.declarationSource(id)}, id, c.reverse(id, obj, ref) && !ref.Acquisition)
				}
				continue
			}
			if ref.Name == "" {
				continue
			}
			wasNeeded := c.role(ref.NamedResource).needed
			c.add(ref.NamedResource, admissionRole{needed: true, dirty: c.states[ref.NamedResource].changed})
			if !ref.Ordering && !wasNeeded && c.object(ref.NamedResource) == nil {
				if _, indexed := c.loader.Index.Lookup(ref.NamedResource); !indexed && !c.loader.IsFileIndexed(ref.NamedResource) {
					materializers(materializerDemand{target: ref.NamedResource, source: c.declarationSource(id)}, id, c.reverse(id, obj, ref) && !ref.Acquisition)
				}
			}
		}
	}
	return nil
}

func (c *admissionClosure) materializerCandidates(ordering bool) [2][]Occurrence {
	var candidates [2][]Occurrence
	if !ordering {
		for _, occurrence := range c.loader.locations {
			if occurrence.facts == nil || occurrence.facts.hasPath && occurrence.facts.policy != nil && len(occurrence.facts.roots) == 0 {
				continue
			}
			candidates[0] = append(candidates[0], occurrence)
		}
		for id, child := range c.loader.candidates {
			if _, located := c.loader.locations[id]; located || c.loader.Index.Records[id].Builder == "" {
				continue
			}
			occurrence := compactOccurrence(child.Obj)
			if occurrence.facts.hasPath && occurrence.facts.policy != nil && len(occurrence.facts.roots) == 0 {
				continue
			}
			occurrence.Declaration.Source = c.declarationSource(id)
			candidates[0] = append(candidates[0], occurrence)
		}
	}
	for _, object := range c.loader.Store.Objects() {
		id := object.Named()
		if _, indexed := c.loader.locations[id]; !ordering && (indexed || c.loader.candidates[id].Obj != nil && c.loader.Index.Records[id].Builder != "") {
			continue
		}
		if _, _, pathBearing := object.SourcePath(); !pathBearing && !ordering {
			continue
		}
		object = c.object(id)
		if object == nil {
			continue
		}
		if _, producer := object.(interface {
			EmissionPolicy() manifest.EmissionPolicy
		}); !producer {
			continue
		}
		occurrence := compactOccurrence(object)
		if occurrence.facts.hasPath && occurrence.facts.policy != nil && len(occurrence.facts.roots) == 0 {
			continue
		}
		if !occurrence.facts.hasPath {
			occurrence.Declaration.Source = c.declarationSource(id)
		}
		candidates[1] = append(candidates[1], occurrence)
	}
	return candidates
}

func (c *admissionClosure) materializers(demand materializerDemand, consumer manifest.NamedResource, open bool, snapshot [2][]Occurrence) bool {
	demand.consumer = consumer
	if pending, searched := c.searched[demand]; searched {
		return pending
	}
	if c.searched == nil {
		c.searched = map[materializerDemand]bool{}
	}
	c.searched[demand] = false
	if !demand.readers {
		owners := func(yield func(manifest.NamedResource) bool) {
			record := c.record(consumer)
			if !yield(record.Parent) || !yield(record.BuilderOwner) {
				return
			}
			for _, production := range c.productions(consumer) {
				if !yield(production.Materializer(consumer)) {
					return
				}
			}
		}
		for owner := range owners {
			if owner == (manifest.NamedResource{}) || owner == consumer || !c.role(owner).needed || c.object(owner) == nil && !c.loader.IsFileIndexed(owner) {
				continue
			}
			status, _ := c.objects.GetStatus(owner)
			if status.Outcome() == store.OutcomeFailed {
				continue
			}
			output, ok := c.objects.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
			if !ok || !output.Complete {
				c.searched[demand] = true
				return true
			}
		}
	}
	target := demand.target
	if !demand.readers && c.objects.ReferenceFacts(target, c.loader.Index).Supplied != nil {
		return false
	}
	matches := func(id manifest.NamedResource) bool {
		if demand.readers {
			if !c.loader.Options.Selection.Matches(id) {
				return false
			}
			for ref := range c.references(id, c.object(id)) {
				if !ref.ContentInput() {
					continue
				}
				inputs, err := c.matchingInputs(ref, true)
				if err == nil && slices.Contains(inputs, target) {
					return true
				}
			}
			return false
		}

		if target.Kind == "" {
			return c.loader.Options.Selection.Matches(id) && c.affected(id, c.object(id)) || c.coversDeclaration(c.object(id), consumer)
		}
		return id.Group == target.Group && id.Kind == target.Kind && (target.Namespace == "" || id.Namespace == "" || id.Namespace == target.Namespace) && (target.Name == "" || id.Name == target.Name)
	}
	candidates := func(yield func(manifest.NamedResource, Occurrence) bool) {
		if !demand.ordering {
			for _, occurrence := range snapshot[0] {
				if !yield(occurrence.Identity, occurrence) {
					return
				}
			}
		}
		if demand.source == "" {
			return
		}
		for _, occurrence := range snapshot[1] {
			id := occurrence.Identity
			if _, indexed := c.loader.locations[id]; !demand.ordering && (indexed || c.loader.candidates[id].Obj != nil && c.loader.Index.Records[id].Builder != "") {
				continue
			}
			_, _, pathBearing := occurrence.SourcePath()
			policy := occurrence.facts.policy
			if !pathBearing && !demand.ordering || policy == nil || !policy.MatchesNamespace(target) {
				continue
			}
			if demand.ordering && !slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool { return p.Owner != (manifest.NamedResource{}) }) {
				continue
			}
			if pathBearing {
				occurrence.Declaration.Source = demand.source
			}
			if !yield(id, occurrence) {
				return
			}
		}
	}
	pending := false
	for id, occurrence := range candidates {
		if id == consumer || id == target || demand.source != "" && demand.source != occurrence.Declaration.Source {
			continue
		}
		if consumer != (manifest.NamedResource{}) && slices.ContainsFunc(c.productions(id), func(p manifest.Production) bool {
			return p.Owner == consumer || p.Declaration == consumer
		}) {
			continue
		}
		facts := occurrence.facts
		if facts == nil {
			continue
		}
		state := c.states[id]
		if demand.readers && id.KindInfo().Render && state.listed && !state.changed && !state.materialize && !c.role(id).dirty {
			continue
		}
		if source, directory, applies := occurrence.SourcePath(); applies && consumer != (manifest.NamedResource{}) {
			if source.Name == "" && c.loader.IsFileIndexed(id) {
				source = manifest.BootstrapSourceID
			}
			input, _ := c.objects.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			if input == nil || input.OriginPath(input.LocalPath) != demand.source || demand.ordering && c.loader.documents.cache[filepath.Join(input.LocalPath, directory)].traversed {
				continue
			}
		}
		if (target.Kind == "" || demand.readers) && facts.hasPath && !slices.ContainsFunc(facts.roots, manifest.HasEnvsubstReference) && !c.coversDeclaration(c.object(id), consumer) {
			continue
		}
		match := demand.ordering
		if !demand.readers {
			for _, produced := range facts.targets {
				ref := produced.NamedResource
				if ref.Group == target.Group && ref.Kind == target.Kind && (target.Name == "" || ref.Name == target.Name || manifest.HasEnvsubstReference(ref.Name)) && (target.Namespace == "" || ref.Namespace == target.Namespace || ref.Namespace == "" || manifest.HasEnvsubstReference(ref.Namespace)) {
					match = true
					break
				}
			}
		}
		if open && facts.policy != nil && facts.policy.MatchesNamespace(target) {
			match = true
		}
		if !demand.readers && facts.needsCandidates && (id.Namespace == target.Namespace || id.Namespace == "") {
			match = true
		}
		if !match {
			continue
		}
		if c.replace[id] {
			present := false
			for child := range c.accepted[id] {
				present = present || matches(child)
			}
			if !present {
				continue
			}
		}
		complete := false
		if output, ok := c.objects.GetArtifact(id, store.ArtifactContent).(*store.Emission); ok && output.Complete {
			status, _ := c.objects.GetStatus(id)
			complete = status.Outcome() == store.OutcomePassed
			if status.Outcome() == store.OutcomePassed && !slices.ContainsFunc(output.Children, func(child store.Child) bool { return matches(child.Obj.Named()) }) {
				continue
			}
		}
		pending = pending || !complete
		c.add(id, admissionRole{needed: true, dirty: c.states[id].changed})
	}
	c.searched[demand] = pending
	return pending
}

// A named input borrows the scratch slot until the next call.
func (c *admissionClosure) matchingInputs(ref manifest.Reference, reverse bool) ([]manifest.NamedResource, error) {
	if ref.Name != "" {
		c.inputScratch[0] = ref.NamedResource
		return c.inputScratch[:], nil
	}
	if ref.Selector == nil {
		return nil, nil
	}
	selector, err := metav1.LabelSelectorAsSelector(ref.Selector)
	if err != nil {
		return nil, fmt.Errorf("%w: input selector: %w", manifest.ErrInput, err)
	}
	var inputs []manifest.NamedResource
	target := ref.NamedResource
	matches := func(id manifest.NamedResource, values map[string]string) {
		if id.Group != target.Group || id.Kind != target.Kind || id.Namespace != target.Namespace || slices.Contains(inputs, id) {
			return
		}
		matched := selector.Matches(labels.Set(values))
		if !matched && reverse && c.loader.sideKnown {
			source := c.source(id)
			if _, _, paired := c.delta(source); paired {
				object := c.object(id)
				if object == nil {
					object = c.loader.candidates[id].Obj
				}
				opposite := c.loader.Options.Changes.OppositeDeclaration(c.loader.side, source, object)
				if labeled, ok := opposite.(interface{ GetLabels() map[string]string }); ok {
					matched = selector.Matches(labels.Set(labeled.GetLabels()))
				}
			}
		}
		if matched {
			inputs = append(inputs, id)
		}
	}
	for _, object := range c.objects.ListObjects(ref.Group, ref.Kind) {
		object = c.object(object.Named())
		if labeled, ok := object.(interface{ GetLabels() map[string]string }); ok {
			matches(object.Named(), labeled.GetLabels())
		}
	}
	candidates := func(yield func(manifest.NamedResource) bool) {
		for id := range c.loader.locations {
			if !yield(id) {
				return
			}
		}
		for id := range c.loader.candidates {
			if _, located := c.loader.locations[id]; !located && !yield(id) {
				return
			}
		}
	}
	if c.loader.frozen {
		candidates = slices.Values(c.loader.inputObjects[ref.NamedResource])
	}
	for id := range candidates {
		occurrence := c.loader.locations[id]
		if id.Group != target.Group || id.Kind != target.Kind || id.Namespace != target.Namespace {
			continue
		}
		if object := c.object(id); object != nil {
			if labeled, ok := object.(interface{ GetLabels() map[string]string }); ok {
				matches(id, labeled.GetLabels())
			}
		} else if candidate := c.loader.candidates[id]; candidate.Obj != nil {
			if labeled, ok := candidate.Obj.(interface{ GetLabels() map[string]string }); ok {
				matches(id, labeled.GetLabels())
			}
		} else if occurrence.facts != nil {
			matches(id, occurrence.facts.labels)
		}
	}
	if c.incoming.Obj != nil {
		if labeled, ok := c.incoming.Obj.(interface{ GetLabels() map[string]string }); ok {
			matches(c.incoming.Obj.Named(), labeled.GetLabels())
		}
	}
	slices.SortFunc(inputs, manifest.NamedResource.Compare)
	return inputs, nil
}

func (c *admissionClosure) unchangedDeclaration(id, owner manifest.NamedResource, object manifest.BaseManifest) bool {
	if changed, known := c.outputChanged(id, manifest.Reference{NamedResource: id}, object); known && !changed {
		return true
	}
	record := c.record(id)
	declaration := id
	_, located := c.loader.occurrence(id)
	owned := owner == manifest.Resolve(record.Parent, "", id.Namespace)
	if !located {
		declaration = manifest.NamedResource{}
	}
	for _, p := range c.productions(id) {
		if p.Owner != owner {
			continue
		}
		owned = true
		if located {
			continue
		}
		origin := p.Declaration
		if origin == (manifest.NamedResource{}) || origin == id {
			origin = p.Authored
		}
		if origin == (manifest.NamedResource{}) || origin == id || declaration != (manifest.NamedResource{}) && declaration != origin {
			declaration = manifest.NamedResource{}
			break
		}
		declaration = origin
	}
	ownerRecord := c.record(owner)
	ownerFile := ownerRecord.File
	if occurrence, located := c.loader.occurrence(owner); located {
		ownerFile = c.loader.fileCoordinate(filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File))
	}
	declared := c.record(declaration)
	if occurrence, located := c.loader.occurrence(declaration); located && owner != (manifest.NamedResource{}) && owned && declared.File != "" && declared.File != declared.Builder {
		source := c.source(declaration)
		delta, input, _ := c.delta(source)
		stable := ownerRecord.File != ownerRecord.Builder && delta != nil && input != nil && input.FS != nil && input.Reads.Valid() && input.OriginPath(input.LocalPath) == occurrence.Declaration.Source && !c.fileChanged(source, declared.File) && !c.fileChanged(c.source(owner), ownerFile) && !slices.ContainsFunc(c.productions(owner), c.changedProduction)
		if stable {
			for ref := range c.references(owner, c.object(owner)) {
				if !c.reverse(owner, c.object(owner), ref) {
					continue
				}
				if c.changedInput(owner, ref.NamedResource) || c.affected(ref.NamedResource, c.object(ref.NamedResource)) || !ref.Artifact && c.role(ref.NamedResource).dirty {
					stable = false
					break
				}
			}
		}
		if stable {
			for file := range c.sourceFiles(source) {
				if slices.Contains(declared.Inputs, file) || slices.Contains(record.Inputs, file) {
					stable = false
					break
				}
			}
		}
		if stable {
			parent := ownerRecord.Parent
			if parent != (manifest.NamedResource{}) && parent != owner && !parent.ReportHidden() {
				stable = c.unchangedDeclaration(owner, parent, c.object(owner))
			}
		}
		if stable {
			return true
		}
	}
	return (record.BuilderOwner == (manifest.NamedResource{}) || owner == record.BuilderOwner) && record.File != record.Builder && record.Declaration != nil && manifest.Equal(record.Declaration, object)
}
