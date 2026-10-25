package loader

import (
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// scopeConstruction is confined to sequential discovery. Physical walks are
// shared by equal contexts; each owner retains its own visits and claims.
type scopeConstruction struct {
	walks         map[graphContext]scopeTraversal
	records       map[manifest.NamedResource]manifest.DiscoveryRecord
	candidates    map[manifest.NamedResource]store.Child
	scanned       map[string]struct{}
	sources       map[sourceState]sourceScope
	inputs        []scopeInput
	complete      bool
	inactive      bool
	visits        map[string][]graphVisit
	contributions []scopeContribution
	claims        []manifest.KSClaim
	roots         []string
}

type scopeTraversal struct {
	visits     []graphVisit
	reached    fileEligibility
	controlled bool
}

type scopeInput struct {
	owner     *manifest.Kustomization
	source    sourceState
	published bool
	active    bool
}

type scopeContribution struct {
	owner, source  manifest.NamedResource
	visits         []graphVisit
	context        graphContext
	unshared       bool
	active         bool
	membershipOnly bool
	input          int
}

type sourceState struct {
	id       manifest.NamedResource
	object   manifest.BaseManifest
	artifact *store.SourceArtifact
	repoRoot string
}

type sourceScope struct {
	root                    string
	pinned, external, ready bool
}

func (l *Loader) sourceState(id manifest.NamedResource, repoRoot string) sourceState {
	art, _ := l.Store.GetArtifact(id).(*store.SourceArtifact)
	return sourceState{id: id, object: l.Store.GetObject(id), artifact: art, repoRoot: repoRoot}
}

func (l *Loader) scopeSource(state sourceState) sourceScope {
	if scope, ok := l.scopes.sources[state]; ok {
		return scope
	}
	root, pinned, external := SourceView(l.Store, state.id, state.repoRoot)
	scope := sourceScope{root: root, pinned: pinned, external: external}
	l.scopes.sources[state] = scope
	return scope
}

func (l *Loader) ensureScopes() {
	if l.scopes == nil {
		l.scopes = &scopeConstruction{walks: map[graphContext]scopeTraversal{}, scanned: map[string]struct{}{}, sources: map[sourceState]sourceScope{}, visits: map[string][]graphVisit{}}
	}
}

// Admit resolves scope evidence and publishes independently runnable discovery roots.
func (l *Loader) Admit(repoRoot string) {
	l.mountSources()
	l.admit(repoRoot)
}

func (l *Loader) admit(repoRoot string) []store.Child {
	l.ensureScopes()
	kss := l.kustomizations()
	if l.scopes.complete && l.scopesMatch(kss, repoRoot) {
		return kss
	}

	if repoRoot == "" {
		var local []*manifest.Kustomization
		for _, child := range kss {
			ks := child.Obj.(*manifest.Kustomization)
			if _, _, external := SourceView(l.Store, ks.SourceID(), repoRoot); !external {
				local = append(local, ks)
			}
		}
		l.scopes.claims = buildClaims(l.Store, repoRoot, l.documents, local)
		l.scopes.complete = true
		return kss
	}
	for {
		l.scopes.inputs = l.scopes.inputs[:0]
		var state sourceState
		for i, child := range kss {
			ks := child.Obj.(*manifest.Kustomization)
			ref := ks.SourceID()
			if i == 0 || ref != state.id {
				state = l.sourceState(ref, repoRoot)
			}
			l.scopes.inputs = append(l.scopes.inputs, scopeInput{owner: ks, source: state, published: l.Store.GetObject(ks.Named()) != nil})
		}
		for i, child := range kss {
			ks := child.Obj.(*manifest.Kustomization)
			state := l.scopes.inputs[i].source
			scope := l.scopeSource(state)
			if !scope.external && !scope.ready && len(l.scopes.scanned) != 0 {
				target := filepath.Join(scope.root, ks.Path)
				if _, scanned := l.scopes.scanned[target]; scanned {
					scope.ready = true
					l.scopes.sources[state] = scope
				}
			}
		}
		for file, visits := range l.scopes.visits {
			clear(visits)
			l.scopes.visits[file] = visits[:0]
		}
		l.scopes.contributions = l.scopes.contributions[:0]
		var local []*manifest.Kustomization
		for i, child := range kss {
			ks := child.Obj.(*manifest.Kustomization)
			ref := l.scopes.inputs[i].source.id
			scope := l.scopeSource(l.scopes.inputs[i].source)
			root, pinned, external := scope.root, scope.pinned, scope.external
			if ref.Name != "" && !external && !scope.ready {
				continue
			}
			if ks.IsFragment() {
				continue
			}
			if external {
				v := graphVisit{root: repoRoot, dir: l.documents.claimBase(repoRoot, ks.Path), role: kustomizeinput.Resource}
				l.scopes.contributions = append(l.scopes.contributions, scopeContribution{owner: ks.Named(), source: ref, visits: l.scopeWalk(v), context: v.graphContext, membershipOnly: true, input: i + 1})
				continue
			}
			local = append(local, ks)
			v := graphVisit{root: root, dir: l.documents.claimBase(root, ks.Path),
				namespace: ks.TargetNamespace, fallback: ks.TargetNamespace, owner: ks.Named(), source: ks.SourceID(), input: i + 1}
			if v.source.Name == "" || v.source.Kind == "" {
				v.source = manifest.BootstrapSourceID
			}
			if root != repoRoot {
				prefix, err := filepath.Rel(repoRoot, root)
				if err != nil {
					continue
				}
				v.prefix = filepath.ToSlash(prefix)
				if !pinned {
					v.localBase = l.documents.localBase(root, repoRoot)
				}
			}
			l.visitScope(v)
		}
		l.scopes.claims = buildClaims(l.Store, repoRoot, l.documents, local)
		l.scopes.roots = l.packageRoots(repoRoot, kss)
		entry := ""
		if l.entry == entryKustomize {
			entry = l.fileCoordinate(l.entryPath)
			if !slices.Contains(l.scopes.roots, entry) {
				v := graphVisit{root: repoRoot, dir: entry, role: kustomizeinput.Resource}
				l.scopes.contributions = append(l.scopes.contributions, scopeContribution{visits: l.scopeWalk(v), context: v.graphContext, membershipOnly: true})
			}
		}
		for _, dir := range l.scopes.roots {
			v := graphVisit{root: repoRoot, dir: dir, source: manifest.BootstrapSourceID, role: kustomizeinput.Resource}
			if l.anchorRoot(repoRoot, dir) && slices.ContainsFunc(l.scopes.claims, func(claim manifest.KSClaim) bool { return strings.HasPrefix(dir+"/", claim.Prefix) }) {
				l.scopes.contributions = append(l.scopes.contributions, scopeContribution{visits: l.scopeWalk(v), context: v.graphContext, membershipOnly: true})
			} else {
				l.visitScope(v)
			}
		}
		l.Index.Claims = make(map[string][]manifest.NamedResource, len(l.scopes.claims))
		for _, claim := range l.scopes.claims {
			l.Index.Claims[claim.Prefix] = appendUniqueProducer(l.Index.Claims[claim.Prefix], claim.ID)
		}
		l.resolveEligibility()
		if !l.projectAuthored(repoRoot) {
			l.scopes.complete = true
			return kss
		}
		kss = l.kustomizations()
		if l.scopesMatch(kss, repoRoot) {
			l.scopes.complete = true
			return kss
		}
	}
}

func (l *Loader) anchorRoot(repoRoot, dir string) bool {
	if l.entry == entryKustomize && filepath.Join(repoRoot, dir) == l.entryPath {
		return true
	}
	var visits []graphVisit
	owned := false
	for _, claim := range l.scopes.claims {
		if !strings.HasPrefix(dir+"/", claim.Prefix) {
			continue
		}
		if !owned {
			visits = l.scopeWalk(graphVisit{root: repoRoot, dir: dir, role: kustomizeinput.Resource})
		}
		owner, _ := l.Index.Lookup(claim.ID)
		origin := l.documents.localPath(owner.Path)
		if owner.Path == "" || !slices.ContainsFunc(visits, func(visit graphVisit) bool {
			return visit.role == kustomizeinput.Resource && (visit.file == owner.Path || visit.file == origin || visit.origin != "" && visit.origin == origin)
		}) {
			return false
		}
		owned = true
	}
	return owned
}

func (l *Loader) scopesMatch(kss []store.Child, repoRoot string) bool {
	if len(kss) != len(l.scopes.inputs) {
		return false
	}
	var previous manifest.NamedResource
	indexed := len(l.Index.Records) != 0
	for i, child := range kss {
		ks := child.Obj.(*manifest.Kustomization)
		input := &l.scopes.inputs[i]
		if ks != input.owner {
			return false
		}
		published := true
		if indexed {
			published = l.Store.GetObject(ks.Named()) != nil
		}
		if published != input.published {
			return false
		}
		ref := input.source.id
		if i == 0 || ref != previous {
			if l.sourceState(ref, repoRoot) != input.source {
				return false
			}
			previous = ref
		}
	}
	return true
}

func (l *Loader) scopeWalk(root graphVisit) []graphVisit {
	traversal, seen := l.scopes.walks[root.graphContext]
	if !seen {
		var visits []graphVisit
		if k := l.documents.cache[filepath.Join(root.root, root.dir)].body; k != nil {
			visits = make([]graphVisit, 0, len(k.Resources)+len(k.Components)+1)
		}
		w := graphWalker{documents: l.documents, visited: map[graphContext]struct{}{}}
		collect := func(v graphVisit) bool {
			if origin := l.documents.localPath(v.file); origin != v.file {
				v.origin = origin
			}
			if local := v.localCoordinate(); local != "" {
				v.localFile = filepath.Join(l.SourceRoot, filepath.FromSlash(local))
			}
			visits = append(visits, v)
			return true
		}
		w.walk(root, collect)
		traversal.visits = visits
		l.scopes.walks[root.graphContext] = traversal
	}
	return traversal.visits
}

func (l *Loader) visitScope(root graphVisit) []graphVisit {
	root.role = kustomizeinput.Resource
	visits := l.scopeWalk(root)
	ks, _ := l.Store.Get[*manifest.Kustomization](root.owner)
	if ks == nil {
		record, _ := l.Index.Lookup(root.owner)
		ks, _ = l.indexedObject(root.owner, record).Obj.(*manifest.Kustomization)
	}
	unshared := ks != nil && len(ks.Components) != 0
	if unshared {
		visits = slices.Clone(visits)
		namespace := root.namespace
		for _, visit := range visits {
			if visit.document != nil && visit.dir == root.dir {
				namespace = visit.namespace
				break
			}
		}
		for _, component := range ks.Components {
			if !fluxkustomize.IsLocalRelativePath(component) {
				continue
			}
			child := root
			child.dir = path.Join(root.dir, filepath.ToSlash(component))
			file := filepath.Join(root.root, child.dir)
			input, name := l.documents.input(file)
			info, err := fs.Stat(input, name)
			l.documents.rememberStat(file, info, err)
			if err != nil && ks.IgnoreMissingComponents {
				continue
			}
			child.namespace = namespace
			visits = append(visits, l.scopeWalk(child)...)
		}
	}
	l.scopes.contributions = append(l.scopes.contributions, scopeContribution{owner: root.owner, source: root.source, visits: visits, context: root.graphContext, unshared: unshared, input: root.input})
	for _, v := range visits {
		v.owner, v.source, v.input = root.owner, root.source, root.input
		l.scopes.visits[v.file] = append(l.scopes.visits[v.file], v)
		if local := v.localCoordinate(); local != "" {
			file := filepath.Join(l.SourceRoot, filepath.FromSlash(local))
			l.scopes.visits[file] = append(l.scopes.visits[file], v)
		}
	}
	return visits
}

type indexedRecord struct {
	id manifest.NamedResource
	manifest.DiscoveryRecord
}

func (l *Loader) projectAuthored(repoRoot string) bool {
	var indexed []indexedRecord
	for id, r := range l.Index.Files() {
		if r.Authored != (manifest.NamedResource{}) && r.Path != "" {
			indexed = append(indexed, indexedRecord{id, r})
		}
	}
	slices.SortFunc(indexed, func(a, b indexedRecord) int {
		if a.Path != b.Path {
			return strings.Compare(a.Path, b.Path)
		}
		if c := a.Authored.Compare(b.Authored); c != 0 {
			return c
		}
		return a.id.Compare(b.id)
	})
	scopes := make(map[string]rootAdmission)
	raw := indexed[:0]
	for start := 0; start < len(indexed); {
		end := start + 1
		for end < len(indexed) && indexed[end].Path == indexed[start].Path {
			end++
		}
		file := indexed[start].Path
		if l.terminalInput(file) {
			doc := l.documents.cache[file]
			doc.objects, doc.lookup, doc.loaded = nil, nil, false
			l.documents.cache[file] = doc
			start = end
			continue
		}
		doc := l.documents.cache[file]
		if doc.bootstrap || l.entry == entryKustomize || !doc.loaded {
			structural := slices.ContainsFunc(indexed[start:end], func(r indexedRecord) bool { return r.id.KindInfo().Structural })
			scopes[file] = l.rootAdmission(file, indexed[start].File, structural)
		}
		for _, r := range indexed[start:end] {
			if len(raw) == 0 || raw[len(raw)-1].Path != file || raw[len(raw)-1].Authored != r.Authored {
				raw = append(raw, r)
			}
		}
		start = end
	}
	next, objects := l.desiredAuthored(raw, repoRoot)
	for id, obj := range l.candidates {
		if r, ok := l.Index.Lookup(id); ok && r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			objects[id], next[id] = obj, r
		}
	}
	allowed := l.roots
	clear(allowed)
	if allowed == nil && len(objects) != 0 {
		allowed = make(map[manifest.NamedResource]bool, len(objects))
	}
	for id := range objects {
		r := next[id]
		if r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			allowed[id] = true
			continue
		}
		doc := l.documents.cache[r.Path]
		allowed[id] = doc.eligible != fileInactive && (l.entry != entryKustomize && doc.loaded && !doc.bootstrap || scopes[r.Path].admits(l, id, r))
	}
	l.scopes.candidates, l.candidates = l.candidates, objects
	l.roots = allowed
	return l.applyAuthored(next, objects, allowed)
}

func (l *Loader) desiredAuthored(raw []indexedRecord, repoRoot string) (map[manifest.NamedResource]manifest.DiscoveryRecord, map[manifest.NamedResource]store.Child) {
	next := l.scopes.records
	if next == nil {
		next = map[manifest.NamedResource]manifest.DiscoveryRecord{}
		l.scopes.records = next
	}
	clear(next)
	objects := l.scopes.candidates
	if objects == nil {
		objects = map[manifest.NamedResource]store.Child{}
	}
	clear(objects)
	prediction := map[string]nsResolution{}
	for _, indexed := range raw {
		r := indexed.DiscoveryRecord
		path, id := r.Path, r.Authored
		authored := l.authoredObject(path, id)
		if authored.Obj == nil {
			continue
		}
		obj := authored.Obj
		forced := ""
		if ks, ok := obj.(*manifest.Kustomization); ok {
			obj, forced = l.predictNamespaces(ks, repoRoot, r, prediction)
		}
		for _, ns := range l.recordNamespaces(r, forced) {
			id := obj.Named()
			id.Namespace = ns
			projected := obj
			// Candidate identities MUST stay stable until their source state changes.
			previous := l.candidates[id]
			if previous.Obj != nil {
				ks, isKS := obj.(*manifest.Kustomization)
				if !isKS || previous.Obj.(*manifest.Kustomization).TargetNamespace == ks.TargetNamespace {
					projected = previous.Obj
				}
			}
			if projected.Named().Namespace != ns {
				projected = projected.WithNamespace(ns)
			}
			record := r
			if old, ok := l.Index.Lookup(id); ok && old.Path == r.Path && previous.Obj == projected {
				record.Refs = old.Refs
			} else {
				record.Refs = nil
				for ref := range manifest.References(projected, manifest.ReferenceRelations) {
					if discoveryReference(ref) {
						record.Refs = append(record.Refs, ref.NamedResource)
					}
				}
			}
			next[id] = record
			if manifest.KindOf(projected).Discovery {
				objects[id] = store.Child{Obj: projected, Doc: authored.Doc}
			}
		}
	}
	return next, objects
}

func (l *Loader) authoredObject(path string, id manifest.NamedResource) store.Child {
	doc := l.documents.cache[path]
	if len(doc.objects) > 1 {
		if doc.lookup == nil {
			doc.lookup = make(map[manifest.NamedResource]store.Child, len(doc.objects))
			for _, child := range doc.objects {
				obj := child.Obj
				if doc.lookup[obj.Named()].Obj == nil {
					doc.lookup[obj.Named()] = child
				}
			}
			l.documents.cache[path] = doc
		}
		return doc.lookup[id]
	}
	if len(doc.objects) == 1 && doc.objects[0].Obj.Named() == id {
		return doc.objects[0]
	}
	return store.Child{}
}

func (l *Loader) applyAuthored(next map[manifest.NamedResource]manifest.DiscoveryRecord, objects map[manifest.NamedResource]store.Child, allowed map[manifest.NamedResource]bool) bool {
	var deletes []manifest.NamedResource
	changed := false
	for id, r := range l.Index.Files() {
		if r.Authored == (manifest.NamedResource{}) || r.Path == "" {
			continue
		}
		if _, survives := next[id]; !survives {
			deletes = append(deletes, id)
		}
	}
	slices.SortFunc(deletes, manifest.NamedResource.Compare)
	for _, id := range deletes {
		delete(l.Index.Records, id)
		l.Store.DeleteObject(id)
		changed = true
	}
	for _, id := range slices.SortedFunc(maps.Keys(next), manifest.NamedResource.Compare) {
		r := next[id]
		if previous, ok := l.Index.Lookup(id); !ok || previous.Authored != r.Authored || previous.Path != r.Path || previous.File != r.File || !slices.Equal(previous.Refs, r.Refs) {
			changed = true
		}
		l.Index.Records[id] = r
	}
	for _, id := range slices.SortedFunc(maps.Keys(l.admitted), manifest.NamedResource.Compare) {
		if _, survives := next[id]; !survives || !allowed[id] {
			if l.Store.GetObject(id) == l.admitted[id] && l.Store.GetArtifact(id) == nil {
				l.Store.DeleteObject(id)
				changed = true
			}
			delete(l.admitted, id)
		}
	}
	if l.admitted == nil {
		l.admitted = map[manifest.NamedResource]manifest.BaseManifest{}
	}
	for _, id := range slices.SortedFunc(maps.Keys(objects), manifest.NamedResource.Compare) {
		child := objects[id]
		obj := child.Obj
		if !allowed[id] || !manifest.KindOf(obj).Discovery {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil {
			ks, isKS := obj.(*manifest.Kustomization)
			if !isKS || existing.(*manifest.Kustomization).TargetNamespace == ks.TargetNamespace {
				continue
			}
		}
		l.Store.AddObject(obj, child.Doc)
		l.admitted[id] = obj
		changed = true
	}
	return changed
}

func (l *Loader) recordNamespaces(r manifest.DiscoveryRecord, forced string) []string {
	var owners []manifest.NamedResource
	depth := -1
	for _, c := range l.scopes.claims {
		if !strings.HasPrefix(r.File, c.Prefix) {
			continue
		}
		if l.selfClaim(c.ID, r.Authored) {
			continue
		}
		if len(c.Prefix) > depth {
			owners, depth = nil, len(c.Prefix)
		}
		if len(c.Prefix) == depth {
			owners = appendUniqueProducer(owners, c.ID)
		}
	}
	var namespaces []string
	inactive := l.scopes.inactive
	for _, v := range l.scopes.visits[r.Path] {
		if inactive && v.input != 0 && !l.scopes.inputs[v.input-1].active {
			continue
		}
		if v.deferMaterialization || v.role != kustomizeinput.Resource || l.selfClaim(v.owner, r.Authored) {
			continue
		}
		fallback := v.fallback
		if v.namespace == "" && len(owners) == 1 {
			if ks, ok := l.Store.Get[*manifest.Kustomization](owners[0]); ok {
				fallback = ks.TargetNamespace
			}
		}
		ns := manifest.Resolve(r.Authored, v.namespace, fallback).Namespace
		if !slices.Contains(namespaces, ns) {
			namespaces = append(namespaces, ns)
		}
	}
	if len(namespaces) == 0 {
		if forced != "" {
			return []string{manifest.Resolve(r.Authored, forced, "").Namespace}
		}
		for _, owner := range owners {
			if ks, ok := l.Store.Get[*manifest.Kustomization](owner); ok {
				ns := manifest.Resolve(r.Authored, "", ks.TargetNamespace).Namespace
				if !slices.Contains(namespaces, ns) {
					namespaces = append(namespaces, ns)
				}
			}
		}
	}
	if len(namespaces) == 0 {
		return []string{r.Authored.Namespace}
	}
	slices.Sort(namespaces)
	return namespaces
}

func (l *Loader) selfClaim(id, authored manifest.NamedResource) bool {
	owner, _ := l.Index.Lookup(id)
	return id == authored || owner.Authored == authored
}

func (l *Loader) terminalInput(file string) bool {
	terminal := false
	if l.scopes != nil {
		for _, v := range l.scopes.visits[file] {
			if v.role == kustomizeinput.Resource && v.readable {
				return false
			}
			terminal = terminal || v.role != kustomizeinput.Resource
		}
	}
	return terminal
}

func (l *Loader) resetAdmissions() {
	clear(l.roots)
	for file, doc := range l.documents.cache {
		doc.eligible, doc.rootKnown, doc.rootAdmitted = fileUnknown, false, false
		l.documents.cache[file] = doc
	}
}

func (l *Loader) resolveEligibility() {
	l.resetAdmissions()
	for context, traversal := range l.scopes.walks {
		traversal.reached, traversal.controlled = fileUnknown, false
		l.scopes.walks[context] = traversal
	}
	mark := func(file string, state fileEligibility) bool {
		if l.documents.cache[file].eligible >= state {
			return false
		}
		doc := l.documents.cache[file]
		doc.eligible = state
		l.documents.cache[file] = doc
		return true
	}
	for _, c := range l.scopes.contributions {
		traversal := l.scopes.walks[c.context]
		if !c.unshared && traversal.controlled {
			continue
		}
		owner, _ := l.Index.Lookup(c.owner)
		source, _ := l.Index.Lookup(c.source)
		declaredSource := c.membershipOnly && l.indexedObject(c.source, source).Obj != nil
		controlled := c.owner != (manifest.NamedResource{}) && !declaredSource
		for _, v := range c.visits {
			if v.role != kustomizeinput.Resource {
				mark(v.file, fileInactive)
				continue
			}
			if c.owner == (manifest.NamedResource{}) || declaredSource || owner.Path == v.file || v.origin != "" && owner.Path == v.origin {
				controlled = false
				continue
			}
			mark(v.file, fileInactive)
			if v.localFile != "" {
				mark(v.localFile, fileInactive)
			}
		}
		if controlled && !c.unshared {
			traversal.controlled = true
			l.scopes.walks[c.context] = traversal
		}
	}
	for file, doc := range l.documents.cache {
		if l.entry == entryPlain && filepath.Dir(file) == l.entryPath && doc.loaded && !l.terminalInput(file) {
			mark(file, fileEligible)
			continue
		}
		if doc.eligible != fileUnknown || doc.objects == nil {
			continue
		}
		_, builder := l.documents.read(filepath.Dir(file))
		if builder != "" || l.terminalInput(file) {
			continue
		}
		unowned := true
		coordinate := l.fileCoordinate(l.documents.localPath(file))
		for _, claim := range l.scopes.claims {
			owner, _ := l.Index.Lookup(claim.ID)
			if strings.HasPrefix(coordinate, claim.Prefix) && owner.Path != file {
				unowned = false
				break
			}
		}
		state := fileInactive
		if unowned {
			state = fileEligible
		}
		mark(file, state)
	}
	for {
		changed := false
		for i := range l.scopes.contributions {
			c := &l.scopes.contributions[i]
			r, indexed := l.Index.Lookup(c.owner)
			object := l.Store.GetObject(c.owner)
			active := c.owner == (manifest.NamedResource{}) || !indexed || r.Path == "" || l.documents.cache[r.Path].eligible >= fileEligible || object != nil && object != l.admitted[c.owner]
			if c.owner == (manifest.NamedResource{}) && !c.membershipOnly && len(c.visits) != 0 {
				claimed, enabled := false, false
				for _, claim := range l.scopes.claims {
					if strings.HasPrefix(c.visits[0].dir+"/", claim.Prefix) {
						owner, _ := l.Index.Lookup(claim.ID)
						claimed = true
						enabled = enabled || owner.Path == "" || l.documents.cache[owner.Path].eligible >= fileEligible
					}
				}
				active = !claimed || enabled
			}
			c.active = active
			if c.input != 0 {
				l.scopes.inputs[c.input-1].active = active
			}
			if !active {
				continue
			}
			reachability := fileEligible
			if c.owner == (manifest.NamedResource{}) && !c.membershipOnly && len(c.visits) != 0 && !slices.ContainsFunc(l.scopes.claims, func(claim manifest.KSClaim) bool { return strings.HasPrefix(c.visits[0].dir+"/", claim.Prefix) }) {
				reachability = fileIndependent
			}
			if !c.unshared {
				traversal := l.scopes.walks[c.context]
				if traversal.reached >= reachability {
					continue
				}
				traversal.reached = reachability
				l.scopes.walks[c.context] = traversal
			}
			for _, v := range c.visits {
				if v.role != kustomizeinput.Resource {
					continue
				}
				changed = mark(v.file, reachability) || changed
				if v.localFile != "" {
					changed = mark(v.localFile, reachability) || changed
				}
			}
		}
		if !changed {
			break
		}
	}
	l.scopes.inactive = false
	for i := range l.scopes.inputs {
		if !l.scopes.inputs[i].active {
			l.scopes.inactive = true
			break
		}
	}
	for file, doc := range l.documents.cache {
		if doc.eligible == fileUnknown && len(doc.objects) != 0 {
			mark(file, fileInactive)
		}
	}
}
