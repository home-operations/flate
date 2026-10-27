package loader

import (
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"

	"github.com/home-operations/flate/internal/claimpath"
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
	sources       []sourceScope
	sourceBuf     [1]sourceScope
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
	source    int
	published bool
	active    bool
	ownerClaims
	derived bool
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
	sourceState
	root                    string
	origin                  string
	pinned, external, ready bool
}

func (l *Loader) sourceState(id manifest.NamedResource, repoRoot string) sourceState {
	art, _ := l.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	return sourceState{id: id, object: l.Store.GetObject(id), artifact: art, repoRoot: repoRoot}
}

func (l *Loader) scopeSource(state sourceState) int {
	for i := range l.scopes.sources {
		if l.scopes.sources[i].sourceState == state {
			return i
		}
	}
	root, pinned, external := SourceView(l.Store, state.id, state.repoRoot)
	l.scopes.sources = append(l.scopes.sources, sourceScope{sourceState: state, root: root, pinned: pinned, external: external, origin: l.documents.localBase(root, state.repoRoot)})
	return len(l.scopes.sources) - 1
}

func (l *Loader) ensureScopes() {
	if l.scopes == nil {
		l.scopes = &scopeConstruction{walks: map[graphContext]scopeTraversal{}, scanned: map[string]struct{}{}, visits: map[string][]graphVisit{}}
		l.scopes.sources = l.scopes.sourceBuf[:0]
	}
}

func (l *Loader) refreshClaims(repoRoot string, kss []store.Child) {
	l.ensureScopes()
	if !l.scopesMatch(kss, repoRoot) {
		l.scopes.complete = false
		previous := l.scopes.inputs
		inputs := previous
		if len(inputs) != len(kss) {
			inputs = make([]scopeInput, len(kss))
		} else {
			for i, child := range kss {
				if inputs[i].owner.Named() != child.Obj.Named() {
					inputs = make([]scopeInput, len(kss))
					break
				}
			}
		}
		for i, child := range kss {
			ks := child.Obj.(*manifest.Kustomization)
			view := l.scopeSource(l.sourceState(ks.SourceID(), repoRoot))
			published := l.Store.GetObject(ks.Named()) != nil
			input := scopeInput{owner: ks, source: view, published: published}
			for len(previous) != 0 && previous[0].owner.Named().Compare(ks.Named()) < 0 {
				previous = previous[1:]
			}
			if len(previous) != 0 && previous[0].owner == ks && previous[0].source == view {
				input = previous[0]
				input.published = published
			}
			inputs[i] = input
		}
		l.scopes.inputs = inputs
	}
	for _, input := range l.scopes.inputs {
		view := &l.scopes.sources[input.source]
		if !view.external && !view.ready && len(l.scopes.scanned) != 0 {
			_, view.ready = l.scopes.scanned[filepath.Join(view.root, input.owner.Path)]
		}
	}
	l.scopes.claims, _ = l.claimView(repoRoot, func(input scopeInput) bool {
		view := l.scopes.sources[input.source]
		return repoRoot == "" || view.id.Name == "" || view.ready
	})
	l.Index.Claims = make(map[string][]manifest.NamedResource, len(l.scopes.claims))
	for _, claim := range l.scopes.claims {
		l.Index.Claims[claim.Prefix] = appendUniqueProducer(l.Index.Claims[claim.Prefix], claim.ID)
	}
}

func (l *Loader) claimView(repoRoot string, eligible func(scopeInput) bool) ([]manifest.KSClaim, bool) {
	var pending []*manifest.Kustomization
	for _, input := range l.scopes.inputs {
		if !l.scopes.sources[input.source].external && eligible(input) && !input.derived {
			pending = append(pending, input.owner)
		}
	}
	var claimBuf [16]ownerClaims
	built := l.documents.buildClaims(repoRoot, pending, func(id manifest.NamedResource) sourceScope {
		return l.scopes.sources[l.scopeSource(l.sourceState(id, repoRoot))]
	}, claimBuf[:0])
	next := 0
	for i := range l.scopes.inputs {
		input := &l.scopes.inputs[i]
		if l.scopes.sources[input.source].external || !eligible(*input) || input.derived {
			continue
		}
		input.ownerClaims = built[next]
		input.derived = true
		next++
	}
	return projectClaims(len(l.scopes.inputs), func(i int) ownerClaims {
		input := l.scopes.inputs[i]
		if l.scopes.sources[input.source].external || !eligible(input) {
			return ownerClaims{}
		}
		return input.ownerClaims
	})
}

// Admit resolves scope evidence and publishes independently runnable discovery roots.
func (l *Loader) Admit(repoRoot string) {
	l.mountSources()
	l.admit(repoRoot)
}

func (l *Loader) admit(repoRoot string) {
	l.ensureScopes()
	kss := l.kustomizations()
	if l.scopes.complete && l.scopesMatch(kss, repoRoot) {
		return
	}

	if repoRoot == "" {
		l.refreshClaims(repoRoot, kss)
		l.scopes.complete = true
		return
	}
	for {
		l.refreshClaims(repoRoot, kss)
		for file, visits := range l.scopes.visits {
			clear(visits)
			l.scopes.visits[file] = visits[:0]
		}
		l.scopes.contributions = l.scopes.contributions[:0]
		for i, child := range kss {
			ks := child.Obj.(*manifest.Kustomization)
			scope := l.scopes.sources[l.scopes.inputs[i].source]
			ref := scope.id
			root, pinned, external := scope.root, scope.pinned, scope.external
			if ref.Name != "" && !external && !scope.ready {
				continue
			}
			if ks.IsFragment() {
				continue
			}
			if external {
				v := graphVisit{root: repoRoot, dir: l.documents.claimBase(repoRoot, ks.Path), owner: ks.Named(), source: ref, input: i + 1}
				l.visitScope(v, true)
				continue
			}
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
					v.localBase = scope.origin
				}
			}
			l.visitScope(v, false)
		}
		l.scopes.roots = l.packageRoots(repoRoot, kss)
		entry := ""
		if l.entry == entryKustomize {
			entry = l.fileCoordinate(l.entryPath)
			if !slices.Contains(l.scopes.roots, entry) {
				v := graphVisit{root: repoRoot, dir: entry, role: kustomizeinput.Resource}
				l.visitScope(v, true)
			}
		}
		for _, dir := range l.scopes.roots {
			v := graphVisit{root: repoRoot, dir: dir, source: manifest.BootstrapSourceID, role: kustomizeinput.Resource}
			if l.anchorRoot(repoRoot, dir) && slices.ContainsFunc(l.scopes.claims, func(claim manifest.KSClaim) bool { return strings.HasPrefix(dir+"/", claim.Prefix) }) {
				l.visitScope(v, true)
			} else {
				l.visitScope(v, false)
			}
		}
		l.resolveEligibility()
		if !l.projectAuthored(repoRoot) {
			l.scopes.complete = true
			return
		}
		kss = l.kustomizations()
		if l.scopesMatch(kss, repoRoot) {
			l.scopes.complete = true
			return
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
		view := l.scopes.sources[input.source]
		ref := view.id
		if i == 0 || ref != previous {
			if l.sourceState(ref, repoRoot) != view.sourceState {
				return false
			}
			previous = ref
		}
	}
	return true
}

func (l *Loader) scopeWalk(root graphVisit) []graphVisit {
	walk, seen := l.scopes.walks[root.graphContext]
	if !seen {
		var visits []graphVisit
		if k := l.documents.cache[filepath.Join(root.root, root.dir)].body; k != nil {
			visits = make([]graphVisit, 0, len(k.Resources)+len(k.Components)+1)
		}
		w := traversal{documents: l.documents, visited: map[graphContext]struct{}{}}
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
		w.walk(nil, root, collect)
		walk.visits = visits
		l.scopes.walks[root.graphContext] = walk
	}
	return walk.visits
}

func (l *Loader) visitScope(root graphVisit, membershipOnly bool) []graphVisit {
	root.role = kustomizeinput.Resource
	visits := l.scopeWalk(root)
	ks, _ := l.Store.Get[*manifest.Kustomization](root.owner)
	if ks == nil {
		record, _ := l.Index.Lookup(root.owner)
		ks, _ = l.indexedObject(root.owner, record).Obj.(*manifest.Kustomization)
	}
	unshared := !membershipOnly && ks != nil && len(ks.Components) != 0
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
	l.scopes.contributions = append(l.scopes.contributions, scopeContribution{owner: root.owner, source: root.source, visits: visits, context: root.graphContext, unshared: unshared, membershipOnly: membershipOnly, input: root.input})
	for _, v := range visits {
		v.owner, v.source, v.input = root.owner, root.source, root.input
		v.membershipOnly = membershipOnly
		if membershipOnly {
			// Membership projects identities without owning or materializing bootstrap files.
			v.owner, v.deferMaterialization = manifest.NamedResource{}, false
		}
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
			if previous.Obj != nil && obj.ProjectionCompatible(previous.Obj) {
				projected = previous.Obj
			}
			if projected.Named().Namespace != ns {
				projected = projected.WithNamespace(ns)
			}
			record := r
			record.Productions = nil
			if old, ok := l.Index.Lookup(id); ok && old.Path == r.Path && previous.Obj == projected {
				record.Refs = old.Refs
			} else {
				record.Refs = discoveryReferences(nil, projected)
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
				if obj == nil || child.outcome.Err != nil {
					continue
				}
				if doc.lookup[obj.Named()].Obj == nil {
					doc.lookup[obj.Named()] = child.Child
				}
			}
			l.documents.cache[path] = doc
		}
		return doc.lookup[id]
	}
	if len(doc.objects) == 1 && doc.objects[0].Obj != nil && doc.objects[0].outcome.Err == nil && doc.objects[0].Obj.Named() == id {
		return doc.objects[0].Child
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
			if l.Store.GetObject(id) == l.admitted[id] && l.Store.GetArtifact(id, store.ArtifactContent) == nil {
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
		if existing := l.Store.GetObject(id); existing != nil && obj.ProjectionCompatible(existing) {
			continue
		}
		l.Store.AddObject(obj, child.Doc)
		l.admitted[id] = obj
		changed = true
	}
	return changed
}

func (l *Loader) recordNamespaces(r manifest.DiscoveryRecord, forced string) []string {
	var owners []manifest.NamedResource
	for _, group := range claimpath.Groups(l.Index.Claims, r.File) {
		for _, id := range group {
			if !l.selfClaim(id, r.Authored) {
				owners = appendUniqueProducer(owners, id)
			}
		}
		if len(owners) != 0 {
			break
		}
	}
	var namespaces []string
	authoritative := false
	inactive := l.scopes.inactive
	for _, v := range l.scopes.visits[r.Path] {
		if inactive && v.input != 0 && !l.scopes.inputs[v.input-1].active {
			continue
		}
		if v.deferMaterialization || v.role != kustomizeinput.Resource || l.selfClaim(v.owner, r.Authored) {
			continue
		}
		if v.membershipOnly {
			if authoritative {
				continue
			}
		} else if !authoritative {
			namespaces = namespaces[:0]
			authoritative = true
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
	if l.scopes == nil {
		return l.documents.cache[file].terminal
	}
	terminal := false
	for _, v := range l.scopes.visits[file] {
		if v.role == kustomizeinput.Resource && v.readable {
			return false
		}
		terminal = terminal || v.role != kustomizeinput.Resource
	}
	return terminal
}

func (l *Loader) resetAdmissions() {
	clear(l.roots)
	for file, doc := range l.documents.cache {
		doc.eligible, doc.rootKnown, doc.rootAdmitted, doc.terminal = fileUnknown, false, false, false
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
