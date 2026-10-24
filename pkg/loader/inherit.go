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
	walks         map[graphContext][]graphVisit
	scanned       map[string]struct{}
	ready         map[sourceState]struct{}
	inputs        []scopeInput
	complete      bool
	visits        map[string][]graphVisit
	contributions []scopeContribution
	claims        []manifest.KSClaim
}

type scopeInput struct {
	owner     *manifest.Kustomization
	source    sourceState
	published bool
}

type scopeContribution struct {
	owner, source manifest.NamedResource
	visits        []graphVisit
}

type sourceState struct {
	id       manifest.NamedResource
	object   manifest.BaseManifest
	artifact *store.SourceArtifact
}

func (l *Loader) sourceState(id manifest.NamedResource) sourceState {
	art, _ := l.Store.GetArtifact(id).(*store.SourceArtifact)
	return sourceState{id: id, object: l.Store.GetObject(id), artifact: art}
}

func (l *Loader) ensureScopes() {
	if l.scopes == nil {
		l.scopes = &scopeConstruction{walks: map[graphContext][]graphVisit{}, scanned: map[string]struct{}{}, ready: map[sourceState]struct{}{}, visits: map[string][]graphVisit{}}
	}
}

// Admit resolves scope evidence and publishes independently runnable discovery roots.
func (l *Loader) Admit(repoRoot string) {
	l.mountSources()
	l.admit(repoRoot)
}

func (l *Loader) admit(repoRoot string) {
	l.ensureScopes()
	if l.scopes.complete && l.scopesMatch(l.Kustomizations()) {
		return
	}

	if repoRoot == "" {
		l.scopes.claims = buildClaims(l.Store, repoRoot, l.documents, localKustomizations(l.Store, repoRoot))
		l.scopes.complete = true
		return
	}
	for {
		kss := l.Kustomizations()
		l.scopes.inputs = l.scopes.inputs[:0]
		for _, ks := range kss {
			l.scopes.inputs = append(l.scopes.inputs, scopeInput{owner: ks, source: l.sourceState(ks.SourceID()), published: l.Store.GetObject(ks.Named()) != nil})
		}
		for _, ks := range kss {
			root, _, external := SourceView(l.Store, ks.SourceID(), repoRoot)
			target := filepath.Clean(filepath.Join(root, ks.Path))
			if !external {
				if _, scanned := l.scopes.scanned[target]; scanned {
					l.scopes.ready[l.sourceState(ks.SourceID())] = struct{}{}
				}
			}
		}
		for file, visits := range l.scopes.visits {
			clear(visits)
			l.scopes.visits[file] = visits[:0]
		}
		l.scopes.contributions = l.scopes.contributions[:0]
		for _, dir := range l.standaloneRoots(repoRoot, kss) {
			l.visitScope(graphVisit{root: repoRoot, dir: dir, source: manifest.BootstrapSourceID})
		}
		var local []*manifest.Kustomization
		for _, ks := range kss {
			ref := ks.SourceID()
			// Named source edges wait for local resolution or bootstrap aliasing.
			if ref.Name != "" {
				if _, ready := l.scopes.ready[l.sourceState(ref)]; !ready {
					continue
				}
			}
			root, pinned, external := SourceView(l.Store, ks.SourceID(), repoRoot)
			if external || ks.IsFragment() {
				continue
			}
			local = append(local, ks)
			v := graphVisit{root: root, dir: l.documents.claimBase(root, ks.Path),
				namespace: ks.TargetNamespace, fallback: ks.TargetNamespace, owner: ks.Named(), source: ks.SourceID()}
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
		l.Index.Claims = make(map[string][]manifest.NamedResource, len(l.scopes.claims))
		for _, claim := range l.scopes.claims {
			l.Index.Claims[claim.Prefix] = appendUniqueProducer(l.Index.Claims[claim.Prefix], claim.ID)
		}
		if changed := l.projectAuthored(repoRoot); !changed || l.scopesMatch(l.Kustomizations()) {
			l.scopes.complete = true
			return
		}
	}
}

func (l *Loader) scopesMatch(kss []*manifest.Kustomization) bool {
	if len(kss) != len(l.scopes.inputs) {
		return false
	}
	for i, ks := range kss {
		input := l.scopes.inputs[i]
		if ks != input.owner || l.sourceState(ks.SourceID()) != input.source || (l.Store.GetObject(ks.Named()) != nil) != input.published {
			return false
		}
	}
	return true
}

func (l *Loader) scopeWalk(root graphVisit) []graphVisit {
	visits, seen := l.scopes.walks[root.graphContext]
	if !seen {
		if k := l.documents.cache[filepath.Join(root.root, root.dir)].body; k != nil {
			visits = make([]graphVisit, 0, len(k.Resources)+len(k.Components)+1)
		}
		w := graphWalker{documents: l.documents, visited: map[graphContext]struct{}{}}
		collect := func(v graphVisit) bool {
			visits = append(visits, v)
			return true
		}
		w.walk(root, collect)
		l.scopes.walks[root.graphContext] = visits
	}
	return visits
}

func (l *Loader) visitScope(root graphVisit) []graphVisit {
	root.role = kustomizeinput.Resource
	visits := l.scopeWalk(root)
	if ks, ok := l.Store.Get[*manifest.Kustomization](root.owner); ok && len(ks.Components) != 0 {
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
	l.scopes.contributions = append(l.scopes.contributions, scopeContribution{owner: root.owner, source: root.source, visits: visits})
	for _, v := range visits {
		v.owner, v.source = root.owner, root.source
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
	allowed := make(map[manifest.NamedResource]bool)
	raw := indexed[:0]
	for start := 0; start < len(indexed); {
		end := start + 1
		for end < len(indexed) && indexed[end].Path == indexed[start].Path {
			end++
		}
		file := indexed[start].Path
		var scope rootAdmission
		doc := l.documents.cache[file]
		if doc.bootstrap || l.entry == entryKustomize || !doc.loaded {
			structural := slices.ContainsFunc(indexed[start:end], func(r indexedRecord) bool { return r.id.KindInfo().Structural })
			scope = l.rootAdmission(file, indexed[start].File, structural)
			scopes[file] = scope
		}
		for _, r := range indexed[start:end] {
			if doc.bootstrap {
				allowed[r.id] = scope.admits(l, r.id, r.DiscoveryRecord)
				if !allowed[r.id] {
					continue
				}
			}
			if len(raw) == 0 || raw[len(raw)-1].Path != file || raw[len(raw)-1].Authored != r.Authored {
				raw = append(raw, r)
			}
		}
		start = end
	}
	next, objects := l.desiredAuthored(raw, repoRoot)
	for id, r := range next {
		if objects[id] != nil {
			previous, _ := l.Index.Lookup(id)
			if _, decided := allowed[id]; decided && previous.Path == r.Path && previous.Authored == r.Authored {
				continue
			}
			allowed[id] = l.entry != entryKustomize && l.documents.cache[r.Path].loaded && !l.documents.cache[r.Path].bootstrap || scopes[r.Path].admits(l, id, r)
		}
	}
	for id, obj := range l.candidates {
		if r, ok := l.Index.Lookup(id); ok && r.Path == "" && r.Authored == (manifest.NamedResource{}) {
			objects[id] = obj
		}
	}
	l.candidates = objects
	return l.applyAuthored(next, objects, allowed)
}

func (l *Loader) desiredAuthored(raw []indexedRecord, repoRoot string) (map[manifest.NamedResource]manifest.DiscoveryRecord, map[manifest.NamedResource]manifest.BaseManifest) {
	next := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	prediction := map[string]nsResolution{}
	for _, indexed := range raw {
		r := indexed.DiscoveryRecord
		path, id := r.Path, r.Authored
		authored := l.authoredObject(path, id)
		if authored == nil {
			continue
		}
		obj := authored
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
			if previous != nil {
				ks, isKS := obj.(*manifest.Kustomization)
				if !isKS || previous.(*manifest.Kustomization).TargetNamespace == ks.TargetNamespace {
					projected = previous
				}
			}
			if projected.Named().Namespace != ns {
				projected = projected.WithNamespace(ns)
			}
			record := r
			if old, ok := l.Index.Lookup(id); ok && old.Path == r.Path && previous == projected {
				record.Refs = old.Refs
			} else {
				record.Refs = nil
				for ref := range manifest.References(projected) {
					if discoveryReference(ref) {
						record.Refs = append(record.Refs, ref.NamedResource)
					}
				}
			}
			next[id] = record
			if manifest.KindOf(projected).Discovery {
				objects[id] = projected
			}
		}
	}
	return next, objects
}

func (l *Loader) authoredObject(path string, id manifest.NamedResource) manifest.BaseManifest {
	doc := l.documents.cache[path]
	if len(doc.objects) > 1 {
		if doc.lookup == nil {
			doc.lookup = make(map[manifest.NamedResource]manifest.BaseManifest, len(doc.objects))
			for _, obj := range doc.objects {
				if doc.lookup[obj.Named()] == nil {
					doc.lookup[obj.Named()] = obj
				}
			}
			l.documents.cache[path] = doc
		}
		return doc.lookup[id]
	}
	if len(doc.objects) == 1 && doc.objects[0].Named() == id {
		return doc.objects[0]
	}
	return nil
}

func (l *Loader) applyAuthored(next map[manifest.NamedResource]manifest.DiscoveryRecord, objects map[manifest.NamedResource]manifest.BaseManifest, allowed map[manifest.NamedResource]bool) bool {
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
			if l.Store.GetObject(id) == l.admitted[id] {
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
		obj := objects[id]
		if !allowed[id] || !manifest.KindOf(obj).Discovery {
			continue
		}
		if existing := l.Store.GetObject(id); existing != nil {
			ks, isKS := obj.(*manifest.Kustomization)
			if !isKS || existing.(*manifest.Kustomization).TargetNamespace == ks.TargetNamespace {
				continue
			}
		}
		l.Store.AddObject(obj)
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
	for _, v := range l.scopes.visits[r.Path] {
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
