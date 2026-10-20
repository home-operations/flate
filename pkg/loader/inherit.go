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
	complete      bool
	visits        map[string][]graphVisit
	contributions []scopeContribution
	claims        []manifest.KSClaim
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

// ApplyNamespaces closes authored namespace contexts using the shared document reader.
func (l *Loader) ApplyNamespaces(repoRoot string) {
	l.mountSources()
	l.ensureScopes()
	if repoRoot == "" {
		l.scopes.claims = buildClaims(l.Store, repoRoot, l.documents, localKustomizations(l.Store, repoRoot))
		l.scopes.complete = true
		return
	}
	for {
		kss := l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)
		for _, ks := range kss {
			root, _, external := SourceView(l.Store, ks.SourceID(), repoRoot)
			target := filepath.Clean(filepath.Join(root, ks.Path))
			if !external {
				if _, scanned := l.scopes.scanned[target]; scanned {
					l.scopes.ready[l.sourceState(ks.SourceID())] = struct{}{}
				}
			}
		}
		clear(l.scopes.visits)
		l.scopes.contributions = nil
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
		if !l.projectAuthored(repoRoot) {
			l.scopes.complete = true
			return
		}
	}
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

func (l *Loader) projectAuthored(repoRoot string) bool {
	raw := map[string]map[manifest.NamedResource]manifest.DiscoveryRecord{}
	for _, r := range l.Index.Files() {
		if r.Authored != (manifest.NamedResource{}) && r.Path != "" {
			if raw[r.Path] == nil {
				raw[r.Path] = map[manifest.NamedResource]manifest.DiscoveryRecord{}
			}
			raw[r.Path][r.Authored] = r
		}
	}
	next, objects := l.desiredAuthored(raw, repoRoot)
	deletes, upserts, changed := l.authoredChanges(raw, next, objects)
	l.applyAuthored(next, objects, deletes, upserts)
	return changed
}

func (l *Loader) desiredAuthored(raw map[string]map[manifest.NamedResource]manifest.DiscoveryRecord, repoRoot string) (map[manifest.NamedResource]manifest.DiscoveryRecord, map[manifest.NamedResource]manifest.BaseManifest) {
	next := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	prediction := map[string]nsResolution{}
	for _, path := range slices.Sorted(maps.Keys(raw)) {
		for _, id := range slices.SortedFunc(maps.Keys(raw[path]), manifest.NamedResource.Compare) {
			r := raw[path][id]
			authored := l.authoredObject(path, id)
			if authored == nil {
				continue
			}
			records, projected := l.projectAuthoredRecord(r, authored, repoRoot, prediction)
			maps.Copy(next, records)
			maps.Copy(objects, projected)
		}
	}
	return next, objects
}

func (l *Loader) authoredObject(path string, id manifest.NamedResource) manifest.BaseManifest {
	for _, obj := range l.documents.cache[path].objects {
		if obj.Named() == id {
			return obj
		}
	}
	return nil
}

func (l *Loader) projectAuthoredRecord(r manifest.DiscoveryRecord, authored manifest.BaseManifest, repoRoot string, prediction map[string]nsResolution) (map[manifest.NamedResource]manifest.DiscoveryRecord, map[manifest.NamedResource]manifest.BaseManifest) {
	obj := authored
	forced := ""
	if ks, ok := obj.(*manifest.Kustomization); ok {
		obj, forced = l.predictNamespaces(ks, repoRoot, r, prediction)
	}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{}
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	for _, ns := range l.recordNamespaces(r, forced) {
		projected := obj
		if ns != obj.Named().Namespace {
			projected = obj.WithNamespace(ns)
		}
		id := projected.Named()
		record := r
		record.Refs = nil
		for ref := range manifest.References(projected) {
			if discoveryReference(ref) {
				record.Refs = append(record.Refs, ref.NamedResource)
			}
		}
		if old, ok := l.Index.Lookup(id); ok && old.Path == r.Path && slices.Equal(old.Refs, record.Refs) {
			record.Refs = old.Refs
		}
		records[id] = record
		if info := manifest.KindOf(projected); info.Tracked && info.Discovery {
			objects[id] = projected
		}
	}
	return records, objects
}

func (l *Loader) authoredChanges(raw map[string]map[manifest.NamedResource]manifest.DiscoveryRecord, next map[manifest.NamedResource]manifest.DiscoveryRecord, objects map[manifest.NamedResource]manifest.BaseManifest) ([]manifest.NamedResource, []manifest.NamedResource, bool) {
	var deletes, upserts []manifest.NamedResource
	changed := false
	for id, r := range l.Index.Files() {
		if _, processed := raw[r.Path][r.Authored]; !processed {
			continue
		}
		if _, survives := next[id]; !survives {
			deletes = append(deletes, id)
			changed = true
		}
	}
	for _, id := range slices.SortedFunc(maps.Keys(next), manifest.NamedResource.Compare) {
		if _, exists := l.Index.Lookup(id); !exists {
			changed = true
		}
		if obj := objects[id]; obj != nil {
			if existing := l.Store.GetObject(id); existing != nil {
				ks, isKS := obj.(*manifest.Kustomization)
				if !isKS || existing.(*manifest.Kustomization).TargetNamespace == ks.TargetNamespace {
					continue
				}
			}
			upserts = append(upserts, id)
			changed = true
		}
	}
	slices.SortFunc(deletes, manifest.NamedResource.Compare)
	return deletes, upserts, changed
}

func (l *Loader) applyAuthored(next map[manifest.NamedResource]manifest.DiscoveryRecord, objects map[manifest.NamedResource]manifest.BaseManifest, deletes, upserts []manifest.NamedResource) {
	for _, id := range deletes {
		delete(l.Index.Records, id)
		l.Store.DeleteObject(id)
	}
	for _, id := range slices.SortedFunc(maps.Keys(next), manifest.NamedResource.Compare) {
		l.Index.Records[id] = next[id]
	}
	for _, id := range upserts {
		if obj := objects[id]; obj != nil {
			l.Store.AddObject(obj)
		}
	}
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
