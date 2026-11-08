package loader

import (
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"slices"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

type scopeTraversal struct {
	visits     []graphVisit
	reached    selection.RootEligibility
	controlled bool
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

func (l *Loader) scopeWalk(rootArg *graphVisit) []graphVisit {
	root := *rootArg

	walk, seen := l.scopes.walks[root.graphContext]
	if seen {
		return walk.visits
	}
	source := l.sourceFacts(root.root)
	key := physicalContext{
		graphContext: root.graphContext,
		sourceRoot:   l.SourceRoot,
		prefix:       root.prefix,
		localBase:    root.localBase,
	}
	physical, known := source.walk(&key)
	if physical.cached != nil {
		physical, known = l.restoreWalk(new(l.documents).mountedRoot(root.root), physical.cached)
		if known {
			source.walks.values[key] = physical
		}
	}
	if known {
		walk.visits = physical.visits
		l.scopes.walks[root.graphContext] = walk
		return walk.visits
	}

	var visits []graphVisit
	var inputs []walkInput
	if k := l.documents.cache[filepath.Join(root.root, root.dir)].body; k != nil {
		visits = make([]graphVisit, 0, len(k.Resources)+len(k.Components)+1)
	}
	work := l.scopeWork(&root)
	w := traversal{work: work, documents: l.documents, loader: l, scopeVisits: &visits}
	if l.inputCaches[root.root] != nil {
		w.scopeInputs = &inputs
	}
	collect := func(v graphVisit) bool {
		if origin := new(l.documents).localPath(v.file); origin != v.file {
			v.origin = origin
		}
		if local := v.localCoordinate(); local != "" {
			v.localFile = filepath.Join(l.SourceRoot, filepath.FromSlash(local))
		}
		visits = append(visits, v)
		return true
	}
	physicalRoot := root
	physicalRoot.owner, physicalRoot.source, physicalRoot.input, physicalRoot.membershipOnly = manifest.NamedResource{}, manifest.NamedResource{}, 0, false
	w.walk(context.TODO(), physicalRoot, collect)
	walk.visits = visits
	source.walks.values[key] = physicalWalk{visits: visits, inputs: inputs}
	l.scopes.walks[root.graphContext] = walk

	return walk.visits
}

func (l *Loader) visitScope(rootArg *graphVisit, membershipOnly bool) []graphVisit {
	root := *rootArg

	root.role = kustomizeinput.Resource
	visits := l.scopeWalk(new(root))
	ks, _ := l.Store.Get[*manifest.Kustomization](root.owner)
	if ks == nil {
		record, _ := l.Index.Lookup(root.owner)
		ks, _ = l.IndexedObject(root.owner, record).Obj.(*manifest.Kustomization)
	}
	unshared := !membershipOnly && ks != nil && len(ks.Components) != 0
	if unshared {
		visits = slices.Clone(visits)
		namespace := root.namespace
		{
			visitValues := visits
			for visitIndex := range visitValues {
				visit := visitValues[visitIndex]
				if visit.document != nil && visit.dir == root.dir {
					namespace = visit.namespace
					break
				}
			}
		}
		for _, component := range ks.Components {
			if !fluxkustomize.IsLocalRelativePath(component) {
				continue
			}
			child := root
			child.dir = path.Join(root.dir, filepath.ToSlash(component))
			file := filepath.Join(root.root, child.dir)
			inputFile := new(l.documents).input(file)
			info, err := fs.Stat(inputFile.fs, inputFile.name)
			new(l.documents).rememberStat(file, info, err)
			if err != nil && ks.IgnoreMissingComponents {
				continue
			}
			child.namespace = namespace
			visits = append(visits, l.scopeWalk(new(child))...)
		}
	}
	return l.recordScopeVisits(&root, visits, membershipOnly, unshared)
}

func (l *Loader) recordScopeVisits(root *graphVisit, visits []graphVisit, membershipOnly, unshared bool) []graphVisit {
	l.scopes.contributions = append(
		l.scopes.contributions,
		scopeContribution{
			owner:          root.owner,
			source:         root.source,
			visits:         visits,
			context:        root.graphContext,
			unshared:       unshared,
			membershipOnly: membershipOnly,
			input:          root.input,
		},
	)
	contribution := len(l.scopes.contributions) - 1
	for i := range visits {
		v := &visits[i]
		ref := scopeVisit{contribution: contribution, visit: i}
		l.scopes.visits[v.file] = append(l.scopes.visits[v.file], ref)
		if v.localFile != "" && v.localFile != v.file {
			l.scopes.visits[v.localFile] = append(l.scopes.visits[v.localFile], ref)
		}
		if v.origin != "" && v.origin != v.file && v.origin != v.localFile {
			l.scopes.visits[v.origin] = append(l.scopes.visits[v.origin], ref)
		}
	}
	return visits
}

func (l *Loader) scopeWork(root *graphVisit) *manifest.WorkRecord {
	var work *manifest.WorkRecord
	if l.work != nil {
		work = l.work
		if sourceWork := l.scopes.traversalWork[root.root]; root.source == (manifest.NamedResource{}) &&
			sourceWork != nil {
			work = sourceWork
		}
		if root.input != 0 {
			source := &l.scopes.sources[l.scopes.inputs[root.input-1].source]
			if source.id == root.source && source.root == root.root {
				work = l.Options.Work.Traversal(source.id)
			}
		}
	}
	return work
}
