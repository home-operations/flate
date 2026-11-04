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
)

type scopeTraversal struct {
	visits     []graphVisit
	reached    fileEligibility
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
	var source parsedSource
	var key physicalContext
	if !seen && l.documents.compact {
		source = l.sourceFacts(root.root)
		key = physicalContext{
			graphContext: root.graphContext,
			sourceRoot:   l.SourceRoot,
			prefix:       root.prefix,
			localBase:    root.localBase,
		}
		walk.visits, seen = source.walks[key]
		if seen {
			l.scopes.walks[root.graphContext] = walk
		}
	}
	if seen {
		return walk.visits
	}

	var visits []graphVisit
	if k := l.documents.cache[filepath.Join(root.root, root.dir)].body; k != nil {
		visits = make([]graphVisit, 0, len(k.Resources)+len(k.Components)+1)
	}
	var work *manifest.WorkRecord
	if l.work != nil {
		work = l.work
		if root.input != 0 {
			source := &l.scopes.sources[l.scopes.inputs[root.input-1].source]
			if source.id == root.source && source.root == root.root {
				work = l.Options.Work.Traversal(source.id)
			}
		}
	}
	w := traversal{work: work, documents: l.documents}
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
	physical := root
	if l.documents.compact {
		physical.owner, physical.source, physical.input, physical.membershipOnly = manifest.NamedResource{}, manifest.NamedResource{}, 0, false
	}
	w.walk(context.TODO(), physical, collect)
	walk.visits = visits
	if l.documents.compact {
		source.walks[key] = visits
	}
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
	{
		vValues := visits
		for vIndex := range vValues {
			v := vValues[vIndex]
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
	}
	return visits
}
