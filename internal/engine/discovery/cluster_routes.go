package discovery

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

func (p *entryPlanner) reach(ctx context.Context, root *clusterRoot) error {
	for p.head < len(p.queue) {
		visit := &p.queue[p.head]
		key := occurrenceIdentity(visit.occurrence)
		state, known := p.seen[key]
		if !known {
			state = rootVisitState{index: len(p.seen) + 1, initial: visit.scope == &p.scope}
			p.seen[key] = state
			visit.first = true
		}

		_, _, hasPath := visit.occurrence.SourcePath()
		p.recordReach(root, visit, hasPath)
		var next *rootScope
		if !visit.occurrence.RequiresBuild && (hasPath || visit.occurrence.Identity.KindInfo().Runnable) {
			var err error
			next, err = p.visitApplications(ctx, root, visit, &state)
			if err != nil {
				return err
			}
		}
		if visit.first {
			root.Occurrences = append(root.Occurrences, *visit.occurrence)
			if state.prepared != nil {
				p.seen[key] = state
			}
		}
		*visit = rootVisit{}
		p.head++
		if next != nil {
			p.enqueue(next)
		}
	}
	return nil
}

func (p *entryPlanner) enqueue(scope *rootScope) {
	p.queue = slices.Grow(p.queue, len(scope.occurrences))
	for i := range scope.occurrences {
		p.queue = append(p.queue, rootVisit{occurrence: &scope.occurrences[i], scope: scope})
	}
}

//nolint:nilnil // An occurrence without an application leaves the queue unchanged.
func (p *entryPlanner) visitApplications(
	ctx context.Context, root *clusterRoot, visit *rootVisit, state *rootVisitState,
) (*rootScope, error) {
	object := visit.occurrence.EffectiveObject()
	application, valid := visit.occurrence.EntryApplication()
	resolved := rootApplication{visit: visit, object: object, application: application, state: state}
	if visit.first {
		if err := p.followReferences(ctx, root, &resolved); err != nil {
			return nil, err
		}
	}
	if !valid || !manifest.KindOf(object).Runnable {
		return nil, nil
	}
	return p.follow(ctx, root, &resolved)
}

func (p *entryPlanner) recordReach(root *clusterRoot, visit *rootVisit, hasPath bool) {
	if len(p.roots) < 2 || visit.scope.parent == nil || !hasPath || visit.occurrence.RequiresBuild {
		return
	}
	if root.reached == nil {
		root.reached = map[selection.OccurrenceID]struct{}{}
	}
	root.reached[physicalDeclaration(visit.occurrence)] = struct{}{}
}

//nolint:nilnil // A completed route has no further visits to queue.
func (p *entryPlanner) follow(
	ctx context.Context, root *clusterRoot, resolved *rootApplication,
) (*rootScope, error) {
	visit := resolved.visit
	occurrence := visit.occurrence
	application := &resolved.application
	state := resolved.state
	resolved.origin = cmp.Or(visit.scope.seed, occurrence)
	var resolvedTarget rootTarget
	target := &resolvedTarget
	replay := state.initial && !visit.first
	if replay {
		if state.prepared == nil {
			return nil, nil
		}
		target = &state.prepared.target
	} else {
		target.reference = sourceReference(resolved.object, application.Source)
		target.reference.NamedResource = application.Source
		source, err := p.source(ctx, root, resolved, &target.reference)
		if err != nil {
			return nil, err
		}
		target.source = source
	}
	route := rootRoute{origin: physicalDeclaration(resolved.origin), index: state.index, source: target.source}
	if _, complete := p.routes[route]; complete {
		return nil, nil
	}
	var scope *rootScope
	if replay {
		// A repeated seed MUST use the original target and the scope prepared under its authority.
		prepared := &state.prepared.scope
		scope = &rootScope{occurrences: prepared.occurrences, parent: prepared.parent, seed: resolved.origin}
	} else {
		var err error
		scope, err = p.prepareRoute(ctx, root, resolved, target)
		if err != nil {
			return nil, err
		}
	}
	p.completeRoute(root, &route, target, scope)
	return scope, nil
}

//nolint:nilnil // An unavailable target does not add a scope to the queue.
func (p *entryPlanner) prepareRoute(
	ctx context.Context, root *clusterRoot, resolved *rootApplication, target *rootTarget,
) (*rootScope, error) {
	visit := resolved.visit
	occurrence := visit.occurrence
	seed := p.coordinates[occurrence.Declaration]
	application := &resolved.application

	input := target.source.input
	application.Path = filepath.ToSlash(filepath.Clean(manifest.NormalizeClaimBase(application.Path)))
	target.path = application.Path
	declaration := physicalDeclaration(occurrence)
	qualifies := seed && p.bootstrapRoot(root, occurrence, input, application)
	absolute, local := p.followTarget(root, input, target.path, seed)
	target.local = local
	if absolute == "" {
		if qualifies {
			root.qualifying = append(root.qualifying, declaration)
		}
		return nil, nil
	}
	total := 0
	_, err := p.session.loadAt(ctx, absolute, &total, &sourceLoad{
		work: p.session.cfg.Work.Source(application.Source).Record(manifest.WorkPreparation, absolute),
	})
	if err != nil {
		return nil, err
	}
	children, err := p.followOutput(ctx, input, resolved, absolute)
	if err != nil {
		return nil, err
	}
	var scope *rootScope
	if resolved.state.initial {
		resolved.state.prepared = &preparedRootRoute{target: *target}
		scope = &resolved.state.prepared.scope
	} else {
		scope = new(rootScope)
	}
	*scope = rootScope{occurrences: children, parent: visit.scope, seed: resolved.origin}
	if err := p.prepareScope(ctx, scope); err != nil {
		return nil, err
	}
	if qualifies {
		root.qualifying = append(root.qualifying, declaration)
	}
	pinned := p.session.pinnedWorkingTreeClaims(
		p.session.cfg.Input.LocalPath,
		[]selection.OccurrenceFacts{*occurrence},
	)
	root.addPinnedClaims(pinned.index.Claims)
	return scope, nil
}

func (p *entryPlanner) completeRoute(
	root *clusterRoot, route *rootRoute, target *rootTarget, scope *rootScope,
) {
	if p.routes == nil {
		p.routes = map[rootRoute]struct{}{}
	}
	p.routes[*route] = struct{}{}
	if scope == nil || !target.local {
		return
	}
	for i := range scope.occurrences {
		if physicalDeclaration(&scope.occurrences[i]) == route.origin {
			root.recordAnchor(target.path, &target.reference)
			return
		}
	}
}

func (r *clusterRoot) recordAnchor(target string, reference *manifest.Reference) {
	var source manifest.NamedResource
	if reference.BindsSource() {
		source = reference.NamedResource
	}
	i := slices.IndexFunc(r.anchors, func(anchor bootstrapAnchor) bool { return anchor.target == target })
	if i < 0 {
		r.anchors = append(r.anchors, bootstrapAnchor{target: target, source: source})
	} else if r.anchors[i].source == (manifest.NamedResource{}) {
		r.anchors[i].source = source
	}
}
