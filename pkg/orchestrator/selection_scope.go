package orchestrator

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func (s *selectionSide) prepareFailures(roots []manifest.NamedResource, other *selectionSide) {
	if len(s.run.Result.Failed) == 0 {
		return
	}
	s.result.Failed = make(map[manifest.NamedResource]store.StatusInfo)
	s.result.Blocked = make(map[manifest.NamedResource][]manifest.NamedResource)
	var required map[manifest.NamedResource]struct{}
	if roots[0].Name != "" {
		required = s.requiredClosure(roots, other)
	}
	unrelated := make(map[manifest.NamedResource]store.StatusInfo)
	for id, info := range s.run.Result.Failed {
		_, needed := required[id]
		if required != nil && !needed {
			if s.includes(id.Namespace) {
				unrelated[id] = info
			}
			continue
		}
		if required == nil && !s.includes(id.Namespace) {
			continue
		}
		s.result.Failed[id] = info
		if deps := s.run.Result.Blocked[id]; len(deps) > 0 {
			s.result.Blocked[id] = deps
		}
	}
	if len(unrelated) > 0 {
		s.result.Unrelated = unrelatedFailures(s.run.Result, unrelated, required)
	}
	s.result.Causes = failureCauses(s.result.Failed, s.result.Blocked)
}

func (s *selectionSide) requiredClosure(roots []manifest.NamedResource, other *selectionSide) map[manifest.NamedResource]struct{} {
	seen := make(map[manifest.NamedResource]struct{})
	var work []manifest.NamedResource
	add := func(id manifest.NamedResource) {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			work = append(work, id)
		}
	}
	kinds := make(map[manifest.NamedResource]bool, len(roots))
	for _, root := range roots {
		kinds[manifest.NamedResource{Group: root.Group, Kind: root.Kind}] = true
	}
	seed := func(id manifest.NamedResource) {
		if kinds[manifest.NamedResource{Group: id.Group, Kind: id.Kind}] && id.Name == roots[0].Name && s.includes(id.Namespace) {
			add(id)
		}
	}
	for _, id := range s.roots {
		seed(id)
	}
	for id := range s.run.Result.Failed {
		seed(id)
	}
	children := s.run.childrenByParent()
	for _, kids := range children {
		for _, id := range kids {
			seed(id)
		}
	}
	// Prerequisites MUST NOT expand downward into unrelated siblings.
	for i := 0; i < len(work); i++ { //nolint:intrange // Descendants extend this traversal.
		for _, child := range children[work[i]] {
			add(child)
		}
	}
	if other != nil {
		for _, id := range other.roots {
			if s.run.store.GetObject(id) == nil {
				if parent, ok := other.run.parentOf(id); ok {
					add(parent)
				}
			}
		}
	}
	for len(work) > 0 {
		id := work[len(work)-1]
		work = work[:len(work)-1]
		if parent, ok := s.run.parentOf(id); ok {
			add(parent)
		}
		for _, dep := range s.run.Result.DependsOn[id] {
			add(dep)
		}
		for _, dep := range s.run.Result.Blocked[id] {
			add(dep)
		}
		if obj := s.run.store.GetObject(id); obj != nil {
			for ref := range manifest.References(obj) {
				add(ref.NamedResource)
			}
		}
		if id.KindInfo().Render {
			for _, dep := range s.run.requiredDataDependencies(id) {
				add(dep)
			}
		}
	}
	return seen
}

func rootsOf(id manifest.NamedResource, blocked map[manifest.NamedResource][]manifest.NamedResource) []manifest.NamedResource {
	var out []manifest.NamedResource
	seen := map[manifest.NamedResource]bool{id: true}
	var walk func(manifest.NamedResource)
	walk = func(n manifest.NamedResource) {
		for _, dep := range blocked[n] {
			if seen[dep] {
				continue
			}
			seen[dep] = true
			if _, derived := blocked[dep]; derived {
				walk(dep)
			} else {
				out = append(out, dep)
			}
		}
	}
	walk(id)
	return out
}

func rootVictims(blocked map[manifest.NamedResource][]manifest.NamedResource) map[manifest.NamedResource][]manifest.NamedResource {
	byRoot := make(map[manifest.NamedResource][]manifest.NamedResource)
	for id := range blocked {
		for _, root := range rootsOf(id, blocked) {
			byRoot[root] = append(byRoot[root], id)
		}
	}
	for root, ids := range byRoot {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
		byRoot[root] = slices.Compact(ids)
	}
	return byRoot
}

func failureCauses(failed map[manifest.NamedResource]store.StatusInfo, blocked map[manifest.NamedResource][]manifest.NamedResource) map[manifest.NamedResource][]manifest.NamedResource {
	byRoot := rootVictims(blocked)
	covered := make(map[manifest.NamedResource]bool)
	for root, ids := range byRoot {
		covered[root] = true
		for _, id := range ids {
			covered[id] = true
		}
	}
	for id := range failed {
		if _, derived := blocked[id]; !derived {
			covered[id] = true
		}
	}
	cycles := make(map[manifest.NamedResource]bool)
	for id := range failed {
		if covered[id] {
			continue
		}
		seen := make(map[manifest.NamedResource]bool)
		var reaches func(manifest.NamedResource) bool
		reaches = func(dep manifest.NamedResource) bool {
			if dep == id {
				return true
			}
			if seen[dep] {
				return false
			}
			seen[dep] = true
			return slices.ContainsFunc(blocked[dep], reaches)
		}
		if slices.ContainsFunc(blocked[id], reaches) {
			cycles[id] = true
		}
	}
	view := blocked
	if len(cycles) > 0 {
		view = maps.Clone(blocked)
		for id := range cycles {
			delete(view, id)
			peers := slices.Clone(blocked[id])
			slices.SortFunc(peers, manifest.NamedResource.Compare)
			names := make([]string, len(peers))
			for i, peer := range peers {
				names[i] = peer.String()
			}
			info := failed[id]
			info.Message = "dependency cycle: blocked by " + strings.Join(names, ", ")
			failed[id] = info
		}
		byRoot = rootVictims(view)
	}
	for id := range failed {
		if _, derived := view[id]; !derived {
			if _, exists := byRoot[id]; !exists {
				byRoot[id] = nil
			}
		}
	}
	return byRoot
}

func unrelatedFailures(raw *Result, unrelated map[manifest.NamedResource]store.StatusInfo, required map[manifest.NamedResource]struct{}) []manifest.Warning {
	byRoot := rootVictims(raw.Blocked)
	covered := make(map[manifest.NamedResource]bool)
	var warnings []manifest.Warning
	children := func(ids []manifest.NamedResource) []string {
		var out []string
		for _, id := range ids {
			if _, ok := unrelated[id]; ok {
				out = append(out, id.String())
				covered[id] = true
			}
		}
		return out
	}
	for id, info := range raw.Failed {
		if _, derived := raw.Blocked[id]; derived {
			continue
		}
		if _, ok := unrelated[id]; ok {
			covered[id] = true
			warnings = append(warnings, manifest.Warning{Resource: id, Message: info.Message, Detail: children(byRoot[id])})
		}
	}
	for root, victims := range byRoot {
		if _, failed := raw.Failed[root]; failed {
			continue
		}
		if _, needed := required[root]; needed {
			continue
		}
		if detail := children(victims); len(detail) > 0 {
			warnings = append(warnings, manifest.Warning{Resource: root, Message: "not found; required by", Detail: detail})
		}
	}
	for id, info := range unrelated {
		if covered[id] {
			continue
		}
		deps := slices.Clone(raw.Blocked[id])
		slices.SortFunc(deps, manifest.NamedResource.Compare)
		var detail []string
		for _, dep := range slices.Compact(deps) {
			detail = append(detail, "blocked by "+dep.String())
		}
		warnings = append(warnings, manifest.Warning{Resource: id, Message: info.Message, Detail: detail})
	}
	slices.SortFunc(warnings, func(a, b manifest.Warning) int { return a.Resource.Compare(b.Resource) })
	return warnings
}

func (s *selectionSide) prepareStatuses(name string) {
	seen := make(map[manifest.NamedResource]bool)
	blocked := make(map[manifest.NamedResource][]manifest.NamedResource)
	add := func(id manifest.NamedResource, info store.StatusInfo) {
		seen[id] = true
		if s.result.Statuses == nil {
			s.result.Statuses = make(map[manifest.NamedResource]store.StatusInfo)
		}
		s.result.Statuses[id] = info
		if info.Status == store.StatusFailed && info.IsDependencyFailure() {
			blocked[id] = info.BlockedBy()
		}
	}
	for _, obj := range s.result.Objects {
		id := obj.Named()
		if facts := manifest.KindOf(obj); facts.Tracked && (!id.ReportHidden() || !facts.SourceArtifact) {
			info, ok := s.run.store.GetStatus(id)
			if !ok {
				info = store.StatusInfo{Status: store.StatusFailed, Message: "no status reported"}
			}
			add(id, info)
		}
	}
	for id := range s.run.Result.Failed {
		if seen[id] || id.ReportHidden() ||
			(name != "" && id.Name != name) || !s.includes(id.Namespace) {
			continue
		}
		if info, ok := s.run.store.GetStatus(id); ok {
			add(id, info)
		}
	}
	for victim, blockers := range blocked {
		roots := rootsOf(victim, blocked)
		if len(roots) == 0 {
			roots = slices.Clone(blockers)
		}
		slices.SortFunc(roots, manifest.NamedResource.Compare)
		names := make([]string, len(roots))
		for i, root := range roots {
			names[i] = root.NamespacedName()
			if info, known := s.run.store.GetStatus(root); !known {
				names[i] += " (not found)"
			} else if !seen[root] && info.Status == store.StatusFailed {
				names[i] += ": " + manifest.TrimSentinelPrefix(info.Message)
			}
		}
		info := s.result.Statuses[victim]
		info.Message = "blocked by " + strings.Join(names, ", ")
		s.result.Statuses[victim] = info
	}
}

func selectedFailures(res *selection.Selection) error {
	if len(res.Failed) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(res.Causes))
	blocked := res.BlockedCount()
	for root := range res.Causes {
		if info, failed := res.Failed[root]; failed {
			msgs = append(msgs, fmt.Sprintf("%s: %s", root, info.Message))
		} else {
			msgs = append(msgs, fmt.Sprintf("%s: not found", root))
		}
	}
	slices.Sort(msgs)
	msg := fmt.Sprintf("reconcile completed with %d failure(s)", len(msgs))
	if len(msgs) > 0 {
		msg += ":\n  " + strings.Join(msgs, "\n  ")
	}
	if blocked > 0 {
		msg += fmt.Sprintf("\n  (+%d blocked by failed/missing dependencies)", blocked)
	}
	return &FailuresError{Message: msg}
}

func nonResourceErrors(err error) error {
	if err == nil {
		return nil
	}
	failure, ok := errors.AsType[*FailuresError](err)
	if !ok {
		return err
	}
	if err == failure { //nolint:errorlint // Prune only the concrete aggregate node, preserving joined siblings.
		return nil
	}
	if e, ok := errors.AsType[interface {
		error
		Unwrap() []error
	}](err); ok {
		children := e.Unwrap()
		projected := make([]error, 0, len(children))
		for _, child := range children {
			projected = append(projected, nonResourceErrors(child))
		}
		return errors.Join(projected...)
	}
	return nonResourceErrors(errors.Unwrap(err))
}

func selectionError(sides []selectionSide, runErr error) (error, error) {
	if runErr == nil {
		return nil, nil
	}
	if len(sides) == 1 {
		remainder := nonResourceErrors(runErr)
		return errors.Join(selectedFailures(sides[0].result), remainder), remainder
	}
	var projectedTree bool
	var project func(error) (error, error)
	project = func(err error) (error, error) {
		if err == nil {
			return nil, nil
		}
		// A descendant tree MUST NOT absorb its ancestor's joined siblings.
		if tree, ok := errors.AsType[*treeErrors](err); ok && errors.Is(tree, err) {
			projectedTree = true
			orig, current := nonResourceErrors(tree.base), nonResourceErrors(tree.head)
			return joinSelectedErrors(
				errors.Join(selectedFailures(sides[0].result), orig),
				errors.Join(selectedFailures(sides[1].result), current),
			), joinSelectedErrors(orig, current)
		}
		if _, ok := errors.AsType[*treeErrors](err); !ok {
			remainder := nonResourceErrors(err)
			return remainder, remainder
		}
		if child := errors.Unwrap(err); child != nil {
			return project(child)
		}
		if e, ok := errors.AsType[interface {
			error
			Unwrap() []error
		}](err); ok {
			children := e.Unwrap()
			out := make([]error, 0, len(children))
			remainders := make([]error, 0, len(children))
			for _, child := range children {
				selected, remainder := project(child)
				out = append(out, selected)
				remainders = append(remainders, remainder)
			}
			return errors.Join(out...), errors.Join(remainders...)
		}
		return err, err
	}
	projected, remainder := project(runErr)
	if projectedTree {
		return projected, remainder
	}
	return errors.Join(joinSelectedErrors(selectedFailures(sides[0].result), selectedFailures(sides[1].result)), projected), remainder
}

func joinSelectedErrors(orig, current error) error {
	switch {
	case orig != nil && current != nil:
		return errors.Join(errors.New("both snapshots had reconcile failures"), fmt.Errorf("orig snapshot: %w", orig), fmt.Errorf("current snapshot: %w", current))
	case orig != nil:
		return fmt.Errorf("orig snapshot: %w", orig)
	case current != nil:
		return fmt.Errorf("current snapshot: %w", current)
	}
	return nil
}
