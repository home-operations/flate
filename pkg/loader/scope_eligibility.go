package loader

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) resetAdmissions() {
	l.roots.retainIndependent()
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
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
	{
		cValues := l.scopes.contributions
		for cIndex := range cValues {
			c := cValues[cIndex]
			l.controlContributionEligibility(new(c), mark)
		}
	}
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for file, doc := range l.documents.cache {
		l.seedFileEligibility(file, new(doc), mark)
	}
	for {
		changed := false
		for i := range l.scopes.contributions {
			c := &l.scopes.contributions[i]
			changed = l.propagateContributionEligibility(c, mark) || changed
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
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for file, doc := range l.documents.cache {
		if doc.eligible == fileUnknown && len(doc.objects) != 0 {
			mark(file, fileInactive)
		}
	}
}

func (l *Loader) controlContributionEligibility(
	contributionArg *scopeContribution,
	mark func(string, fileEligibility) bool,
) {
	c := *contributionArg
	traversal := l.scopes.walks[c.context]
	if !c.unshared && traversal.controlled {
		return
	}
	owner, _ := l.Index.Lookup(c.owner)
	source, _ := l.Index.Lookup(c.source)
	declaredSource := c.membershipOnly && l.IndexedObject(c.source, source).Obj != nil
	controlled := c.owner != (manifest.NamedResource{}) && !declaredSource
	{
		vValues4 := c.visits
		for vIndex4 := range vValues4 {
			v := vValues4[vIndex4]
			if v.role != kustomizeinput.Resource {
				mark(v.file, fileInactive)
				continue
			}
			if c.owner == (manifest.NamedResource{}) || declaredSource || owner.Path == v.file ||
				v.origin != "" && owner.Path == v.origin {
				controlled = false
				continue
			}
			mark(v.file, fileInactive)
			if v.localFile != "" {
				mark(v.localFile, fileInactive)
			}
		}
	}
	if controlled && !c.unshared {
		traversal.controlled = true
		l.scopes.walks[c.context] = traversal
	}
}

func (l *Loader) seedFileEligibility(file string, documentArg *document, mark func(string, fileEligibility) bool) {
	doc := *documentArg
	if l.entry == entryPlain && filepath.Dir(file) == l.entryPath && doc.loaded && !l.terminalInput(file) {
		mark(file, fileEligible)
		return
	}
	if doc.eligible != fileUnknown || doc.objects == nil {
		return
	}
	builder := l.documents.read(filepath.Dir(file)).file
	if builder != "" || l.terminalInput(file) {
		return
	}
	unowned := true
	coordinate := l.fileCoordinate(new(l.documents).localPath(file))
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

func (l *Loader) contributionActive(c *scopeContribution) bool {
	r, indexed := l.Index.Lookup(c.owner)
	object := l.Store.GetObject(c.owner)
	active := c.owner == (manifest.NamedResource{}) || !indexed || r.Path == "" ||
		l.documents.cache[r.Path].eligible >= fileEligible ||
		object != nil && object != l.admitted[c.owner]
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
	return active
}

func (l *Loader) propagateContributionEligibility(c *scopeContribution, mark func(string, fileEligibility) bool) bool {
	changed := false
	active := l.contributionActive(c)
	c.active = active
	if c.input != 0 {
		l.scopes.inputs[c.input-1].active = active
	}
	if !active {
		return false
	}
	reachability := fileEligible
	if c.owner == (manifest.NamedResource{}) && !c.membershipOnly && len(c.visits) != 0 &&
		!slices.ContainsFunc(
			l.scopes.claims,
			func(claim manifest.KSClaim) bool { return strings.HasPrefix(c.visits[0].dir+"/", claim.Prefix) },
		) {
		reachability = fileIndependent
	}
	if !c.unshared {
		traversal := l.scopes.walks[c.context]
		if traversal.reached >= reachability {
			return false
		}
		traversal.reached = reachability
		l.scopes.walks[c.context] = traversal
	}
	{
		vValues5 := c.visits
		for vIndex5 := range vValues5 {
			v := vValues5[vIndex5]
			if v.role != kustomizeinput.Resource {
				continue
			}
			changed = mark(v.file, reachability) || changed
			if v.localFile != "" {
				changed = mark(v.localFile, reachability) || changed
			}
		}
	}
	return changed
}
