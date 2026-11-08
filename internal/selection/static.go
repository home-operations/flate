// Package selection owns render membership. Physical acquisition and storage remain with their factual owners.
package selection

import (
	"iter"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// RootEligibility MUST preserve its order because propagation only raises eligibility.
type RootEligibility uint8

// Static reduction MUST start at RootUnknown for each construction generation.
const (
	RootUnknown RootEligibility = iota
	RootInactive
	RootEligible
	RootIndependent
)

// StaticFile borrows presence facts; it MUST NOT retain or expose parsed document objects.
type StaticFile struct {
	Eligibility  RootEligibility
	Loaded       bool
	ObjectsKnown bool
	HasObjects   bool
}

// StaticContribution retains source identity even when owners share physical visits.
type StaticContribution struct {
	Owner, Source            manifest.NamedResource
	Visits                   int
	Unshared, MembershipOnly bool
}

// StaticVisit coordinates MUST stay in the original source view.
type StaticVisit struct {
	File, LocalFile, Origin, Dir string
	Resource                     bool
}

// StaticTraversal contains only mutable reduction results, never physical visits.
type StaticTraversal struct {
	Reached    RootEligibility
	Controlled bool
}

// staticScope borrows one sequential construction generation. Setters publish only
// reduction outputs into the owner's existing storage, never physical graph facts.
type staticScope interface {
	ResetStatic()
	Files() iter.Seq2[string, StaticFile]
	Eligibility(string) RootEligibility
	SetEligibility(string, RootEligibility)
	PlainEntry() string
	Terminal(string) bool
	DirectoryFile(string) string
	Coordinate(string) string
	Claims() []manifest.KSClaim
	Owner(manifest.NamedResource) (path string, indexed bool)
	Replaced(manifest.NamedResource) bool
	Declared(manifest.NamedResource) bool
	Contributions() int
	Contribution(int) StaticContribution
	Visit(int, int) StaticVisit
	Traversal(int) StaticTraversal
	SetTraversal(int, StaticTraversal)
	SetActive(int, bool)
	Inputs() int
	InputActive(int) bool
	SetInactive(bool)
}

// ReduceStatic yields exact parent-directory reads to its caller. The caller MUST
// complete each read before resuming and exhaust the iterator before publication.
// No borrowed input or mutable result escapes this construction call.
func ReduceStatic[S staticScope](scope S) iter.Seq[string] {
	return func(read func(string) bool) {
		scope.ResetStatic()
		for i := range scope.Contributions() {
			controlContribution(scope, i)
		}
		if !seedFiles(scope, read) {
			return
		}
		for {
			changed := false
			for i := range scope.Contributions() {
				changed = propagateContribution(scope, i) || changed
			}
			if !changed {
				break
			}
		}
		finishStatic(scope)
	}
}

func markFile[S staticScope](scope S, file string, state RootEligibility) bool {
	if scope.Eligibility(file) >= state {
		return false
	}
	scope.SetEligibility(file, state)
	return true
}

func controlContribution[S staticScope](scope S, i int) {
	c := scope.Contribution(i)
	traversal := scope.Traversal(i)
	if !c.Unshared && traversal.Controlled {
		return
	}
	owner, _ := scope.Owner(c.Owner)
	declared := c.MembershipOnly && scope.Declared(c.Source)
	controlled := c.Owner != (manifest.NamedResource{}) && !declared
	for j := range c.Visits {
		v := scope.Visit(i, j)
		if !v.Resource {
			markFile(scope, v.File, RootInactive)
			continue
		}
		ownerVisit := owner == v.File || v.Origin != "" && owner == v.Origin
		if c.Owner == (manifest.NamedResource{}) || declared || ownerVisit {
			controlled = false
			continue
		}
		markFile(scope, v.File, RootInactive)
		if v.LocalFile != "" {
			markFile(scope, v.LocalFile, RootInactive)
		}
	}
	if controlled && !c.Unshared {
		traversal.Controlled = true
		scope.SetTraversal(i, traversal)
	}
}

func propagateContribution[S staticScope](scope S, i int) bool {
	c := scope.Contribution(i)
	owner, indexed := scope.Owner(c.Owner)
	ownerEligible := !indexed || owner == "" || scope.Eligibility(owner) >= RootEligible
	active := c.Owner == (manifest.NamedResource{}) || ownerEligible || scope.Replaced(c.Owner)
	unowned := false
	if c.Owner == (manifest.NamedResource{}) && !c.MembershipOnly && c.Visits != 0 {
		claimed, enabled := contributionClaims(scope, i)
		active, unowned = !claimed || enabled, !claimed
	}
	scope.SetActive(i, active)
	if !active {
		return false
	}
	reach := RootEligible
	if unowned {
		reach = RootIndependent
	}
	if !c.Unshared {
		traversal := scope.Traversal(i)
		if traversal.Reached >= reach {
			return false
		}
		traversal.Reached = reach
		scope.SetTraversal(i, traversal)
	}
	changed := false
	for j := range c.Visits {
		v := scope.Visit(i, j)
		if !v.Resource {
			continue
		}
		changed = markFile(scope, v.File, reach) || changed
		if v.LocalFile != "" {
			changed = markFile(scope, v.LocalFile, reach) || changed
		}
	}
	return changed
}

func seedFiles[S staticScope](scope S, read func(string) bool) bool {
	for file, doc := range scope.Files() {
		if scope.PlainEntry() != "" && filepath.Dir(file) == scope.PlainEntry() && doc.Loaded && !scope.Terminal(file) {
			markFile(scope, file, RootEligible)
			continue
		}
		if doc.Eligibility != RootUnknown || !doc.ObjectsKnown {
			continue
		}
		if !read(filepath.Dir(file)) {
			return false
		}
		if scope.DirectoryFile(filepath.Dir(file)) != "" || scope.Terminal(file) {
			continue
		}
		state := RootEligible
		coordinate := scope.Coordinate(file)
		for _, claim := range scope.Claims() {
			owner, _ := scope.Owner(claim.ID)
			if strings.HasPrefix(coordinate, claim.Prefix) && owner != file {
				state = RootInactive
				break
			}
		}
		markFile(scope, file, state)
	}
	return true
}

//nolint:gocritic // The pair preserves separate claimed and enabled facts.
func contributionClaims[S staticScope](scope S, i int) (bool, bool) {
	claimed, enabled := false, false
	dir := scope.Visit(i, 0).Dir + "/"
	for _, claim := range scope.Claims() {
		if strings.HasPrefix(dir, claim.Prefix) {
			owner, _ := scope.Owner(claim.ID)
			claimed = true
			enabled = enabled || owner == "" || scope.Eligibility(owner) >= RootEligible
		}
	}
	return claimed, enabled
}

func finishStatic[S staticScope](scope S) {
	inactive := false
	for i := range scope.Inputs() {
		if !scope.InputActive(i) {
			inactive = true
			break
		}
	}
	scope.SetInactive(inactive)
	for file, doc := range scope.Files() {
		if doc.Eligibility == RootUnknown && doc.HasObjects {
			markFile(scope, file, RootInactive)
		}
	}
}
