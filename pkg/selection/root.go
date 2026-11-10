package selection

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// RootResult keeps physical availability, independent membership and entry qualification distinct.
type RootResult struct {
	NeedsOwnerSupport  bool
	Construction       bool
	InitialAdmitted    bool
	InitialIndependent bool
	InitialEntry       bool
	Owner              manifest.NamedResource
	Occurrence         OccurrenceFacts
	Admitted           bool
	Independent        bool
	Entry              bool
	PackageRoot        bool
}

func classifyRoot(evidence *RootEvidence) RootResult {
	scope := &evidence.Scope
	if !scope.NamespaceEligible || scope.Terminal {
		return RootResult{}
	}
	if scope.NativeEntry {
		return RootResult{Admitted: true, Independent: true, Occurrence: evidence.Occurrence}
	}
	if scope.PackageRoot {
		independent := packageIndependent(scope)
		return RootResult{Admitted: independent && scope.PackageRunnable, Independent: independent, PackageRoot: true}
	}
	if evidence.Record.Path == "" {
		native := scope.Compact && evidence.Record.Parent.ReportHidden() &&
			evidence.Record.Parent == evidence.Record.BuilderOwner
		admitted := evidence.Record.Authored == (manifest.NamedResource{}) &&
			(evidence.Record.Parent == (manifest.NamedResource{}) || native) && scope.Candidate
		return RootResult{Admitted: admitted, Independent: admitted, Occurrence: evidence.Occurrence}
	}
	return classifyAuthoredRoot(evidence)
}

func classifyAuthoredRoot(evidence *RootEvidence) RootResult {
	scope := &evidence.Scope
	// An authored root MUST have an indexed effective identity after projection.
	projected := evidence.Record.Authored != (manifest.NamedResource{}) &&
		evidence.Identity != evidence.Record.Authored && scope.Records != nil
	if _, indexed := scope.Records[evidence.Identity]; projected && !indexed {
		return RootResult{}
	}
	owner := scopeOwner(evidence.Identity, evidence.Record.Authored, scope)
	admitted := scope.Eligibility != RootInactive && owner == (manifest.NamedResource{})
	if scope.Eligibility != RootInactive && evidence.Identity.KindInfo().Discovery && !scope.EntryKustomize &&
		scope.Loaded && !scope.Bootstrap {
		admitted = true
	}
	result := RootResult{Owner: owner, Admitted: admitted, Occurrence: evidence.Occurrence}
	if evidence.InitialScope != nil {
		initial := *evidence
		initial.Scope, initial.InitialScope = *evidence.InitialScope, nil
		initial.Occurrence = *evidence.InitialOccurrence
		if previous := classifyRoot(&initial); previous.Independent {
			result.Occurrence, result.Independent = previous.Occurrence, true
		}
		return result
	}
	result.Independent = initialRootIndependent(evidence, owner)
	return result
}

func initialRootIndependent(evidence *RootEvidence, owner manifest.NamedResource) bool {
	scope := &evidence.Scope
	if !scope.Initial || scope.Eligibility == RootInactive {
		return false
	}
	if owner != (manifest.NamedResource{}) {
		for i := range scope.Owners {
			candidate := &scope.Owners[i]
			if !selfClaim(candidate.Identity, evidence.Record.Authored, scope.Records) &&
				(!candidate.Indexed || candidate.Initial) {
				return false
			}
		}
	}
	if scope.PackageKnown && (scope.NativePackage || !packageIndependent(scope)) {
		return false
	}
	if !scope.EntryKustomize || scope.Bootstrap {
		return !scope.NativeOperand
	}
	return scope.Application && (!scope.NativeOperand && !evidence.Identity.KindInfo().Structural ||
		scope.LocalApplication && scope.SameEntryOrigin)
}

func scopeOwner(id, authored manifest.NamedResource, scope *RootScopeFacts) manifest.NamedResource {
	for i := range scope.Owners {
		owner := &scope.Owners[i]
		if owner.Identity == id || selfClaim(owner.Identity, authored, scope.Records) {
			continue
		}
		if scope.Bootstrap && owner.Path != scope.Path {
			return owner.Identity
		}
		if !id.KindInfo().Structural || !slices.ContainsFunc(scope.Peers, func(group []manifest.NamedResource) bool {
			return slices.Contains(group, owner.Identity) &&
				slices.ContainsFunc(group, func(peer manifest.NamedResource) bool {
					return selfClaim(peer, authored, scope.Records)
				})
		}) {
			return owner.Identity
		}
	}
	return manifest.NamedResource{}
}

func selfClaim(id, authored manifest.NamedResource, records map[manifest.NamedResource]manifest.DiscoveryRecord) bool {
	return id == authored || records[id].Authored == authored
}

func packageIndependent(scope *RootScopeFacts) bool {
	if !scope.PackageEligible {
		return false
	}
	for _, owner := range packageAncestors(scope.PackageOwners, scope.PackageIndex) {
		if !slices.Contains(packageAncestors(scope.PackageOwners, owner), scope.PackageIndex) ||
			filepath.ToSlash(scope.PackagePaths[owner]) < filepath.ToSlash(scope.PackagePaths[scope.PackageIndex]) {
			return false
		}
	}
	return true
}

func packageAncestors(owners [][]int, root int) []int {
	seen := make([]bool, len(owners))
	queue := slices.Clone(owners[root])
	var ancestors []int
	for len(queue) != 0 {
		owner := queue[0]
		queue = queue[1:]
		if owner == root || seen[owner] {
			continue
		}
		seen[owner] = true
		ancestors = append(ancestors, owner)
		queue = append(queue, owners[owner]...)
	}
	return ancestors
}

func rootConstruction(root *NativeRootFacts) bool {
	if root == nil || root.Excluded || root.Selected || root.Built || root.Component && !root.Entry {
		return false
	}
	if root.Entry {
		return !slices.ContainsFunc(root.Claims, func(claim manifest.KSClaim) bool {
			return strings.HasPrefix(root.Path+"/", claim.Prefix)
		})
	}
	return len(root.Owners) == 0
}
