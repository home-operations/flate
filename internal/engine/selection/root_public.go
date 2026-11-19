package selection

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Root decides membership and qualification from one immutable factual snapshot.
//
//nolint:gocritic // The public predicate owns an independent evidence snapshot for current support enrichment.
func Root(facts RootEvidence) RootResult {
	record := facts.Record
	if facts.StaticRecord != nil {
		facts.Record = *facts.StaticRecord
	}
	result := classifyRoot(&facts)
	facts.Record = record
	initialAdmitted, initialIndependent := result.Admitted, result.Independent
	initialOccurrence := result.Occurrence.Declaration
	if facts.Captured {
		if result.Occurrence.Declaration == (OccurrenceID{}) {
			result.Independent = result.Admitted || preparedCandidate(&facts)
		}
		needsOwners := !facts.Stale && result.Independent && !facts.Supplied && !facts.Scope.NativeEntry
		var owner manifest.NamedResource
		accepted := false
		if needsOwners {
			owner = publicRootOwner(&facts, result.Occurrence.Declaration)
			accepted = acceptedRootEntry(&facts)
		}
		supportRoot(&facts, &result, owner, accepted)
		result.NeedsOwnerSupport = needsOwners
	}
	initialLocated := facts.StaticRecord != nil && initialOccurrence != (OccurrenceID{})
	result.InitialAdmitted = initialLocated && initialAdmitted
	result.InitialIndependent = initialLocated && initialIndependent
	preparedAuthored := facts.Scope.Prepared && facts.Record.Authored != (manifest.NamedResource{})
	runtimeRoot := facts.Needed && !preparedAuthored || localEntryApplication(&facts.Scope)
	if result.Occurrence.Declaration == (OccurrenceID{}) && !runtimeRoot && !facts.Supplied &&
		!facts.Scope.NativeEntry {
		result.Entry = false
	}
	result.InitialEntry = facts.Scope.NativeEntry || result.InitialIndependent
	result.Construction = rootConstruction(facts.Native)
	return result
}

func publicRootOwner(facts *RootEvidence, occurrence OccurrenceID) manifest.NamedResource {
	id, record := facts.Identity, &facts.Record
	productions := facts.Productions
	if occurrence != (OccurrenceID{}) {
		productions = facts.LiveProductions
	}
	for i := range productions {
		production := &productions[i]
		owner := production.Materializer(id)
		if owner == (manifest.NamedResource{}) && facts.StaticOwner {
			owner = production.Owner
		}
		if preparedRootBuilder(facts, owner) {
			continue
		}
		if owner != (manifest.NamedResource{}) && owner != id && !publicRootPeer(facts, owner) {
			return owner
		}
	}
	if record.Path != "" {
		for i := range facts.CoveringOwners {
			candidate := &facts.CoveringOwners[i]
			if candidate.Identity != id && coveringRoot(facts, candidate) &&
				!publicRootPeer(facts, candidate.Identity) {
				return candidate.Identity
			}
		}
	}
	return manifest.NamedResource{}
}

func preparedRootBuilder(facts *RootEvidence, owner manifest.NamedResource) bool {
	return preparedNativeBuilder(facts) && owner == facts.Record.BuilderOwner
}

func preparedNativeBuilder(facts *RootEvidence) bool {
	return facts.Scope.Prepared && facts.Record.Parent.ReportHidden() &&
		facts.Record.Parent == facts.Record.BuilderOwner
}

func coveringRoot(facts *RootEvidence, candidate *RootCoveringOwner) bool {
	if candidate.Object == nil || candidate.Input == nil || candidate.Path == "" ||
		!slices.ContainsFunc(candidate.Productions, func(p manifest.Production) bool {
			owner := p.Materializer(candidate.Identity)
			return owner != (manifest.NamedResource{}) && owner != candidate.Identity
		}) {
		return false
	}
	if !manifest.KindOf(candidate.Object).Parent ||
		!candidate.Object.EmissionPolicy().MatchesNamespace(facts.Identity) {
		return false
	}
	return candidate.covering
}

func publicRootPeer(facts *RootEvidence, owner manifest.NamedResource) bool {
	id := facts.Identity
	if id.KindInfo().Parent && rootAncestor(facts, owner) {
		return true
	}
	if !id.KindInfo().Structural || !facts.Scope.Application || !facts.Scope.LocalApplication ||
		facts.Scope.Claim == "" {
		return false
	}
	group := facts.Scope.Claims[facts.Scope.Claim]
	return slices.Contains(group, owner) && slices.ContainsFunc(group, func(peer manifest.NamedResource) bool {
		return peer == id || selfClaim(peer, facts.Record.Authored, facts.Scope.Records)
	})
}

func rootAncestor(facts *RootEvidence, owner manifest.NamedResource) bool {
	pending := []manifest.NamedResource{owner}
	for cursor := 0; cursor < len(pending); cursor++ {
		for i := range facts.CoveringOwners {
			candidate := &facts.CoveringOwners[i]
			if candidate.Identity != pending[cursor] {
				continue
			}
			for j := range candidate.Productions {
				ancestor := candidate.Productions[j].Materializer(candidate.Identity)
				if ancestor == facts.Identity {
					return true
				}
				if ancestor != (manifest.NamedResource{}) && !slices.Contains(pending, ancestor) {
					pending = append(pending, ancestor)
				}
			}
		}
	}
	return false
}

func acceptedRootEntry(facts *RootEvidence) bool {
	if facts.Occurrence.Declaration == (OccurrenceID{}) || facts.Declaration == nil {
		return false
	}
	for i := range facts.CoveringOwners {
		candidate := &facts.CoveringOwners[i]
		output := candidate.Output
		if output == nil || !output.Complete || len(output.Failures) != 0 || !output.OutputReads.Valid() {
			continue
		}
		if !slices.ContainsFunc(facts.Productions, func(production manifest.Production) bool {
			return production.Owner == candidate.Identity && candidate.Identity != facts.Identity
		}) {
			continue
		}
		if acceptedRootApplication(facts, candidate) {
			return true
		}
	}
	return false
}

func acceptedRootApplication(facts *RootEvidence, candidate *RootCoveringOwner) bool {
	if !candidate.Replaced {
		return !facts.Incoming && slices.ContainsFunc(candidate.Output.Children, func(child store.Child) bool {
			return manifest.Equal(child.Obj, facts.Declaration)
		})
	}
	application := candidate.Applications[facts.Identity].Application
	if application == "" {
		return false
	}
	parsed, valid := parseApplication(application)
	return valid && manifest.Equal(parsed, facts.Declaration)
}

func supportRoot(facts *RootEvidence, evidence *RootResult, owner manifest.NamedResource, accepted bool) {
	if facts.Stale {
		*evidence = RootResult{}
		return
	}
	if facts.Supplied {
		*evidence = RootResult{Admitted: true, Independent: true, Entry: true}
		return
	}
	if !evidence.Independent {
		return
	}
	supported := evidence.Occurrence.Declaration == (OccurrenceID{}) && !facts.Scope.NativeEntry
	open := supported
	for _, source := range facts.Sources {
		open = open || source.Allowed
		if source.Allowed && source.Available {
			supported = true
			break
		}
	}
	evidence.Admitted = evidence.Admitted && open
	if owner != (manifest.NamedResource{}) {
		evidence.Owner = owner
		evidence.Admitted = false
		supported = false
	}
	evidence.Independent = supported
	evidence.Entry = supported || accepted
	if supported {
		evidence.Admitted = evidence.Admitted || !preparedCandidate(facts) || facts.Needed
		evidence.Owner = manifest.NamedResource{}
	} else if !evidence.Entry {
		evidence.Occurrence = OccurrenceFacts{}
	}
}

func preparedCandidate(facts *RootEvidence) bool {
	record := &facts.Record
	return preparedNativeBuilder(facts) && facts.Scope.Candidate && record.Path == "" &&
		record.Authored != (manifest.NamedResource{})
}
