package selection

import (
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/store"
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
			result.Independent = result.Admitted
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
	result.InitialAdmitted = facts.StaticRecord != nil && initialAdmitted
	result.InitialIndependent = facts.StaticRecord != nil && initialIndependent
	if result.Occurrence.Declaration == (OccurrenceID{}) && !facts.Needed && !facts.Supplied &&
		!facts.Scope.NativeEntry {
		result.Entry = false
	}
	result.InitialEntry = facts.Scope.NativeEntry ||
		result.InitialIndependent && initialOccurrence != (OccurrenceID{})
	result.Construction = rootConstruction(facts.Native)
	return result
}

func publicRootOwner(facts *RootEvidence, occurrence OccurrenceID) manifest.NamedResource {
	id, record := facts.Identity, &facts.Record
	native := record.Parent.ReportHidden() && record.Parent == record.BuilderOwner && facts.Scope.Prepared
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
		if native && owner == record.BuilderOwner {
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

func coveringRoot(facts *RootEvidence, candidate *RootCoveringOwner) bool {
	if candidate.Object == nil || candidate.Input == nil || candidate.Path == "" ||
		!slices.ContainsFunc(candidate.Productions, func(p manifest.Production) bool {
			owner := p.Materializer(candidate.Identity)
			return owner != (manifest.NamedResource{}) && owner != candidate.Identity
		}) {
		return false
	}
	producer, produces := candidate.Object.(interface {
		EmissionPolicy() manifest.EmissionPolicy
	})
	if !produces || !producer.EmissionPolicy().MatchesNamespace(facts.Identity) {
		return false
	}
	return rootCoversPath(facts, candidate)
}

func rootCoversPath(facts *RootEvidence, candidate *RootCoveringOwner) bool {
	if candidate.Input == nil || facts.Record.Path == "" {
		return false
	}
	source := facts.DeclarationSource
	if source == "" {
		source = facts.Occurrence.Declaration.Source
	}
	if source == "" {
		source = facts.Sources[0].Origin
	}
	if source != candidate.Input.LocalPath && source != candidate.Root {
		return false
	}
	file := candidate.Input.OriginPath(facts.Record.Path)
	return containsRoot(candidate.Root, candidate.Path) && containsRoot(candidate.Path, file)
}

func containsRoot(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func publicRootPeer(facts *RootEvidence, owner manifest.NamedResource) bool {
	id := facts.Identity
	if id.KindInfo().Parent && rootAncestor(facts, owner) {
		return true
	}
	if !id.KindInfo().Structural || !facts.Scope.Application || !facts.Scope.LocalApplication {
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
	if !facts.Incoming && slices.ContainsFunc(candidate.Output.Children, func(child store.Child) bool {
		return manifest.Equal(child.Obj, facts.Declaration)
	}) {
		return true
	}
	if !candidate.Replaced {
		return false
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
		evidence.Admitted = true
		evidence.Owner = manifest.NamedResource{}
	} else if !evidence.Entry {
		evidence.Occurrence = OccurrenceFacts{}
	}
}
