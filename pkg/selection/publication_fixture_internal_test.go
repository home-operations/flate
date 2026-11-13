package selection

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type publicationFacts struct {
	index manifest.DiscoveryIndex
}

func (f *publicationFacts) SelectionIndex() *manifest.DiscoveryIndex { return &f.index }
func (f *publicationFacts) Occurrence(manifest.NamedResource) (OccurrenceFacts, bool) {
	return OccurrenceFacts{}, false
}

func (f *publicationFacts) AdmissionCandidate(manifest.NamedResource) store.Child {
	return store.Child{}
}

//nolint:gocritic // The fixture MUST retain the census result signature.
func (f *publicationFacts) ResolveRoot(
	context.Context,
	*store.ReadSet,
	manifest.NamedResource,
	string,
) (string, bool, error) {
	return "", false, nil
}

func (f *publicationFacts) OccurrenceObject(
	context.Context,
	*OccurrenceFacts,
	*store.SourceArtifact,
) (store.Child, error) {
	return store.Child{}, nil
}

//nolint:gocritic // The fixture implements the census method exactly.
func (f *publicationFacts) OccurrenceRoots(manifest.NamedResource) (manifest.NamedResource, []string) {
	return manifest.NamedResource{}, nil
}

func (f *publicationFacts) PreparedObject(manifest.NamedResource, *store.ReadSet) (store.Child, bool) {
	return store.Child{}, false
}

func (f *publicationFacts) RootEvidence(manifest.NamedResource, *store.ReadSet, bool) RootEvidence {
	return RootEvidence{}
}

func (f *publicationFacts) ProducerFacts(manifest.BaseManifest) ProducerFacts { return ProducerFacts{} }

func (f *publicationFacts) SourceFacts(manifest.NamedResource, *store.ReadSet) SourceFacts {
	return SourceFacts{}
}

func newPublicationSession(t *testing.T) *Session {
	t.Helper()
	s, err := New(store.New(), &publicationFacts{}, Request{Changed: true})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func publicationProposal(s *Session, update *Admission, revision uint64) *Admission {
	proposal := update
	proposal.Revision = revision
	proposal.owned.Init(s.store, manifest.NamedResource{}, nil)
	proposal.reads = &proposal.owned
	proposal.preparation = &Preparation{closure: admissionClosure{session: s, facts: s.facts}}
	proposal.local = proposal.preparation
	return proposal
}

func publishDecision(t *testing.T, s *Session, update *Admission, revision uint64) ([]manifest.NamedResource, bool) {
	t.Helper()
	before := s.Snapshot()
	proposal := publicationProposal(s, update, revision)
	_, committed, err := s.publishProposal(t.Context(), proposal, Execution)
	if err != nil {
		t.Errorf("publication committed=%v err=%v", committed, err)
	}
	retry := !committed
	proposal.Retire()
	var included []manifest.NamedResource
	for _, bucket := range s.Snapshot().buckets {
		for id := range bucket {
			if !before.Reconcile(id) || before.OrdinaryDemand(id) != s.Snapshot().OrdinaryDemand(id) {
				included = append(included, id)
			}
		}
	}
	slices.SortFunc(included, manifest.NamedResource.Compare)
	return included, retry
}

func (s *Session) publishProposal(
	ctx context.Context,
	proposal *Admission,
	stage CommitStage,
) (*Admission, bool, error) {
	if err := ctx.Err(); err != nil {
		proposal.Retire()
		return nil, false, err
	}
	if !s.guardCurrent(proposal) {
		proposal.Retire()
		return nil, false, nil
	}
	next, accepted := s.publishAdmission(ctx, proposal, stage)
	if accepted {
		proposal.accept(stage, next)
	}
	return proposal, accepted, nil
}

func decisionNames(d *Decision) []string {
	var names []string
	for _, bucket := range d.buckets {
		for id := range bucket {
			names = append(names, id.String())
		}
	}
	slices.Sort(names)
	return names
}
