package store

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordEmitted_InvocationSourceWithdrawal(t *testing.T) {
	t.Parallel()
	for _, tc := range []invocationWithdrawalCase{
		{name: "invocation_only"},
		{name: "mixed_source", mixed: true},
		{name: "mixed_with_parent", mixed: true, parent: true},
		{name: "independent_input", mixed: true, independent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRecordEmittedInvocationSourceWithdrawal(t, tc)
		})
	}
}

type invocationWithdrawalCase struct {
	name                       string
	mixed, parent, independent bool
}

func testRecordEmittedInvocationSourceWithdrawal(t *testing.T, tc invocationWithdrawalCase) {
	t.Helper()
	s := New()
	owner := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	seed := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://fixture.invalid/self"}
	replacement := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://fixture.invalid/foreign"}
	s.AddObject(owner)
	s.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Key: "input"})
	var construction ReadSet
	construction.Init(s, manifest.BootstrapSourceID, nil)
	defer construction.Retire()
	construction.Admit(Child{Obj: seed}, manifest.BootstrapSourceID, manifest.NamedResource{})
	productions := []manifest.Production{{Source: manifest.BootstrapSourceID}}
	if tc.mixed {
		productions = append(
			productions,
			manifest.Production{
				Owner:    owner.Named(),
				Source:   manifest.BootstrapSourceID,
				Authored: seed.Named(),
				Path:     "entry/source.yaml",
				File:     "entry/source.yaml",
			},
		)
	}
	if tc.independent {
		productions = append(
			productions,
			manifest.Production{Source: ownerID("independent-input"), Path: "other/source.yaml", File: "source.yaml"},
		)
	}
	record := manifest.DiscoveryRecord{
		Productions: productions,
		Declaration: seed,
		Path:        "entry/source.yaml",
		File:        "entry/source.yaml",
	}
	if tc.parent {
		record.Parent = owner.Named()
	}
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{seed.Named(): record}},
	)
	if s.GetDeclaration(seed.Named()) != seed {
		t.Fatal("invocation did not admit seed")
	}
	var before ReadSet
	before.Init(s, ownerID("reader"), nil)
	defer before.Retire()
	before.GetDeclaration(seed.Named())
	replaced := make(chan struct{})
	withdraw := make(chan struct{})
	done := make(chan struct{})
	go func() {
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true}, nil)
		close(replaced)
		<-withdraw
		s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
		close(done)
	}()
	<-replaced
	if before.Valid() {
		t.Error("replacement did not invalidate captured seed")
	}
	if s.GetDeclaration(seed.Named()) != replacement {
		t.Error("foreign accepted replacement did not win")
	}
	close(withdraw)
	<-done
	got := s.GetDeclaration(seed.Named())
	if tc.independent {
		if got != seed {
			t.Fatal("withdrawal removed a truly independent input")
		}
	} else if got != nil {
		t.Fatalf("withdrawal resurrected invocation seed: %v", got)
	}
}
