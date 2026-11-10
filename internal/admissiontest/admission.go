// Package admissiontest prepares execution membership for controller fixtures.
package admissiontest

import (
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

// Config describes a fixture's declaration inventory and changed inputs.
type Config struct {
	Changes   *change.Set
	Request   selection.Request
	Index     *manifest.DiscoveryIndex
	Inventory interface {
		Objects() []manifest.BaseManifest
	}
	Root string
}

// Decision requires an executed closure before membership queries.
//
//nolint:gocritic // Fixture setup MUST snapshot the requested inventory and selection.
func Decision(tb testing.TB, cfg Config) *selection.Decision {
	tb.Helper()
	l, session := New(tb, cfg)
	Execute(tb, l, nil)
	return session.Snapshot()
}

// New MUST bind each fixture loader and session to the same store.
//
//nolint:gocritic // Fixture setup MUST snapshot the requested inventory and selection.
func New(tb testing.TB, cfg Config) (*loader.Loader, *selection.Session) {
	tb.Helper()
	st, ok := cfg.Inventory.(*store.Store)
	if !ok {
		st = store.New()
		if cfg.Inventory != nil {
			st.AddObjects(cfg.Inventory.Objects())
		}
	}
	root := cfg.Root
	if root == "" {
		if input, ok := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok {
			root = input.LocalPath
		}
	}
	l := loader.New(st)
	l.SourceRoot, l.Options.Changes = root, cfg.Changes
	if cfg.Index != nil {
		l.Index = cfg.Index
	}
	request := cfg.Request
	request.Changed = request.Changed || cfg.Changes != nil
	session, err := selection.New(st, l, request)
	if err != nil {
		tb.Fatal(err)
	}
	l.Options.Session = session
	return l, session
}

// Require MUST capture qualification separately from ordinary input obligations.
//
//nolint:revive // The fixture requires its test, store, session, identities and obligation mode.
func Require(
	tb testing.TB,
	st *store.Store,
	session *selection.Session,
	ids []manifest.NamedResource,
	qualification bool,
) {
	tb.Helper()
	if session == nil {
		return
	}
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	for _, id := range ids {
		if qualification {
			reads.RequireQualification(id)
		} else {
			reads.RequireData(id)
		}
	}
	proposal, err := session.Prepare(
		tb.Context(),
		selection.PrepareRequest{Reads: &reads, Missing: depwait.NewMissingPolicy(nil)},
	)
	if err != nil {
		tb.Fatal(err)
	}
	Commit(tb, session, proposal)
}

// Commit requires a settled proposal without physical demands.
func Commit(tb testing.TB, session *selection.Session, proposal *selection.Admission) {
	tb.Helper()
	if session == nil {
		return
	}
	var err error
	if proposal == nil {
		proposal, err = session.Prepare(
			tb.Context(),
			selection.PrepareRequest{Invocation: true, Missing: depwait.NewMissingPolicy(nil)},
		)
	}
	if err != nil {
		tb.Fatal(err)
	}
	defer proposal.Retire()
	_, committed, err := session.Commit(tb.Context(), proposal, selection.Execution)
	if err != nil {
		tb.Fatal(err)
	}
	if !committed {
		tb.Fatal("fixture admission did not commit")
	}
}
