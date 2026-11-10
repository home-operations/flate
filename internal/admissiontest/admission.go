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
	_, committed, err := session.Admit(tb.Context(), selection.AdmissionOptions{
		Request: selection.PrepareRequest{Reads: &reads, Missing: depwait.NewMissingPolicy(nil)},
		Stage:   selection.Execution,
	})
	if err != nil {
		tb.Fatal(err)
	}
	if !committed {
		tb.Fatal("fixture admission did not commit")
	}
}

// Commit MUST retire proposal payloads before the session owns publication retries.
func Commit(tb testing.TB, session *selection.Session, proposal *selection.Admission) {
	tb.Helper()
	if session == nil {
		return
	}
	request := selection.PrepareRequest{Invocation: true, Missing: depwait.NewMissingPolicy(nil)}
	if proposal != nil {
		request = proposal.Request
		proposal.Retire()
	}
	_, committed, err := session.Admit(tb.Context(), selection.AdmissionOptions{
		Request: request,
		Stage:   selection.Execution,
	})
	if err != nil {
		tb.Fatal(err)
	}
	if !committed {
		tb.Fatal("fixture admission did not commit")
	}
}
