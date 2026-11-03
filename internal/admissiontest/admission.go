// Package admissiontest prepares execution membership for controller fixtures.
package admissiontest

import (
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Config describes a fixture's declaration inventory and changed inputs.
type Config struct {
	HistoryOwners []manifest.NamedResource
	Changes       *change.Set
	Index         *manifest.DiscoveryIndex
	Inventory     interface {
		Objects() []manifest.BaseManifest
	}
	Root string
}

// Filter prepares and commits a fixture's initial local demand.
func Filter(tb testing.TB, cfg Config) *change.Filter {
	tb.Helper()
	if cfg.Changes == nil {
		return nil
	}
	l := New(tb, cfg)
	Commit(tb, l, nil)
	return l.Options.Filter
}

// New constructs a fixture Loader without committing startup membership.
func New(tb testing.TB, cfg Config) *loader.Loader {
	tb.Helper()
	changes, index, inventory, root := cfg.Changes, cfg.Index, cfg.Inventory, cfg.Root
	st, ok := inventory.(*store.Store)
	if !ok {
		st = store.New()
		if inventory != nil {
			st.AddObjects(inventory.Objects())
		}
	}
	if root == "" {
		if input, ok := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok {
			root = input.LocalPath
		}
	}
	l := loader.New(st)
	l.SourceRoot, l.Options.Changes = root, changes
	l.Options.HistoryOwners = cfg.HistoryOwners
	if index != nil {
		l.Index = index
	}
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{Changed: true}
	return l
}

// Commit publishes a prepared fixture's startup roles.
func Commit(tb testing.TB, l *loader.Loader, proposal *loader.Admission) {
	tb.Helper()
	var err error
	if proposal == nil {
		proposal, err = l.Prepare(tb.Context(), store.Child{}, nil, nil)
	}
	if err != nil {
		tb.Fatal(err)
	}
	_, retry := l.Options.Filter.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
	if retry {
		tb.Fatal("fixture membership changed during initial admission")
	}
}
