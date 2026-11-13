package orchestrator

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func selectionIdentitiesForTest(st *store.Store, index *manifest.DiscoveryIndex) []manifest.NamedResource {
	seen := map[manifest.NamedResource]bool{}
	var identities []manifest.NamedResource
	add := func(id manifest.NamedResource) {
		if id == (manifest.NamedResource{}) || seen[id] {
			return
		}
		seen[id] = true
		identities = append(identities, id)
	}
	add(manifest.BootstrapSourceID)
	for _, object := range st.Objects() {
		add(object.Named())
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if ref.Name != "" {
				add(ref.NamedResource)
			}
		}
	}
	addSelectionIndexIdentitiesForTest(index, add)
	//nolint:intrange // Required inputs MUST extend the identity worklist.
	for cursor := 0; cursor < len(identities); cursor++ {
		id := identities[cursor]
		for required := range st.RequiredInputs(id) {
			add(required)
		}
		productions := st.Productions(id, index)
		for i := range productions {
			production := &productions[i]
			add(production.Owner)
			add(production.Declaration)
			add(production.Source)
		}
	}
	return identities
}

func addSelectionIndexIdentitiesForTest(index *manifest.DiscoveryIndex, add func(manifest.NamedResource)) {
	if index != nil {
		for id := range index.Records {
			record := index.Records[id]
			add(id)
			add(record.Parent)
			add(record.BuilderOwner)
			for _, ref := range record.Refs {
				add(ref)
			}
			for i := range record.Productions {
				production := &record.Productions[i]
				add(production.Owner)
				add(production.Declaration)
				add(production.Source)
			}
		}
	}
}

func selectionNamesForTest(session *selection.Session, st *store.Store, index *manifest.DiscoveryIndex) []string {
	if session == nil {
		return nil
	}
	var names []string
	decision := session.Snapshot()
	for _, id := range selectionIdentitiesForTest(st, index) {
		if decision.Reconcile(id) {
			names = append(names, id.String())
		}
	}
	slices.Sort(names)
	return names
}

func selectionNamespacesForTest(
	session *selection.Session,
	st *store.Store,
	index *manifest.DiscoveryIndex,
) map[string]struct{} {
	if session.Snapshot().Output(manifest.NamedResource{}) {
		return nil
	}
	var namespaces map[string]struct{}
	decision := session.Snapshot()
	for _, id := range selectionIdentitiesForTest(st, index) {
		if decision.Output(id) && !id.ReportHidden() && id.Namespace != "" {
			if namespaces == nil {
				namespaces = map[string]struct{}{}
			}
			namespaces[id.Namespace] = struct{}{}
		}
	}
	return namespaces
}

func prepareSelectionForTest(
	t *testing.T,
	run *Rendered,
	needed, selected []manifest.NamedResource,
) bool {
	t.Helper()
	if run.loader == nil {
		run.loader = loader.New(run.store)
	}
	run.loader.Index = run.index
	changes := run.cfg.ExternalChanges
	if len(selected) == 0 && changes == nil {
		changes = change.NewSet(nil)
	}
	session, err := selection.New(run.store, run.loader, selection.Request{
		Roots: selected, Changed: changes != nil,
	})
	assert.NoError(t, err)
	run.selection, run.loader.Options.Session = session, session
	t.Cleanup(func() { run.admissionPreparation.Retire() })
	var reads store.ReadSet
	reads.Init(run.store, manifest.BootstrapSourceID, nil)
	defer reads.Retire()
	for _, id := range needed {
		reads.RequireData(id)
	}
	_, committed, err := run.prepareAdmission(t.Context(), selection.AdmissionOptions{
		Request: selection.PrepareRequest{Invocation: true, Reads: &reads, Preparation: &run.admissionPreparation},
		Stage:   selection.Execution,
	})
	assert.NoError(t, err)
	run.Result.catalog = run.loader.DetachCatalog()
	return !committed
}

//nolint:gocritic // The fixture MUST match the SDK configuration value boundary.
func renderTreesForTest(ctx context.Context, tb testing.TB, trees []Tree, cfg Config) ([]Rendered, error) {
	tb.Helper()
	runs, err := RenderTrees(ctx, trees, cfg)
	for _, run := range runs {
		tb.Cleanup(run.Result.Close)
	}
	return runs, err
}
