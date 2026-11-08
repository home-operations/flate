package discovery

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func configureSelection(tb testing.TB, l *loader.Loader, request selection.Request) {
	tb.Helper()
	session, err := selection.New(l.Store, l, request)
	if err != nil {
		tb.Fatal(err)
	}
	l.Options.Session = session
}

func prepareSelection(
	ctx context.Context,
	tb testing.TB,
	l *loader.Loader,
	request selection.PrepareRequest, //nolint:gocritic // The test driver owns each acquisition request snapshot.
) (*selection.Admission, error) {
	tb.Helper()
	return admissiontest.Prepare(ctx, tb, l, request)
}

func commitSelection(
	ctx context.Context,
	l *loader.Loader,
	proposal *selection.Admission,
	stage selection.CommitStage,
) (bool, error) {
	_, committed, err := l.Options.Session.Commit(ctx, proposal, stage)
	return committed, err
}

func selectedNames(l *loader.Loader) []manifest.NamedResource {
	var ids []manifest.NamedResource
	for id := range l.Index.Records {
		if l.Options.Session.Snapshot().Output(id) {
			ids = append(ids, id)
		}
	}
	for _, object := range l.Store.Objects() {
		id := object.Named()
		if l.Options.Session.Snapshot().Output(id) && !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

func rootAtForTest(l *loader.Loader, object manifest.BaseManifest, file string) selection.RootEvidence {
	if object == nil {
		return selection.RootEvidence{}
	}
	record, known := l.Index.Lookup(object.Named())
	if !known || record.Path != file {
		return selection.RootEvidence{}
	}
	return l.RootEvidence(object.Named(), nil)
}

func qualifyForTest(t *testing.T, l *loader.Loader, ids ...manifest.NamedResource) {
	t.Helper()
	if l.Options.Session == nil {
		return
	}
	reads := new(store.ReadSet)
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	t.Cleanup(reads.Retire)
	for _, id := range ids {
		reads.RequireQualification(id)
	}
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Reads: reads})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := commitSelection(t.Context(), l, proposal, selection.Construction)
	if err != nil || !committed {
		t.Fatalf("qualification membership: committed=%v err=%v", committed, err)
	}
}
