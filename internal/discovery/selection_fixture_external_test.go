package discovery_test

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/selection"
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
