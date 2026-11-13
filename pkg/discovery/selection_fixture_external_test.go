package discovery_test

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/selection"
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

func admitSelection(
	ctx context.Context,
	tb testing.TB,
	l *loader.Loader,
	request selection.PrepareRequest, //nolint:gocritic // The fixture retains its immutable request snapshot.
) (*selection.AdmissionResult, error) {
	tb.Helper()
	if request.Preparation == nil {
		var preparation *selection.Preparation
		request.Preparation = &preparation
		tb.Cleanup(func() { preparation.Retire() })
	}
	request.Changes = l.Options.Changes
	request.Missing = l.Options.Missing
	result, committed, err := l.Options.Session.Admit(ctx, selection.AdmissionOptions{
		Request: request, Stage: selection.Construction,
	})
	if err == nil && !committed {
		tb.Fatal("fixture construction admission was not accepted")
	}
	return &result, err
}

func commitSelection(
	ctx context.Context,
	l *loader.Loader,
	proposal *selection.Admission,
	stage selection.CommitStage,
) (bool, error) {
	request := proposal.Request
	proposal.Retire()
	_, committed, err := l.Options.Session.Admit(ctx, selection.AdmissionOptions{Request: request, Stage: stage})
	return committed, err
}
