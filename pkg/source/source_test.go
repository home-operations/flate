package source

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// stubTypedFetcher records its Fetch calls so the Wrap test can pin
// the typed-dispatch path.
type stubTypedFetcher struct {
	called *manifest.GitRepository
}

func (s *stubTypedFetcher) Fetch(_ context.Context, obj *manifest.GitRepository) (*store.SourceArtifact, error) {
	s.called = obj
	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: obj.URL}, nil
}

// TestWrap_DispatchesTypedPayload pins the happy path: Wrap turns a
// TypedFetcher into a Fetcher whose Fetch routes a matching payload
// through the typed inner function.
func TestWrap_DispatchesTypedPayload(t *testing.T) {
	inner := &stubTypedFetcher{}
	f := Wrap(manifest.KindGitRepository, inner)
	repo := &manifest.GitRepository{Name: "r", Namespace: "ns"}
	repo.URL = "https://example.com/repo.git"

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if inner.called != repo {
		t.Errorf("Wrap did not pass typed payload through")
	}
	if art.URL != repo.URL {
		t.Errorf("art.URL = %q, want %q", art.URL, repo.URL)
	}
}

// TestWrap_MismatchedPayloadReturnsInputError pins the single
// type-assertion site: a wrong-kind payload returns a wrapped
// ErrInput naming the responsible kind. Previously every concrete
// fetcher's Fetch opened with its own assertion — four copies of
// the same boilerplate, each a potential panic site if anyone
// forgot the `, ok` check.
func TestWrap_MismatchedPayloadReturnsInputError(t *testing.T) {
	inner := &stubTypedFetcher{}
	f := Wrap(manifest.KindGitRepository, inner)
	// An OCIRepository payload going through a Git-typed wrapper.
	wrong := &manifest.OCIRepository{Name: "o", Namespace: "ns"}

	_, err := f.Fetch(context.Background(), wrong)
	if err == nil {
		t.Fatal("expected ErrInput on mismatched payload")
	}
	if !errors.Is(err, manifest.ErrInput) {
		t.Errorf("expected wrapped ErrInput; got %v", err)
	}
	if inner.called != nil {
		t.Errorf("inner Fetch was invoked on mismatched payload")
	}
}
