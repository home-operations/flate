package source

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

// stubTypedFetcher records its Fetch calls so the Wrap test can pin
// the typed-dispatch path.
type stubTypedFetcher struct {
	called *manifest.GitRepository
	policy sourcecache.Policy
}

func (s *stubTypedFetcher) Fetch(_ context.Context, obj *manifest.GitRepository, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	s.called = obj
	s.policy = policy
	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: obj.URL}, nil
}

// TestWrap_DispatchesTypedPayload pins the happy path: Wrap turns a
// TypedFetcher into a Fetcher whose Fetch routes a matching payload
// through the typed inner function.
func TestWrap_DispatchesTypedPayload(t *testing.T) {
	t.Parallel()
	inner := &stubTypedFetcher{}
	f := Wrap(manifest.KindGitRepository, inner)
	repo := &manifest.GitRepository{Name: "r", Namespace: "ns"}
	repo.URL = "https://example.com/repo.git"

	policy := sourcecache.Policy{MaxAge: time.Hour, Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Second}}
	art, err := f.Fetch(context.Background(), repo, policy)
	if inner.policy != policy {
		t.Fatalf("policy=%+v, want %+v", inner.policy, policy)
	}
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
	t.Parallel()
	inner := &stubTypedFetcher{}
	f := Wrap(manifest.KindGitRepository, inner)
	// An OCIRepository payload going through a Git-typed wrapper.
	wrong := &manifest.OCIRepository{Name: "o", Namespace: "ns"}

	_, err := f.Fetch(context.Background(), wrong, sourcecache.Policy{})
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

func TestOperationView_AcquisitionBoundary(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", sourcecache.New)
	var buf bytes.Buffer
	testutil.WriteTar(t, &buf, map[string]string{"file": "content"}, 0o600)
	data := buf.Bytes()
	acquired, _, _, err := owner.Acquire(t.Context(), sourcecache.ArchiveKey{Content: "admission"}, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		_, err := w.Write(data)
		return sourcecache.ProducerResult{}, err
	})
	assert.NoError(t, err)
	r, _, _, err := owner.Blob(t.Context(), sourcecache.BlobKey{Content: "blob"}, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		_, err := io.WriteString(w, "content")
		return sourcecache.ProducerResult{}, err
	})
	assert.NoError(t, err)
	blob := sourcecache.FileFS("file", r)
	assert.NoError(t, r.Close())
	builder := tree.NewBuilder(nil)
	assert.NoError(t, builder.AddFile("file", []byte("content"), 0o600))
	bare, err := builder.Build()
	assert.NoError(t, err)
	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "content")
	testutil.WriteFile(t, root, ".sourceignore", "# policy evidence\n")
	location, err := gitrepo.Locate(t.Context(), root, root)
	assert.NoError(t, err)
	admitted, err := gittree.AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	shaped, pin, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: admitted.FS})
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	if shaped.PolicyEvidence() == nil {
		t.Fatal("source view did not retain policy evidence")
	}
	for _, tc := range []struct {
		name     string
		input    fs.FS
		acquired bool
	}{
		{"archive", acquired, true},
		{"blob", blob, true},
		{"bare tree", bare, false},
		{"anonymous", fstest.MapFS{"file": {Data: []byte("content")}}, false},
		{"opaque wrapper", struct{ fs.FS }{acquired}, false},
		{"mutable admitted", admitted.FS, false},
		{"policy evidence", shaped, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				view, pin, err := OperationView(t.Context(), tc.input)
				assert.NoError(t, err)
				assert.Equal(t, pin != "", tc.acquired)
				data, err := fs.ReadFile(view, "file")
				assert.NoError(t, err)
				assert.Equal(t, string(data), "content")
			}
		})
	}
}
