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
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gitrepo"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/tree"
	"github.com/home-operations/flate/pkg/manifest"
)

type stubTypedFetcher struct {
	called   *manifest.GitRepository
	policy   sourcecache.Policy
	reads    *store.ReadSet
	artifact *store.SourceArtifact
}

func (s *stubTypedFetcher) Fetch(
	_ context.Context,
	obj *manifest.GitRepository,
	reads *store.ReadSet,
	policyValue *sourcecache.Policy,
) (PreparedFetch, error) {
	policy := *policyValue
	s.called = obj
	s.reads = reads
	s.policy = policy
	return PreparedFetch{Fetch: func(_ context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
		s.artifact = local
		return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: obj.URL}, nil
	}}, nil
}

func TestWrap_DispatchesTypedPayload(t *testing.T) {
	t.Parallel()
	inner := &stubTypedFetcher{}
	f := Wrap(manifest.KindGitRepository, inner)
	repo := &manifest.GitRepository{Name: "r", Namespace: "ns"}
	repo.URL = "https://example.com/repo.git"

	policy := sourcecache.Policy{MaxAge: time.Hour, Retry: sourcecache.RetryConfig{Attempts: 3, MinWait: time.Second}}
	reads := testReadSet(repo)
	admitted := &store.SourceArtifact{InputAlias: true}
	prepared, err := f.Fetch(t.Context(), repo, reads, &policy)
	assert.NoError(t, err)
	art, err := prepared.Fetch(t.Context(), admitted)
	assert.Equal(t, inner.artifact, admitted)
	assert.FatalIf(t, inner.reads != reads, "%s", "ReadSet was not forwarded")

	if inner.policy.MaxAge != policy.MaxAge || inner.policy.Retry != policy.Retry ||
		inner.policy.Timeout != policy.Timeout ||
		inner.policy.Admit != nil {
		t.Fatalf("policy=%+v, want %+v", inner.policy, &policy)
	}
	assert.FatalIf(t, err != nil, "Fetch: %v", err)
	assert.ErrorIf(t, inner.called != repo, "Wrap did not pass typed payload through")
	if art.URL != repo.URL {
		t.Errorf("art.URL = %q, want %q", art.URL, repo.URL)
	}
}

func TestWrap_MismatchedPayloadReturnsInputError(t *testing.T) {
	t.Parallel()
	inner := &stubTypedFetcher{}
	f := Wrap(manifest.KindGitRepository, inner)
	wrong := &manifest.OCIRepository{Name: "o", Namespace: "ns"}

	_, err := f.Fetch(
		context.Background(),
		wrong,
		testReadSet(wrong), &sourcecache.Policy{},
	)
	assert.FatalIf(t, err == nil, "%s", "expected ErrInput on mismatched payload")

	assert.ErrorIf(t, !errors.Is(err, manifest.ErrInput), "expected wrapped ErrInput; got %v", err)
	assert.ErrorIf(t, inner.called != nil, "inner Fetch was invoked on mismatched payload")
}

func TestOperationView_AcquisitionBoundary(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", sourcecache.New)
	var buf bytes.Buffer
	testutil.WriteTar(t, &buf, map[string]string{"file": "content"}, 0o600)
	data := buf.Bytes()
	acquired, _, _, err := owner.Acquire(
		t.Context(),
		sourcecache.ArchiveKey{Content: "admission"},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, err := w.Write(data)
			return sourcecache.ProducerResult{}, err
		},
	)
	assert.NoError(t, err)
	r, _, _, err := owner.Blob(
		t.Context(),
		sourcecache.BlobKey{Content: "blob"},
		&sourcecache.Policy{MaxAge: 0},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, writeStringErr := io.WriteString(w, "content")
			return sourcecache.ProducerResult{}, writeStringErr
		},
	)
	assert.NoError(t, err)
	blob := sourcecache.NameBlob("file", r)

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
	shaped, pin, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: admitted.FS},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.FatalIf(t, shaped.PolicyEvidence() == nil, "%s", "source view did not retain policy evidence")

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
