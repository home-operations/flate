package git

import (
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherFreshnessCase(t *testing.T, tc *struct {
	name     string
	interval time.Duration
	depth    int
	recurse  bool
	fail     bool
	pinned   bool
	commit   bool
	mixed    bool
},
) {
	t.Helper()

	src := t.TempDir()
	mustInitRepo(t, src)
	initial := mustHead(t, src)
	var advertisements, uploads, connections atomic.Int32
	handler := gitHTTPHandler(t, src)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			advertisements.Add(1)
		} else {
			uploads.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	cacheDir := t.TempDir()
	f := &Fetcher{Network: networktest.New(t), Cache: cachetest.New(t, cacheDir, sourcecache.New), Depth: tc.depth}
	repo := &manifest.GitRepository{
		Name:      "test",
		Namespace: "flux-system",
		URL:       srv.URL,
		Interval: metav1.Duration{
			Duration: tc.interval,
		},
		RecurseSubmodules: tc.recurse,
	}
	if tc.pinned || tc.commit {
		repo.Reference = &sourcev1.GitRepositoryRef{Commit: initial}
		if tc.mixed {
			repo.Reference.Tag = "ignored"
		}
	}
	first, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, first.Revision, initial)
	t.Logf(
		"initial: advertisements=%d upload_pack=%d accepted_connections=%d",
		advertisements.Load(),
		uploads.Load(),
		connections.Load(),
	)
	if advertisements.Load() != 1 || uploads.Load() != 1 {
		t.Errorf(
			"initial clone: advertisements=%d upload_pack=%d, want 1 each",
			advertisements.Load(),
			uploads.Load(),
		)
	}
	testFetcherFreshnessCaseTail(
		t,
		&testFetcherFreshnessCaseTailState{
			tc:             &tc,
			src:            &src,
			initial:        &initial,
			advertisements: &advertisements,
			uploads:        &uploads,
			connections:    &connections,
			srv:            &srv,
			cacheDir:       &cacheDir,
			f:              &f,
			repo:           &repo,
			first:          &first,
			err:            &err,
		},
	)
}

func testFetcherFreshnessCaseCompletion(t *testing.T, captures *testFetcherFreshnessCaseCompletionInputs) {
	t.Helper()
	if (*captures.tc).interval == 0 && !(*captures.tc).pinned &&
		(captures.advertisements.Load()-(*captures.beforeAdvertisements) != 1 || captures.uploads.Load()-(*captures.beforeUploads) != 1) {
		t.Errorf(
			"due changed ref: advertisements=%d upload_pack=%d, want 1 each", captures.advertisements.
				Load()-(*captures.beforeAdvertisements), captures.uploads.
				Load()-(*captures.beforeUploads),
		)
	}
	want := (*captures.tip)
	if (*captures.tc).interval != 0 || (*captures.tc).pinned {
		want = (*captures.initial)
	}
	assert.Equal(t, (*captures.second).Revision, want)
	assert.Equal(t, (*captures.second).Key, (*captures.first).Key)
	_, (*captures.err) = fs.Stat((*captures.second).FS, "later.txt")
	if (*captures.tc).interval != 0 || (*captures.tc).pinned {
		assert.FatalIf(t, !os.IsNotExist((*captures.err)), "fresh interval included later commit: %v", (*captures.err))
	} else {
		assert.NoError(t, (*captures.err))
	}
	cached := *(*captures.repo)
	cached.Interval.Duration = time.Hour
	(*captures.beforeAdvertisements), (*captures.beforeUploads), (*captures.beforeConnections) = captures.advertisements.Load(), captures.uploads.Load(), captures.connections.Load()
	var hit *store.SourceArtifact

	hit, (*captures.err) = fetchtest.Fetch(
		t.Context(),
		(*captures.f).Fetch,
		&cached,
		(testInputs{}).readSet(&cached),
		&sourcecache.Policy{MaxAge: cached.SourceInterval()},
		nil,
	)
	assert.NoError(t, (*captures.err))
	assert.Equal(t, hit.Revision, want)
	assert.Equal(t, hit.Key, (*captures.second).Key)
	if captures.advertisements.Load() != (*captures.beforeAdvertisements) ||
		captures.uploads.Load() != (*captures.beforeUploads) ||
		captures.connections.Load() != (*captures.beforeConnections) {
		t.Error("fresh follow-up performed network work")
	}
	for _, art := range []*store.SourceArtifact{(*captures.first), (*captures.second), hit} {
		view, pin, operationViewErr := source.OperationView(t.Context(), art.FS)
		assert.NoError(t, operationViewErr)
		assert.Equal(t, pin != "", true)
		assert.Equal(t, pin == "", false)
		assert.Equal(t, view == art.FS, true)
		data, operationViewErr := fs.ReadFile(view, "hello.txt")
		assert.NoError(t, operationViewErr)
		content := "updated"
		if art == (*captures.first) || (*captures.tc).interval != 0 || (*captures.tc).pinned {
			content = "hi"
		}
		assert.Equal(t, string(data), content)
	}
	assert.NoError(t, (*captures.f).Cache.Close())
	(*captures.f) = &Fetcher{
		Network: networktest.New(t),
		Cache:   cachetest.New(t, (*captures.cacheDir), sourcecache.New),
		Depth:   (*captures.tc).depth,
	}
	var reopened *store.SourceArtifact

	reopened, (*captures.err) = fetchtest.Fetch(
		t.Context(),
		(*captures.f).Fetch,
		&cached,
		(testInputs{}).readSet(&cached),
		&sourcecache.Policy{MaxAge: cached.SourceInterval()},
		nil,
	)
	assert.NoError(t, (*captures.err))
	assert.Equal(t, reopened.Revision, want)
	t.Logf(
		"reopened fresh: advertisements=%d upload_pack=%d new_connections=%d", captures.advertisements.
			Load()-(*captures.beforeAdvertisements), captures.uploads.
			Load()-(*captures.beforeUploads), captures.connections.
			Load()-(*captures.beforeConnections),
	)
	if captures.advertisements.Load() != (*captures.beforeAdvertisements) ||
		captures.uploads.Load() != (*captures.beforeUploads) ||
		captures.connections.Load() != (*captures.beforeConnections) {
		t.Error("reopened fresh owner performed network work")
	}
}

type testFetcherFreshnessCaseCompletionInputs struct {
	tc **struct {
		name     string
		interval time.Duration
		depth    int
		recurse  bool
		fail     bool
		pinned   bool
		commit   bool
		mixed    bool
	}
	advertisements       *atomic.Int32
	beforeAdvertisements *int32
	uploads              *atomic.Int32
	beforeUploads        *int32
	tip                  *string
	initial              *string
	second               **store.SourceArtifact
	first                **store.SourceArtifact
	err                  *error
	repo                 **manifest.GitRepository
	beforeConnections    *int32
	connections          *atomic.Int32
	f                    **Fetcher
	cacheDir             *string
}

type testFetcherFreshnessCaseTailState struct {
	tc **struct {
		name     string
		interval time.Duration
		depth    int
		recurse  bool
		fail     bool
		pinned   bool
		commit   bool
		mixed    bool
	}
	src            *string
	initial        *string
	advertisements *atomic.Int32
	uploads        *atomic.Int32
	connections    *atomic.Int32
	srv            **httptest.Server
	cacheDir       *string
	f              **Fetcher
	repo           **manifest.GitRepository
	first          **store.SourceArtifact
	err            *error
}

func testFetcherFreshnessCaseTail(t *testing.T, scope *testFetcherFreshnessCaseTailState) {
	t.Helper()
	var second *store.SourceArtifact
	beforeAdvertisements, beforeUploads, beforeConnections := scope.advertisements.Load(), scope.uploads.Load(), scope.connections.Load()
	if (*scope.tc).fail {
		(*scope.srv).Close()
		{
			_, fetchErr := fetchtest.Fetch(
				t.Context(),
				(*scope.f).Fetch,
				(*scope.repo),
				(testInputs{}).readSet((*scope.repo)),
				&sourcecache.Policy{MaxAge: (*scope.repo).SourceInterval()},
				nil,
			)
			assert.FatalIf(t, fetchErr == nil, "%s", "expected refresh to fail after upstream shutdown")
		}

		{
			_, statErr3 := fs.Stat((*scope.first).FS, "hello.txt")
			assert.FatalIf(t, statErr3 != nil, "failed refresh removed held generation: %v", statErr3)
		}
		data, readFileErr := fs.ReadFile((*scope.first).FS, "hello.txt")
		assert.NoError(t, readFileErr)
		assert.Equal(t, string(data), "hi")
		return
	}

	if (*scope.tc).commit {
		(*scope.srv).Close()
		assert.NoError(t, os.RemoveAll((*scope.src)))
		committed, fetchErr2 := fetchtest.Fetch(
			t.Context(),
			(*scope.f).Fetch,
			(*scope.repo),
			(testInputs{}).readSet((*scope.repo)),
			&sourcecache.Policy{MaxAge: (*scope.tc).interval},
			nil,
		)
		if (*scope.tc).mixed {
			assert.FatalIf(t, fetchErr2 == nil || committed != nil, "%s", "mixed commit unexpectedly retained forever")
		} else {
			assert.NoError(t, fetchErr2)
			assert.Equal(t, committed.Revision, (*scope.initial))
		}
		return
	}

	mustCommitFile(t, (*scope.src), "hello.txt", "updated")
	tip := mustCommitFile(t, (*scope.src), "later.txt", "new content")
	assert.FatalIf(t, tip == (*scope.initial), "%s", "tip did not move")

	second, (*scope.err) = fetchtest.Fetch(
		t.Context(),
		(*scope.f).Fetch,
		(*scope.repo),
		(testInputs{}).readSet((*scope.repo)),
		&sourcecache.Policy{MaxAge: (*scope.repo).SourceInterval()},
		nil,
	)
	assert.NoError(t, (*scope.err))
	t.Logf(
		"refresh: advertisements=%d upload_pack=%d new_connections=%d",
		scope.advertisements.Load()-beforeAdvertisements,
		scope.uploads.Load()-beforeUploads,
		scope.connections.Load()-beforeConnections,
	)
	if (*scope.tc).interval != 0 || (*scope.tc).pinned {
		if scope.advertisements.Load() != beforeAdvertisements || scope.uploads.Load() != beforeUploads ||
			scope.connections.Load() != beforeConnections {
			t.Error("fresh interval or commit-only pin performed network work")
		}
	}
	testFetcherFreshnessCaseCompletion(
		t,
		&testFetcherFreshnessCaseCompletionInputs{
			tc:                   scope.tc,
			advertisements:       scope.advertisements,
			beforeAdvertisements: &beforeAdvertisements,
			uploads:              scope.uploads,
			beforeUploads:        &beforeUploads,
			tip:                  &tip,
			initial:              scope.initial,
			second:               &second,
			first:                scope.first,
			err:                  scope.err,
			repo:                 scope.repo,
			beforeConnections:    &beforeConnections,
			connections:          scope.connections,
			f:                    scope.f,
			cacheDir:             scope.cacheDir,
		},
	)
}
