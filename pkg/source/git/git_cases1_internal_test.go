package git

import (
	"context"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func testGenerationWaiter(
	t *testing.T,
	disk bool,
	fetch func(context.Context, *Fetcher, string) (*store.SourceArtifact, error),
	want []string,
) {
	t.Helper()
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{
		"apps/keep.yaml": "keep", "apps/drop.tmp": "drop", "other/keep.yaml": "other",
		".hidden/secret.yaml": "hidden", ".sourceignore": "other/\n",
	})
	repository, err := git.PlainOpen(src)
	assert.NoError(t, err)
	head, err := repository.Head()
	assert.NoError(t, err)
	assert.NoError(
		t,
		repository.Storer.SetReference(
			plumbing.NewHashReference(head.Name(), submoduleCommit(t, repository, head.Hash())),
		),
	)
	revision := mustHead(t, src)
	started, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var advertisements atomic.Int32
	handler := gitHTTPHandler(t, src)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && advertisements.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(unblock)
	root := ""
	if disk {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}

	results := make(chan testtestGenerationWaiterResult, 2)
	go func() {
		artifact, fetchErr3 := fetch(t.Context(), f, srv.URL)
		results <- testtestGenerationWaiterResult{artifact, fetchErr3}
	}()
	<-started
	waiter := &acquisitionWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	go func() {
		artifact, fetchErr4 := fetch(waiter, f, srv.URL)
		results <- testtestGenerationWaiterResult{artifact, fetchErr4}
	}()
	<-waiter.waiting
	assert.Equal(t, advertisements.Load(), 1)
	unblock()
	first, second := <-results, <-results
	assert.NoError(t, first.err)
	assert.NoError(t, second.err)
	assert.FatalIf(t, first.artifact.FS == second.artifact.FS, "%s", "waiter reused the producer's view")

	check := func(artifact *store.SourceArtifact) {
		assert.Equal(t, artifact.Revision, revision)
		assert.Equal(t, artifact.Key, first.artifact.Key)
		_, identity, viewErr := source.OperationView(t.Context(), artifact.FS)
		assert.NoError(t, viewErr)
		assert.Equal(t, artifact.Content, identity)
		assert.Equal(t, artifact.Content, first.artifact.Content)
		assert.FatalIf(t, artifact.Content == "", "%s", "missing acquired filesystem identity")
		assert.FatalIf(t, artifact.Key == "", "%s", "missing cache identity")

		if len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule ||
			artifact.Warnings[0].Detail[0] != "module" {
			t.Fatalf("generation warning lost: %+v", artifact.Warnings)
		}
		var files []string
		assert.NoError(t, fs.WalkDir(artifact.FS, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				files = append(files, name)
			}
			return nil
		}))
		assert.Diff(t, files, want)
		assertWorktree(t, artifact.FS, "apps/keep.yaml", "keep")
	}
	check(first.artifact)
	check(second.artifact)
	before := advertisements.Load()
	srv.Close()
	if disk {
		assert.NoError(t, f.Cache.Close())
		f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
	}
	hit, err := fetch(t.Context(), f, srv.URL)
	assert.NoError(t, err)
	assert.Equal(t, advertisements.Load(), before)
	check(hit)
}

func TestGitWarnings_ImmutableCoordinates(t *testing.T) {
	t.Parallel()
	raw := manifest.Warning{
		Category: manifest.WarnSkippedSubmodule,
		Message:  "skipping submodule",
		Detail:   []string{"module", "commit"},
	}
	input := &gittree.FS{Warnings: []manifest.Warning{raw}}
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	warnings := gitWarnings(
		input,
		id,
		"https://credential:password@example.test/repository.git?token=private#private",
		"main",
	)
	assert.Equal(t, warnings[0].Resource, id)
	assert.Diff(t, warnings[0].Detail, []string{"module", "commit", "https://example.test/repository.git", "main"})
	assert.Diff(t, input.Warnings, []manifest.Warning{raw})
}

type testFetcherPackedContentCaseInputs struct {
	files    *map[string]string
	src      *string
	mode     *string
	attempts *int
}

func testFetcherPackedContentCase(t *testing.T, testCaseInput testFetcherPackedContentCaseInputs) {
	t.Helper()

	var acquisitions atomic.Int32
	handler := gitHTTPHandler(t, (*testCaseInput.src))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && acquisitions.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, "001e# service=git-upload-pack\n00000040abc")
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	root := ""
	if (*testCaseInput.mode) == "disk" {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
	art, err := f.Fetch(
		t.Context(),
		&manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: srv.URL},
		(testInputs{}).readSet(
			&manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: srv.URL},
		),
		sourcecache.Policy{
			Retry: sourcecache.RetryConfig{
				Attempts: (*testCaseInput.attempts),
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			},
		},
	)
	if (*testCaseInput.attempts) == 1 {
		if err == nil || art != nil || acquisitions.Load() != 1 {
			t.Fatalf("retry-off artifact=%v, error=%v, acquisitions=%d", art, err, acquisitions.Load())
		}
		return
	}
	if err != nil || acquisitions.Load() != 2 {
		t.Fatalf("original caller error=%v, acquisitions=%d, want 2", err, acquisitions.Load())
	}
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for file, want := range *testCaseInput.files {
				got, err := fs.ReadFile(art.FS, file)
				assert.ErrorIf(
					t,
					err != nil || string(got) != want,
					"packed %s content differs: bytes=%d, err=%v",
					file,
					len(got),
					err,
				)
			}
		})
	}
	readers.Wait()
}

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
	f := &Fetcher{Cache: cachetest.New(t, cacheDir, sourcecache.New), Depth: tc.depth}
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
	first, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
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
		(captures.advertisements.Load()-(*captures.beforeAdvertisements) != 1 || (*captures.uploads).Load()-(*captures.beforeUploads) != 1) {
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
	(*captures.beforeAdvertisements), (*captures.beforeUploads), (*captures.beforeConnections) = captures.advertisements.Load(), (*captures.uploads).Load(), (*captures.connections).Load()
	var hit *store.SourceArtifact

	hit, (*captures.err) = (*captures.f).Fetch(
		t.Context(),
		&cached,
		(testInputs{}).readSet(&cached),
		sourcecache.Policy{MaxAge: cached.SourceInterval()},
	)
	assert.NoError(t, (*captures.err))
	assert.Equal(t, hit.Revision, want)
	assert.Equal(t, hit.Key, (*captures.second).Key)
	if captures.advertisements.Load() != (*captures.beforeAdvertisements) ||
		(*captures.uploads).Load() != (*captures.beforeUploads) ||
		(*captures.connections).
			Load() != (*captures.beforeConnections) {
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
		Cache: cachetest.New(t, (*captures.cacheDir), sourcecache.New),
		Depth: (*captures.tc).depth,
	}
	var reopened *store.SourceArtifact

	reopened, (*captures.err) = (*captures.f).Fetch(
		t.Context(),
		&cached,
		(testInputs{}).readSet(&cached),
		sourcecache.Policy{MaxAge: cached.SourceInterval()},
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
		(*captures.uploads).Load() != (*captures.beforeUploads) ||
		(*captures.connections).
			Load() != (*captures.beforeConnections) {
		t.Error("reopened fresh owner performed network work")
	}
}

func testFetcherReferencesCase(t *testing.T, src *string,
	tc *struct {
		name       string
		ref        *sourcev1.GitRepositoryRef
		depth      int
		sparse     []string
		want       string
		files      []string
		absent     []string
		warmCommit string
		recurse    bool
		wantErr    bool
		errText    string
	},
) {
	t.Helper()

	f := &Fetcher{Cache: cachetest.New(t, t.TempDir(), sourcecache.New), Depth: tc.depth}
	repo := &manifest.GitRepository{
		Name:              "test",
		Namespace:         "flux-system",
		URL:               "file://" + (*src),
		Reference:         tc.ref,
		SparseCheckout:    tc.sparse,
		RecurseSubmodules: tc.recurse,
	}
	if tc.warmCommit != "" {
		unconstrained := *repo
		unconstrained.Reference = &sourcev1.GitRepositoryRef{Commit: tc.warmCommit}
		_, err := f.Fetch(
			t.Context(),
			&unconstrained,
			(testInputs{}).readSet(&unconstrained),
			sourcecache.Policy{MaxAge: unconstrained.SourceInterval()},
		)
		assert.NoError(t, err)
	}
	art, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	if tc.wantErr {
		if err == nil || tc.
			errText != "" && !strings.Contains(err.Error(), tc.errText) {
			t.Fatalf("Fetch error = %v, want failure containing %q", err, tc.errText)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, art.Revision, tc.want)
	for _, file := range tc.files {
		{
			_, err := fs.Stat(art.FS, file)
			assert.ErrorIf(t, err != nil, "included file %s missing: %v", file, err)
		}
	}
	for _, file := range tc.absent {
		{
			_, err := fs.Stat(art.FS, file)
			assert.ErrorIf(t, !os.IsNotExist(err), "excluded file %s present: %v", file, err)
		}
	}
}

type testFetcherSparseCheckoutCaseInputs struct {
	src   *string
	files *map[string]string
	f     **Fetcher
	tc    *struct {
		prefix string
		want   []string
	}
	recursive *bool
}

func testFetcherSparseCheckoutCase(t *testing.T, testCaseInput testFetcherSparseCheckoutCaseInputs) {
	t.Helper()

	spec := &manifest.GitRepository{
		Name: "test", Namespace: "flux-system", URL: "file://" + (*testCaseInput.src),
		SparseCheckout: []string{testCaseInput.tc.prefix}, RecurseSubmodules: (*testCaseInput.recursive),
	}
	before := *spec
	before.SparseCheckout = slices.Clone(spec.SparseCheckout)
	want := make(map[string]string, len(testCaseInput.tc.want))
	for _, file := range testCaseInput.tc.want {
		want[file] = (*testCaseInput.files)[file]
	}
	var previous map[string]string
	for run := range 2 {
		art, err := (*testCaseInput.f).Fetch(
			t.Context(),
			spec,
			(testInputs{}).readSet(spec),
			sourcecache.Policy{MaxAge: time.Hour},
		)
		assert.NoError(t, err)
		got := map[string]string{}
		err = fs.WalkDir(art.FS, ".", func(file string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(art.FS, file)
			got[file] = string(data)
			return err
		})
		assert.NoError(t, err)
		assert.Diff(t, got, want)
		assert.Diff(t, *spec, before)
		if run > 0 {
			assert.Diff(t, got, previous)
		}
		previous = got
	}
}

type testtestGenerationWaiterResult struct {
	artifact *store.SourceArtifact
	err      error
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
	beforeAdvertisements, beforeUploads, beforeConnections := (*scope.advertisements).Load(), (*scope.uploads).Load(), (*scope.connections).Load()
	if (*scope.tc).fail {
		(*scope.srv).Close()
		{
			_, fetchErr := (*scope.f).Fetch(
				t.Context(), (*scope.repo), (testInputs{}).readSet((*scope.repo)),
				sourcecache.Policy{MaxAge: (*scope.repo).SourceInterval()},
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
		second, fetchErr2 := (*scope.f).Fetch(
			t.Context(), (*scope.repo), (testInputs{}).readSet((*scope.repo)),
			sourcecache.Policy{MaxAge: (*scope.tc).interval},
		)
		if (*scope.tc).mixed {
			assert.FatalIf(t, fetchErr2 == nil || second != nil, "%s", "mixed commit unexpectedly retained forever")
		} else {
			assert.NoError(t, fetchErr2)
			assert.Equal(t, second.Revision, (*scope.initial))
		}
		return
	}

	mustCommitFile(t, (*scope.src), "hello.txt", "updated")
	tip := mustCommitFile(t, (*scope.src), "later.txt", "new content")
	assert.FatalIf(t, tip == (*scope.initial), "%s", "tip did not move")

	second, (*scope.err) = (*scope.f).Fetch(
		t.Context(), (*scope.repo), (testInputs{}).readSet((*scope.repo)),
		sourcecache.Policy{MaxAge: (*scope.repo).SourceInterval()},
	)
	assert.NoError(t, (*scope.err))
	t.Logf(
		"refresh: advertisements=%d upload_pack=%d new_connections=%d",
		(*scope.advertisements).Load()-beforeAdvertisements,
		(*scope.uploads).Load()-beforeUploads,
		(*scope.connections).Load()-beforeConnections,
	)
	if (*scope.tc).interval != 0 || (*scope.tc).pinned {
		if (*scope.advertisements).Load() != beforeAdvertisements || (*scope.uploads).Load() != beforeUploads ||
			(*scope.connections).Load() != beforeConnections {
			t.Error("fresh interval or commit-only pin performed network work")
		}
	}
	testFetcherFreshnessCaseCompletion(
		t,
		&testFetcherFreshnessCaseCompletionInputs{
			tc:                   scope.tc,
			advertisements:       &(*scope.advertisements),
			beforeAdvertisements: &beforeAdvertisements,
			uploads:              &(*scope.uploads),
			beforeUploads:        &beforeUploads,
			tip:                  &tip,
			initial:              &(*scope.initial),
			second:               &second,
			first:                scope.first,
			err:                  &(*scope.err),
			repo:                 scope.repo,
			beforeConnections:    &beforeConnections,
			connections:          &(*scope.connections),
			f:                    scope.f,
			cacheDir:             &(*scope.cacheDir),
		},
	)
}
