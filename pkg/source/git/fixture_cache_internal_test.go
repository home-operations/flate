package git

import (
	"context"
	"io/fs"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"
	"github.com/go-git/go-git/v6/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type countedFixtureLoader struct {
	fixture *testfixture.Fixture
	calls   atomic.Int64
}

//nolint:ireturn // go-git's loader contract requires storer.Storer.
func (l *countedFixtureLoader) Load(endpoint *url.URL) (storage.Storer, error) {
	l.calls.Add(1)
	return l.fixture.Load(endpoint)
}

func TestFetcher_FixtureConcurrentAndWarmCache(t *testing.T) {
	t.Parallel()
	fixture := testfixture.Materialize(t, "remote-inputs", "git-source")
	loader := &countedFixtureLoader{fixture: fixture}
	server := file.NewTransport(file.Options{Loader: loader})
	fetcher := &Fetcher{
		Network: networktest.New(t),
		Cache:   cachetest.New(t, t.TempDir(), sourcecache.New),
		Depth:   0,
		GitTransports: Transports{"git": func(ctx context.Context, req TransportRequest) (Connection, error) {
			return server.Connect(
				ctx,
				&transport.Request{URL: req.URL, Command: req.Command, Protocol: protocol.Version(req.Protocol)},
			)
		}},
	}
	repo := &manifest.GitRepository{
		Name: "remote", Namespace: "apps", URL: "git://remote.fixture.invalid/source",
		Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
	}
	var artifacts [2]*store.SourceArtifact
	var errors [2]error
	var workers sync.WaitGroup
	for i := range artifacts {
		workers.Go(func() {
			artifacts[i], errors[i] = fetchtest.Fetch(
				t.Context(),
				fetcher.Fetch,
				repo,
				(testInputs{}).readSet(repo),
				&sourcecache.Policy{MaxAge: sourcecache.Forever},
				nil,
			)
		})
	}
	workers.Wait()
	for _, err := range errors {
		assert.NoError(t, err)
	}
	assert.Equal(t, loader.calls.Load(), int64(1))
	assert.FatalIf(t, artifacts[0].Key == "", "source cache supplied no identity")
	assert.Equal(t, artifacts[0].Key, artifacts[1].Key)
	warm, err := fetchtest.Fetch(
		t.Context(),
		fetcher.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, loader.calls.Load(), int64(1))
	assert.Equal(t, warm.Key, artifacts[0].Key)
	assert.Equal(t, warm.Revision, fixture.Resolve(t, "@@commit:remote@@", ""))
	data, err := fs.ReadFile(warm.FS, "app/cm.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), fixture.Read(t, "remote/app/cm.yaml", ""))
}
