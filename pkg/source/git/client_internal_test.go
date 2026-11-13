package git

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
	"github.com/home-operations/flate/pkg/store"
)

func TestDefaultClientIsBounded(t *testing.T) {
	t.Parallel()
	network := networktest.New(t)
	c, err := NewClient(ClientConfig{Network: network})
	assert.NoError(t, err)
	assert.FatalIf(t, c.network != network || len(c.options) == 0, "default client did not retain invocation network")
	tr := source.NewHTTPTransport(source.TransportMaterial{})
	defer tr.CloseIdleConnections()
	assert.FatalIf(t, tr.ResponseHeaderTimeout != 0, "transport overrides operation deadline")
}

//nolint:paralleltest // The test changes process egress policy.
func TestDefaultClient_PerCallControls(t *testing.T) {
	for _, name := range []string{"ca-cancel", "insecure-cancel", "restricted"} {
		t.Run(name, func(t *testing.T) {
			testDefaultClientPerCallControlsCase(t, &name)
		},
		)
	}
}

func testDefaultClientPerCallControlsCase(t *testing.T, name *string,
) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	}))
	defer srv.Close()
	defer close(release)
	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{Name: "origin", URLs: []string{srv.URL}})
	network := networktest.New(t)
	material, err := source.ResolveTransport(func(_, _ string) store.ReferenceFacts {
		secret := &manifest.Secret{
			StringData: map[string]any{
				"ca.crt": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})),
			},
		}
		return store.ReferenceFacts{Data: secret, Original: secret}
	}, manifest.DiagnosticIdentity{}, []source.TransportInput{{Reference: &manifest.LocalObjectReference{Name: "tls"}, Use: source.TransportCert}})
	assert.NoError(t, err)
	c, err := NewClient(ClientConfig{Network: network, Material: material})
	assert.NoError(t, err)
	opts := &git.ListOptions{ClientOptions: c.options}
	if *name == "insecure-cancel" {
		tr, transportErr := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
		assert.NoError(t, transportErr)
		opts.ClientOptions = append(opts.ClientOptions, client.WithHTTPClient(&http.Client{Transport: tr}))
	}
	if (*name) == "restricted" {
		ssrfguard.Restrict(true)
		defer ssrfguard.Restrict(false)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, listErr := remote.ListContext(ctx, opts); done <- listErr }()
	select {
	case <-started:
		assert.FatalIf(t, (*name) == "restricted", "%s", "restricted egress allowed a request")

		cancel()
		err = <-done
	case err = <-done:
		assert.FatalIf(t, (*name) != "restricted", "operation returned before request arrival: %v", err)
	}
	switch *name {
	case "restricted":
		dial, ok := errors.AsType[*net.OpError](err)
		assert.FatalIf(t, !ok || dial.Op != "dial", "egress was not blocked at dial: %v", err)
	default:
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "caller cancellation: %v", err)
	}
	ssrfguard.Restrict(false)
	defaultClient, defaultErr := NewClient(ClientConfig{Network: network})
	assert.NoError(t, defaultErr)
	_, defaultErr = remote.ListContext(t.Context(), &git.ListOptions{ClientOptions: defaultClient.options})
	_, unknownCA := errors.AsType[x509.UnknownAuthorityError](defaultErr)
	assert.FatalIf(t, !unknownCA, "per-call TLS changed default trust: %v", defaultErr)
}

func TestClient_InvocationIsolation(t *testing.T) {
	t.Parallel()
	const origin = "git://fixture.invalid/repo"
	var clients [2]*Client
	var owners [2]*sourcecache.Cache
	var expected [2]string
	var roots [2]string
	var calls [2]atomic.Int32
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	var routes [2]Transports
	for i := range clients {
		roots[i] = t.TempDir()
		expected[i] = fmt.Sprintf("invocation-%d", i)
		mustInitRepoWithFiles(t, roots[i], map[string]string{"hello.txt": expected[i]})
		repo, err := git.PlainOpen(roots[i])
		assert.NoError(t, err)
		server := file.NewTransport(
			file.Options{Loader: transport.MapLoader{"/repo": struct{ storage.Storer }{repo.Storer}}},
		)
		routes[i] = Transports{"git": func(ctx context.Context, req TransportRequest) (Connection, error) {
			calls[i].Add(1)
			arrived <- struct{}{}
			<-release
			return server.Connect(
				ctx,
				&transport.Request{URL: req.URL, Command: req.Command, Protocol: protocol.Version(req.Protocol)},
			)
		}}
		clients[i], err = NewClient(ClientConfig{Transports: routes[i], Network: networktest.New(t)})
		assert.NoError(t, err)
		owners[i] = cachetest.New(t, t.TempDir(), sourcecache.New)
	}
	routes[0]["git"], routes[1]["git"] = routes[1]["git"], routes[0]["git"]
	var generations [2]*sourcecache.GitGeneration
	var failures [2]error
	var workers sync.WaitGroup
	for i := range clients {
		workers.Go(func() {
			generations[i], _, failures[i] = owners[i].Git(
				t.Context(),
				sourcecache.GitKey{Content: origin, Ref: "HEAD"},
				&sourcecache.Policy{},
				func(ctx context.Context, st storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
					repo, err := git.CloneContext(
						ctx,
						st,
						worktree,
						&git.CloneOptions{URL: origin, NoCheckout: true, ClientOptions: clients[i].options},
					)
					if err != nil {
						return plumbing.ZeroHash, err
					}
					head, err := repo.Head()
					if err != nil {
						return plumbing.ZeroHash, err
					}
					return head.Hash(), nil
				},
			)
		})
	}
	<-arrived
	<-arrived
	close(release)
	workers.Wait()
	for i, generation := range generations {
		assert.NoError(t, failures[i])
		input, _, err := gittree.New(t.Context(), generation.Backing, generation.Commit, gittree.ViewOptions{})
		assert.NoError(t, err)
		data, err := fs.ReadFile(input, "hello.txt")
		assert.NoError(t, err)
		assert.Equal(t, string(data), expected[i])
		assert.Equal(t, calls[i].Load(), int32(1))
	}
	repo, err := git.CloneContext(
		t.Context(),
		memory.NewStorage(),
		nil,
		&git.CloneOptions{URL: roots[0], NoCheckout: true, ClientOptions: clients[0].options},
	)
	assert.NoError(t, err)
	head, err := repo.Head()
	assert.NoError(t, err)
	assert.Equal(t, head.Hash(), generations[0].Commit)
	assert.Equal(t, calls[0].Load(), int32(1))
}

func TestClient_InvalidTransports(t *testing.T) {
	t.Parallel()
	connect := func(context.Context, TransportRequest) (Connection, error) {
		t.Fatal("invalid map reached a connector")
		return nil, errors.New("invalid connector reached")
	}
	for _, tc := range []struct {
		name           string
		transports     Transports
		missingNetwork bool
	}{
		{name: "empty scheme", transports: Transports{"": connect}},
		{name: "nil connector", transports: Transports{"git": nil}},
		{name: "uppercase scheme", transports: Transports{"Git": connect}},
		{name: "nil network", missingNetwork: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := ClientConfig{Transports: tc.transports}
			if !tc.missingNetwork {
				cfg.Network = networktest.New(t)
			}
			c, err := NewClient(cfg)
			assert.Equal(t, c, nil)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		})
	}
}

func TestClient_FetchCloseErrors(t *testing.T) {
	t.Parallel()
	responseErr, sessionErr := errors.New("response close"), errors.New("session close")
	for _, tc := range []struct {
		name        string
		primary     error
		wantSession bool
	}{
		{name: "primary and session close", primary: responseErr},
		{name: "session close only", wantSession: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := &clientCloseTransport{responseErr: tc.primary, sessionErr: sessionErr}
			c, err := NewClient(ClientConfig{Network: networktest.New(t)})
			assert.NoError(t, err)
			c.options = append(c.options, client.WithTransport("close", fixture))
			err = c.Fetch(t.Context(), memory.NewStorage(), "close://fixture/repository", &git.FetchOptions{
				RefSpecs: []config.RefSpec{"+refs/heads/main:refs/remotes/origin/main"}, Tags: git.NoTags,
			})
			assert.Equal(t, fixture.closed, 1)
			assert.Equal(t, errors.Is(err, responseErr), tc.primary != nil)
			assert.Equal(t, errors.Is(err, sessionErr), tc.wantSession)
		})
	}
}

type clientCloseTransport struct {
	responseErr, sessionErr error
	closed                  int
}

//nolint:ireturn // The upstream transport contract requires a session.
func (f *clientCloseTransport) Handshake(context.Context, *transport.Request) (transport.Session, error) {
	return &clientCloseSession{fixture: f}, nil
}

type clientCloseSession struct {
	transport.Session

	fixture      *clientCloseTransport
	capabilities capability.List
}

func (s *clientCloseSession) Capabilities() *capability.List { return &s.capabilities }

func (*clientCloseSession) GetRemoteRefs(
	context.Context,
	*transport.GetRemoteRefsOptions,
) (*transport.RemoteRefs, error) {
	return transport.NewRemoteRefs([]*plumbing.Reference{
		plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), plumbing.NewHash(strings.Repeat("a", 40))),
	}), nil
}

func (s *clientCloseSession) Fetch(context.Context, storage.Storer, *transport.FetchRequest) error {
	return s.fixture.responseErr
}

func (s *clientCloseSession) Close() error {
	s.fixture.closed++
	return s.fixture.sessionErr
}
