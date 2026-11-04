package git

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func localGitSSH(t *testing.T, root string, allowed ssh.PublicKey) (string, *atomic.Int32) {
	t.Helper()
	_, host := testSSHIdentity(t)
	var authenticated atomic.Int32
	config := &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if !bytes.Equal(key.Marshal(), allowed.Marshal()) {
				return nil, transport.ErrAuthorizationFailed
			}
			return nil, nil //nolint:nilnil // A nil value and nil error represent an optional absence or a reusable-cache miss.
		},
	}
	config.AddHostKey(host)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(
		t,
		homeDir,
		".ssh/known_hosts",
		knownhosts.Line([]string{listener.Addr().String()}, host.PublicKey())+"\n",
	)
	endpoint, err := transport.NewEndpoint(filepath.Join(root, ".git"))
	assert.NoError(t, err)
	var workers sync.WaitGroup
	workers.Go(func() {
		acceptGitSSH(t, listener, &testGitSSHServer{
			root: &root, config: &config, endpoint: &endpoint, workers: &workers, authenticated: &authenticated,
		})
	})
	t.Cleanup(func() { _ = listener.Close(); workers.Wait() })
	return "ssh://git@" + listener.Addr().String() + root, &authenticated
}

type testGitSSHServer struct {
	root          *string
	config        **ssh.ServerConfig
	endpoint      **transport.Endpoint
	workers       *sync.WaitGroup
	authenticated *atomic.Int32
}

func acceptGitSSH(t *testing.T, listener net.Listener, fixture *testGitSSHServer) {
	t.Helper()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		fixture.workers.Go(func() { serveGitSSH(t, conn, fixture) })
	}
}

func serveGitSSH(t *testing.T, conn net.Conn, fixture *testGitSSHServer) {
	t.Helper()
	defer conn.Close()
	connection, channels, requests, err := ssh.NewServerConn(conn, *fixture.config)
	if err != nil {
		return
	}
	fixture.authenticated.Add(1)
	defer connection.Close()
	fixture.workers.Go(func() { ssh.DiscardRequests(requests) })
	for incoming := range channels {
		if incoming.ChannelType() != "session" {
			_ = incoming.Reject(ssh.UnknownChannelType, "session required")
			continue
		}
		channel, requests, err := incoming.Accept()
		if err != nil {
			return
		}
		fixture.workers.Go(func() { serveGitSSHSession(t, channel, requests, fixture) })
	}
}

func serveGitSSHSession(t *testing.T, channel ssh.Channel, requests <-chan *ssh.Request, fixture *testGitSSHServer) {
	t.Helper()
	defer channel.Close()
	for {
		request, ok := <-requests
		if !ok {
			return
		}
		var command struct{ Command string }
		if request.Type != "exec" || ssh.Unmarshal(request.Payload, &command) != nil ||
			command.Command != fmt.Sprintf("git-upload-pack '%s'", *fixture.root) {
			_ = request.Reply(false, nil)
			continue
		}
		_ = request.Reply(true, nil)
		break
	}
	session, err := server.DefaultServer.NewUploadPackSession(*fixture.endpoint, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer session.Close()
	refs, err := session.AdvertisedReferencesContext(t.Context())
	if err != nil {
		t.Error(err)
		return
	}
	if encodeErr := refs.Encode(channel); encodeErr != nil {
		t.Error(encodeErr)
		return
	}
	input := packp.NewUploadPackRequest()
	if decodeErr := input.Decode(channel); decodeErr != nil {
		t.Error(decodeErr)
		return
	}
	response, err := session.UploadPack(t.Context(), input)
	if err != nil {
		t.Error(err)
		return
	}
	defer response.Close()
	if err := response.Encode(channel); err != nil {
		t.Error(err)
		return
	}
	_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_LocalAgentSSH(t *testing.T) {
	for _, shape := range []string{"accepted", "rejected", "absent", "explicit rejected"} {
		t.Run(
			shape,
			func(t *testing.T) { testFetcherLocalAgentSSHCase(t, &shape) },
		)
	}
}

func testFetcherLocalAgentSSHCase(t *testing.T, shape *string,
) {
	t.Helper()
	identity, key := testSSHIdentity(t)
	allowed := key.PublicKey()
	if (*shape) == "rejected" || (*shape) == "explicit rejected" {
		_, other := testSSHIdentity(t)
		allowed = other.PublicKey()
	}
	root := t.TempDir()
	mustInitRepo(t, root)
	url, authenticated := localGitSSH(t, root, allowed)
	var closed <-chan struct{}
	var opened *atomic.Int32
	if (*shape) == "absent" {
		t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	} else {
		closed, opened, _ = acquisitionAgent(t, identity)
	}
	repo := gitRepoWithSecret("agent", url, "auth")
	inputs := testInputs{}
	if (*shape) == "explicit rejected" {
		inputs.secrets = func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"identity": identity}}
		}
	}
	reads := inputs.readSet(repo)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	artifact, err := f.Fetch(t.Context(), repo, reads, sourcecache.Policy{})
	if (*shape) == "explicit rejected" {
		if err == nil || errors.Is(err, manifest.ErrMissingSecret) != ((*shape) == "absent") ||
			artifact != nil ||
			opened.Load() != 0 {
			t.Fatalf(
				"explicit rejection used ambient: artifact=%v error=%v opened=%d",
				artifact,
				err,
				opened.Load(),
			)
		}
		return
	}
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "prepare = %v", err)
	artifact, err = missing.RetryAmbient(t.Context())
	if closed != nil {
		<-closed
	}
	if (*shape) == "accepted" {
		assert.NoError(t, err)
		if artifact == nil || authenticated.Load() == 0 {
			t.Fatalf("native acquisition = %+v; authenticated=%d", artifact, authenticated.Load())
		}
	} else if artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) != ((*shape) == "absent") || !errors.Is(err, manifest.ErrFlux) || authenticated.Load() != 0 {
		t.Fatalf("failed native acquisition = %+v, %v; authenticated=%d", artifact, err, authenticated.Load())
	}
}
