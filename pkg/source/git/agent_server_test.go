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
	config := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if !bytes.Equal(key.Marshal(), allowed.Marshal()) {
			return nil, transport.ErrAuthorizationFailed
		}
		return nil, nil
	}}
	config.AddHostKey(host)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{listener.Addr().String()}, host.PublicKey())+"\n")
	endpoint, err := transport.NewEndpoint(filepath.Join(root, ".git"))
	assert.NoError(t, err)
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer conn.Close()
				connection, channels, requests, err := ssh.NewServerConn(conn, config)
				if err != nil {
					return
				}
				authenticated.Add(1)
				defer connection.Close()
				workers.Go(func() { ssh.DiscardRequests(requests) })
				for incoming := range channels {
					if incoming.ChannelType() != "session" {
						_ = incoming.Reject(ssh.UnknownChannelType, "session required")
						continue
					}
					channel, requests, err := incoming.Accept()
					if err != nil {
						return
					}
					workers.Go(func() {
						defer channel.Close()
						for request := range requests {
							var command struct{ Command string }
							if request.Type != "exec" || ssh.Unmarshal(request.Payload, &command) != nil || command.Command != fmt.Sprintf("git-upload-pack '%s'", root) {
								_ = request.Reply(false, nil)
								continue
							}
							_ = request.Reply(true, nil)
							session, err := server.DefaultServer.NewUploadPackSession(endpoint, nil)
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
							if err := refs.Encode(channel); err != nil {
								t.Error(err)
								return
							}
							input := packp.NewUploadPackRequest()
							if err := input.Decode(channel); err != nil {
								t.Error(err)
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
							return
						}
					})
				}
			})
		}
	})
	t.Cleanup(func() { _ = listener.Close(); workers.Wait() })
	return "ssh://git@" + listener.Addr().String() + root, &authenticated
}

func TestFetcher_LocalAgentSSH(t *testing.T) {
	for _, shape := range []string{"accepted", "rejected", "absent", "explicit rejected"} {
		t.Run(shape, func(t *testing.T) {
			identity, key := testSSHIdentity(t)
			allowed := key.PublicKey()
			if shape == "rejected" || shape == "explicit rejected" {
				_, other := testSSHIdentity(t)
				allowed = other.PublicKey()
			}
			root := t.TempDir()
			mustInitRepo(t, root)
			url, authenticated := localGitSSH(t, root, allowed)
			var closed <-chan struct{}
			var opened *atomic.Int32
			if shape == "absent" {
				t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
			} else {
				closed, opened = acquisitionAgent(t, identity)
			}
			repo := gitRepoWithSecret("agent", url, "auth")
			inputs := testInputs{}
			if shape == "explicit rejected" {
				inputs.secrets = func(_, _ string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{"identity": identity}}
				}
			}
			reads := inputs.readSet(repo)
			defer reads.Close()
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
			artifact, err := f.Fetch(t.Context(), repo, reads, sourcecache.Policy{})
			if shape == "explicit rejected" {
				if err == nil || errors.Is(err, manifest.ErrMissingSecret) != (shape == "absent") || artifact != nil || opened.Load() != 0 {
					t.Fatalf("explicit rejection used ambient: artifact=%v error=%v opened=%d", artifact, err, opened.Load())
				}
				return
			}
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				t.Fatalf("prepare = %v", err)
			}
			artifact, err = missing.RetryAmbient(t.Context())
			if closed != nil {
				<-closed
			}
			if shape == "accepted" {
				assert.NoError(t, err)
				if artifact == nil || authenticated.Load() == 0 {
					t.Fatalf("native acquisition = %+v; authenticated=%d", artifact, authenticated.Load())
				}
			} else if artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) != (shape == "absent") || !errors.Is(err, manifest.ErrFlux) || authenticated.Load() != 0 {
				t.Fatalf("failed native acquisition = %+v, %v; authenticated=%d", artifact, err, authenticated.Load())
			}
		})
	}
}
