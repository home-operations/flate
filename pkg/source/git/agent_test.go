package git

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type agentAuthentication struct {
	transport.Transport
	host, allowed ssh.PublicKey
	authenticated atomic.Int32
	afterAuth     func() error
}

func (a *agentAuthentication) NewUploadPackSession(endpoint *transport.Endpoint, auth transport.AuthMethod) (transport.UploadPackSession, error) {
	keys, ok := auth.(*gitssh.PublicKeysCallback)
	if !ok || keys.User != "git" {
		return nil, transport.ErrAuthenticationRequired
	}
	if err := keys.HostKeyCallback("fixture:22", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22}, a.host); err != nil {
		return nil, err
	}
	signers, err := keys.Callback()
	if err != nil {
		return nil, err
	}
	challenge := []byte("flate acquisition fixture")
	accepted := false
	for _, signer := range signers {
		if !bytes.Equal(signer.PublicKey().Marshal(), a.allowed.Marshal()) {
			continue
		}
		signature, err := signer.Sign(rand.Reader, challenge)
		if err != nil {
			return nil, err
		}
		if err := a.allowed.Verify(challenge, signature); err != nil {
			return nil, err
		}
		accepted = true
		break
	}
	if !accepted {
		return nil, transport.ErrAuthorizationFailed
	}
	a.authenticated.Add(1)
	if a.afterAuth != nil {
		if err := a.afterAuth(); err != nil {
			return nil, err
		}
	}
	local := *endpoint
	local.Protocol = "file"
	local.Path = filepath.Join(local.Path, ".git")
	return a.Transport.NewUploadPackSession(&local, nil)
}

func acquisitionAgent(t *testing.T, identity string) (<-chan struct{}, *atomic.Int32) {
	t.Helper()
	key, err := ssh.ParseRawPrivateKey([]byte(identity))
	assert.NoError(t, err)
	keyring := agent.NewKeyring()
	assert.NoError(t, keyring.Add(agent.AddedKey{PrivateKey: key}))
	// Unix socket paths must fit even with a long TMPDIR and test name.
	t.Chdir(t.TempDir())
	path := "a"
	listener, err := net.Listen("unix", path)
	assert.NoError(t, err)
	t.Setenv("SSH_AUTH_SOCK", path)
	var opened atomic.Int32
	closed := make(chan struct{}, 16)
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			opened.Add(1)
			workers.Go(func() {
				_ = agent.ServeAgent(keyring, conn)
				_ = conn.Close()
				closed <- struct{}{}
			})
		}
	})
	t.Cleanup(func() { _ = listener.Close(); workers.Wait() })
	return closed, &opened
}

func TestFetcher_AmbientAcquisition(t *testing.T) {
	for _, shape := range []string{"accepted", "rejected", "absent", "unverified", "bad ref", "source only", "retry"} {
		t.Run(shape, func(t *testing.T) {
			identity, signer := testSSHIdentity(t)
			_, host := testSSHIdentity(t)
			root := t.TempDir()
			mustInitRepo(t, root)
			tr := &agentAuthentication{Transport: server.DefaultServer, host: host.PublicKey(), allowed: signer.PublicKey()}
			if shape == "retry" {
				tr.afterAuth = func() error {
					if tr.authenticated.Load() == 1 {
						return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
					}
					return nil
				}
			}
			previous := client.Protocols["ssh"]
			client.InstallProtocol("ssh", tr)
			t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
			var closed <-chan struct{}
			var opened *atomic.Int32
			if shape == "absent" {
				t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
			} else {
				closed, opened = acquisitionAgent(t, identity)
			}
			if shape == "rejected" {
				tr.allowed = host.PublicKey()
			}
			if shape == "unverified" {
				testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, signer.PublicKey())+"\n")
			}
			repo := gitRepoWithSecret("agent", "ssh://git@fixture"+root, "auth")
			if shape == "source only" {
				repo.SecretRef = nil
			}
			if shape == "bad ref" {
				repo.Reference = &sourcev1.GitRepositoryRef{Branch: "absent"}
			}
			cacheDir := t.TempDir()
			f := &Fetcher{Cache: cachetest.New(t, cacheDir, sourcecache.New)}
			reads := (testInputs{}).readSet(repo)
			defer reads.Close()
			policy := sourcecache.Policy{MaxAge: sourcecache.Forever}
			if shape == "retry" {
				policy.Retry = sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
			}
			artifact, err := f.Fetch(t.Context(), repo, reads, policy)
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil || artifact != nil {
				t.Fatalf("ambient preparation = %v, %v", artifact, err)
			}
			if (missing.Secret == (manifest.NamedResource{})) != (shape == "source only") {
				t.Fatalf("missing identity = %s", missing.Secret)
			}
			artifact, err = missing.RetryAmbient(t.Context())
			success := shape == "accepted" || shape == "source only" || shape == "retry"
			if !success {
				if artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("failed ambient acquisition = %v, %v; want acquisition error", artifact, err)
				}
			} else {
				assert.NoError(t, err)
				if artifact == nil || artifact.FS == nil || !artifact.UsedLocalSSHAgent {
					t.Fatalf("accepted agent artifact = %+v", artifact)
				}
			}
			if opened != nil {
				attempts := int32(1)
				if shape == "retry" {
					attempts = 2
				}
				for range attempts {
					<-closed
				}
				assert.Equal(t, opened.Load(), attempts)
			}
			if shape == "bad ref" && tr.authenticated.Load() == 0 {
				t.Fatal("bad ref did not reach authenticated acquisition")
			}
			if success {
				assert.NoError(t, f.Cache.Close())
				f.Cache = cachetest.New(t, cacheDir, sourcecache.New)
				before := opened.Load()
				warm, err := missing.RetryAmbient(t.Context())
				assert.NoError(t, err)
				if warm == nil || warm.UsedLocalSSHAgent || opened.Load() != before {
					t.Fatalf("disk reuse asserted fresh authentication: %+v", warm)
				}
				if shape == "accepted" {
					mustCommitFile(t, root, "hello.txt", "refreshed")
					_, err := f.Fetch(t.Context(), repo, reads, sourcecache.Policy{})
					refresh, ok := errors.AsType[*source.MissingSecretError](err)
					if !ok || refresh.RetryAmbient == nil {
						t.Fatalf("refresh preparation = %v", err)
					}
					fresh, err := refresh.RetryAmbient(t.Context())
					assert.NoError(t, err)
					<-closed
					data, err := fs.ReadFile(fresh.FS, "hello.txt")
					assert.NoError(t, err)
					if string(data) != "refreshed" || fresh.Content == warm.Content || fresh.Revision == warm.Revision || opened.Load() != before+1 {
						t.Fatal("refreshed acquisition reused stale content or attempt credentials")
					}
				}
			}
			t.Logf("authenticated sessions=%d; success=%t", tr.authenticated.Load(), success)
		})
	}
}

func TestFetcher_AmbientPreparation(t *testing.T) {
	for _, shape := range []string{"source only", "missing proxy", "invalid proxy", "malformed identity", "malformed hosts", "HTTPS"} {
		t.Run(shape, func(t *testing.T) {
			identity, _ := testSSHIdentity(t)
			_, opened := acquisitionAgent(t, identity)
			repo := gitRepoWithSecret("prepare", "ssh://git@fixture/repo", "auth")
			var auth, proxy *manifest.Secret
			if shape == "source only" {
				repo.SecretRef = nil
			}
			if shape == "HTTPS" {
				repo.URL = "https://fixture/repo"
			}
			if shape == "missing proxy" || shape == "invalid proxy" {
				repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
			}
			if shape == "invalid proxy" {
				proxy = &manifest.Secret{StringData: map[string]any{"address": "http://proxy.invalid/%"}}
			}
			if shape == "malformed identity" {
				auth = &manifest.Secret{StringData: map[string]any{"identity": "invalid private material"}}
			}
			if shape == "malformed hosts" {
				auth = &manifest.Secret{StringData: map[string]any{"identity": identity, "known_hosts": "invalid host material"}}
			}
			reads := (testInputs{secrets: func(_, name string) *manifest.Secret {
				if name == "proxy" {
					return proxy
				}
				return auth
			}}).readSet(repo)
			defer reads.Close()
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
			artifact, err := f.Fetch(t.Context(), repo, reads, sourcecache.Policy{})
			if artifact != nil || err == nil || opened.Load() != 0 {
				t.Fatalf("preparation opened agent or acquired content: artifact=%v error=%v opened=%d", artifact, err, opened.Load())
			}
			missing, unavailable := errors.AsType[*source.MissingSecretError](err)
			if shape == "invalid proxy" || shape == "malformed identity" || shape == "malformed hosts" {
				if !errors.Is(err, manifest.ErrInput) || unavailable {
					t.Fatalf("malformed material = %v", err)
				}
			} else {
				if !unavailable || (missing.RetryAmbient != nil) != (shape == "source only") {
					t.Fatalf("ambient admission = %v", err)
				}
				if shape == "missing proxy" && missing.Secret.Name != "proxy" {
					t.Fatalf("missing identity = %s", missing.Secret)
				}
			}
		})
	}
}

func TestFetcher_StalledAgent(t *testing.T) {
	for _, cause := range []string{"timeout", "cancel"} {
		t.Run(cause, func(t *testing.T) {
			_, key := testSSHIdentity(t)
			_, host := testSSHIdentity(t)
			tr := &agentAuthentication{Transport: server.DefaultServer, host: host.PublicKey(), allowed: key.PublicKey()}
			previous := client.Protocols["ssh"]
			client.InstallProtocol("ssh", tr)
			t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			testutil.WriteFile(t, homeDir, ".ssh/known_hosts", knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n")
			t.Chdir(t.TempDir())
			path := "agent"
			listener, err := net.Listen("unix", path)
			assert.NoError(t, err)
			t.Setenv("SSH_AUTH_SOCK", path)
			accepted := make(chan net.Conn, 1)
			observed, closed := make(chan error, 1), make(chan error, 1)
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				conn, err := listener.Accept()
				if err != nil {
					observed <- err
					return
				}
				accepted <- conn
				defer conn.Close()
				_, err = io.ReadFull(conn, make([]byte, 5))
				observed <- err
				if err != nil {
					return
				}
				_, err = conn.Read(make([]byte, 1))
				closed <- err
			}()
			repo := gitRepoWithSecret("stalled", "ssh://git@fixture/repo", "auth")
			repo.Timeout = &metav1.Duration{Duration: time.Second}
			if cause == "cancel" {
				repo.Timeout.Duration = time.Hour
			}
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
			reads := (testInputs{}).readSet(repo)
			defer reads.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				_ = listener.Close()
				watchdog := time.NewTimer(5 * time.Second)
				defer watchdog.Stop()
				select {
				case conn := <-accepted:
					_ = conn.Close()
				case <-serverDone:
				case <-watchdog.C:
					t.Fatal("stalled-agent listener did not stop")
				}
				for _, worker := range []<-chan struct{}{serverDone, done} {
					select {
					case <-worker:
					case <-watchdog.C:
						t.Fatal("stalled-agent cleanup did not release workers")
					}
				}
			})
			_, err = f.Fetch(ctx, repo, reads, sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 1}})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				close(done)
				t.Fatalf("prepare = %v", err)
			}
			var artifact *store.SourceArtifact
			started := time.Now()
			go func() {
				artifact, err = missing.RetryAmbient(ctx)
				close(done)
			}()
			watchdog := time.NewTimer(5 * time.Second)
			defer watchdog.Stop()
			select {
			case requestErr := <-observed:
				assert.NoError(t, requestErr)
			case <-watchdog.C:
				t.Fatal("agent identity request was not observed")
			}
			if cause == "timeout" && time.Since(started) >= repo.Timeout.Duration {
				t.Fatal("agent request was not observed before the source timeout")
			}
			want := context.DeadlineExceeded
			if cause == "cancel" {
				want = context.Canceled
				cancel()
			}
			select {
			case <-done:
			case <-watchdog.C:
				t.Fatal("accepted unanswered agent request outlived attempt termination")
			}
			if !errors.Is(err, want) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("ambient failure = %v; want %v and ErrFlux", err, want)
			}
			if artifact != nil && (artifact.FS != nil || artifact.Content != "" || artifact.UsedLocalSSHAgent) {
				t.Fatalf("failed attempt published successful content or agent warning: %+v", artifact)
			}
			select {
			case err := <-closed:
				if !errors.Is(err, io.EOF) {
					t.Fatalf("agent peer closure = %v; want EOF", err)
				}
			case <-watchdog.C:
				t.Fatal("attempt did not close the agent peer")
			}
		})
	}
}
