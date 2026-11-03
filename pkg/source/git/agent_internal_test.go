package git

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
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

func (a *agentAuthentication) NewUploadPackSession(
	endpoint *transport.Endpoint,
	auth transport.AuthMethod,
) (transport.UploadPackSession, error) {
	keys, ok := auth.(*gitssh.PublicKeysCallback)
	if !ok || keys.User != "git" {
		return nil, transport.ErrAuthenticationRequired
	}
	if err := keys.HostKeyCallback(
		"fixture:22",
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 22},
		a.host,
	); err != nil {
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

func acquisitionAgent(t *testing.T, identity string) (<-chan struct{}, *atomic.Int32, agent.Agent) {
	t.Helper()
	keyring := agent.NewKeyring()
	if identity != "" {
		key, err := ssh.ParseRawPrivateKey([]byte(identity))
		assert.NoError(t, err)
		assert.NoError(t, keyring.Add(agent.AddedKey{PrivateKey: key}))
	}
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
	return closed, &opened, keyring
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_AmbientAcquisition(t *testing.T) {
	for _, shape := range []string{"accepted", "rejected", "absent", "refused", "unset", "empty agent", "provider", "unverified", "bad ref", "source only", "retry", "fallback"} {
		t.Run(shape, func(t *testing.T) {
			testFetcherAmbientAcquisitionCase(t, &shape)
		},
		)
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_AmbientPreparation(t *testing.T) {
	for _, shape := range []string{"source only", "missing proxy", "invalid proxy", "malformed identity", "malformed hosts", "HTTPS"} {
		t.Run(shape, func(t *testing.T) {
			testFetcherAmbientPreparationCase(t, &shape)
		},
		)
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_StalledAgent(t *testing.T) {
	for _, cause := range []string{"timeout", "cancel"} {
		t.Run(
			cause,
			func(t *testing.T) { testFetcherStalledAgentCase(t, &cause) },
		)
	}
}

func testFetcherAmbientAcquisitionCase(t *testing.T, shape *string,
) {
	t.Helper()
	identity, signer := testSSHIdentity(t)
	_, host := testSSHIdentity(t)
	root := t.TempDir()
	mustInitRepo(t, root)
	tr := &agentAuthentication{
		Transport: server.DefaultServer,
		host:      host.PublicKey(),
		allowed:   signer.PublicKey(),
	}
	if (*shape) == "retry" {
		tr.afterAuth = func() error {
			if tr.authenticated.Load() == 1 {
				return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
			}
			return nil
		}
	}

	var diagnostic bytes.Buffer
	if (*shape) == "fallback" {
		previousLogger := slog.Default()
		slog.SetDefault(
			slog.New(slog.NewTextHandler(&diagnostic, &slog.HandlerOptions{Level: slog.LevelDebug})),
		)
		t.Cleanup(func() { slog.SetDefault(previousLogger) })
		tr.afterAuth = func() error {
			if tr.authenticated.Load() == 1 {
				return errors.New("diagnostic-canary")
			}
			return nil
		}
	}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(
		t,
		homeDir,
		".ssh/known_hosts",
		knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n",
	)
	var closed <-chan struct{}
	var opened *atomic.Int32
	var keyring agent.Agent
	switch *shape {
	case "refused":
		t.Chdir(t.TempDir())
		listener, err := net.Listen("unix", "a")
		assert.NoError(t, err)
		listener.(*net.UnixListener).SetUnlinkOnClose(false)
		assert.NoError(t, listener.Close())
		t.Setenv("SSH_AUTH_SOCK", "a")
	case "unset":
		t.Setenv("SSH_AUTH_SOCK", "")
	case "absent":
		t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "absent"))
	default:
		if (*shape) == "empty agent" {
			identity = ""
		}
		closed, opened, keyring = acquisitionAgent(t, identity)
	}
	if (*shape) == "rejected" {
		tr.allowed = host.PublicKey()
	}
	if (*shape) == "unverified" {
		testutil.WriteFile(
			t,
			homeDir,
			".ssh/known_hosts",
			knownhosts.Line([]string{"fixture"}, signer.PublicKey())+"\n",
		)
	}
	repo := gitRepoWithSecret("agent", "ssh://git@fixture"+root, "auth")
	if (*shape) == "source only" {
		repo.SecretRef = nil
	}
	if (*shape) == "provider" {
		repo.Provider = "aws"
	}
	if (*shape) == "fallback" {
		repo.Reference = &sourcev1.GitRepositoryRef{Commit: mustHead(t, root)}
	}
	if (*shape) == "bad ref" {
		repo.Reference = &sourcev1.GitRepositoryRef{Branch: "absent"}
	}
	testFetcherAmbientAcquisitionCaseCompletion(
		t,
		&testFetcherAmbientAcquisitionCaseCompletionInputs{
			repo:       &repo,
			shape:      &shape,
			opened:     &opened,
			closed:     &closed,
			tr:         &tr,
			diagnostic: &diagnostic,
			root:       &root,
			keyring:    &keyring,
		},
	)
}

func testFetcherAmbientAcquisitionCaseCompletion(
	t *testing.T,
	captures *testFetcherAmbientAcquisitionCaseCompletionInputs,
) {
	t.Helper()
	cacheDir := t.TempDir()
	f := &Fetcher{Cache: cachetest.New(t, cacheDir, sourcecache.New)}
	reads := (testInputs{}).readSet((*captures.repo))
	defer reads.Close()
	policy := sourcecache.Policy{MaxAge: sourcecache.Forever}
	if (*(*captures.shape)) == "retry" {
		policy.Retry = sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
	}
	artifact, err := f.Fetch(t.Context(), (*captures.repo), reads, policy)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(
		t,
		!ok || missing.RetryAmbient == nil || artifact != nil,
		"ambient preparation = %v, %v",
		artifact,
		err,
	)
	if (missing.Secret == (manifest.NamedResource{})) != ((*(*captures.shape)) == "source only" || (*(*captures.shape)) == "HTTPS") {
		t.Fatalf("missing identity = %s", missing.Secret)
	}
	artifact, err = missing.RetryAmbient(t.Context())
	success := (*(*captures.shape)) == "fallback" || (*(*captures.shape)) == "accepted" ||
		(*(*captures.shape)) == "source only" ||
		(*(*captures.shape)) == "retry" ||
		(*(*captures.shape)) ==
			"provider"
	if !success {
		assert.FatalIf(t, artifact != nil || err == nil ||
			errors.Is(
				err,
				manifest.ErrMissingSecret,
			) != ((*(*captures.shape)) == "absent" || (*(*captures.shape)) == "refused" || (*(*captures.shape)) == "unset" || (*(*captures.shape)) == "empty agent" || (*(*captures.shape)) == "rejected") ||
			!errors.Is(
				err,
				manifest.ErrFlux,
			), "failed ambient acquisition = %v, %v; want acquisition error", artifact, err)
	} else {
		assert.NoError(t, err)
		assert.FatalIf(t, artifact == nil || artifact.FS == nil, "accepted agent artifact = %+v", artifact)
	}
	testFetcherAmbientAcquisitionCaseCompletionTail(
		t,
		&testFetcherAmbientAcquisitionCaseCompletionTailState{
			captures: &captures,
			cacheDir: &cacheDir,
			f:        &f,
			reads:    &reads,
			artifact: &artifact,
			missing:  &missing,
			success:  &success,
		},
	)
}

func testFetcherAmbientPreparationCase(t *testing.T, shape *string,
) {
	t.Helper()
	identity, _ := testSSHIdentity(t)
	_, opened, _ := acquisitionAgent(t, identity)
	repo := gitRepoWithSecret("prepare", "ssh://git@fixture/repo", "auth")
	var auth, proxy *manifest.Secret
	if (*shape) == "source only" {
		repo.SecretRef = nil
	}
	if (*shape) == "HTTPS" {
		repo.URL = "https://fixture/repo"
	}
	if (*shape) == "missing proxy" || (*shape) == "invalid proxy" {
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	}
	if (*shape) == "invalid proxy" {
		proxy = &manifest.Secret{StringData: map[string]any{"address": "http://proxy.invalid/%"}}
	}
	if (*shape) == "malformed identity" {
		auth = &manifest.Secret{StringData: map[string]any{"identity": "invalid private material"}}
	}
	if (*shape) == "malformed hosts" {
		auth = &manifest.Secret{
			StringData: map[string]any{"identity": identity, "known_hosts": "invalid host material"},
		}
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
		t.Fatalf(
			"preparation opened agent or acquired content: artifact=%v error=%v opened=%d",
			artifact,
			err,
			opened.Load(),
		)
	}
	missing, unavailable := errors.AsType[*source.MissingSecretError](err)
	if (*shape) == "invalid proxy" || (*shape) == "malformed identity" ||
		(*shape) == "malformed hosts" {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || unavailable, "malformed material = %v", err)
	} else {
		assert.FatalIf(
			t,
			!unavailable ||
				(missing.RetryAmbient != nil) != ((*shape) == "source only" || (*shape) == "HTTPS"),
			"ambient admission = %v",
			err,
		)
		if (*shape) == "missing proxy" && missing.Secret.Name != "proxy" {
			t.Fatalf("missing identity = %s", missing.Secret)
		}
	}
}

func testFetcherStalledAgentCase(t *testing.T, cause *string,
) {
	t.Helper()
	_, key := testSSHIdentity(t)
	_, host := testSSHIdentity(t)
	tr := &agentAuthentication{
		Transport: server.DefaultServer,
		host:      host.PublicKey(),
		allowed:   key.PublicKey(),
	}
	previous := client.Protocols["ssh"]
	client.InstallProtocol("ssh", tr)
	t.Cleanup(func() { client.InstallProtocol("ssh", previous) })
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	testutil.WriteFile(
		t,
		homeDir,
		".ssh/known_hosts",
		knownhosts.Line([]string{"fixture"}, host.PublicKey())+"\n",
	)
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
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			observed <- acceptErr
			return
		}
		accepted <- conn
		defer conn.Close()
		_, acceptErr = io.ReadFull(conn, make([]byte, 5))
		observed <- acceptErr
		if acceptErr != nil {
			return
		}
		_, acceptErr = conn.Read(make([]byte, 1))
		closed <- acceptErr
	}()
	repo := gitRepoWithSecret("stalled", "ssh://git@fixture/repo", "auth")
	repo.Timeout = &metav1.Duration{Duration: time.Second}
	if (*cause) == "cancel" {
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
	testFetcherStalledAgentCaseCompletion(
		t,
		&testFetcherStalledAgentCaseCompletionInputs{
			ctx:      &ctx,
			err:      &err,
			f:        &f,
			repo:     &repo,
			reads:    &reads,
			done:     &done,
			observed: &observed,
			cause:    &cause,
			cancel:   &cancel,
			closed:   &closed,
		},
	)
}

func testFetcherStalledAgentCaseCompletion(t *testing.T, captures *testFetcherStalledAgentCaseCompletionInputs) {
	t.Helper()
	_, (*captures.err) = (*captures.f).Fetch(
		(*captures.ctx),
		(*captures.repo),
		(*captures.reads),
		sourcecache.Policy{Retry: sourcecache.RetryConfig{Attempts: 1}},
	)
	missing, ok := errors.AsType[*source.MissingSecretError]((*captures.err))
	if !ok || missing.RetryAmbient == nil {
		close((*captures.done))
		t.Fatalf("prepare = %v", (*captures.err))
	}
	var artifact *store.SourceArtifact
	started := time.Now()
	go func() {
		artifact, (*captures.err) = missing.RetryAmbient((*captures.ctx))
		close((*captures.done))
	}()
	watchdog := time.NewTimer(5 * time.Second)
	defer watchdog.Stop()
	select {
	case requestErr := <-(*captures.observed):
		assert.NoError(t, requestErr)
	case <-watchdog.C:
		t.Fatal("agent identity request was not observed")
	}
	assert.FatalIf(
		t,
		(*(*captures.cause)) == "timeout" && time.Since(started) >= (*captures.repo).Timeout.Duration,
		"%s",
		"agent request was not observed before the source timeout",
	)

	want := context.DeadlineExceeded
	if (*(*captures.cause)) == "cancel" {
		want = context.Canceled
		(*captures.cancel)()
	}
	select {
	case <-(*captures.done):
	case <-watchdog.C:
		t.Fatal("accepted unanswered agent request outlived attempt termination")
	}
	assert.FatalIf(
		t,
		!errors.Is((*captures.err), want) || !errors.Is((*captures.err), manifest.ErrFlux),
		"ambient failure = %v; want %v and ErrFlux", (*captures.err), want,
	)
	assert.FatalIf(
		t,
		artifact != nil && (artifact.FS != nil || artifact.Content != ""),
		"failed attempt published successful content or agent warning: %+v",
		artifact,
	)
	select {
	case err := <-(*captures.closed):
		assert.FatalIf(t, !errors.Is(err, io.EOF), "agent peer closure = %v; want EOF", err)
	case <-watchdog.C:
		t.Fatal("attempt did not close the agent peer")
	}
}

type testFetcherAmbientAcquisitionCaseCompletionInputs struct {
	repo       **manifest.GitRepository
	shape      **string
	opened     **atomic.Int32
	closed     *<-chan struct{}
	tr         **agentAuthentication
	diagnostic *bytes.Buffer
	root       *string
	keyring    *agent.Agent
}
type testFetcherStalledAgentCaseCompletionInputs struct {
	ctx      *context.Context
	err      *error
	f        **Fetcher
	repo     **manifest.GitRepository
	reads    **store.ReadSet
	done     *chan struct{}
	observed *chan error
	cause    **string
	cancel   *context.CancelFunc
	closed   *chan error
}

type testFetcherAmbientAcquisitionCaseCompletionTailState struct {
	captures **testFetcherAmbientAcquisitionCaseCompletionInputs
	cacheDir *string
	f        **Fetcher
	reads    **store.ReadSet
	artifact **store.SourceArtifact
	missing  **source.MissingSecretError
	success  *bool
}

func testFetcherAmbientAcquisitionCaseCompletionTail(
	t *testing.T,
	scope *testFetcherAmbientAcquisitionCaseCompletionTailState,
) {
	t.Helper()
	if (*(*scope.captures).opened) != nil {
		<-(*(*scope.captures).closed)
		assert.Equal(t, (*(*scope.captures).opened).Load(), int32(1))
	}
	if (*(*(*scope.captures).shape)) == "fallback" &&
		((*(*scope.captures).tr).authenticated.Load() != 2 || strings.Contains((*scope.captures).diagnostic.String(), "diagnostic-canary")) {
		t.Fatalf(
			"fallback diagnostics/selection: calls=%d %s",
			(*(*scope.captures).tr).authenticated.Load(), (*scope.captures).diagnostic.String(),
		)
	}
	assert.FatalIf(
		t,
		(*(*(*scope.captures).shape)) == "bad ref" && (*(*scope.captures).tr).authenticated.Load() == 0,
		"%s",
		"bad ref did not reach authenticated acquisition",
	)

	if *scope.success {
		assert.NoError(t, (*scope.f).Cache.Close())
		(*scope.f).Cache = cachetest.New(t, (*scope.cacheDir), sourcecache.New)
		before := (*(*scope.captures).opened).Load()
		warm, err := (*scope.missing).RetryAmbient(t.Context())
		assert.NoError(t, err)
		<-(*(*scope.captures).closed)
		assert.FatalIf(
			t,
			warm == nil || warm.Key != (*scope.artifact).Key || (*(*scope.captures).opened).Load() != before+1,
			"disk reuse asserted fresh authentication: %+v",
			warm,
		)
		if (*(*(*scope.captures).shape)) == "accepted" {
			mustCommitFile(t, (*(*scope.captures).root), "hello.txt", "refreshed")
			_, err := (*scope.f).Fetch(t.Context(), (*(*scope.captures).repo), (*scope.reads), sourcecache.Policy{})
			refresh, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok || refresh.RetryAmbient == nil, "refresh preparation = %v", err)
			fresh, err := refresh.RetryAmbient(t.Context())
			assert.NoError(t, err)
			<-(*(*scope.captures).closed)
			data, err := fs.ReadFile(fresh.FS, "hello.txt")
			assert.NoError(t, err)
			assert.FatalIf(t, string(data) != "refreshed" || fresh.Content == warm.Content ||
				fresh.Revision == warm.Revision || (*(*scope.captures).opened).
				Load() != before+2, "%s", "refreshed acquisition reused stale content or attempt credentials")

			rotatedIdentity, rotatedSigner := testSSHIdentity(t)
			raw, err := ssh.ParseRawPrivateKey([]byte(rotatedIdentity))
			assert.NoError(t, err)
			assert.NoError(t, (*(*scope.captures).keyring).RemoveAll())
			assert.NoError(t, (*(*scope.captures).keyring).Add(agent.AddedKey{PrivateKey: raw}))
			(*(*scope.captures).tr).
				allowed = rotatedSigner.PublicKey()
			authenticated := (*(*scope.captures).tr).authenticated.Load()
			rotated, err := (*scope.missing).RetryAmbient(t.Context())
			assert.NoError(t, err)
			<-(*(*scope.captures).closed)
			assert.FatalIf(
				t,
				rotated.Key == warm.Key || (*(*scope.captures).tr).authenticated.Load() != authenticated+1 ||
					(*(*scope.captures).opened).
						Load() != before+3,
				"%s",
				"signer rotation reused cache identity or agent selection",
			)
		}
	}
	t.Logf("authenticated sessions=%d; success=%t", (*(*scope.captures).tr).authenticated.Load(), (*scope.success))
}
