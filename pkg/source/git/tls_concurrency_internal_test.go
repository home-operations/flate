package git

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/pkg/store"

	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherConcurrentCustomTLSCase(t *testing.T, concurrency *int,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	root := t.TempDir()
	mustInitRepo(t, root)
	arrived := make(chan struct{}, (*concurrency))
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var workers sync.WaitGroup
	fInputs := testInputs{}
	f := &Fetcher{Network: networktest.New(t), Cache: cachetest.New(t, "", sourcecache.New)}
	secrets := map[string]*manifest.Secret{}
	fInputs.secrets = func(_, name string) store.ReferenceFacts {
		secretInput := secrets[name]
		if secretInput == nil {
			return store.ReferenceFacts{}
		}
		return store.ReferenceFacts{Data: secretInput, Original: secretInput}
	}
	repos := make([]*manifest.GitRepository, (*concurrency)-1)
	proxies := make([]*atomic.Int32, len(repos))
	for i := range repos {
		certificate2 := tlsGitCertificate(t)

		certificate3 := testutil.SelfSignedClientCert(t)

		clients := x509.NewCertPool()
		clients.AppendCertsFromPEM([]byte(certificate3.Cert))
		handler := gitHTTPHandler(t, root)
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if user, _, _ := r.BasicAuth(); user == "alice" && r.Method == http.MethodGet {
				arrived <- struct{}{}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			handler.ServeHTTP(w, r)
		}))
		srv.TLS = &tls.Config{
			Certificates: []tls.Certificate{certificate2.certificate},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clients,
		}
		srv.Config.SetKeepAlivesEnabled(false)
		srv.StartTLS()
		t.Cleanup(srv.Close)
		name := fmt.Sprintf("ca%d", i)
		secrets[name] = &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "p",
				"ca.crt":   certificate2.pem,
				"tls.crt":  certificate3.Cert,
				"tls.key":  certificate3.Key,
			},
		}
		repos[i] = gitRepoWithSecret(name, srv.URL, name)
		proxy, connects := tlsGitProxy(t, true)
		proxies[i] = connects
		secrets["proxy"+name] = &manifest.Secret{
			StringData: map[string]any{"address": proxy.URL, "username": "proxy", "password": "password"},
		}
		repos[i].ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy" + name}
	}
	wrongCA := maps.Clone(secrets[repos[0].SecretRef.Name].StringData)
	wrongCA["ca.crt"] = testutil.SelfSignedCA(t)
	secrets["wrong-ca"] = &manifest.Secret{StringData: wrongCA}
	wrongClient := maps.Clone(secrets[repos[0].SecretRef.Name].StringData)
	certificate4 := testutil.SelfSignedClientCert(t)
	wrongClient["tls.crt"], wrongClient["tls.key"] = certificate4.Cert, certificate4.Key
	secrets["wrong-client"] = &manifest.Secret{StringData: wrongClient}
	t.Cleanup(func() { unblock(); workers.Wait() })
	done := make(chan error, len(repos))
	for _, repo := range repos {
		workers.Go(func() {
			art, err := fetchtest.Fetch(
				ctx,
				f.Fetch,
				repo,
				fInputs.readSet(repo),
				&sourcecache.Policy{MaxAge: repo.SourceInterval()},
				nil,
			)
			if err == nil {
				var content []byte
				content, err = fs.ReadFile(art.FS, "hello.txt")
				if err == nil && string(content) != "hi" {
					err = fmt.Errorf("content=%q", content)
				}
			}
			done <- err
		})
	}
	testFetcherConcurrentCustomTLSCaseCompletion(
		t,
		&testFetcherConcurrentCustomTLSCaseCompletionInputs{
			ctx:     &ctx,
			repos:   &repos,
			arrived: &arrived,
			f:       &f,
			fInputs: &fInputs,
			root:    &root,
			unblock: &unblock,
			done:    &done,
			proxies: &proxies,
		},
	)
}

func testFetcherConcurrentCustomTLSCaseCompletion(
	t *testing.T,
	captures *testFetcherConcurrentCustomTLSCaseCompletionInputs,
) {
	t.Helper()
	assert.NoError(t, waitForTLSFetchOverlap(
		*captures.ctx, *captures.arrived, *captures.done, len(*captures.repos),
	))
	_, err := fetchtest.Fetch(
		(*captures.ctx),
		(*captures.f).Fetch,
		gitRepoWithSecret("anonymous", (*captures.repos)[0].URL, ""),
		captures.fInputs.
			readSet(gitRepoWithSecret("anonymous", (*captures.repos)[0].URL, "")),
		&sourcecache.Policy{},
		nil,
	)
	{
		_, ok := errors.AsType[x509.UnknownAuthorityError](err)
		assert.FatalIf(t, !ok, "anonymous fetch inherited private trust: %v", err)
	}
	for _, name := range []string{"wrong-ca", "wrong-client"} {
		bad := gitRepoWithSecret(name, (*captures.repos)[0].URL, name)
		bad.ProxySecretRef = (*captures.repos)[0].ProxySecretRef
		{
			_, err := fetchtest.Fetch((*captures.ctx), (*captures.f).Fetch, bad, captures.fInputs.
				readSet(bad), &sourcecache.Policy{MaxAge: bad.SourceInterval()}, nil)
			assert.FatalIf(t, err == nil, "%s fetch inherited another operation's credentials", name)
		}
	}
	for _, ref := range []string{"", mustHead(t, (*captures.root))} {
		_, err := (*captures.f).FetchRemoteBase(
			(*captures.ctx),
			&RemoteBaseRequest{URL: (*captures.repos)[0].URL, Ref: ref, Timeout: 27 * time.Second, Submodules: true},
			&sourcecache.Policy{},
		)
		{
			_, ok := errors.AsType[x509.UnknownAuthorityError](err)
			assert.FatalIf(t, !ok, "anonymous remote base inherited private trust: %v", err)
		}
	}
	(*captures.unblock)()
	for range *captures.repos {
		{
			err := <-(*captures.done)
			assert.NoError(t, err)
		}
	}
	for _, connects := range *captures.proxies {
		assert.FatalIf(t, connects.Load() < 2, "%s", "custom clone bypassed its proxy")
	}
}

type testFetcherConcurrentCustomTLSCaseCompletionInputs struct {
	ctx     *context.Context
	repos   *[]*manifest.GitRepository
	arrived *chan struct{}
	f       **Fetcher
	fInputs *testInputs
	root    *string
	unblock *func()
	done    *chan error
	proxies *[]*atomic.Int32
}

func waitForTLSFetchOverlap(ctx context.Context, arrived <-chan struct{}, done <-chan error, count int) error {
	for range count {
		select {
		case <-arrived:
		case err := <-done:
			if err != nil {
				return err
			}
			return errors.New("custom TLS acquisition completed before release")
		case <-ctx.Done():
			return fmt.Errorf("custom TLS acquisitions did not overlap: %w", ctx.Err())
		}
	}
	return nil
}

func TestFetcher_ConcurrentCustomTLSFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		arrived := make(chan struct{}, 2)
		done := make(chan error, 2)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		var workers sync.WaitGroup
		defer func() {
			cancel()
			unblock()
			workers.Wait()
		}()

		secret := &manifest.Secret{StringData: map[string]any{
			"username": "alice", "password": "p",
			"tls.crt": "invalid", "tls.key": "invalid",
		}}
		inputs := testInputs{secrets: func(_, _ string) store.ReferenceFacts {
			return store.ReferenceFacts{Data: secret, Original: secret}
		}}
		repo := gitRepoWithSecret("failure", "https://example.invalid/repo.git", "tls")
		reads := inputs.readSet(repo)
		f := &Fetcher{}
		workers.Go(func() {
			arrived <- struct{}{}
			<-release
			done <- nil
		})
		synctest.Wait()

		var fetchErr error
		workers.Go(func() {
			_, fetchErr = f.Fetch(ctx, repo, reads, &sourcecache.Policy{})
			done <- fetchErr
		})
		result := make(chan error, 1)
		workers.Go(func() {
			result <- waitForTLSFetchOverlap(ctx, arrived, done, 2)
		})
		synctest.Wait()

		select {
		case err := <-result:
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, err, fetchErr)
		default:
			t.Fatal("overlap wait did not report the TLS setup error")
		}
		assert.NoError(t, ctx.Err())
		select {
		case err := <-done:
			t.Fatalf("worker completed before release: %v", err)
		default:
		}
		unblock()
		workers.Wait()
		assert.NoError(t, <-done)
	})
}

func TestWaitForTLSFetchOverlap_Completion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		cancel bool
		want   string
	}{
		{name: "completed before release", want: "custom TLS acquisition completed before release"},
		{name: "cancelled", cancel: true, want: "custom TLS acquisitions did not overlap: context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			arrived := make(chan struct{})
			done := make(chan error, 1)
			if tc.cancel {
				cancel()
			} else {
				done <- nil
			}
			err := waitForTLSFetchOverlap(ctx, arrived, done, 1)
			assert.FatalIf(t, err == nil, "overlap wait accepted %s", tc.name)
			assert.Equal(t, err.Error(), tc.want)
			assert.Equal(t, errors.Is(err, context.Canceled), tc.cancel)
		})
	}
}
