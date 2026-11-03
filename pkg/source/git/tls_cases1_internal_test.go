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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherTLSOperationsCase(t *testing.T, root *string,
	head *string,
	route *string,
) {
	t.Helper()

	for _, tc := range []struct {
		name            string
		ref             *sourcev1.GitRepositoryRef
		recurse, refuse bool
	}{
		{name: "clone"},
		{name: "branch", ref: &sourcev1.GitRepositoryRef{Branch: "master"}},
		{name: "tag", ref: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "semver", ref: &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"}},
		{name: "named", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1.0.0"}},
		{name: "clone-fetch", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1.0.0"}, recurse: true},
		{name: "exact", ref: &sourcev1.GitRepositoryRef{Commit: (*head)}},
		{name: "fallback", ref: &sourcev1.GitRepositoryRef{Commit: (*head)}, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherTLSOperationsCaseCase(
				t,
				testFetcherTLSOperationsCaseCaseInputs{root: &root, route: &route, tc: &tc},
			)
		},
		)
	}
}

type testFetcherRecursiveTLSDefaultsCaseInputs struct {
	module     *string
	moduleHash *plumbing.Hash
	child      *string
	exact      *bool
}

func testFetcherRecursiveTLSDefaultsCase(t *testing.T, testCaseInput testFetcherRecursiveTLSDefaultsCaseInputs) {
	t.Helper()

	root := t.TempDir()
	cert, ca := tlsGitCertificate(t)
	parentHandler, moduleHandler := gitHTTPHandler(t, root), gitHTTPHandler(t, (*testCaseInput.module))
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/module/") {
			moduleHandler.ServeHTTP(w, r)
		} else {
			parentHandler.ServeHTTP(w, r)
		}
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	url := "file://" + (*testCaseInput.module)
	if (*testCaseInput.child) == "private-ca" {
		url = srv.URL + "/module"
	}
	mustInitRepoWithFiles(
		t,
		root,
		map[string]string{
			"hello.txt":   "parent",
			".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = " + url + "\n",
		},
	)
	r, err := git.PlainOpen(root)
	assert.NoError(t, err)

	head, err := r.Head()
	assert.NoError(t, err)

	commit, err := r.CommitObject(head.Hash())
	assert.NoError(t, err)

	tree, err := commit.Tree()
	assert.NoError(t, err)

	tree.Entries = append(
		tree.Entries,
		object.TreeEntry{Name: "lib", Mode: filemode.Submodule, Hash: (*testCaseInput.moduleHash)},
	)
	encoded := r.Storer.NewEncodedObject()
	{
		encodeErr2 := tree.Encode(encoded)
		assert.NoError(t, encodeErr2)
	}

	commit.TreeHash, err = r.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)

	commit.ParentHashes = []plumbing.Hash{head.Hash()}
	encoded = r.Storer.NewEncodedObject()
	{
		encodeErr3 := commit.Encode(encoded)
		assert.NoError(t, encodeErr3)
	}

	hash, err := r.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	{
		setReferenceErr := r.Storer.SetReference(
			plumbing.NewHashReference(head.Name(), hash),
		)
		assert.NoError(t, setReferenceErr)
	}

	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca},
		}
	}}
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	repo := gitRepoWithSecret("recursive", srv.URL, "tls")
	repo.RecurseSubmodules = true
	if *testCaseInput.exact {
		repo.Reference = &sourcev1.GitRepositoryRef{Commit: hash.String()}
	}
	art, err := f.Fetch(
		t.Context(),
		repo,
		fInputs.readSet(repo),
		sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	if (*testCaseInput.child) == "private-ca" {
		{
			_, ok := errors.AsType[x509.UnknownAuthorityError](err)
			assert.FatalIf(t, !ok, "child inherited private CA: %v", err)
		}
		return
	}
	assert.NoError(t, err)

	{
		data, err := fs.ReadFile(art.FS, "lib/hello.txt")
		assert.FatalIf(t, err != nil || string(data) != "hi", "child content=%q, error=%v", data, err)
	}
}

func testFetcherConcurrentCustomTLSCase(t *testing.T, concurrency *int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	root := t.TempDir()
	mustInitRepo(t, root)
	arrived := make(chan struct{}, (*concurrency))
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var workers sync.WaitGroup
	fInputs := testInputs{}
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
	secrets := map[string]*manifest.Secret{}
	fInputs.secrets = func(_, name string) *manifest.Secret { return secrets[name] }
	repos := make([]*manifest.GitRepository, (*concurrency)-1)
	proxies := make([]*atomic.Int32, len(repos))
	for i := range repos {
		cert, ca := tlsGitCertificate(t)
		crt, key := testutil.SelfSignedClientCert(t)
		clients := x509.NewCertPool()
		clients.AppendCertsFromPEM([]byte(crt))
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
			Certificates: []tls.Certificate{cert},
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
				"ca.crt":   ca,
				"tls.crt":  crt,
				"tls.key":  key,
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
	wrongClient["tls.crt"], wrongClient["tls.key"] = testutil.SelfSignedClientCert(t)
	secrets["wrong-client"] = &manifest.Secret{StringData: wrongClient}
	t.Cleanup(func() { unblock(); workers.Wait() })
	done := make(chan error, len(repos))
	for _, repo := range repos {
		workers.Go(func() {
			art, err := f.Fetch(
				ctx,
				repo,
				fInputs.readSet(repo),
				sourcecache.Policy{MaxAge: repo.SourceInterval()},
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
	for range *captures.repos {
		select {
		case <-(*captures.arrived):
		case <-(*captures.ctx).Done():
			t.Fatal("custom TLS acquisitions did not overlap")
		}
	}
	_, err := (*captures.f).Fetch(
		(*captures.ctx),
		gitRepoWithSecret("anonymous", (*captures.repos)[0].URL, ""),
		captures.fInputs.
			readSet(gitRepoWithSecret("anonymous", (*captures.repos)[0].URL, "")),
		sourcecache.Policy{},
	)
	{
		_, ok := errors.AsType[x509.UnknownAuthorityError](err)
		assert.FatalIf(t, !ok, "anonymous fetch inherited private trust: %v", err)
	}
	for _, name := range []string{"wrong-ca", "wrong-client"} {
		bad := gitRepoWithSecret(name, (*captures.repos)[0].URL, name)
		bad.ProxySecretRef = (*captures.repos)[0].ProxySecretRef
		{
			_, err := (*captures.f).Fetch((*captures.ctx), bad, captures.fInputs.
				readSet(bad), sourcecache.Policy{MaxAge: bad.SourceInterval()},
			)
			assert.FatalIf(t, err == nil, "%s fetch inherited another operation's credentials", name)
		}
	}
	for _, ref := range []string{"", mustHead(t, (*captures.root))} {
		_, err := (*captures.f).FetchRemoteBase(
			(*captures.ctx),
			RemoteBaseRequest{URL: (*captures.repos)[0].URL, Ref: ref, Timeout: 27 * time.Second, Submodules: true},
			sourcecache.Policy{},
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

func testFetcherResolveTransportSecretMaterialCase(
	t *testing.T, ca *string,
	tc *struct {
		name        string
		errorKey    string
		data        map[string]any
		encodedData map[string]any
		wantInput   bool
		wantMissing bool
		wantBearer  bool
		proxyRead   bool
	},
) {
	t.Helper()

	sec := &manifest.Secret{
		Name:       "different",
		Namespace:  "elsewhere",
		StringData: tc.data,
		Data:       tc.encodedData,
	}
	s := store.New()
	captured := *sec
	captured.Name, captured.Namespace = "creds", "ns"
	s.AddObject(&captured)
	s.AddObject(
		&manifest.Secret{
			Name:       "proxy",
			Namespace:  "ns",
			StringData: map[string]any{"address": "http://proxy.invalid/%"},
		},
	)

	f := &Fetcher{}
	repo := gitRepoWithSecret("g", "https://host/repo", "creds")
	if tc.wantInput || tc.wantMissing {
		repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	}
	var reads store.ReadSet
	reads.Init(s, repo.Named(), nil)
	request, err := (&operation{Fetcher: f, reads: &reads, secrets: source.SecretLookup(&reads)}).resolveTransport(
		repo,
		sourcecache.Policy{},
	)
	s.AddObject(
		&manifest.Secret{
			Name:       "proxy",
			Namespace:  "ns",
			StringData: map[string]any{"address": "http://changed.invalid"},
		},
	)
	assert.Equal(t, reads.Valid(), !tc.proxyRead)
	s.AddObject(&manifest.Secret{Name: "creds", Namespace: "ns"})
	assert.Equal(t, reads.Valid(), false)
	if tc.wantInput || tc.wantMissing {
		assert.Equal(t, request.spec == nil, true)
		assert.Equal(t, request.material == nil, true)
		assert.Diff(t, request.options, git.CloneOptions{})
		assert.Equal(t, errors.Is(err, manifest.ErrInput), tc.wantInput)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), tc.wantMissing)
		secret, owner := "ns/creds", manifest.DiagnosticFor(repo.Named(), repo).Label()
		if tc.proxyRead {
			secret, owner = "ns/proxy", "GitRepository ns/g"
		}
		assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), tc.errorKey) ||
			!strings.Contains(err.Error(), secret) ||
			strings.Contains(err.Error(), owner), "wrong selected error context: %v", err)
		assert.FatalIf(
			t,
			strings.Contains(err.Error(), "%%%fixture") || strings.Contains(err.Error(), "credential-fixture"),
			"%s",
			"credential payload leaked",
		)

		if tc.wantMissing {
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok, "expected MissingSecretError; got %v", err)
			assert.Equal(t, missing.Owner, repo.Named())
			assert.Equal(
				t,
				missing.Secret,
				manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"},
			)
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, request.spec, repo)
	assert.Equal(t, string(request.options.CABundle), (*ca))
	assert.Equal(t, request.material != nil, true)
	if tc.wantBearer {
		auth, ok := request.options.Auth.(*githttp.TokenAuth)
		if !ok {
			t.Fatalf("expected token auth; got %T", request.options.Auth)
		}
		assert.Equal(t, auth.Token, "fixture-token")
	} else {
		auth, ok := request.options.Auth.(*githttp.BasicAuth)
		if !ok {
			t.Fatalf("expected basic auth; got %T", request.options.Auth)
		}
		assert.Equal(t, auth.Username, "u")
		assert.Equal(t, auth.Password, "p")
	}
}

type testFetcherHTTPSBasicAuthSnapshotRotationCaseInputs struct {
	fail      *atomic.Bool
	requests  *atomic.Int32
	expected  *atomic.Pointer[manifest.Secret]
	lookups   *atomic.Int32
	ca        *string
	sec       **manifest.Secret
	proxy     **httptest.Server
	cacheRoot *string
	repo      **manifest.GitRepository
	previous  *string
	wave      *string
}

func testFetcherHTTPSBasicAuthSnapshotRotationCase(
	t *testing.T, testCaseInputValue *testFetcherHTTPSBasicAuthSnapshotRotationCaseInputs,
) {
	t.Helper()
	testCaseInput := *testCaseInputValue
	if (*testCaseInput.wave) == "password" {
		(*testCaseInput.sec) = &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "second", "ca.crt": (*testCaseInput.ca)},
		}
	}
	if (*testCaseInput.wave) == "certificate" {
		(*testCaseInput.sec) = &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "second",
				"ca.crt":   testutil.SelfSignedCA(t),
			},
		}
	}
	testCaseInput.expected.
		Store((*testCaseInput.sec))
	proxyURL := strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:first@", 1)
	if (*testCaseInput.wave) == "proxy" || (*testCaseInput.wave) == "direct" {
		proxyURL = strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:second@", 1)
	}
	t.Setenv("http_proxy", proxyURL)
	if (*testCaseInput.wave) == "direct" {
		t.Setenv("no_proxy", "*")
	}
	c := cachetest.New(t, (*testCaseInput.cacheRoot), sourcecache.New)
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		testCaseInput.lookups.Add(1)
		return (*testCaseInput.sec)
	}}
	f := &Fetcher{Cache: c}
	before := testCaseInput.requests.Load()
	art, err := f.Fetch(
		t.Context(), (*testCaseInput.repo), fInputs.readSet((*testCaseInput.repo)),
		sourcecache.Policy{MaxAge: (*testCaseInput.repo).SourceInterval(), Retry: sourcecache.RetryConfig{Attempts: 3}},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		art.Key == (*testCaseInput.previous) || testCaseInput.requests.Load() <= before,
		"%s",
		"changed material reused an acquisition",
	)

	(*testCaseInput.previous) = art.Key
	before = testCaseInput.requests.Load()
	{
		_, fetchErr := f.Fetch(
			t.Context(), (*testCaseInput.repo), fInputs.readSet((*testCaseInput.repo)),
			sourcecache.Policy{MaxAge: (*testCaseInput.repo).SourceInterval()},
		)
		assert.FatalIf(t, fetchErr != nil || testCaseInput.requests.
			Load() != before, "warm snapshot missed: %v", fetchErr)
	}
	testCaseInput.fail.
		Store(true)
	failed := *(*testCaseInput.repo)
	failed.Interval.Duration = 0
	_, err = f.Fetch(
		t.Context(),
		&failed,
		fInputs.readSet(&failed),
		sourcecache.Policy{MaxAge: failed.SourceInterval()},
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "failed snapshot: %v", err)
	testCaseInput.fail.
		Store(false)
}

type testFetcherTLSOperationsCaseCaseInputs struct {
	root  **string
	route **string
	tc    *struct {
		name string
		ref  *sourcev1.
			GitRepositoryRef
		recurse bool
		refuse  bool
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
