package git

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/pkg/store"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"golang.org/x/crypto/ssh"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

func tlsGitProxy(t *testing.T, authenticated bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var connects atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect ||
			authenticated &&
				r.Header.Get(
					"Proxy-Authorization",
				) != "Basic "+base64.StdEncoding.EncodeToString(
					[]byte("proxy:password"),
				) {
			http.Error(w, "proxy auth", http.StatusProxyAuthRequired)
			return
		}
		upstream, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", r.Host)
		if err != nil {
			t.Error(err)
			return
		}
		defer upstream.Close()
		conn, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		connects.Add(1)
		if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			t.Error(err)
			return
		}
		done := make(chan struct{})
		go func() {
			_, _ = io.Copy(conn, upstream)
			_ = conn.Close()
			close(done)
		}()
		_, _ = io.Copy(upstream, buffered)
		_ = upstream.Close()
		<-done
	}))
	t.Cleanup(srv.Close)
	return srv, &connects
}

func tlsGitCertificate(t *testing.T) gitCertificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	assert.NoError(t, err)

	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, BasicConstraintsValid: true, IsCA: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	assert.NoError(t, err)

	return gitCertificate{certificate: tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}, pem: string(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	)}
}

func gitHTTPHandler(t *testing.T, root string) http.Handler {
	t.Helper()
	ep, err := transport.NewEndpoint(root + "/.git")
	assert.NoError(t, err)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveGitHTTP(t, w, r, ep)
	})
}

func serveGitHTTP(t *testing.T, w http.ResponseWriter, r *http.Request, ep *transport.Endpoint) {
	t.Helper()
	session, err := server.DefaultServer.NewUploadPackSession(ep, nil)
	if err != nil {
		t.Error(err)
		http.Error(w, "session", http.StatusInternalServerError)
		return
	}
	defer session.Close()
	refs, err := session.AdvertisedReferencesContext(r.Context())
	if err != nil {
		t.Error(err)
		return
	}
	if setErr := refs.Capabilities.Set(capability.Shallow); setErr != nil {
		t.Error(setErr)
		return
	}
	if setErr2 := refs.Capabilities.Set(capability.AllowReachableSHA1InWant); setErr2 != nil {
		t.Error(setErr2)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		encoder := pktline.NewEncoder(w)
		if encodeStringErr := encoder.EncodeString("# service=git-upload-pack\n"); encodeStringErr != nil {
			t.Error(encodeStringErr)
			return
		}
		if flushErr := encoder.Flush(); flushErr != nil {
			t.Error(flushErr)
			return
		}
		if encodeErr := refs.Encode(w); encodeErr != nil {
			t.Error(encodeErr)
		}
		return
	}
	request := packp.NewUploadPackRequest()
	if decodeErr := request.Decode(r.Body); decodeErr != nil {
		t.Error(decodeErr)
		return
	}
	response, err := session.UploadPack(r.Context(), request)
	if err != nil {
		t.Error(err)
		return
	}
	defer response.Close()
	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	if err := response.Encode(w); err != nil {
		t.Error(err)
	}
}

func TestFetcher_TLSOperations(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustInitRepo(t, root)
	head := mustTagHEAD(t, root, "v1.0.0")
	for _, route := range []string{"direct", "proxy", "authenticated-proxy"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			testFetcherTLSOperationsCase(t, &root, &head, &route)
		},
		)
	}
}

//nolint:paralleltest // The test changes the process egress policy.
//nolint:paralleltest // The test changes process egress policy.
func TestFetcher_RestrictEgress(t *testing.T) {
	t.Run("custom-ca", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.NotFoundHandler())
		defer srv.Close()
		ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
		fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
		}}
		f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
		ssrfguard.Restrict(true)
		defer ssrfguard.Restrict(false)
		_, err := f.Fetch(
			t.Context(),
			gitRepoWithSecret("restricted", srv.URL, "tls"),
			fInputs.readSet(gitRepoWithSecret("restricted", srv.URL, "tls")),
			&sourcecache.Policy{},
		)
		{
			op, ok := errors.AsType[*net.OpError](err)
			assert.FatalIf(t, !ok || op.Op != "dial", "fetch error = %v, want dial error", err)
		}
	})
}

func TestFetcher_RecursiveTLSDefaults(t *testing.T) {
	t.Parallel()
	module := t.TempDir()
	mustInitRepo(t, module)
	moduleHash := plumbing.NewHash(mustHead(t, module))
	for _, child := range []string{"file", "private-ca"} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exact=%t", child, exact), func(t *testing.T) {
				t.Parallel()
				testFetcherRecursiveTLSDefaultsCase(
					t,
					testFetcherRecursiveTLSDefaultsCaseInputs{
						module:     &module,
						moduleHash: &moduleHash,
						child:      &child,
						exact:      &exact,
					},
				)
			},
			)
		}
	}
}

func TestFetcher_ConcurrentCustomTLS(t *testing.T) {
	t.Parallel()

	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			t.Parallel()

			testFetcherConcurrentCustomTLSCase(t, &concurrency)
		},
		)
	}
}

func gitRepoWithSecret(name, url, secretName string) *manifest.GitRepository {
	repo := &manifest.GitRepository{
		Name: name, Namespace: "ns",
		URL: url,
	}
	if secretName != "" {
		repo.SecretRef = &manifest.LocalObjectReference{Name: secretName}
	}
	return repo
}

func TestFetcher_ResolveTLS(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	migratedResult1 := testutil.SelfSignedClientCert(t)
	crt, key := migratedResult1.Cert, migratedResult1.Key
	_, private, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(private, "fixture")
	assert.NoError(t, err)
	identity := string(pem.EncodeToMemory(block))

	cases := make([]gitTLSCase, 0, 34)
	cases = append(cases, []gitTLSCase{
		{name: "anonymous", encodedData: map[string]any{"ca.crt": "%%%fixture"}},
		{
			name:        "ssh",
			url:         "ssh://git@example.com/x.git",
			secret:      "tls",
			encodedData: map[string]any{"ca.crt": "%%%fixture"},
		},
		{name: "scp", url: "git@example.com:x.git", secret: "tls", data: map[string]any{"ca.crt": "invalid"}},
		{name: "git-at-path", url: "git://h/x@y", secret: "tls", data: map[string]any{"ca.crt": ca}, wantCA: ca},
		{name: "auth-only", secret: "tls", data: map[string]any{"username": "alice", "password": "p"}},
		{
			name:   "empty TLS",
			secret: "tls",
			data:   map[string]any{"tls.crt": "", "tls.key": "", "ca.crt": "", "caFile": ""},
		},
		{name: "ca.crt", secret: "tls", data: map[string]any{"ca.crt": ca}, wantCA: ca},
		{name: "caFile", secret: "tls", data: map[string]any{"caFile": ca}, wantCA: ca},
		{name: "ca.crt-precedence", secret: "tls", data: map[string]any{"ca.crt": ca, "caFile": "invalid"}, wantCA: ca},
		{name: "ca.crt over empty caFile", secret: "tls", data: map[string]any{"ca.crt": ca, "caFile": ""}, wantCA: ca},
		{
			name:    "mtls",
			secret:  "tls",
			data:    map[string]any{"ca.crt": ca, "tls.crt": crt, "tls.key": key},
			wantCA:  ca,
			wantCRT: crt,
			wantKey: key,
		},
		{name: "unpaired", secret: "tls", data: map[string]any{"tls.crt": crt}, wantInput: true},
		{name: "invalid-ca", secret: "tls", data: map[string]any{"ca.crt": "invalid"}, wantInput: true},
		{
			name:      "invalid-client",
			secret:    "tls",
			data:      map[string]any{"tls.crt": "invalid", "tls.key": "invalid"},
			wantInput: true,
		},
		{
			name:      "empty client invalid CA",
			secret:    "tls",
			data:      map[string]any{"tls.crt": "", "ca.crt": "invalid"},
			wantInput: true,
			errorKey:  "ca.crt",
		},
		{
			name:      "invalid client empty key",
			secret:    "tls",
			data:      map[string]any{"tls.crt": "invalid", "tls.key": ""},
			wantInput: true,
			errorKey:  "tls.crt",
		},
		{
			name:      "valid client empty key",
			secret:    "tls",
			data:      map[string]any{"tls.crt": crt, "tls.key": ""},
			wantInput: true,
		},
		{
			name:        "empty crt malformed key",
			secret:      "tls",
			data:        map[string]any{"tls.crt": ""},
			encodedData: map[string]any{"tls.key": "%%%fixture"},
			wantInput:   true,
			errorKey:    "tls.key",
		},
		{
			name:        "empty key malformed ca",
			secret:      "tls",
			data:        map[string]any{"tls.key": ""},
			encodedData: map[string]any{"ca.crt": "%%%fixture"},
			wantInput:   true,
			errorKey:    "ca.crt",
		},
		{
			name:        "empty ca malformed caFile",
			secret:      "tls",
			data:        map[string]any{"ca.crt": ""},
			encodedData: map[string]any{"caFile": "%%%fixture"},
			wantInput:   true,
			errorKey:    "caFile",
		},
		{
			name:        "caFile decoding despite valid ca",
			secret:      "tls",
			data:        map[string]any{"ca.crt": ca},
			encodedData: map[string]any{"caFile": "%%%fixture"},
			wantInput:   true,
			errorKey:    "caFile",
		},
		{
			name:   "first decoding error wins",
			secret: "tls",
			encodedData: map[string]any{
				"tls.crt": "%%%fixture",
				"tls.key": "%%%fixture",
				"ca.crt":  "%%%fixture",
				"caFile":  "%%%fixture",
			},
			wantInput: true,
			errorKey:  "tls.crt",
		},
	}...)
	for _, field := range []string{"tls.crt", "tls.key", "ca.crt", "caFile"} {
		cases = append(cases, gitTLSCase{name: field + "/empty", secret: "tls", data: map[string]any{field: ""}})
		for _, malformed := range []struct {
			name  string
			value any
		}{
			{"encoding", "%%%fixture"}, {"nonscalar", map[string]any{"credential-fixture": "payload"}},
		} {
			cases = append(
				cases,
				gitTLSCase{
					name:        field + "/" + malformed.name,
					secret:      "tls",
					encodedData: map[string]any{field: malformed.value},
					wantInput:   true,
					errorKey:    field,
				},
			)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherResolveTLSCase(t, &testFetcherResolveTLSCaseInputs{tc: &tc, identity: &identity})
		})
	}
}

func TestFetcher_ResolveTransport_EndpointValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, secret string
		wantInput    bool
	}{
		{name: "without secretRef", wantInput: true},
		{name: "with secretRef", secret: "creds", wantInput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p"}}
			}}
			f := &Fetcher{}
			repo := gitRepoWithSecret("g", "http://[::1", tc.secret)
			request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
				repo,
				&sourcecache.Policy{},
			)
			if tc.wantInput {
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				assert.FatalIf(
					t,
					err == nil || !strings.Contains(err.Error(), "parse git endpoint"),
					"missing endpoint validation: %v",
					err,
				)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, request.options.URL, repo.URL)
		})
	}
}

func TestFetcher_ResolveTransport_SecretMaterial(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	for _, tc := range []struct {
		name, errorKey                                string
		data, encodedData                             map[string]any
		wantInput, wantMissing, wantBearer, proxyRead bool
	}{
		{name: "hard auth before hard TLS", encodedData: map[string]any{"bearerToken": "%%%fixture", "caFile": "%%%fixture"}, wantInput: true, errorKey: "bearerToken"},
		{name: "hard auth before missing TLS", data: map[string]any{"ca.crt": ""}, encodedData: map[string]any{"bearerToken": "%%%fixture"}, wantInput: true, errorKey: "bearerToken"},
		{name: "malformed proxy before unavailable auth", data: map[string]any{"username": "u"}, wantInput: true, proxyRead: true, errorKey: "proxy"},
		{name: "optional TLS before invalid proxy", data: map[string]any{"bearerToken": "", "ca.crt": ""}, wantInput: true, proxyRead: true, errorKey: "proxy"},
		{name: "hard TLS before missing auth", data: map[string]any{"username": "u"}, encodedData: map[string]any{"ca.crt": "%%%fixture"}, wantInput: true, errorKey: "ca.crt"},
		{name: "real TLS before empty auth", data: map[string]any{"bearerToken": "", "ca.crt": "invalid"}, wantInput: true, errorKey: "ca.crt"},
		{name: "valid auth hard TLS", data: map[string]any{"username": "u", "password": "p"}, encodedData: map[string]any{"caFile": "%%%fixture"}, wantInput: true, errorKey: "caFile"},
		{name: "valid auth optional TLS invalid proxy", data: map[string]any{"username": "u", "password": "p", "ca.crt": ""}, wantInput: true, proxyRead: true, errorKey: "proxy"},
		{name: "empty bearer uses malformed basic", data: map[string]any{"bearerToken": ""}, encodedData: map[string]any{"username": "%%%fixture"}, wantInput: true, errorKey: "username"},
		{name: "valid basic", data: map[string]any{"username": "u", "password": "p", "ca.crt": ca}},
		{name: "valid bearer ignores malformed basic", data: map[string]any{"bearerToken": "fixture-token", "ca.crt": ca}, encodedData: map[string]any{"username": "%%%fixture", "password": map[string]any{"credential-fixture": "payload"}}, wantBearer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherResolveTransportSecretMaterialCase1(t, &tc, &ca)
		})
	}
}

func testFetcherResolveTransportSecretMaterialCase1(t *testing.T, tc *struct {
	name        string
	errorKey    string
	data        map[string]any
	encodedData map[string]any
	wantInput   bool
	wantMissing bool
	wantBearer  bool
	proxyRead   bool
}, ca *string,
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
		&sourcecache.Policy{},
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
		testFetcherResolveTransportSecretMaterialCase1Part1(
			t,
			&testFetcherResolveTransportSecretMaterialCase1Part1Inputs{
				request: &request,
				err:     err,
				tc:      &tc,
				repo:    &repo,
			},
		)
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, request.spec, repo)
	assert.Equal(t, string(request.options.CABundle), (*ca))
	assert.Equal(t, request.material != nil, true)
	if tc.wantBearer {
		auth, ok := request.options.Auth.(*githttp.TokenAuth)
		assert.FatalIf(t, !ok,
			"expected token auth; got %T", request.options.Auth)

		assert.Equal(t, auth.Token, "fixture-token")
	} else {
		auth, ok := request.options.Auth.(*githttp.BasicAuth)
		assert.FatalIf(t, !ok,
			"expected basic auth; got %T", request.options.Auth)

		assert.Equal(t, auth.Username, "u")
		assert.Equal(t, auth.Password, "p")
	}
}

func testFetcherResolveTransportSecretMaterialCase1Part1(
	t *testing.T, inputs *testFetcherResolveTransportSecretMaterialCase1Part1Inputs,
) {
	t.Helper()
	assert.Equal(t, inputs.request.spec == nil, true)
	assert.Equal(t, inputs.request.material == nil, true)
	assert.Diff(t, inputs.request.options, git.CloneOptions{})
	assert.Equal(t, errors.Is(inputs.err, manifest.ErrInput), (*inputs.tc).wantInput)
	assert.Equal(t, errors.Is(inputs.err, manifest.ErrFlux), true)
	assert.Equal(t, errors.Is(inputs.err, manifest.ErrMissingSecret), (*inputs.tc).wantMissing)
	secret, owner := "ns/creds", manifest.DiagnosticFor((*inputs.repo).Named(), (*inputs.repo)).Label()
	if (*inputs.tc).proxyRead {
		secret, owner = "ns/proxy", "GitRepository ns/g"
	}
	assert.FatalIf(t, inputs.err == nil || !strings.Contains(inputs.err.Error(), (*inputs.tc).errorKey) ||
		!strings.Contains(inputs.err.Error(), secret) ||
		strings.Contains(inputs.err.Error(), owner),
		"wrong selected error context: %v", inputs.err)
	assert.FatalIf(
		t,
		strings.Contains(inputs.err.Error(), "%%%fixture") ||
			strings.Contains(inputs.err.Error(), "credential-fixture"),
		"%s",
		"credential payload leaked",
	)

	if (*inputs.tc).wantMissing {
		missing, ok := errors.AsType[*source.MissingSecretError](inputs.err)
		assert.FatalIf(t, !ok,
			"expected MissingSecretError; got %v", inputs.err)

		assert.Equal(t, missing.Owner, (*inputs.repo).Named())
		assert.Equal(
			t,
			missing.Secret,
			manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"},
		)
	}
}

func TestFetcher_ResolveTransport_MalformedProxy(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
		if name == "proxy" {
			return &manifest.Secret{
				StringData: map[string]any{"address": "http://proxy-user:proxy-password@proxy.invalid/%"},
			}
		}
		return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
	}}
	f := &Fetcher{}
	repo := &manifest.GitRepository{
		Name:      "tls",
		Namespace: "ns",
		URL:       "https://example.com/x.git",
		Provider:  sourcev1.GitProviderGitHub,
	}
	repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	_, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), &sourcecache.Policy{MaxAge: repo.SourceInterval()})
	assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "proxy validation before cache admission: %v", err)
	assert.FatalIf(
		t,
		strings.Contains(err.Error(), "proxy-user") || strings.Contains(err.Error(), "proxy-password"),
		"%s",
		"proxy credentials leaked in validation error",
	)
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_HTTPSBasicAuth_SnapshotRotation(t *testing.T) {
	root := t.TempDir()
	mustInitRepo(t, root)
	handler := gitHTTPHandler(t, root)
	var fail atomic.Bool
	var requests atomic.Int32
	var expected atomic.Pointer[manifest.Secret]
	var lookups atomic.Int32
	ca := testutil.SelfSignedCA(t)
	sec := &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "first", "ca.crt": ca}}
	expected.Store(sec)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherHTTPSBasicAuthSnapshotRotationCallback(
			w,
			r,
			testFetcherHTTPSBasicAuthSnapshotRotationCallbackInputs{
				t:        &t,
				handler:  &handler,
				fail:     &fail,
				requests: &requests,
				expected: &expected,
			},
		)
	},
	))
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)

	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	proxy := httptest.NewServer(srv.Config.Handler)
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	cacheRoot := t.TempDir()
	repo := gitRepoWithSecret("snapshot", srv.URL, "creds")
	repo.Interval.Duration = time.Hour
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			testFetcherHTTPSBasicAuthSnapshotRotationCase(
				t, &testFetcherHTTPSBasicAuthSnapshotRotationCaseInputs{
					fail:      &fail,
					requests:  &requests,
					expected:  &expected,
					lookups:   &lookups,
					ca:        &ca,
					sec:       &sec,
					proxy:     &proxy,
					cacheRoot: &cacheRoot,
					repo:      &repo,
					previous:  &previous,
					wave:      &wave,
				},
			)
		},
		)
	}
	assert.FatalIf(t, lookups.Load() != 15,
		"auth and TLS reread Secrets: %d", lookups.Load())
}

type testFetcherHTTPSBasicAuthSnapshotRotationCallbackInputs struct {
	t        **testing.T
	handler  *http.Handler
	fail     *atomic.Bool
	requests *atomic.Int32
	expected *atomic.Pointer[manifest.Secret]
}

func testFetcherHTTPSBasicAuthSnapshotRotationCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherHTTPSBasicAuthSnapshotRotationCallbackInputs,
) {
	testCaseInput.requests.
		Add(1)
	user, pass, _ := r.BasicAuth()
	result1, _ := testCaseInput.expected.Load().Value("password")
	want := result1.Value
	if user != "alice" || pass != want {
		(*testCaseInput.t).
			Error("producer used different credential snapshot")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if testCaseInput.fail.Load() {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			(*testCaseInput.t).
				Error(err)
			return
		}
		if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
			(*testCaseInput.t).
				Error(err)
		}
		_ = conn.Close()
		return
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}

type gitTLSCase struct {
	name, url, secret, wantCA, wantCRT, wantKey, errorKey string
	data, encodedData                                     map[string]any
	wantInput, wantMissing                                bool
}

type gitCertificate struct {
	certificate tls.Certificate
	pem         string
}
type testFetcherResolveTLSTailInputs struct {
	err *error

	opts *git.
		CloneOptions

	tc *gitTLSCase
}

func testFetcherResolveTLSTail(t *testing.T, inputs *testFetcherResolveTLSTailInputs) {
	t.Helper()

	assert.NoError(t, (*inputs.err))
	assert.Equal(t, string(inputs.opts.CABundle), inputs.tc.wantCA)
	assert.Equal(t, string(inputs.opts.ClientCert), inputs.tc.wantCRT)
	assert.Equal(t, string(inputs.opts.ClientKey), inputs.tc.wantKey)
}

type testFetcherResolveTransportSecretMaterialCase1Part1Inputs struct {
	request *gitAcquisition
	err     error
	tc      **struct {
		name        string
		errorKey    string
		data        map[string]any
		encodedData map[string]any
		wantInput   bool
		wantMissing bool
		wantBearer  bool
		proxyRead   bool
	}
	repo **manifest.GitRepository
}
type testFetcherResolveTLSCaseInputs struct {
	tc       *gitTLSCase
	identity *string
}

func testFetcherResolveTLSCase(t *testing.T, inputs *testFetcherResolveTLSCaseInputs) {
	t.Helper()

	data := maps.Clone(inputs.tc.data)
	if data == nil {
		data = map[string]any{}
	}
	data["username"], data["password"], data["identity"] = "alice", "p", (*inputs.identity)
	signer, keyErr := ssh.ParsePrivateKey([]byte(*inputs.identity))
	assert.NoError(t, keyErr)
	data["known_hosts"] = "host " + string(ssh.MarshalAuthorizedKey(signer.PublicKey()))
	sec := &manifest.Secret{Name: "different", Namespace: "elsewhere", StringData: data, Data: inputs.tc.encodedData}
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return sec }}
	f := &Fetcher{}
	url := inputs.tc.url
	if url == "" {
		url = "https://example.com/x.git"
	}
	repo := gitRepoWithSecret("g", url, inputs.tc.secret)
	request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
		repo,
		&sourcecache.Policy{},
	)
	opts := request.options
	if inputs.tc.wantInput || inputs.tc.wantMissing {
		assert.Diff(t, opts, git.CloneOptions{})
		assert.Equal(t, errors.Is(err, manifest.ErrInput), inputs.tc.wantInput)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), inputs.tc.wantMissing)
		if inputs.tc.wantMissing {
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok,
				"expected MissingSecretError; got %v", err)

			assert.Equal(t, missing.Owner, repo.Named())
			assert.Equal(
				t,
				missing.Secret,
				manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "tls"},
			)
		} else {
			assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), inputs.tc.errorKey) ||
				!strings.Contains(err.Error(), "ns/tls") ||
				strings.Contains(err.Error(), manifest.DiagnosticFor(repo.Named(), repo).Label()),
				"missing input context: %v", err)
			assert.FatalIf(t, strings.Contains(err.Error(), "%%%fixture") ||
				strings.Contains(err.Error(), "credential-fixture"), "%s", "TLS credential payload leaked")
		}
		return
	}
	testFetcherResolveTLSTail(t, &testFetcherResolveTLSTailInputs{err: &err, opts: &opts, tc: inputs.tc})
}
