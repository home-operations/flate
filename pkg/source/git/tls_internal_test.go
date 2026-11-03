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

func tlsGitCertificate(t *testing.T) (tls.Certificate, string) {
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

	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}, string(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	)
}

func gitHTTPHandler(t *testing.T, root string) http.Handler {
	t.Helper()
	ep, err := transport.NewEndpoint(root + "/.git")
	assert.NoError(t, err)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})
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
			sourcecache.Policy{},
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
	crt, key := testutil.SelfSignedClientCert(t)
	_, private, err := ed25519.GenerateKey(nil)
	assert.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(private, "fixture")
	assert.NoError(t, err)
	identity := string(pem.EncodeToMemory(block))

	cases := []testFetcherResolveTLSTlsCase{
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
		{
			name:   "ca.crt over wiped caFile",
			secret: "tls",
			data:   map[string]any{"ca.crt": ca, "caFile": "..PLACEHOLDER_caFile.."},
			wantCA: ca,
		},
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
			name:      "wiped client invalid CA",
			secret:    "tls",
			data:      map[string]any{"tls.crt": "..PLACEHOLDER_crt..", "ca.crt": "invalid"},
			wantInput: true,
			errorKey:  "ca.crt",
		},
		{
			name:      "invalid client wiped key",
			secret:    "tls",
			data:      map[string]any{"tls.crt": "invalid", "tls.key": "..PLACEHOLDER_key.."},
			wantInput: true,
			errorKey:  "tls.crt",
		},
		{
			name:        "valid client wiped key",
			secret:      "tls",
			data:        map[string]any{"tls.crt": crt, "tls.key": "..PLACEHOLDER_key.."},
			wantMissing: true,
		},
		{
			name:        "wiped crt malformed key",
			secret:      "tls",
			data:        map[string]any{"tls.crt": "..PLACEHOLDER_crt.."},
			encodedData: map[string]any{"tls.key": "%%%fixture"},
			wantInput:   true,
			errorKey:    "tls.key",
		},
		{
			name:        "wiped key malformed ca",
			secret:      "tls",
			data:        map[string]any{"tls.key": "..PLACEHOLDER_key.."},
			encodedData: map[string]any{"ca.crt": "%%%fixture"},
			wantInput:   true,
			errorKey:    "ca.crt",
		},
		{
			name:        "wiped ca malformed caFile",
			secret:      "tls",
			data:        map[string]any{"ca.crt": "..PLACEHOLDER_ca.."},
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
	}
	for _, field := range []string{"tls.crt", "tls.key", "ca.crt", "caFile"} {
		cases = append(
			cases,
			testFetcherResolveTLSTlsCase{
				name:        field + "/wiped",
				secret:      "tls",
				data:        map[string]any{field: "..PLACEHOLDER_" + field + ".."},
				wantMissing: true,
			},
		)
		for _, malformed := range []struct {
			name  string
			value any
		}{
			{"encoding", "%%%fixture"}, {"nonscalar", map[string]any{"credential-fixture": "payload"}},
		} {
			cases = append(
				cases,
				testFetcherResolveTLSTlsCase{
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
			testFetcherResolveTLSCase(t, testFetcherResolveTLSCaseInputs{identity: &identity, tc: &tc})
		},
		)
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
				sourcecache.Policy{},
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
		{name: "hard auth before missing TLS", data: map[string]any{"ca.crt": "..PLACEHOLDER_ca.."}, encodedData: map[string]any{"bearerToken": "%%%fixture"}, wantInput: true, errorKey: "bearerToken"},
		{name: "malformed proxy before unavailable auth", data: map[string]any{"username": "u"}, wantInput: true, proxyRead: true, errorKey: "proxy"},
		{name: "missing TLS before unavailable auth", data: map[string]any{"bearerToken": "..PLACEHOLDER_token..", "ca.crt": "..PLACEHOLDER_ca.."}, wantMissing: true, errorKey: "TLS material"},
		{name: "hard TLS before missing auth", data: map[string]any{"username": "u"}, encodedData: map[string]any{"ca.crt": "%%%fixture"}, wantInput: true, errorKey: "ca.crt"},
		{name: "real TLS before wiped auth", data: map[string]any{"bearerToken": "..PLACEHOLDER_token..", "ca.crt": "invalid"}, wantInput: true, errorKey: "ca.crt"},
		{name: "valid auth hard TLS", data: map[string]any{"username": "u", "password": "p"}, encodedData: map[string]any{"caFile": "%%%fixture"}, wantInput: true, errorKey: "caFile"},
		{name: "valid auth missing TLS", data: map[string]any{"username": "u", "password": "p", "ca.crt": "..PLACEHOLDER_ca.."}, wantMissing: true, errorKey: "TLS material"},
		{name: "empty bearer uses malformed basic", data: map[string]any{"bearerToken": ""}, encodedData: map[string]any{"username": "%%%fixture"}, wantInput: true, errorKey: "username"},
		{name: "valid basic", data: map[string]any{"username": "u", "password": "p", "ca.crt": ca}},
		{name: "valid bearer ignores malformed basic", data: map[string]any{"bearerToken": "fixture-token", "ca.crt": ca}, encodedData: map[string]any{"username": "%%%fixture", "password": map[string]any{"credential-fixture": "payload"}}, wantBearer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherResolveTransportSecretMaterialCase(
				t,
				&ca, &tc,
			)
		},
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
	_, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
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
	if lookups.Load() != 15 {
		t.Fatalf("auth and TLS reread Secrets: %d", lookups.Load())
	}
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
	want, _, _ := testCaseInput.expected.Load().Value("password")
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

type testFetcherResolveTLSTlsCase struct {
	name, url, secret, wantCA, wantCRT, wantKey, errorKey string
	data, encodedData                                     map[string]any
	wantInput, wantMissing                                bool
}
type testFetcherResolveTLSCaseInputs struct {
	identity *string
	tc       *testFetcherResolveTLSTlsCase
}

func testFetcherResolveTLSCase(t *testing.T, testCaseInput testFetcherResolveTLSCaseInputs) {
	t.Helper()

	data := maps.Clone(testCaseInput.tc.data)
	if data == nil {
		data = map[string]any{}
	}
	data["username"], data["password"], data["identity"] = "alice", "p", (*testCaseInput.identity)
	sec := &manifest.Secret{
		Name:       "different",
		Namespace:  "elsewhere",
		StringData: data,
		Data:       testCaseInput.tc.encodedData,
	}
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return sec }}
	f := &Fetcher{}
	url := testCaseInput.tc.url
	if url == "" {
		url = "https://example.com/x.git"
	}
	repo := gitRepoWithSecret("g", url, testCaseInput.tc.secret)
	request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(
		repo,
		sourcecache.Policy{},
	)
	opts := request.options
	if testCaseInput.tc.wantInput || testCaseInput.tc.wantMissing {
		assert.Diff(t, opts, git.CloneOptions{})
		assert.Equal(t, errors.Is(err, manifest.ErrInput), testCaseInput.tc.wantInput)
		assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), testCaseInput.tc.wantMissing)
		if testCaseInput.tc.wantMissing {
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok, "expected MissingSecretError; got %v", err)
			assert.Equal(t, missing.Owner, repo.Named())
			assert.Equal(
				t,
				missing.Secret,
				manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "tls"},
			)
		} else {
			assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), testCaseInput.tc.errorKey) ||
				!strings.Contains(err.Error(), "ns/tls") ||
				strings.Contains(
					err.Error(),
					manifest.DiagnosticFor(repo.Named(), repo).Label(),
				), "missing input context: %v", err)
			assert.FatalIf(t, strings.Contains(err.Error(), "%%%fixture") ||
				strings.Contains(err.Error(), "credential-fixture"), "%s", "TLS credential payload leaked")
		}
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, string(opts.CABundle), testCaseInput.tc.wantCA)
	assert.Equal(t, string(opts.ClientCert), testCaseInput.tc.wantCRT)
	assert.Equal(t, string(opts.ClientKey), testCaseInput.tc.wantKey)
}
