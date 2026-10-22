package git

import (
	"context"
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
	"io/fs"
	"maps"
	"math/big"
	"net"
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
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/server"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
	"github.com/home-operations/flate/pkg/store"
	"golang.org/x/crypto/ssh"
)

func tlsGitProxy(t *testing.T, authenticated bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var connects atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || authenticated && r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("proxy:password")) {
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
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, BasicConstraintsValid: true, IsCA: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func gitHTTPHandler(t *testing.T, root string) http.Handler {
	t.Helper()
	ep, err := transport.NewEndpoint(root + "/.git")
	if err != nil {
		t.Fatal(err)
	}
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
		if err := refs.Capabilities.Set(capability.Shallow); err != nil {
			t.Error(err)
			return
		}
		if err := refs.Capabilities.Set(capability.AllowReachableSHA1InWant); err != nil {
			t.Error(err)
			return
		}
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			encoder := pktline.NewEncoder(w)
			if err := encoder.EncodeString("# service=git-upload-pack\n"); err != nil {
				t.Error(err)
				return
			}
			if err := encoder.Flush(); err != nil {
				t.Error(err)
				return
			}
			if err := refs.Encode(w); err != nil {
				t.Error(err)
			}
			return
		}
		request := packp.NewUploadPackRequest()
		if err := request.Decode(r.Body); err != nil {
			t.Error(err)
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
				{name: "exact", ref: &sourcev1.GitRepositoryRef{Commit: head}},
				{name: "fallback", ref: &sourcev1.GitRepositoryRef{Commit: head}, refuse: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					cert, ca := tlsGitCertificate(t)
					crt, key := testutil.SelfSignedClientCert(t)
					clients := x509.NewCertPool()
					clients.AppendCertsFromPEM([]byte(crt))
					handler := gitHTTPHandler(t, root)
					var requests, advertisements, connections atomic.Int32
					srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == http.MethodGet {
							advertisements.Add(1)
						}
						if r.Method == http.MethodPost && requests.Add(1) == 1 && tc.refuse {
							http.Error(w, "exact request refused", http.StatusBadRequest)
							return
						}
						handler.ServeHTTP(w, r)
					}))
					srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clients}
					srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
						if state == http.StateNew {
							connections.Add(1)
						}
					}
					srv.StartTLS()
					t.Cleanup(srv.Close)
					proxy, connects := tlsGitProxy(t, route == "authenticated-proxy")
					fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
						if name == "proxy" {
							data := map[string]any{"address": proxy.URL}
							if route == "authenticated-proxy" {
								data["username"], data["password"] = "proxy", "password"
							}
							return &manifest.Secret{StringData: data}
						}
						return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca, "tls.crt": crt, "tls.key": key}}
					}}
					f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
					repo := gitRepoWithSecret("tls", srv.URL, "tls")
					repo.Reference, repo.RecurseSubmodules = tc.ref, tc.recurse
					if route != "direct" {
						repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
					}
					art, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
					if err != nil {
						_, fallback := f.fallback.Load(srv.URL)
						t.Fatalf("fetch: %v (requests=%d, fallback=%t)", err, requests.Load(), fallback)
					}
					if data, err := fs.ReadFile(art.FS, "hello.txt"); err != nil || string(data) != "hi" {
						t.Fatalf("content=%q, error=%v", data, err)
					}
					want := int32(1)
					if tc.refuse {
						want = 2
					}
					if requests.Load() != want {
						t.Fatalf("upload requests=%d, want %d", requests.Load(), want)
					}
					wantAdvertisements := int32(1)
					if tc.recurse || tc.refuse {
						wantAdvertisements = 2
					}
					if advertisements.Load() != wantAdvertisements {
						t.Errorf("initial advertisements=%d, want %d", advertisements.Load(), wantAdvertisements)
					}
					t.Logf("initial: advertisements=%d upload_pack=%d accepted_connections=%d proxy_CONNECT=%d", advertisements.Load(), requests.Load(), connections.Load(), connects.Load())
					_, err = f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("after compatible operation: advertisements=%d upload_pack=%d accepted_connections=%d proxy_CONNECT=%d", advertisements.Load(), requests.Load(), connections.Load(), connects.Load())
					if tc.ref == nil || tc.ref.Commit == "" {
						wantAdvertisements *= 2
						want *= 2
					}
					if advertisements.Load() != wantAdvertisements || requests.Load() != want {
						t.Errorf("two operations: advertisements=%d upload_pack=%d, want %d and %d", advertisements.Load(), requests.Load(), wantAdvertisements, want)
					}

				})
			}
		})
	}
}

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
		_, err := f.Fetch(t.Context(), gitRepoWithSecret("restricted", srv.URL, "tls"), fInputs.readSet(gitRepoWithSecret("restricted", srv.URL, "tls")), sourcecache.Policy{})
		if op, ok := errors.AsType[*net.OpError](err); !ok || op.Op != "dial" {
			t.Fatalf("fetch error = %v, want dial error", err)
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
				root := t.TempDir()
				cert, ca := tlsGitCertificate(t)
				parentHandler, moduleHandler := gitHTTPHandler(t, root), gitHTTPHandler(t, module)
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
				url := "file://" + module
				if child == "private-ca" {
					url = srv.URL + "/module"
				}
				mustInitRepoWithFiles(t, root, map[string]string{"hello.txt": "parent", ".gitmodules": "[submodule \"lib\"]\n\tpath = lib\n\turl = " + url + "\n"})
				r, err := git.PlainOpen(root)
				if err != nil {
					t.Fatal(err)
				}
				head, err := r.Head()
				if err != nil {
					t.Fatal(err)
				}
				commit, err := r.CommitObject(head.Hash())
				if err != nil {
					t.Fatal(err)
				}
				tree, err := commit.Tree()
				if err != nil {
					t.Fatal(err)
				}
				tree.Entries = append(tree.Entries, object.TreeEntry{Name: "lib", Mode: filemode.Submodule, Hash: moduleHash})
				encoded := r.Storer.NewEncodedObject()
				if err := tree.Encode(encoded); err != nil {
					t.Fatal(err)
				}
				commit.TreeHash, err = r.Storer.SetEncodedObject(encoded)
				if err != nil {
					t.Fatal(err)
				}
				commit.ParentHashes = []plumbing.Hash{head.Hash()}
				encoded = r.Storer.NewEncodedObject()
				if err := commit.Encode(encoded); err != nil {
					t.Fatal(err)
				}
				hash, err := r.Storer.SetEncodedObject(encoded)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Storer.SetReference(plumbing.NewHashReference(head.Name(), hash)); err != nil {
					t.Fatal(err)
				}
				fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
				}}
				f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
				repo := gitRepoWithSecret("recursive", srv.URL, "tls")
				repo.RecurseSubmodules = true
				if exact {
					repo.Reference = &sourcev1.GitRepositoryRef{Commit: hash.String()}
				}
				art, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
				if child == "private-ca" {
					if _, ok := errors.AsType[x509.UnknownAuthorityError](err); !ok {
						t.Fatalf("child inherited private CA: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if data, err := fs.ReadFile(art.FS, "lib/hello.txt"); err != nil || string(data) != "hi" {
					t.Fatalf("child content=%q, error=%v", data, err)
				}
			})
		}
	}
}

func TestFetcher_ConcurrentCustomTLS(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			root := t.TempDir()
			mustInitRepo(t, root)
			arrived := make(chan struct{}, concurrency)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			var workers sync.WaitGroup
			fInputs := testInputs{}
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New)}
			secrets := map[string]*manifest.Secret{}
			fInputs.secrets = func(_, name string) *manifest.Secret { return secrets[name] }
			repos := make([]*manifest.GitRepository, concurrency-1)
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
				srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clients}
				srv.Config.SetKeepAlivesEnabled(false)
				srv.StartTLS()
				t.Cleanup(srv.Close)
				name := fmt.Sprintf("ca%d", i)
				secrets[name] = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca, "tls.crt": crt, "tls.key": key}}
				repos[i] = gitRepoWithSecret(name, srv.URL, name)
				proxy, connects := tlsGitProxy(t, true)
				proxies[i] = connects
				secrets["proxy"+name] = &manifest.Secret{StringData: map[string]any{"address": proxy.URL, "username": "proxy", "password": "password"}}
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
					art, err := f.Fetch(ctx, repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
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
			for range repos {
				select {
				case <-arrived:
				case <-ctx.Done():
					t.Fatal("custom TLS acquisitions did not overlap")
				}
			}
			_, err := f.Fetch(ctx, gitRepoWithSecret("anonymous", repos[0].URL, ""), fInputs.readSet(gitRepoWithSecret("anonymous", repos[0].URL, "")), sourcecache.Policy{})
			if _, ok := errors.AsType[x509.UnknownAuthorityError](err); !ok {
				t.Fatalf("anonymous fetch inherited private trust: %v", err)
			}
			for _, name := range []string{"wrong-ca", "wrong-client"} {
				bad := gitRepoWithSecret(name, repos[0].URL, name)
				bad.ProxySecretRef = repos[0].ProxySecretRef
				if _, err := f.Fetch(ctx, bad, fInputs.readSet(bad), sourcecache.Policy{MaxAge: bad.SourceInterval()}); err == nil {
					t.Fatalf("%s fetch inherited another operation's credentials", name)
				}
			}
			for _, ref := range []string{"", mustHead(t, root)} {
				_, err := f.FetchRemoteBase(ctx, RemoteBaseRequest{URL: repos[0].URL, Ref: ref}, sourcecache.Policy{})
				if _, ok := errors.AsType[x509.UnknownAuthorityError](err); !ok {
					t.Fatalf("anonymous remote base inherited private trust: %v", err)
				}
			}
			unblock()
			for range repos {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			for _, connects := range proxies {
				if connects.Load() < 2 {
					t.Fatal("custom clone bypassed its proxy")
				}
			}
		})
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
	type tlsCase struct {
		name, url, secret, wantCA, wantCRT, wantKey, errorKey string
		data, encodedData                                     map[string]any
		wantInput, wantMissing                                bool
	}
	cases := []tlsCase{
		{name: "anonymous", encodedData: map[string]any{"ca.crt": "%%%fixture"}},
		{name: "ssh", url: "ssh://git@example.com/x.git", secret: "tls", encodedData: map[string]any{"ca.crt": "%%%fixture"}},
		{name: "scp", url: "git@example.com:x.git", secret: "tls", data: map[string]any{"ca.crt": "invalid"}},
		{name: "git-at-path", url: "git://h/x@y", secret: "tls", data: map[string]any{"ca.crt": ca}, wantCA: ca},
		{name: "auth-only", secret: "tls", data: map[string]any{"username": "alice", "password": "p"}},
		{name: "empty TLS", secret: "tls", data: map[string]any{"tls.crt": "", "tls.key": "", "ca.crt": "", "caFile": ""}},
		{name: "ca.crt", secret: "tls", data: map[string]any{"ca.crt": ca}, wantCA: ca},
		{name: "caFile", secret: "tls", data: map[string]any{"caFile": ca}, wantCA: ca},
		{name: "ca.crt-precedence", secret: "tls", data: map[string]any{"ca.crt": ca, "caFile": "invalid"}, wantCA: ca},
		{name: "ca.crt over wiped caFile", secret: "tls", data: map[string]any{"ca.crt": ca, "caFile": "..PLACEHOLDER_caFile.."}, wantCA: ca},
		{name: "mtls", secret: "tls", data: map[string]any{"ca.crt": ca, "tls.crt": crt, "tls.key": key}, wantCA: ca, wantCRT: crt, wantKey: key},
		{name: "unpaired", secret: "tls", data: map[string]any{"tls.crt": crt}, wantInput: true},
		{name: "invalid-ca", secret: "tls", data: map[string]any{"ca.crt": "invalid"}, wantInput: true},
		{name: "invalid-client", secret: "tls", data: map[string]any{"tls.crt": "invalid", "tls.key": "invalid"}, wantInput: true},
		{name: "wiped client invalid CA", secret: "tls", data: map[string]any{"tls.crt": "..PLACEHOLDER_crt..", "ca.crt": "invalid"}, wantInput: true, errorKey: "ca.crt"},
		{name: "invalid client wiped key", secret: "tls", data: map[string]any{"tls.crt": "invalid", "tls.key": "..PLACEHOLDER_key.."}, wantInput: true, errorKey: "tls.crt"},
		{name: "valid client wiped key", secret: "tls", data: map[string]any{"tls.crt": crt, "tls.key": "..PLACEHOLDER_key.."}, wantMissing: true},
		{name: "wiped crt malformed key", secret: "tls", data: map[string]any{"tls.crt": "..PLACEHOLDER_crt.."}, encodedData: map[string]any{"tls.key": "%%%fixture"}, wantInput: true, errorKey: "tls.key"},
		{name: "wiped key malformed ca", secret: "tls", data: map[string]any{"tls.key": "..PLACEHOLDER_key.."}, encodedData: map[string]any{"ca.crt": "%%%fixture"}, wantInput: true, errorKey: "ca.crt"},
		{name: "wiped ca malformed caFile", secret: "tls", data: map[string]any{"ca.crt": "..PLACEHOLDER_ca.."}, encodedData: map[string]any{"caFile": "%%%fixture"}, wantInput: true, errorKey: "caFile"},
		{name: "caFile decoding despite valid ca", secret: "tls", data: map[string]any{"ca.crt": ca}, encodedData: map[string]any{"caFile": "%%%fixture"}, wantInput: true, errorKey: "caFile"},
		{name: "first decoding error wins", secret: "tls", encodedData: map[string]any{"tls.crt": "%%%fixture", "tls.key": "%%%fixture", "ca.crt": "%%%fixture", "caFile": "%%%fixture"}, wantInput: true, errorKey: "tls.crt"},
	}
	for _, field := range []string{"tls.crt", "tls.key", "ca.crt", "caFile"} {
		cases = append(cases, tlsCase{name: field + "/wiped", secret: "tls", data: map[string]any{field: "..PLACEHOLDER_" + field + ".."}, wantMissing: true})
		for _, malformed := range []struct {
			name  string
			value any
		}{
			{"encoding", "%%%fixture"}, {"nonscalar", map[string]any{"credential-fixture": "payload"}},
		} {
			cases = append(cases, tlsCase{name: field + "/" + malformed.name, secret: "tls", encodedData: map[string]any{field: malformed.value}, wantInput: true, errorKey: field})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := maps.Clone(tc.data)
			if data == nil {
				data = map[string]any{}
			}
			data["username"], data["password"], data["identity"] = "alice", "p", identity
			sec := &manifest.Secret{Name: "different", Namespace: "elsewhere", StringData: data, Data: tc.encodedData}
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return sec }}
			f := &Fetcher{}
			url := tc.url
			if url == "" {
				url = "https://example.com/x.git"
			}
			repo := gitRepoWithSecret("g", url, tc.secret)
			request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(repo)
			opts := request.options
			if tc.wantInput || tc.wantMissing {
				assert.Diff(t, opts, git.CloneOptions{})
				assert.Equal(t, errors.Is(err, manifest.ErrInput), tc.wantInput)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), tc.wantMissing)
				if tc.wantMissing {
					missing, ok := errors.AsType[*source.MissingSecretError](err)
					if !ok {
						t.Fatalf("expected MissingSecretError; got %v", err)
					}
					assert.Equal(t, missing.Owner, repo.Named())
					assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "tls"})
				} else {
					if err == nil || !strings.Contains(err.Error(), tc.errorKey) || !strings.Contains(err.Error(), "ns/tls") || !strings.Contains(err.Error(), source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name)) {
						t.Fatalf("missing input context: %v", err)
					}
					if strings.Contains(err.Error(), "%%%fixture") || strings.Contains(err.Error(), "credential-fixture") {
						t.Fatal("TLS credential payload leaked")
					}
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, string(opts.CABundle), tc.wantCA)
			assert.Equal(t, string(opts.ClientCert), tc.wantCRT)
			assert.Equal(t, string(opts.ClientKey), tc.wantKey)
		})
	}
}

func TestFetcher_ResolveTransport_EndpointValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, secret string
		wantInput    bool
	}{
		{name: "without secretRef"},
		{name: "with secretRef", secret: "creds", wantInput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p"}}
			}}
			f := &Fetcher{}
			repo := gitRepoWithSecret("g", "http://[::1", tc.secret)
			request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(repo)}).resolveTransport(repo)
			if tc.wantInput {
				assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
				if err == nil || !strings.Contains(err.Error(), "parse git endpoint") {
					t.Fatalf("missing endpoint validation: %v", err)
				}
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
		name, errorKey                     string
		data, encodedData                  map[string]any
		wantInput, wantMissing, wantBearer bool
	}{
		{name: "hard auth before hard TLS", encodedData: map[string]any{"bearerToken": "%%%fixture", "caFile": "%%%fixture"}, wantInput: true, errorKey: "bearerToken"},
		{name: "hard auth before missing TLS", data: map[string]any{"ca.crt": "..PLACEHOLDER_ca.."}, encodedData: map[string]any{"bearerToken": "%%%fixture"}, wantInput: true, errorKey: "bearerToken"},
		{name: "missing auth before absent TLS", data: map[string]any{"username": "u"}, wantMissing: true, errorKey: "username/password"},
		{name: "missing auth before missing TLS", data: map[string]any{"bearerToken": "..PLACEHOLDER_token..", "ca.crt": "..PLACEHOLDER_ca.."}, wantMissing: true, errorKey: "bearerToken"},
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
			sec := &manifest.Secret{Name: "different", Namespace: "elsewhere", StringData: tc.data, Data: tc.encodedData}
			s := store.New()
			captured := *sec
			captured.Name, captured.Namespace = "creds", "ns"
			s.AddObject(&captured)
			s.AddObject(&manifest.Secret{Name: "proxy", Namespace: "ns", StringData: map[string]any{"address": "http://proxy.invalid/%"}})

			f := &Fetcher{}
			repo := gitRepoWithSecret("g", "https://host/repo", "creds")
			if tc.wantInput || tc.wantMissing {
				repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
			}
			var reads store.ReadSet
			reads.Init(s, repo.Named(), nil)
			request, err := (&operation{Fetcher: f, reads: &reads, secrets: source.SecretLookup(&reads)}).resolveTransport(repo)
			s.AddObject(&manifest.Secret{Name: "proxy", Namespace: "ns", StringData: map[string]any{"address": "http://changed.invalid"}})
			assert.Equal(t, reads.Valid(), true)
			s.AddObject(&manifest.Secret{Name: "creds", Namespace: "ns"})
			assert.Equal(t, reads.Valid(), false)
			if tc.wantInput || tc.wantMissing {
				assert.Equal(t, request.spec == nil, true)
				assert.Equal(t, request.material == nil, true)
				assert.Diff(t, request.options, git.CloneOptions{})
				assert.Equal(t, errors.Is(err, manifest.ErrInput), tc.wantInput)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
				assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), tc.wantMissing)
				if err == nil || !strings.Contains(err.Error(), tc.errorKey) || !strings.Contains(err.Error(), "ns/creds") || !strings.Contains(err.Error(), source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name)) {
					t.Fatalf("wrong selected error context: %v", err)
				}
				if strings.Contains(err.Error(), "%%%fixture") || strings.Contains(err.Error(), "credential-fixture") {
					t.Fatal("credential payload leaked")
				}
				if tc.wantMissing {
					missing, ok := errors.AsType[*source.MissingSecretError](err)
					if !ok {
						t.Fatalf("expected MissingSecretError; got %v", err)
					}
					assert.Equal(t, missing.Owner, repo.Named())
					assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"})
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, request.spec, repo)
			assert.Equal(t, string(request.options.CABundle), ca)
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
		})
	}
}

func TestFetcher_ResolveTransport_MalformedProxy(t *testing.T) {
	t.Parallel()
	ca := testutil.SelfSignedCA(t)
	fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
		if name == "proxy" {
			return &manifest.Secret{StringData: map[string]any{"address": "http://proxy-user:proxy-password@proxy.invalid/%"}}
		}
		return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
	}}
	f := &Fetcher{}
	repo := gitRepoWithSecret("tls", "https://example.com/x.git", "tls")
	repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	_, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("proxy validation before cache admission: %v", err)
	}
	if strings.Contains(err.Error(), "proxy-user") || strings.Contains(err.Error(), "proxy-password") {
		t.Fatal("proxy credentials leaked in validation error")
	}
}

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
		requests.Add(1)
		user, pass, _ := r.BasicAuth()
		want, _, _ := expected.Load().Value("password")
		if user != "alice" || pass != want {
			t.Error("producer used different credential snapshot")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if fail.Load() {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
				t.Error(err)
			}
			_ = conn.Close()
			return
		}
		handler.ServeHTTP(w, r)
	}))
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
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
			if wave == "password" {
				sec = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "second", "ca.crt": ca}}
			}
			if wave == "certificate" {
				sec = &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "second", "ca.crt": testutil.SelfSignedCA(t)}}
			}
			expected.Store(sec)
			proxyURL := strings.Replace(proxy.URL, "://", "://proxy:first@", 1)
			if wave == "proxy" || wave == "direct" {
				proxyURL = strings.Replace(proxy.URL, "://", "://proxy:second@", 1)
			}
			t.Setenv("http_proxy", proxyURL)
			if wave == "direct" {
				t.Setenv("no_proxy", "*")
			}
			c := cachetest.New(t, cacheRoot, sourcecache.New)
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { lookups.Add(1); return sec }}
			f := &Fetcher{Cache: c}
			before := requests.Load()
			art, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval(), Retry: sourcecache.RetryConfig{Attempts: 3}})
			if err != nil {
				t.Fatal(err)
			}
			if art.Key == previous || requests.Load() <= before {
				t.Fatal("changed material reused an acquisition")
			}
			previous = art.Key
			before = requests.Load()
			if _, err := f.Fetch(t.Context(), repo, fInputs.readSet(repo), sourcecache.Policy{MaxAge: repo.SourceInterval()}); err != nil || requests.Load() != before {
				t.Fatalf("warm snapshot missed: %v", err)
			}
			fail.Store(true)
			failed := *repo
			failed.Interval.Duration = 0
			_, err = f.Fetch(t.Context(), &failed, fInputs.readSet(&failed), sourcecache.Policy{MaxAge: failed.SourceInterval()})
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !terminal.Attempted {
				t.Fatalf("failed snapshot: %v", err)
			}
			fail.Store(false)

		})
	}
	if lookups.Load() != 15 {
		t.Fatalf("auth and TLS reread Secrets: %d", lookups.Load())
	}
}
