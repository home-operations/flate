package git

import (
	"context"
	"crypto/ecdsa"
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

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

func tlsGitProxy(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var connects atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("proxy:password")) {
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
	root := t.TempDir()
	mustInitRepo(t, root)
	head := mustTagHEAD(t, root, "v1.0.0")
	for _, tc := range []struct {
		name            string
		ref             *manifest.GitRepositoryRef
		recurse, refuse bool
	}{
		{name: "clone"},
		{name: "branch", ref: &manifest.GitRepositoryRef{Branch: "master"}},
		{name: "tag", ref: &manifest.GitRepositoryRef{Tag: "v1.0.0"}},
		{name: "semver", ref: &manifest.GitRepositoryRef{SemVer: ">=1.0.0"}},
		{name: "named", ref: &manifest.GitRepositoryRef{Name: "refs/tags/v1.0.0"}},
		{name: "clone-fetch", ref: &manifest.GitRepositoryRef{Name: "refs/tags/v1.0.0"}, recurse: true},
		{name: "exact", ref: &manifest.GitRepositoryRef{Commit: head}},
		{name: "fallback", ref: &manifest.GitRepositoryRef{Commit: head}, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert, ca := tlsGitCertificate(t)
			crt, key := testutil.SelfSignedClientCert(t)
			clients := x509.NewCertPool()
			clients.AppendCertsFromPEM([]byte(crt))
			handler := gitHTTPHandler(t, root)
			var requests atomic.Int32
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && requests.Add(1) == 1 && tc.refuse {
					http.Error(w, "exact request refused", http.StatusBadRequest)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clients}
			srv.Config.SetKeepAlivesEnabled(false)
			srv.StartTLS()
			t.Cleanup(srv.Close)
			proxy, connects := tlsGitProxy(t)
			f := &Fetcher{Cache: newGitCache(t, ""), Secrets: func(_, name string) *manifest.Secret {
				if name == "proxy" {
					return &manifest.Secret{StringData: map[string]any{"address": proxy.URL, "username": "proxy", "password": "password"}}
				}
				return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca, "tls.crt": crt, "tls.key": key}}
			}}
			repo := gitRepoWithSecret("tls", srv.URL, "tls")
			repo.Reference, repo.RecurseSubmodules = tc.ref, tc.recurse
			repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
			art, err := f.Fetch(t.Context(), repo)
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
			if requests.Load() != want || connects.Load() < 2 {
				t.Fatalf("upload requests=%d, want %d; proxy connects=%d", requests.Load(), want, connects.Load())
			}
		})
	}
}

func TestFetcher_RestrictEgress(t *testing.T) {
	t.Run("custom-ca", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.NotFoundHandler())
		defer srv.Close()
		ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
		f := &Fetcher{Cache: newGitCache(t, ""), Secrets: func(_, _ string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
		}}
		ssrfguard.Restrict(true)
		defer ssrfguard.Restrict(false)
		_, err := f.Fetch(t.Context(), gitRepoWithSecret("restricted", srv.URL, "tls"))
		if op, ok := errors.AsType[*net.OpError](err); !ok || op.Op != "dial" {
			t.Fatalf("fetch error = %v, want dial error", err)
		}
	})
}

func TestFetcher_RecursiveTLSDefaults(t *testing.T) {
	module := t.TempDir()
	mustInitRepo(t, module)
	moduleHash := plumbing.NewHash(mustHead(t, module))
	for _, child := range []string{"file", "private-ca"} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exact=%t", child, exact), func(t *testing.T) {
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
				f := &Fetcher{Cache: newGitCache(t, ""), Secrets: func(_, _ string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
				}}
				repo := gitRepoWithSecret("recursive", srv.URL, "tls")
				repo.RecurseSubmodules = true
				if exact {
					repo.Reference = &manifest.GitRepositoryRef{Commit: hash.String()}
				}
				art, err := f.Fetch(t.Context(), repo)
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
			f := &Fetcher{Cache: newGitCache(t, "")}
			secrets := map[string]*manifest.Secret{}
			f.Secrets = func(_, name string) *manifest.Secret { return secrets[name] }
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
				proxy, connects := tlsGitProxy(t)
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
					art, err := f.Fetch(ctx, repo)
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
			_, err := f.Fetch(ctx, gitRepoWithSecret("anonymous", repos[0].URL, ""))
			if _, ok := errors.AsType[x509.UnknownAuthorityError](err); !ok {
				t.Fatalf("anonymous fetch inherited private trust: %v", err)
			}
			for _, name := range []string{"wrong-ca", "wrong-client"} {
				bad := gitRepoWithSecret(name, repos[0].URL, name)
				bad.ProxySecretRef = repos[0].ProxySecretRef
				if _, err := f.Fetch(ctx, bad); err == nil {
					t.Fatalf("%s fetch inherited another operation's credentials", name)
				}
			}
			for _, ref := range []string{"", mustHead(t, root)} {
				_, err := f.FetchRemoteBase(ctx, RemoteBaseRequest{URL: repos[0].URL, Ref: ref})
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
	ca := testutil.SelfSignedCA(t)
	crt, key := testutil.SelfSignedClientCert(t)
	for _, tc := range []struct {
		name, url, secret, wantCA, wantCRT, wantKey, wantError string
		data                                                   map[string]any
	}{
		{name: "anonymous"},
		{name: "ssh", url: "ssh://git@example.com/x.git", secret: "tls", data: map[string]any{"ca.crt": "invalid"}},
		{name: "auth-only", secret: "tls", data: map[string]any{"username": "alice", "password": "p"}},
		{name: "ca.crt", secret: "tls", data: map[string]any{"ca.crt": ca}, wantCA: ca},
		{name: "caFile", secret: "tls", data: map[string]any{"caFile": ca}, wantCA: ca},
		{name: "ca.crt-precedence", secret: "tls", data: map[string]any{"ca.crt": ca, "caFile": "invalid"}, wantCA: ca},
		{name: "mtls", secret: "tls", data: map[string]any{"ca.crt": ca, "tls.crt": crt, "tls.key": key}, wantCA: ca, wantCRT: crt, wantKey: key},
		{name: "unpaired", secret: "tls", data: map[string]any{"tls.crt": crt}, wantError: "tls.crt and tls.key"},
		{name: "invalid-ca", secret: "tls", data: map[string]any{"ca.crt": "invalid"}, wantError: "did not parse as PEM"},
		{name: "invalid-client", secret: "tls", data: map[string]any{"tls.crt": "invalid", "tls.key": "invalid"}, wantError: "parse tls.crt/tls.key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &Fetcher{Secrets: func(_, _ string) *manifest.Secret { return &manifest.Secret{StringData: tc.data} }}
			url := tc.url
			if url == "" {
				url = "https://example.com/x.git"
			}
			opts, err := f.resolveTLS(gitRepoWithSecret("g", url, tc.secret))
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("material validation: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(opts.CABundle) != tc.wantCA || string(opts.ClientCert) != tc.wantCRT || string(opts.ClientKey) != tc.wantKey {
				t.Fatal("TLS material was not preserved")
			}
		})
	}
}

func TestFetcher_ResolveTransport_MalformedProxy(t *testing.T) {
	ca := testutil.SelfSignedCA(t)
	f := &Fetcher{Secrets: func(_, name string) *manifest.Secret {
		if name == "proxy" {
			return &manifest.Secret{StringData: map[string]any{"address": "http://proxy-user:proxy-password@proxy.invalid/%"}}
		}
		return &manifest.Secret{StringData: map[string]any{"username": "alice", "password": "p", "ca.crt": ca}}
	}}
	repo := gitRepoWithSecret("tls", "https://example.com/x.git", "tls")
	repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
	_, err := f.Fetch(t.Context(), repo)
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("proxy validation before cache admission: %v", err)
	}
	if strings.Contains(err.Error(), "proxy-user") || strings.Contains(err.Error(), "proxy-password") {
		t.Fatal("proxy credentials leaked in validation error")
	}
}
