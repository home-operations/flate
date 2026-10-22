package bucket

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
)

func newTestNetwork(tb testing.TB) *source.Network {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	return network
}

func TestFetcher_ResolveTransport(t *testing.T) {
	t.Parallel()
	for _, cert := range []bool{false, true} {
		t.Run(fmt.Sprint(cert), func(t *testing.T) {
			t.Parallel()
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			var srv *httptest.Server
			if cert {
				srv = httptest.NewTLSServer(handler)
			} else {
				srv = httptest.NewServer(handler)
			}
			t.Cleanup(srv.Close)
			fInputs := testInputs{}
			f := &Fetcher{Network: newTestNetwork(t)}
			b := &manifest.Bucket{Name: "b", Namespace: "ns", Endpoint: srv.URL, BucketName: "fixture", Region: "us-east-1", Insecure: !cert}
			if cert {
				b.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
				ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
				fInputs.secrets = func(_, _ string) *manifest.Secret { return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}} }
			}
			client, _, err := (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).newMinioClient(b)
			assert.NoError(t, err)
			exists, err := client.BucketExists(t.Context(), "fixture")
			assert.NoError(t, err)
			assert.Equal(t, exists, true)
		})
	}
}

func TestFetcher_SecretReferences(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, ref, secret, wantError string
		data, rawData                map[string]any
		unwired, encoded, missing    bool
		field                        string
	}{
		{name: "auth unwired", ref: "auth", secret: "creds", unwired: true, wantError: "SecretGetter"},
		{name: "auth missing key", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "k"}, missing: true, wantError: "missing accesskey/secretkey"},
		{name: "accesskey/encoded=false", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "..PLACEHOLDER_accesskey..", "secretkey": "secret"}, missing: true},
		{name: "accesskey/encoded=true", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "..PLACEHOLDER_accesskey..", "secretkey": "secret"}, encoded: true, missing: true},
		{name: "secretkey/encoded=false", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "access", "secretkey": "..PLACEHOLDER_secretkey.."}, missing: true},
		{name: "secretkey/encoded=true", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "access", "secretkey": "..PLACEHOLDER_secretkey.."}, encoded: true, missing: true},
		{name: "auth absent", ref: "auth", secret: "creds", missing: true, wantError: "secret ns/creds not found"},
		{name: "auth empty", ref: "auth", secret: "creds", data: map[string]any{}, missing: true},
		{name: "malformed accesskey", ref: "auth", secret: "creds", rawData: map[string]any{"accesskey": "credential-payload!"}, data: map[string]any{"secretkey": "secret"}, field: "accesskey"},
		{name: "malformed secretkey", ref: "auth", secret: "creds", rawData: map[string]any{"secretkey": "credential-payload!"}, data: map[string]any{"accesskey": "access"}, field: "secretkey"},
		{name: "nonscalar accesskey", ref: "auth", secret: "creds", data: map[string]any{"accesskey": []any{"credential-payload"}, "secretkey": "secret"}, field: "accesskey"},
		{name: "nonscalar secretkey", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "access", "secretkey": map[string]any{"secret": "credential-payload"}}, field: "secretkey"},
		{name: "missing accesskey malformed secretkey", ref: "auth", secret: "creds", rawData: map[string]any{"secretkey": "credential-payload!"}, field: "secretkey"},
		{name: "wiped accesskey malformed secretkey", ref: "auth", secret: "creds", data: map[string]any{"accesskey": "..PLACEHOLDER_accesskey.."}, rawData: map[string]any{"secretkey": "credential-payload!"}, field: "secretkey"},
		{name: "malformed accesskey wiped secretkey", ref: "auth", secret: "creds", rawData: map[string]any{"accesskey": "credential-payload!"}, data: map[string]any{"secretkey": "..PLACEHOLDER_secretkey.."}, field: "accesskey"},
		{name: "both malformed choose accesskey", ref: "auth", secret: "creds", rawData: map[string]any{"accesskey": "credential-payload!", "secretkey": "credential-payload!"}, field: "accesskey"},
		{name: "cert partial pair", ref: "cert", secret: "tls", data: map[string]any{"tls.crt": "-only-cert-"}, wantError: "must provide both"},
		{name: "cert empty", ref: "cert", secret: "tls", data: map[string]any{"unrelated": "x"}, missing: true},
		{name: "cert absent", ref: "cert", secret: "missing", missing: true},
		{name: "proxy absent", ref: "proxy", secret: "missing", missing: true},
		{name: "cert unwired", ref: "cert", secret: "tls", unwired: true, wantError: "source.SecretGetter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fInputs := testInputs{}
			f := &Fetcher{Network: newTestNetwork(t)}
			if !tc.unwired {
				fInputs.secrets = func(_, _ string) *manifest.Secret {
					if tc.data == nil && tc.rawData == nil {
						return nil
					}
					sec := &manifest.Secret{Name: "metadata-secret", Namespace: "elsewhere", StringData: tc.data, Data: tc.rawData}
					if tc.encoded {
						sec.StringData, sec.Data = nil, map[string]any{}
						for k, v := range tc.data {
							sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
						}
					}
					return sec
				}
			}
			b := &manifest.Bucket{Name: "b", Namespace: "ns", BucketName: "fixture", Endpoint: "127.0.0.1:1", Insecure: true}
			ref := &manifest.LocalObjectReference{Name: tc.secret}
			var err error
			switch tc.ref {
			case "auth":
				b.SecretRef = ref
				creds, credentialErr := (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).resolveCredentials(b)
				if creds != nil || credentialErr == nil {
					t.Fatal("credential failure returned credentials or no error")
				}
				reads := fInputs.readSet(b)
				if tc.unwired {
					reads = nil
				}
				artifact, fetchErr := f.Fetch(t.Context(), b, reads, cache.Policy{MaxAge: b.SourceInterval()})
				if artifact != nil {
					t.Fatal("credential failure returned an acquired artifact")
				}
				err = fetchErr
			case "cert":
				b.CertSecretRef = ref
				_, _, err = (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).newMinioClient(b)
			case "proxy":
				b.ProxySecretRef = ref
				_, _, err = (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).newMinioClient(b)
			}
			if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("missing=%t, error=%v, want %q", tc.missing, err, tc.wantError)
			}
			if tc.field != "" {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("expected hard input error, got %v", err)
				}
				for _, context := range []string{"Bucket ns/b", "secret ns/creds", "[" + tc.field + "]"} {
					if !strings.Contains(err.Error(), context) {
						t.Errorf("input error missing context %q: %v", context, err)
					}
				}
				if strings.Contains(err.Error(), "credential-payload") {
					t.Error("input error exposed credential payload")
				}
			}
			if tc.missing {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok {
					t.Fatalf("expected MissingSecretError; got %T", err)
				}
				owner := b.Named()
				if tc.ref != "auth" {
					owner.Name = "ns/b"
				}
				assert.Equal(t, missing.Owner, owner)
				assert.Equal(t, missing.Secret, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: tc.secret})
				if tc.ref != "auth" && tc.data == nil {
					assert.Equal(t, err.Error(), manifest.ErrMissingSecret.Error()+": Bucket ns/ns/b: secret ns/missing not found")
				}
			}
		})
	}
	t.Run("successful credentials", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name string
			auth bool
		}{
			{"anonymous", false},
			{"valid", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				b := &manifest.Bucket{Name: "b", Namespace: "ns", Endpoint: "https://bucket.invalid", BucketName: "fixture"}
				if tc.auth {
					b.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				}
				fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
					if !tc.auth {
						t.Error("anonymous credentials consulted a Secret")
					}
					return &manifest.Secret{StringData: map[string]any{"accesskey": "access", "secretkey": "secret"}}
				}}
				f := &Fetcher{}
				creds, err := (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).resolveCredentials(b)
				assert.NoError(t, err)
				value, err := creds.GetWithContext(nil)
				assert.NoError(t, err)
				access, secret := "", ""
				if tc.auth {
					access, secret = "access", "secret"
				}
				if value.AccessKeyID != access || value.SecretAccessKey != secret {
					t.Error("credential values were not preserved")
				}
			})
		}
	})
}

func TestFetcher_NetworkSuppressionSkipsBackoff(t *testing.T) {

	var network source.Network
	var calls atomic.Int32
	testutil.StubDNS(t, func(context.Context, string) error { calls.Add(1); return os.ErrDeadlineExceeded })
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{})
	assert.NoError(t, err)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://bucket.test", nil)
	_, err = (&http.Client{Transport: tr}).Do(req)
	terminal, ok := errors.AsType[*cache.TerminalError](err)
	if !ok || !terminal.Attempted {
		t.Fatalf("seed: %v", err)
	}
	cause, before := terminal.Err, calls.Load()
	if before == 0 {
		t.Fatal("seed performed no DNS lookup")
	}
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", cache.New)
		start := time.Now()
		_, err = (&Fetcher{Cache: c, Network: &network}).Fetch(t.Context(), &manifest.Bucket{Name: "network", Namespace: "test", BucketName: "data", Endpoint: "bucket.test", Insecure: true, Region: "us-east-1"}, (testInputs{}).readSet(&manifest.Bucket{Name: "network", Namespace: "test", BucketName: "data", Endpoint: "bucket.test", Insecure: true, Region: "us-east-1"}), cache.Policy{Retry: cache.RetryConfig{Attempts: 3, MinWait: time.Hour}})
		terminal, ok = errors.AsType[*cache.TerminalError](err)
		if !ok || terminal.Attempted || !errors.Is(err, cause) || calls.Load() != before || time.Since(start) > minio.DefaultRetryUnit {
			t.Fatalf("suppression: %v calls=%d elapsed=%v", err, calls.Load(), time.Since(start))
		}
	})
}

func TestFetcher_ResolveTransport_SnapshotRotation(t *testing.T) {
	var fail atomic.Bool
	var requests, lookups atomic.Int32
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.IsAbs() && r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("proxy:"+proxyPassword.Load().(string))) {
			t.Error("transport used a different proxy snapshot")
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		requests.Add(1)
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=access/") {
			t.Error("static credentials lost")
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
	})
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	root := t.TempDir()
	b := &manifest.Bucket{Name: "snapshot", Namespace: "ns", Endpoint: srv.URL, Insecure: true, Region: "us-east-1", BucketName: "data",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"}, CertSecretRef: &manifest.LocalObjectReference{Name: "creds"}}
	b.Interval.Duration = time.Hour
	password, ca := "first", testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			if wave == "password" {
				password = "second"
			}
			if wave == "certificate" {
				ca = testutil.SelfSignedCA(t)
			}
			proxyURL := strings.Replace(proxy.URL, "://", "://proxy:first@", 1)
			if wave == "proxy" || wave == "direct" {
				proxyURL = strings.Replace(proxy.URL, "://", "://proxy:second@", 1)
				proxyPassword.Store("second")
			}
			t.Setenv("http_proxy", proxyURL)
			if wave == "direct" {
				t.Setenv("no_proxy", "*")
			}
			c := cachetest.New(t, root, cache.New)
			fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				lookups.Add(1)
				return &manifest.Secret{StringData: map[string]any{"accesskey": "access", "secretkey": password, "ca.crt": ca}}
			}}
			f := &Fetcher{Network: newTestNetwork(t), Cache: c}
			before := requests.Load()
			art, err := f.Fetch(t.Context(), b, fInputs.readSet(b), cache.Policy{MaxAge: b.SourceInterval(), Retry: cache.RetryConfig{Attempts: 3}})
			if err != nil {
				t.Fatal(err)
			}
			if previous == art.Key || requests.Load() != before+1 {
				t.Fatal("changed material reused acquisition")
			}
			previous = art.Key
			before = requests.Load()
			if _, err := f.Fetch(t.Context(), b, fInputs.readSet(b), cache.Policy{MaxAge: b.SourceInterval()}); err != nil || requests.Load() != before {
				t.Fatalf("warm snapshot missed: %v", err)
			}
			fail.Store(true)
			failed := *b
			failed.Interval.Duration = 0
			_, err = f.Fetch(t.Context(), &failed, fInputs.readSet(&failed), cache.Policy{MaxAge: failed.SourceInterval()})
			terminal, ok := errors.AsType[*cache.TerminalError](err)
			if !ok || !terminal.Attempted {
				t.Fatalf("failed snapshot: %v", err)
			}
			fail.Store(false)
		})
	}
	if lookups.Load() != 15 {
		t.Fatalf("credentials and cert reread Secrets: %d", lookups.Load())
	}
}
