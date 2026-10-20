package source_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_TransportMaterialAbsentFromCache(t *testing.T) {
	type generation struct {
		name, username, password, ca, cert, key, authorization string
		requests                                               atomic.Int32
	}
	generations := []generation{{name: "initial"}, {name: "rotated"}}
	for i := range generations {
		g := &generations[i]
		g.username = "flate-" + g.name + "-proxy-user-79a6b321"
		g.password = "flate-" + g.name + "-proxy-password-c46e081f"
		g.ca = testutil.SelfSignedCA(t)
		g.cert, g.key = testutil.SelfSignedClientCert(t)
		g.authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(g.username+":"+g.password))
	}
	var expected atomic.Pointer[generation]
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := expected.Load()
		if r.Method != http.MethodGet || !r.URL.IsAbs() || r.URL.Scheme != "http" || r.URL.Host != "origin.invalid" || r.URL.Path != "/data/" || r.URL.Query().Get("list-type") != "2" {
			t.Error("unexpected proxy request target")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if g == nil || r.Header.Get("Proxy-Authorization") != g.authorization {
			t.Error("proxy request did not carry the expected generation's credentials")
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		g.requests.Add(1)
		_, err := io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
		if err != nil {
			t.Errorf("write bucket listing: %v", err)
		}
	}))
	t.Cleanup(proxy.Close)
	proxyAddress, err := url.Parse(proxy.URL)
	assert.NoError(t, err)
	type marker struct {
		label string
		value []byte
	}
	var markers []marker
	for i := range generations {
		g := &generations[i]
		credentialURL := *proxyAddress
		credentialURL.User = url.UserPassword(g.username, g.password)
		for _, material := range []struct{ label, value string }{
			{"proxy username", g.username}, {"proxy password", g.password},
			{"proxy URL", credentialURL.String()}, {"proxy Basic payload", strings.TrimPrefix(g.authorization, "Basic ")},
			{"CA PEM", g.ca}, {"client certificate PEM", g.cert}, {"client key PEM", g.key},
		} {
			label := g.name + " " + material.label
			markers = append(markers, marker{label, []byte(material.value)})
			if strings.HasSuffix(material.label, "PEM") {
				for j, line := range strings.Split(material.value, "\n") {
					if len(line) >= 32 && !strings.HasPrefix(line, "-----") {
						markers = append(markers, marker{fmt.Sprintf("%s payload line %d", label, j), []byte(line)})
					}
				}
			}
		}
	}

	root := t.TempDir()
	b := &manifest.Bucket{Name: "material", Namespace: "ns", Endpoint: "http://origin.invalid", Insecure: true, Region: "us-east-1", BucketName: "data",
		ProxySecretRef: &manifest.LocalObjectReference{Name: "proxy"}, CertSecretRef: &manifest.LocalObjectReference{Name: "tls"}}
	b.Interval.Duration = time.Hour
	var previousKey string
	for i := range generations {
		g := &generations[i]
		t.Run(g.name, func(t *testing.T) {
			expected.Store(g)
			var acquiredKey string
			for _, phase := range []string{"acquire", "reopen"} {
				owner, err := cache.New(t.Context(), root)
				assert.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, owner.Close()) })
				certLookups := 0
				network := new(source.Network)
				t.Cleanup(network.Close)
				f := &bucket.Fetcher{Network: network, Cache: owner, Secrets: func(ns, name string) *manifest.Secret {
					assert.Equal(t, ns, b.Namespace)
					switch name {
					case "proxy":
						return &manifest.Secret{StringData: map[string]any{"address": proxy.URL, "username": g.username, "password": g.password}}
					case "tls":
						certLookups++
						return &manifest.Secret{StringData: map[string]any{"ca.crt": g.ca, "tls.crt": g.cert, "tls.key": g.key}}
					default:
						t.Errorf("unexpected Secret reference %q", name)
						return nil
					}
				}}
				artifact, err := f.Fetch(t.Context(), b, cache.Policy{MaxAge: b.SourceInterval()})
				assert.NoError(t, err)
				if artifact.Key == "" || artifact.Revision == "" {
					t.Fatal("fetch returned an empty key or revision")
				}
				assert.Equal(t, artifact.Metadata["objectCount"], "0")
				entries, err := fs.ReadDir(artifact.FS, ".")
				assert.NoError(t, err)
				assert.Equal(t, len(entries), 0)
				assert.Equal(t, certLookups, 1)
				assert.Equal(t, g.requests.Load(), int32(1))
				if phase == "acquire" {
					if artifact.Key == previousKey {
						t.Fatal("rotated material reused the previous acquisition key")
					}
					acquiredKey = artifact.Key
				} else {
					assert.Equal(t, artifact.Key, acquiredKey)
				}
				assert.NoError(t, owner.Close())
				t.Logf("%s: key=%s proxy requests=%d certificate lookups=%d", phase, artifact.Key, g.requests.Load(), certLookups)
			}
			previousKey = acquiredKey
			var inspectedBytes int
			err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.Type().IsRegular() {
					return nil
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				inspectedBytes += len(data)
				for _, m := range markers {
					if bytes.Contains(data, m.value) {
						t.Errorf("cache file %s contains %s", path, m.label)
					}
				}
				return nil
			})
			assert.NoError(t, err)
			if inspectedBytes == 0 {
				t.Fatal("cache scan inspected no bytes")
			}
			t.Logf("scanned %d cache bytes for both generations' material", inspectedBytes)
		})
	}
}
