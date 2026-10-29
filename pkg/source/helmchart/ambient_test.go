package helmchart

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRepositoryURLEqual(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target, entry string
		want                bool
	}{
		{"exact", "https://charts.invalid/repo", "https://charts.invalid/repo", true},
		{"empty path", "https://charts.invalid", "https://charts.invalid/", true},
		{"trailing slash", "https://charts.invalid/repo", "https://charts.invalid/repo/", true},
		{"dot paths", "https://charts.invalid/repo/../charts/.", "https://charts.invalid/charts", true},
		{"host", "https://charts.invalid", "https://other.invalid", false},
		{"host case", "https://CHARTS.invalid", "https://charts.invalid", false},
		{"port", "https://charts.invalid", "https://charts.invalid:443", false},
		{"scheme", "https://charts.invalid", "http://charts.invalid", false},
		{"path", "https://charts.invalid/repo", "https://charts.invalid/other", false},
		{"query", "https://charts.invalid?q=1", "https://charts.invalid?q=2", false},
		{"query order", "https://charts.invalid?a=1&b=2", "https://charts.invalid?b=2&a=1", false},
		{"fragment", "https://charts.invalid#one", "https://charts.invalid#two", false},
		{"escaped path", "https://charts.invalid/a%2Fb", "https://charts.invalid/a/b", false},
		{"escape case", "https://charts.invalid/a%2Fb", "https://charts.invalid/a%2fb", false},
		{"malformed target", "%zz/./repo", "%zz/repo", true},
		{"malformed unequal", "%zz/repo", "%zz/other", false},
		{"malformed entry", "https://charts.invalid", "%zz", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, repositoryURLEqual(tc.target, tc.entry), tc.want)
		})
	}
}

func helmLogin(t *testing.T, entries ...*repo.Entry) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("HELM_CONFIG_HOME", root)
	t.Setenv("HELM_REPOSITORY_CONFIG", "")
	data, err := json.Marshal(&repo.File{APIVersion: "v1", Repositories: entries})
	assert.NoError(t, err)
	path := filepath.Join(root, "repositories.yaml")
	testutil.WriteFileAt(t, path, string(data))
	return path
}

type helmPipeConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (c *helmPipeConn) Close() error {
	err := c.Conn.Close()
	c.cancel()
	return err
}

func helmPipe(t *testing.T, handle func(*http.Request) (*http.Response, error)) {
	t.Helper()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		t.Setenv(key, "")
	}
	old := http.DefaultTransport
	transport := old.(*http.Transport).Clone()
	transport.DialTLSContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		// Only client teardown may cancel the server request, after transport cancellation is recorded.
		ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		conn := &helmPipeConn{Conn: client, cancel: cancel}
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer server.Close()
			request, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			defer request.Body.Close()
			response, err := handle(request.WithContext(ctx))
			if err != nil {
				return
			}
			response.Close = true
			_ = response.Write(server)
		}()
		t.Cleanup(func() {
			_ = conn.Close()
			<-done
		})
		return conn, nil
	}
	http.DefaultTransport = transport
	t.Cleanup(func() { transport.CloseIdleConnections(); http.DefaultTransport = old })
}

func helmReply(r *http.Request, status int, data []byte) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d credential-canary", status), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: r}
}

func TestFetcher_LocalHelmContinuationEligibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		auth, cert  *manifest.Secret
		certRef     bool
		wantAmbient bool
		wantInput   bool
	}{
		{name: "missing auth", wantAmbient: true},
		{name: "wiped auth", auth: &manifest.Secret{StringData: map[string]any{"username": "user", "password": manifest.ValuePlaceholder("password")}}, wantAmbient: true},
		{name: "malformed auth", auth: &manifest.Secret{Data: map[string]any{"username": "credential-canary!"}}, wantInput: true},
		{name: "missing TLS", certRef: true},
		{name: "malformed TLS", certRef: true, cert: &manifest.Secret{StringData: map[string]any{"ca.crt": "credential-canary"}}, wantInput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httpRepo("https://charts.invalid")
			r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			if tc.certRef {
				r.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
			}
			f, inputs := newHTTPFetcherWithSecrets(t, r, func(_, name string) *manifest.Secret {
				if name == "auth" {
					return tc.auth
				}
				return tc.cert
			})
			hc := helmChart(r.Name, "app-template", "1.0.0")
			artifact, err := f.Fetch(t.Context(), hc, inputs.readSet(hc), cache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if err == nil || artifact != nil || (ok && missing.RetryAmbient != nil) != tc.wantAmbient || errors.Is(err, manifest.ErrInput) != tc.wantInput || strings.Contains(err.Error(), "canary") {
				t.Fatalf("ambient=%t input=%t: %v", ok && missing.RetryAmbient != nil, errors.Is(err, manifest.ErrInput), err)
			}
		})
	}
}

func TestFetcher_LocalHelmFiles(t *testing.T) {
	for _, state := range []string{"missing", "directory", "malformed", "no usable match"} {
		t.Run(state, func(t *testing.T) {
			path := helmLogin(t, nil, &repo.Entry{URL: "https://other.invalid", Username: "credential-canary", Password: "password-canary"}, &repo.Entry{URL: "https://charts.invalid", Username: "incomplete-canary"})
			switch state {
			case "missing":
				assert.NoError(t, os.Remove(path))
			case "directory":
				assert.NoError(t, os.Remove(path))
				assert.NoError(t, os.Mkdir(path, 0o700))
			case "malformed":
				testutil.WriteFileAt(t, path, "repositories:\n- username: [credential-canary]\n")
			}
			r := httpRepo("https://charts.invalid")
			r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			f, inputs := newHTTPFetcher(t, r)
			hc := helmChart(r.Name, "app-template", "1.0.0")
			_, err := f.Fetch(t.Context(), hc, inputs.readSet(hc), cache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				t.Fatalf("missing continuation: %v", err)
			}
			artifact, err := missing.RetryAmbient(t.Context())
			unavailable := state == "missing" || state == "no usable match"
			if err == nil || artifact != nil || errors.Is(err, manifest.ErrInput) == unavailable || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, manifest.ErrMissingSecret) != unavailable || strings.Contains(err.Error(), "canary") {
				t.Fatalf("local configuration produced content or lost the acquisition cause: %v", err)
			}
		})
	}
}

func TestFetcher_LocalHelmContinuation(t *testing.T) {
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	var requests atomic.Int32
	var username string
	var rejected bool
	helmPipe(t, func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		user, password, auth := r.BasicAuth()
		if username == "" && auth || username != "" && (!auth || user != username || password != "password-canary") {
			t.Error("request used the wrong credential tier")
		}
		if rejected {
			return helmReply(r, http.StatusUnauthorized, []byte("credential-canary")), nil
		}
		if r.URL.Path == "/index.yaml" {
			return helmReply(r, http.StatusOK, []byte(helmRepoIndex(chartDigest(chart)))), nil
		}
		return helmReply(r, http.StatusOK, chart), nil
	})
	for _, tier := range []string{"repository", "supply", "partial supply", "machine", "public", "rejected"} {
		t.Run(tier, func(t *testing.T) {
			t.Setenv("HELM_CONFIG_HOME", t.TempDir())
			r := httpRepo("https://charts.invalid")
			r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			hc := helmChart(r.Name, "app-template", "1.0.0")
			st := store.New()
			username, rejected = "repository", tier == "rejected"
			var path string
			switch tier {
			case "machine":
				username = "machine-canary"
				path = helmLogin(t, nil,
					&repo.Entry{URL: r.URL, Username: "incomplete-canary"},
					&repo.Entry{URL: r.URL, Username: manifest.ValuePlaceholder("username"), Password: "ignored"},
					&repo.Entry{URL: r.URL + "/./", Username: username, Password: "password-canary", CAFile: "/invalid-ca", CertFile: "/invalid-cert", KeyFile: "/invalid-key", InsecureSkipTLSVerify: true, PassCredentialsAll: true},
					&repo.Entry{URL: r.URL, Username: "later-canary", Password: "ignored"})
				customPath := filepath.Join(filepath.Dir(path), "custom-repositories.yaml")
				assert.NoError(t, os.Rename(path, customPath))
				path = customPath
				t.Setenv("HELM_REPOSITORY_CONFIG", path)
			case "public":
				r.SecretRef = nil
				username = ""
			default:
				secret := &manifest.Secret{Name: "auth", Namespace: r.Namespace, StringData: map[string]any{"username": username, "password": "password-canary"}}
				if tier == "partial supply" {
					secret.StringData["password"] = manifest.ValuePlaceholder("password")
				}
				if tier != "supply" {
					st.AddObject(secret)
				}
				if tier == "supply" || tier == "partial supply" {
					supplied, err := store.NewSupplied([]store.SuppliedObject{{Object: &manifest.Secret{Name: "auth", Namespace: r.Namespace, StringData: map[string]any{"username": "supply", "password": "password-canary"}}}})
					assert.NoError(t, err)
					st.SetSupplied(supplied)
					if tier == "supply" {
						username = "supply"
					}
				}
			}
			st.AddObject(r)
			st.AddObject(hc)
			var reads store.ReadSet
			reads.Init(st, hc.Named(), nil)
			defer reads.Close()
			root := t.TempDir()
			f := New(nil, Config{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)})
			before := requests.Load()
			artifact, err := f.Fetch(t.Context(), hc, &reads, cache.Policy{})
			if rejected {
				response, ok := errors.AsType[*fetcherr.ResponseError](err)
				if artifact != nil || !ok || response.StatusCode != http.StatusUnauthorized || errors.Is(err, manifest.ErrMissingSecret) || requests.Load() != before+1 || strings.Contains(err.Error(), "canary") {
					t.Fatalf("explicit rejection advanced credential tiers or leaked payload: %v", err)
				}
				return
			}
			var retry func(context.Context) (*store.SourceArtifact, error)
			if tier == "machine" {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryAmbient == nil || artifact != nil || requests.Load() != before {
					t.Fatalf("missing auth did not retain a lazy continuation: %v", err)
				}
				prepared, prepErr := (&operation{Fetcher: f, secrets: source.SecretLookup(&reads)}).prepareHTTP(r)
				assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
				transport, transportErr := f.network.HTTPTransport(source.HTTPTransportConfig{DisableCompression: true})
				assert.NoError(t, transportErr)
				if prepared.material != nil || prepared.client.Transport != transport || prepared.username != "" || prepared.password != "" {
					t.Fatal("machine values entered captured identity or transport")
				}
				retry = missing.RetryAmbient
				artifact, err = retry(t.Context())
			}
			assert.NoError(t, err)
			if artifact == nil {
				t.Fatal("successful acquisition returned no artifact")
			}
			if len(artifact.Warnings) != 0 {
				t.Fatalf("warnings=%+v, want %d", artifact.Warnings, 0)
			}
			if tier != "machine" {
				return
			}
			if strings.Contains(fmt.Sprint(artifact.Key, artifact.Content, artifact.Metadata, artifact.Warnings), "canary") {
				t.Fatal("local Helm values entered identity or output")
			}
			assert.NoError(t, os.Remove(path))
			before = requests.Load()
			warm, err := retry(t.Context())
			assert.NoError(t, err)
			if warm.Key != artifact.Key || warm.Content != artifact.Content || len(warm.Warnings) != 0 || requests.Load() != before {
				t.Fatal("warm cache consulted credentials, changed identity or manufactured an advisory")
			}
			data, err := fs.ReadFile(warm.FS, source.HelmChartArchiveFilename)
			assert.NoError(t, err)
			if !bytes.Equal(data, chart) {
				t.Fatal("warm acquisition changed chart bytes")
			}
			assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := os.ReadFile(path)
				if bytes.Contains(data, []byte("canary")) {
					t.Error("machine credentials entered persistent cache data")
				}
				return err
			}))
		})
	}
}

func TestFetcher_LocalHelmAuthenticatedRequests(t *testing.T) {
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	for _, warmIndex := range []bool{false, true} {
		for _, pass := range []bool{false, true} {
			t.Run(fmt.Sprintf("warm-%t/pass-%t", warmIndex, pass), func(t *testing.T) {
				r := httpRepo("https://charts.invalid")
				r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
				r.PassCredentials = pass
				helmLogin(t, &repo.Entry{URL: r.URL, Username: "machine-canary", Password: "password-canary", PassCredentialsAll: true})
				var indexRequests, signedCharts atomic.Int32
				helmPipe(t, func(req *http.Request) (*http.Response, error) {
					_, _, signed := req.BasicAuth()
					if req.Host == "charts.invalid" {
						indexRequests.Add(1)
						index := strings.ReplaceAll(helmRepoIndex(chartDigest(chart)), "app-template-1.0.0.tgz", "https://other.invalid/app-template-1.0.0.tgz")
						return helmReply(req, http.StatusOK, []byte(index)), nil
					}
					if signed {
						signedCharts.Add(1)
					}
					return helmReply(req, http.StatusOK, chart), nil
				})
				f, inputs := newHTTPFetcher(t, r)
				hc := helmChart(r.Name, "app-template", "1.0.0")
				if warmIndex {
					prepared, err := (&operation{Fetcher: f, secrets: source.SecretLookup(inputs.readSet(hc))}).prepareHTTP(r)
					assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
					_, err = f.fetchIndex(t.Context(), prepared, cache.RetryConfig{})
					assert.NoError(t, err)
				}
				_, err := f.Fetch(t.Context(), hc, inputs.readSet(hc), cache.Policy{})
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryAmbient == nil {
					t.Fatalf("missing continuation: %v", err)
				}
				artifact, err := missing.RetryAmbient(t.Context())
				assert.NoError(t, err)
				if len(artifact.Warnings) != 0 || (signedCharts.Load() == 1) != pass || indexRequests.Load() != 1 {
					t.Fatalf("warnings=%d signed charts=%d index=%d", len(artifact.Warnings), signedCharts.Load(), indexRequests.Load())
				}
			})
		}
	}
}

func TestFetcher_LocalHelmAcquisitionFailures(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	for _, stage := range []string{"index", "chart", "malformed index", "digest", "publication", "cancellation"} {
		t.Run(stage, func(t *testing.T) {
			r := httpRepo("https://charts.invalid")
			r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
			helmLogin(t, &repo.Entry{URL: r.URL, Username: "machine-canary", Password: "password-canary"}, &repo.Entry{URL: r.URL, Username: "later-canary", Password: "ignored"})
			root := t.TempDir()
			c := cachetest.New(t, root, cache.New)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{})
			var requests atomic.Int32
			helmPipe(t, func(req *http.Request) (*http.Response, error) {
				requests.Add(1)
				if stage == "index" || stage == "chart" && req.URL.Path != "/index.yaml" {
					return helmReply(req, http.StatusForbidden, []byte("machine-canary password-canary")), nil
				}
				if req.URL.Path == "/index.yaml" {
					if stage == "malformed index" {
						return helmReply(req, http.StatusOK, []byte("apiVersion: v1\nentries: machine-canary\n")), nil
					}
					digest := chartDigest(chart)
					if stage == "digest" {
						digest = "invalid-digest"
					}
					return helmReply(req, http.StatusOK, []byte(helmRepoIndex(digest))), nil
				}
				if stage == "publication" {
					assert.NoError(t, c.Close())
				}
				if stage == "cancellation" {
					close(entered)
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				return helmReply(req, http.StatusOK, chart), nil
			})
			f := New(nil, Config{Cache: c, Network: newTestNetwork(t)})
			inputs := testInputs{repos: func(string, string) *manifest.HelmRepository { return r }}
			hc := helmChart(r.Name, "app-template", "1.0.0")
			_, err := f.Fetch(ctx, hc, inputs.readSet(hc), cache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				t.Fatalf("missing continuation: %v", err)
			}
			var artifact *store.SourceArtifact
			if stage == "cancellation" {
				finished := make(chan error, 1)
				go func() {
					var failure error
					artifact, failure = missing.RetryAmbient(ctx)
					finished <- failure
				}()
				<-entered
				cancel()
				err = <-finished
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("lost cancellation identity: %v", err)
				}
			} else {
				artifact, err = missing.RetryAmbient(ctx)
			}
			if artifact != nil || err == nil || strings.Contains(err.Error(), "canary") {
				t.Fatalf("failed acquisition published or exposed credentials: %v", err)
			}
			if stage == "index" || stage == "chart" {
				response, ok := errors.AsType[*fetcherr.ResponseError](err)
				wantRequests := int32(1)
				if stage == "chart" {
					wantRequests = 2
				}
				if !ok || response.StatusCode != http.StatusForbidden || requests.Load() != wantRequests {
					t.Fatalf("lost typed rejection or tried another credential pair: %v", err)
				}
			}
			assert.NoError(t, c.Close())
			if strings.Contains(logs.String(), "canary") {
				t.Fatal("machine credential material entered diagnostics")
			}
			assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := os.ReadFile(path)
				if bytes.Contains(data, []byte("canary")) {
					t.Error("failed machine acquisition persisted credential material")
				}
				return err
			}))
		})
	}
}
