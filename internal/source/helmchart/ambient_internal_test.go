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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	repo "helm.sh/helm/v4/pkg/repo/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
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
	http.DefaultTransport = transport //nolint:reassign // The test replaces the process transport to exercise offline acquisition and restores it afterward.
	t.Cleanup(
		func() {
			transport.CloseIdleConnections()
			http.DefaultTransport = old //nolint:reassign // The test must restore the process HTTP transport after offline acquisition.
		},
	)
}

func helmReply(r *http.Request, status int, data []byte) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d credential-canary", status),
		Header:        make(http.Header),
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: int64(len(data)),
		Request:       r,
	}
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
		{name: "empty auth", auth: &manifest.Secret{StringData: map[string]any{"username": "user", "password": ""}}, wantAmbient: true},
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
			artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, hc, inputs.readSet(hc), &cache.Policy{}, nil)
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, err == nil || artifact != nil || (ok && missing.RetryAmbient != nil) != tc.wantAmbient ||
				errors.Is(err, manifest.ErrInput) != tc.wantInput ||
				strings.Contains(err.Error(), "canary"),

				"ambient=%t input=%t: %v",
				ok && missing.RetryAmbient != nil,
				errors.Is(err, manifest.ErrInput),
				err)
		})
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_LocalHelmFiles(t *testing.T) {
	for _, state := range []string{"missing", "directory", "malformed", "no usable match"} {
		t.Run(state, func(t *testing.T) {
			path := helmLogin(
				t,
				nil,
				&repo.Entry{URL: "https://other.invalid", Username: "credential-canary", Password: "password-canary"},
				&repo.Entry{URL: "https://charts.invalid", Username: "incomplete-canary"},
			)
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
			_, err := fetchtest.Fetch(t.Context(), f.Fetch, hc, inputs.readSet(hc), &cache.Policy{}, nil)
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
			artifact, err := fetchtest.Retry(t.Context(), missing.RetryAmbient)
			unavailable := state == "missing" || state == "no usable match"
			assert.FatalIf(t, err == nil || artifact != nil || errors.Is(err, manifest.ErrInput) == unavailable ||
				!errors.Is(err, manifest.ErrFlux) ||
				errors.Is(err, manifest.ErrMissingSecret) != unavailable ||
				strings.Contains(
					err.Error(),
					"canary",
				), "local configuration produced content or lost the acquisition cause: %v", err)
		})
	}
}

//nolint:paralleltest // Helm continuation fixtures change process environment and transport state.
func TestFetcher_LocalHelmContinuation(t *testing.T) {
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	var requests atomic.Int32
	var username string
	var rejected bool
	helmPipe(t, func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		user, password, auth := r.BasicAuth()
		assert.ErrorIf(
			t,
			username == "" && auth || username != "" && (!auth || user != username || password != "password-canary"),
			"%s",
			"request used the wrong credential tier",
		)

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
			testFetcherLocalHelmContinuationCase1(
				t,
				&testFetcherLocalHelmContinuationCase1Inputs{
					username: &username,
					rejected: &rejected,
					tier:     &tier,
					requests: &requests,
					chart:    &chart,
				},
			)
		})
	}
}

func testFetcherLocalHelmContinuationCase1(
	t *testing.T, inputs *testFetcherLocalHelmContinuationCase1Inputs,
) {
	t.Helper()
	t.Setenv("HELM_CONFIG_HOME", t.TempDir())
	r := httpRepo("https://charts.invalid")
	r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	hc := helmChart(r.Name, "app-template", "1.0.0")
	st := store.New()
	(*inputs.username), (*inputs.rejected) = "repository", (*inputs.tier) == "rejected"
	var path string
	switch *inputs.tier {
	case "machine":
		(*inputs.username) = "machine-canary"
		path = helmLogin(
			t,
			nil,
			&repo.Entry{URL: r.URL, Username: "incomplete-canary"},
			&repo.Entry{URL: r.URL, Username: "", Password: "ignored"},
			&repo.Entry{
				URL:                   r.URL + "/./",
				Username:              (*inputs.username),
				Password:              "password-canary",
				CAFile:                "/invalid-ca",
				CertFile:              "/invalid-cert",
				KeyFile:               "/invalid-key",
				InsecureSkipTLSVerify: true,
				PassCredentialsAll:    true,
			},
			&repo.Entry{URL: r.URL, Username: "later-canary", Password: "ignored"},
		)
		customPath := filepath.Join(filepath.Dir(path), "custom-repositories.yaml")
		assert.NoError(t, os.Rename(path, customPath))
		path = customPath
		t.Setenv("HELM_REPOSITORY_CONFIG", path)
	case "public":
		r.SecretRef = nil
		(*inputs.username) = ""
	default:
		secret := &manifest.Secret{
			Name:       "auth",
			Namespace:  r.Namespace,
			StringData: map[string]any{"username": (*inputs.username), "password": "password-canary"},
		}
		if (*inputs.tier) == "partial supply" {
			delete(secret.StringData, "password")
		}
		if (*inputs.tier) != "supply" {
			st.AddObject(secret)
		}
		if (*inputs.tier) == "supply" || (*inputs.tier) == "partial supply" {
			supplied := []store.Declaration{
				{
					Doc: testutil.ObjectDocument(
						&manifest.Secret{
							Name:       "auth",
							Namespace:  r.Namespace,
							StringData: map[string]any{"username": "supply", "password": "password-canary"},
						},
					),
					Obj: &manifest.Secret{
						Name:       "auth",
						Namespace:  r.Namespace,
						StringData: map[string]any{"username": "supply", "password": "password-canary"},
					},
					Origin: manifest.SuppliedDeclaration,
				},
			}
			for _, declaration := range supplied {
				st.AddDeclaration(declaration)
			}
			if (*inputs.tier) == "supply" {
				(*inputs.username) = "supply"
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
	before := inputs.requests.Load()
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, hc, &reads, &cache.Policy{}, nil)
	if *inputs.rejected {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		assert.FatalIf(t, artifact != nil || !ok || response.StatusCode != http.StatusUnauthorized ||
			!errors.Is(err, manifest.ErrMissingSecret) || inputs.requests.
			Load() != before+1 ||
			strings.Contains(err.Error(), "canary"),
			"explicit rejection advanced credential tiers or leaked payload: %v", err)

		return
	}
	testFetcherLocalHelmContinuationCase1Tail(
		t,
		&testFetcherLocalHelmContinuationCase1TailInputs{
			tier:     &inputs.tier,
			err:      &err,
			artifact: &artifact,
			requests: &inputs.requests,
			before:   &before,
			f:        &f,
			reads:    &reads,
			r:        &r,
			path:     &path,
			chart:    &inputs.chart,
			root:     &root,
		},
	)
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_LocalHelmAuthenticatedRequests(t *testing.T) {
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	for _, warmIndex := range []bool{false, true} {
		for _, pass := range []bool{false, true} {
			t.Run(fmt.Sprintf("warm-%t/pass-%t", warmIndex, pass), func(t *testing.T) {
				testFetcherLocalHelmAuthenticatedRequestsCase(
					t,

					&chart,
					&warmIndex,
					&pass,
				)
			},
			)
		}
	}
}

//nolint:paralleltest // The test changes the process logging destination.
//nolint:paralleltest // The test changes process logging state.
func TestFetcher_LocalHelmAcquisitionFailures(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	chart := buildChartTarGz(t, "app-template", "1.0.0")
	for _, stage := range []string{"index", "retry index", "chart", "malformed index", "digest", "publication", "cancellation"} {
		t.Run(stage, func(t *testing.T) {
			testFetcherLocalHelmAcquisitionFailuresCase(
				t,
				&logs, &chart, &stage,
			)
		},
		)
	}
}

func testFetcherLocalHelmAuthenticatedRequestsCase(
	t *testing.T, chart *[]byte,
	warmIndex *bool,
	pass *bool,
) {
	t.Helper()
	r := httpRepo("https://charts.invalid")
	r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	r.PassCredentials = (*pass)
	helmLogin(
		t,
		&repo.Entry{
			URL:                r.URL,
			Username:           "machine-canary",
			Password:           "password-canary",
			PassCredentialsAll: true,
		},
	)
	var indexRequests, signedCharts atomic.Int32
	helmPipe(t, func(req *http.Request) (*http.Response, error) {
		_, _, signed := req.BasicAuth()
		if req.Host == "charts.invalid" {
			indexRequests.Add(1)
			index := strings.ReplaceAll(
				helmRepoIndex(chartDigest((*chart))),
				"app-template-1.0.0.tgz",
				"https://other.invalid/app-template-1.0.0.tgz",
			)
			return helmReply(req, http.StatusOK, []byte(index)), nil
		}
		if signed {
			signedCharts.Add(1)
		}
		return helmReply(req, http.StatusOK, (*chart)), nil
	})
	f, inputs := newHTTPFetcher(t, r)
	hc := helmChart(r.Name, "app-template", "1.0.0")
	if *warmIndex {
		prepared, err := (&operation{Fetcher: f, secrets: source.SecretLookup(inputs.readSet(hc))}).resolvedHTTP(
			r,
		)
		assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
		_, err = f.fetchIndex(t.Context(), prepared, &cache.Policy{Retry: cache.RetryConfig{}})
		assert.NoError(t, err)
	}
	_, err := fetchtest.Fetch(t.Context(), f.Fetch, hc, inputs.readSet(hc), &cache.Policy{}, nil)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
	artifact, err := fetchtest.Retry(t.Context(), missing.RetryAmbient)
	assert.NoError(t, err)
	assert.FatalIf(t, len(artifact.Warnings) != 0 || (signedCharts.Load() == 1) != (*pass) ||
		indexRequests.Load() != map[bool]int32{false: 1, true: 2}[(*warmIndex)],

		"warnings=%d signed charts=%d index=%d",
		len(artifact.Warnings),
		signedCharts.Load(),
		indexRequests.Load())
}

func testFetcherLocalHelmAcquisitionFailuresCase(
	t *testing.T, logs *bytes.Buffer,
	chart *[]byte,
	stage *string,
) {
	t.Helper()
	r := httpRepo("https://charts.invalid")
	r.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	helmLogin(
		t,
		&repo.Entry{URL: r.URL, Username: "machine-canary", Password: "password-canary"},
		&repo.Entry{URL: r.URL, Username: "later-canary", Password: "ignored"},
	)
	root := t.TempDir()
	c := cachetest.New(t, root, cache.New)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	var requests atomic.Int32
	helmPipe(t, func(req *http.Request) (*http.Response, error) {
		return testFetcherLocalHelmAcquisitionFailuresCaseCallback(
			req,
			testFetcherLocalHelmAcquisitionFailuresCaseCallbackInputs{
				t:        &t,
				chart:    &chart,
				stage:    &stage,
				c:        &c,
				entered:  &entered,
				requests: &requests,
			},
		)
	},
	)
	f := New(nil, Config{Cache: c, Network: newTestNetwork(t)})
	inputs := testInputs{repos: func(string, string) *manifest.HelmRepository { return r }}
	hc := helmChart(r.Name, "app-template", "1.0.0")
	policy := &cache.Policy{
		Retry: cache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond},
	}
	_, err := fetchtest.Fetch(ctx, f.Fetch, hc, inputs.readSet(hc), policy, nil)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
	var artifact *store.SourceArtifact
	if (*stage) == "cancellation" {
		finished := make(chan error, 1)
		go func() {
			var failure error
			artifact, failure = fetchtest.Retry(ctx, missing.RetryAmbient)
			finished <- failure
		}()
		<-entered
		cancel()
		err = <-finished
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "lost cancellation identity: %v", err)
	} else {
		artifact, err = fetchtest.Retry(ctx, missing.RetryAmbient)
	}
	assert.FatalIf(
		t,
		artifact != nil || err == nil || strings.Contains(err.Error(), "canary"),
		"failed acquisition published or exposed credentials: %v",
		err,
	)
	if (*stage) == "index" || (*stage) == "chart" {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		wantRequests := int32(1)
		if (*stage) == "chart" {
			wantRequests = 2
		}
		assert.FatalIf(
			t,
			!ok || response.StatusCode != http.StatusForbidden || requests.Load() != wantRequests,
			"lost typed rejection or tried another credential pair: %v",
			err,
		)
	}

	if (*stage) == "retry index" {
		terminal, ok := errors.AsType[*cache.TerminalError](err)
		response, typed := errors.AsType[*fetcherr.ResponseError](err)
		assert.FatalIf(
			t,
			!ok || !terminal.Attempted || !typed || response.StatusCode != http.StatusServiceUnavailable ||
				requests.Load() != 2,
			"nested retry budget/cause: requests=%d %v",
			requests.Load(),
			err,
		)

		assert.NoError(t, c.Close())
		reopened := cachetest.New(t, root, cache.New)
		replay := New(nil, Config{Cache: reopened, Network: newTestNetwork(t)})
		_, err = fetchtest.Fetch(ctx, replay.Fetch, hc, inputs.readSet(hc), policy, nil)
		pending, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok, "%s", err)

		_, err = fetchtest.Machine(ctx, pending, nil)
		terminal, ok = errors.AsType[*cache.TerminalError](err)
		response, typed = errors.AsType[*fetcherr.ResponseError](err)
		assert.FatalIf(t, !ok || terminal.Attempted || !typed || response.StatusCode != http.StatusServiceUnavailable ||
			requests.Load() != 2,
			"nested replay lost cause or repeated HTTP: requests=%d %v", requests.Load(), err)
	}
	assert.NoError(t, c.Close())
	assert.FatalIf(
		t,
		strings.Contains(logs.String(), "canary"),
		"%s",
		"machine credential material entered diagnostics",
	)

	assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		return testFetcherLocalHelmAcquisitionFailuresCaseCallback2(
			path,
			entry,
			err,
			testFetcherLocalHelmAcquisitionFailuresCaseCallback2Inputs{t: &t, stage: &stage},
		)
	},
	))
}

type testFetcherLocalHelmAcquisitionFailuresCaseCallbackInputs struct {
	t        **testing.T
	chart    **[]byte
	stage    **string
	c        **cache.Cache
	entered  *chan struct{}
	requests *atomic.Int32
}

func testFetcherLocalHelmAcquisitionFailuresCaseCallback(
	req *http.Request,
	testCaseInput testFetcherLocalHelmAcquisitionFailuresCaseCallbackInputs,
) (*http.Response, error) {
	testCaseInput.requests.
		Add(1)
	if (*(*testCaseInput.stage)) == "retry index" {
		return helmReply(req, http.StatusServiceUnavailable, []byte("machine-canary password-canary")), nil
	}
	if (*(*testCaseInput.stage)) == "index" || (*(*testCaseInput.stage)) == "chart" && req.URL.Path != "/index.yaml" {
		return helmReply(req, http.StatusForbidden, []byte("machine-canary password-canary")), nil
	}
	if req.URL.Path == "/index.yaml" {
		if (*(*testCaseInput.stage)) == "malformed index" {
			return helmReply(req, http.StatusOK, []byte("apiVersion: v1\nentries: machine-canary\n")), nil
		}
		digest := chartDigest((*(*testCaseInput.chart)))
		if (*(*testCaseInput.stage)) == "digest" {
			digest = "invalid-digest"
		}
		return helmReply(req, http.StatusOK, []byte(helmRepoIndex(digest))), nil
	}
	if (*(*testCaseInput.stage)) == "publication" {
		assert.NoError((*testCaseInput.t), (*testCaseInput.c).Close())
	}
	if (*(*testCaseInput.stage)) == "cancellation" {
		close((*testCaseInput.entered))
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	return helmReply(req, http.StatusOK, (*(*testCaseInput.chart))), nil
}

type testFetcherLocalHelmAcquisitionFailuresCaseCallback2Inputs struct {
	t     **testing.T
	stage **string
}

func testFetcherLocalHelmAcquisitionFailuresCaseCallback2(
	path string,
	entry fs.DirEntry,
	err error,
	testCaseInput testFetcherLocalHelmAcquisitionFailuresCaseCallback2Inputs,
) error {
	if err != nil || entry.IsDir() {
		return err
	}
	data, err := os.ReadFile(path)
	if (*(*testCaseInput.stage)) == "retry index" && bytes.Count(data, []byte(`"kind":"machine"`)) > 1 {
		(*testCaseInput.t).
			Error("nested outcome contains repeated machine wrappers")
	}
	if bytes.Contains(data, []byte("canary")) {
		(*testCaseInput.t).
			Error("failed machine acquisition persisted credential material")
	}
	return err
}

type testFetcherLocalHelmContinuationCase1TailInputs struct {
	tier **string

	err *error

	artifact **store.
			SourceArtifact

	requests **atomic.
			Int32

	before *int32

	f **Fetcher

	reads *store.
		ReadSet

	r **manifest.
		HelmRepository

	path *string

	chart **[]byte

	root *string
}

func testFetcherLocalHelmContinuationCase1Tail(t *testing.T, inputs *testFetcherLocalHelmContinuationCase1TailInputs) {
	t.Helper()

	var retry func(context.Context) (*store.SourceArtifact, error)
	if (*(*inputs.tier)) == "machine" {
		missing, ok := errors.AsType[*source.MissingSecretError]((*inputs.err))
		assert.FatalIf(
			t,
			!ok || missing.RetryAmbient == nil || (*inputs.artifact) != nil ||
				(*inputs.requests).Load() != (*inputs.before),
			"missing auth did not retain a lazy continuation: %v",
			(*inputs.err),
		)

		prepared, prepErr := (&operation{Fetcher: (*inputs.f), secrets: source.SecretLookup(inputs.reads)}).resolvedHTTP(
			(*inputs.r),
		)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		transport, transportErr := (*inputs.f).network.HTTPTransport(
			source.HTTPTransportConfig{DisableCompression: true},
		)
		assert.NoError(t, transportErr)
		assert.FatalIf(
			t,
			prepared.material != nil || prepared.client.Transport != transport || prepared.username != "" ||
				prepared.password != "",
			"%s",
			"machine values entered captured identity or transport",
		)

		retry = func(ctx context.Context) (*store.SourceArtifact, error) {
			return fetchtest.Retry(ctx, missing.RetryAmbient)
		}
		(*inputs.artifact), (*inputs.err) = retry(t.Context())
	}
	assert.NoError(t, (*inputs.err))
	assert.FatalIf(t, (*inputs.artifact) == nil, "%s", "successful acquisition returned no artifact")
	assert.FatalIf(t, len((*inputs.artifact).Warnings) != 0,
		"warnings=%+v, want %d", (*inputs.artifact).Warnings, 0)

	if (*(*inputs.tier)) != "machine" {
		return
	}
	assert.FatalIf(t, strings.Contains(
		fmt.Sprint(
			(*inputs.artifact).Key,
			(*inputs.artifact).Content,
			(*inputs.artifact).Metadata,
			(*inputs.artifact).Warnings,
		),
		"canary",
	), "%s", "local Helm values entered identity or output")
	(*inputs.before) = (*inputs.requests).Load()
	var warm *store.
		SourceArtifact

	warm, (*inputs.err) = retry(t.Context())
	assert.NoError(t, (*inputs.err))
	assert.FatalIf(
		t,
		warm.Key != (*inputs.artifact).Key || warm.Content != (*inputs.artifact).Content || len(warm.Warnings) != 0 ||
			(*inputs.requests).
				Load() != (*inputs.before),
		"%s",
		"warm cache consulted credentials, changed identity or manufactured an advisory",
	)

	var concurrent sync.WaitGroup
	start := make(chan struct{})
	for range 2 {
		concurrent.Go(func() {
			<-start
			artifact, err := retry(t.Context())
			assert.ErrorIf(t, err != nil || artifact == nil || artifact.Key != warm.Key,
				"concurrent prepared chart: %v %v", artifact, err)
		})
	}
	close(start)
	concurrent.Wait()
	assert.FatalIf(t, (*inputs.requests).Load() != (*inputs.before), "%s", "concurrent warm acquisitions repeated HTTP")

	assert.NoError(t, os.Remove((*inputs.path)))
	var failed *store.
		SourceArtifact

	failed, (*inputs.err) = retry(t.Context())
	assert.FatalIf(
		t,
		failed != nil || !errors.Is((*inputs.err), manifest.ErrMissingSecret) ||
			(*inputs.requests).Load() != (*inputs.before),
		"unavailable login reused warm artifact: %v",
		(*inputs.err),
	)
	var data []byte

	data, (*inputs.err) = fs.ReadFile(warm.FS, source.HelmChartArchiveFilename)
	assert.NoError(t, (*inputs.err))
	assert.FatalIf(t, !bytes.Equal(data, (*(*inputs.chart))), "%s", "warm acquisition changed chart bytes")

	assert.NoError(t, filepath.WalkDir((*inputs.root), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		assert.ErrorIf(
			t,
			bytes.Contains(data, []byte("canary")),
			"%s",
			"machine credentials entered persistent cache data",
		)

		return err
	}))
}

type testFetcherLocalHelmContinuationCase1Inputs struct {
	username *string
	rejected *bool
	tier     *string
	requests *atomic.Int32
	chart    *[]byte
}
