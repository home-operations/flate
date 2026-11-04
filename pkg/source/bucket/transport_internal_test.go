package bucket

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
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
		t.Run(strconv.FormatBool(cert), func(t *testing.T) {
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
			b := &manifest.Bucket{
				Name:       "b",
				Namespace:  "ns",
				Endpoint:   srv.URL,
				BucketName: "fixture",
				Region:     "us-east-1",
				Insecure:   !cert,
			}
			if cert {
				b.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
				ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
				fInputs.secrets = func(_, _ string) *manifest.Secret { return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}} }
			}
			request, err := (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).prepare(b)
			assert.NoError(t, err)
			client, err := minio.New(request.endpoint, &request.options)
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
			testFetcherSecretReferencesCase(t, &tc)
		},
		)
	}
	t.Run(
		"successful credentials",
		func(t *testing.T) { t.Parallel(); testFetcherSecretReferencesSuccessfulCredentials(t) },
	)
}

//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_NetworkSuppressionSkipsBackoff(t *testing.T) {
	var network source.Network
	var calls atomic.Int32
	testutil.StubDNS(t, func(context.Context, string) error { calls.Add(1); return os.ErrDeadlineExceeded })
	tr, err := network.HTTPTransport(source.HTTPTransportConfig{})
	assert.NoError(t, err)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://bucket.test", http.NoBody)
	_, err = (&http.Client{Transport: tr}).Do(req)
	terminal, ok := errors.AsType[*cache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "seed: %v", err)
	cause, before := terminal.Err, calls.Load()
	assert.FatalIf(t, before == 0, "%s", "seed performed no DNS lookup")

	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, "", cache.New)
		start := time.Now()
		_, err = (&Fetcher{Cache: c, Network: &network}).Fetch(
			t.Context(),
			&manifest.Bucket{
				Name:       "network",
				Namespace:  "test",
				BucketName: "data",
				Endpoint:   "bucket.test",
				Insecure:   true,
				Region:     "us-east-1",
			},
			(testInputs{}).readSet(
				&manifest.Bucket{
					Name:       "network",
					Namespace:  "test",
					BucketName: "data",
					Endpoint:   "bucket.test",
					Insecure:   true,
					Region:     "us-east-1",
				},
			),
			cache.Policy{Retry: cache.RetryConfig{Attempts: 3, MinWait: time.Hour}},
		)
		terminal, ok = errors.AsType[*cache.TerminalError](err)
		if !ok || terminal.Attempted || !errors.Is(err, cause) || calls.Load() != before ||
			time.Since(start) > minio.DefaultRetryUnit {
			t.Fatalf("suppression: %v calls=%d elapsed=%v", err, calls.Load(), time.Since(start))
		}
	})
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_ResolveTransport_SnapshotRotation(t *testing.T) {
	var fail atomic.Bool
	var requests, lookups atomic.Int32
	var proxyPassword atomic.Value
	proxyPassword.Store("first")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherResolveTransportSnapshotRotationCallback(
			w,
			r,
			testFetcherResolveTransportSnapshotRotationCallbackInputs{
				t:             &t,
				fail:          &fail,
				requests:      &requests,
				proxyPassword: &proxyPassword,
			},
		)
	},
	)
	srv := httptest.NewUnstartedServer(handler)
	_ = srv.Listener.Close()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	assert.NoError(t, err)

	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	proxy := httptest.NewServer(handler)
	t.Cleanup(proxy.Close)
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	root := t.TempDir()
	b := &manifest.Bucket{
		Name:       "snapshot",
		Namespace:  "ns",
		Endpoint:   srv.URL,
		Insecure:   true,
		Region:     "us-east-1",
		BucketName: "data",
		SecretRef: &manifest.LocalObjectReference{
			Name: "creds",
		},
		CertSecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	b.Interval.Duration = time.Hour
	password, ca := "first", testutil.SelfSignedCA(t)
	var previous string
	for _, wave := range []string{"first", "password", "certificate", "proxy", "direct"} {
		t.Run(wave, func(t *testing.T) {
			testFetcherResolveTransportSnapshotRotationCase(

				t, &testFetcherResolveTransportSnapshotRotationCaseInputs{
					fail:          &fail,
					requests:      &requests,
					lookups:       &lookups,
					proxyPassword: &proxyPassword,
					proxy:         &proxy,
					root:          &root,
					b:             &b,
					password:      &password,
					ca:            &ca,
					previous:      &previous,
					wave:          &wave,
				})
		},
		)
	}
	if lookups.Load() != 15 {
		t.Fatalf("credentials and cert reread Secrets: %d", lookups.Load())
	}
}

func TestFetcher_ProviderTransportValidation(t *testing.T) {
	t.Parallel()
	for _, use := range []string{"cert", "proxy"} {
		t.Run(use, func(t *testing.T) {
			t.Parallel()
			repo := &manifest.Bucket{
				Name:       "provider",
				Namespace:  "apps",
				Endpoint:   "s3.amazonaws.com",
				BucketName: "fixture",
				Provider:   "aws",
			}
			if use == "cert" {
				repo.CertSecretRef = &manifest.LocalObjectReference{Name: "transport"}
			} else {
				repo.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
			}
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
				return &manifest.Secret{
					StringData: map[string]any{"ca.crt": "invalid", "address": "http://proxy.invalid/%"},
				}
			}}
			fetcher := &Fetcher{Network: newTestNetwork(t)}
			_, err := fetcher.Fetch(t.Context(), repo, inputs.readSet(repo), cache.Policy{})
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || errors.Is(err, manifest.ErrMissingSecret),
				"provider hid malformed transport: %v",
				err,
			)
		})
	}
}

func TestFetcher_ProviderCredentials(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, provider          string
		sts, malformed, missing bool
	}{
		{name: "AWS static", provider: "aws"},
		{name: "GCP is not S3", provider: "gcp", missing: true},
		{name: "Azure is not S3", provider: "azure", missing: true},
		{name: "STS unavailable", provider: "aws", sts: true, missing: true},
		{name: "generic STS unavailable", provider: "generic", sts: true, missing: true},
		{name: "malformed static", provider: "aws", malformed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherProviderCredentialsCase(t, &tc)
		},
		)
	}
}

func testFetcherSecretReferencesCase(t *testing.T, tc *struct {
	name      string
	ref       string
	secret    string
	wantError string
	data      map[string]any
	rawData   map[string]any
	unwired   bool
	encoded   bool
	missing   bool
	field     string
},
) {
	t.Helper()

	fInputs := testInputs{}
	f := &Fetcher{Network: newTestNetwork(t)}
	if !tc.unwired {
		fInputs.secrets = func(callbackArg1, callbackArg2 string) *manifest.Secret {
			return testFetcherSecretReferencesCaseCallback(
				callbackArg1,
				callbackArg2,
				testFetcherSecretReferencesCaseCallbackInputs{tc: &tc},
			)
		}
	}
	b := &manifest.Bucket{
		Name:       "b",
		Namespace:  "ns",
		BucketName: "fixture",
		Endpoint:   "127.0.0.1:1",
		Insecure:   true,
	}
	ref := &manifest.LocalObjectReference{Name: tc.secret}
	var err error
	switch tc.ref {
	case "auth":
		b.SecretRef = ref
		creds, credentialErr := (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).resolveCredentials(b)
		assert.FatalIf(
			t,
			creds != nil || credentialErr == nil,
			"%s",
			"credential failure returned credentials or no error",
		)

		reads := fInputs.readSet(b)
		if tc.unwired {
			reads = nil
		}
		artifact, fetchErr := f.Fetch(t.Context(), b, reads, cache.Policy{MaxAge: b.SourceInterval()})
		assert.FatalIf(t, artifact != nil, "%s", "credential failure returned an acquired artifact")

		err = fetchErr
	case "cert":
		b.CertSecretRef = ref
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).prepare(b)
	case "proxy":
		b.ProxySecretRef = ref
		_, err = (&operation{Fetcher: f, secrets: fInputs.lookup(b)}).prepare(b)
	}
	if err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing ||
		!strings.Contains(err.Error(), tc.wantError) {
		t.Fatalf("missing=%t, error=%v, want %q", tc.missing, err, tc.wantError)
	}
	if tc.field != "" {
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
			"expected hard input error, got %v",
			err,
		)
		for _, context := range []string{"secret ns/creds", "[" + tc.field + "]"} {
			assert.ErrorIf(
				t,
				!strings.Contains(err.Error(), context),
				"input error missing context %q: %v",
				context,
				err,
			)
		}
		if strings.Contains(err.Error(), "credential-payload") {
			t.Error("input error exposed credential payload")
		}
	}
	if tc.missing {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok, "expected MissingSecretError; got %T", err)
		owner := b.Named()

		assert.Equal(t, missing.Owner, owner)
		assert.Equal(
			t,
			missing.Secret,
			manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: tc.secret},
		)
		if tc.ref != "auth" && tc.data == nil {
			assert.Equal(t, err.Error(), "secret ns/missing not found")
		}
	}
}

func testFetcherSecretReferencesSuccessfulCredentials(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name string
		auth bool
	}{
		{"anonymous", false},
		{"valid", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherSecretReferencesSuccessfulCredentialsCase(
				t,
				testFetcherSecretReferencesSuccessfulCredentialsCaseInputs{tc: &tc},
			)
		},
		)
	}
}

type testFetcherResolveTransportSnapshotRotationCaseInputs struct {
	fail          *atomic.Bool
	requests      *atomic.Int32
	lookups       *atomic.Int32
	proxyPassword *atomic.Value
	proxy         **httptest.Server
	root          *string
	b             **manifest.Bucket
	password      *string
	ca            *string
	previous      *string
	wave          *string
}

func testFetcherResolveTransportSnapshotRotationCase(
	t *testing.T, testCaseInputValue *testFetcherResolveTransportSnapshotRotationCaseInputs,
) {
	t.Helper()
	testCaseInput := *testCaseInputValue
	if (*testCaseInput.wave) == "password" {
		(*testCaseInput.password) = "second"
	}
	if (*testCaseInput.wave) == "certificate" {
		(*testCaseInput.ca) = testutil.SelfSignedCA(t)
	}
	proxyURL := strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:first@", 1)
	if (*testCaseInput.wave) == "proxy" || (*testCaseInput.wave) == "direct" {
		proxyURL = strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:second@", 1)
		testCaseInput.proxyPassword.
			Store("second")
	}
	t.Setenv("http_proxy", proxyURL)
	if (*testCaseInput.wave) == "direct" {
		t.Setenv("no_proxy", "*")
	}
	c := cachetest.New(t, (*testCaseInput.root), cache.New)
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		testCaseInput.lookups.
			Add(1)
		return &manifest.Secret{
			StringData: map[string]any{
				"accesskey": "access",
				"secretkey": (*testCaseInput.password),
				"ca.crt":    (*testCaseInput.ca),
			},
		}
	}}
	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	before := testCaseInput.requests.Load()
	art, err := f.Fetch(
		t.Context(), (*testCaseInput.b), fInputs.readSet((*testCaseInput.b)),
		cache.Policy{MaxAge: (*testCaseInput.b).SourceInterval(), Retry: cache.RetryConfig{Attempts: 3}},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		(*testCaseInput.previous) == art.Key || testCaseInput.requests.Load() != before+1,
		"%s",
		"changed material reused acquisition",
	)

	(*testCaseInput.previous) = art.Key
	before = testCaseInput.requests.Load()
	{
		_, fetchErr2 := f.Fetch(
			t.Context(), (*testCaseInput.b), fInputs.readSet((*testCaseInput.b)),
			cache.Policy{MaxAge: (*testCaseInput.b).SourceInterval()},
		)
		assert.FatalIf(t, fetchErr2 != nil || testCaseInput.requests.
			Load() != before, "warm snapshot missed: %v", fetchErr2)
	}
	testCaseInput.fail.
		Store(true)
	failed := *(*testCaseInput.b)
	failed.Interval.Duration = 0
	_, err = f.Fetch(
		t.Context(),
		&failed,
		fInputs.readSet(&failed),
		cache.Policy{MaxAge: failed.SourceInterval()},
	)
	terminal, ok := errors.AsType[*cache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "failed snapshot: %v", err)
	testCaseInput.fail.
		Store(false)
}

func testFetcherProviderCredentialsCase(t *testing.T, tc *struct {
	name      string
	provider  string
	sts       bool
	malformed bool
	missing   bool
},
) {
	t.Helper()

	repo := &manifest.Bucket{
		Name:       "provider",
		Namespace:  "apps",
		Endpoint:   "s3.amazonaws.com",
		BucketName: "fixture",
		Provider:   tc.provider,
		SecretRef:  &manifest.LocalObjectReference{Name: "auth"},
	}
	if tc.sts {
		repo.STS = &sourcev1.BucketSTSSpec{Provider: "aws", Endpoint: "https://sts.fixture.invalid"}
	}
	inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		if tc.malformed {
			return &manifest.Secret{Data: map[string]any{"accesskey": "%%%fixture"}}
		}
		return &manifest.Secret{StringData: map[string]any{"accesskey": "fixture", "secretkey": "fixture"}}
	}}
	fetcher := &Fetcher{Network: newTestNetwork(t)}
	request, err := (&operation{Fetcher: fetcher, secrets: inputs.lookup(repo)}).prepare(repo)
	var client *minio.Client
	if err == nil {
		client, err = minio.New(request.endpoint, &request.options)
	}
	switch {
	case tc.missing:
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrMissingSecret) || client != nil,
			"unavailable provider: %v",
			err,
		)
	case tc.malformed:
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || client != nil, "malformed credential: %v", err)
	default:
		assert.FatalIf(t, err != nil || client == nil, "static AWS credentials rejected: %v", err)
	}
}

type testFetcherSecretReferencesSuccessfulCredentialsCaseInputs struct {
	tc *struct {
		name string
		auth bool
	}
}

func testFetcherSecretReferencesSuccessfulCredentialsCase(
	t *testing.T,
	testCaseInput testFetcherSecretReferencesSuccessfulCredentialsCaseInputs,
) {
	t.Helper()

	b := &manifest.Bucket{
		Name:       "b",
		Namespace:  "ns",
		Endpoint:   "https://bucket.invalid",
		BucketName: "fixture",
	}
	if testCaseInput.tc.auth {
		b.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
	}
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		if !testCaseInput.tc.auth {
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
	if testCaseInput.tc.auth {
		access, secret = "access", "secret"
	}
	if value.AccessKeyID != access || value.SecretAccessKey != secret {
		t.Error("credential values were not preserved")
	}
}

type testFetcherResolveTransportSnapshotRotationCallbackInputs struct {
	t             **testing.T
	fail          *atomic.Bool
	requests      *atomic.Int32
	proxyPassword *atomic.Value
}

func testFetcherResolveTransportSnapshotRotationCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherResolveTransportSnapshotRotationCallbackInputs,
) {
	if r.URL.IsAbs() &&
		r.Header.Get(
			"Proxy-Authorization",
		) != "Basic "+base64.StdEncoding.EncodeToString(
			[]byte("proxy:"+testCaseInput.proxyPassword.Load().(string)),
		) {
		(*testCaseInput.t).
			Error("transport used a different proxy snapshot")
		w.WriteHeader(http.StatusProxyAuthRequired)
		return
	}
	testCaseInput.requests.
		Add(1)
	if !strings.Contains(r.Header.Get("Authorization"), "Credential=access/") {
		(*testCaseInput.t).
			Error("static credentials lost")
	}
	if testCaseInput.fail.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	_, _ = io.WriteString(
		w,
		`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`,
	)
}

type testFetcherSecretReferencesCaseCallbackInputs struct {
	tc **struct {
		name string
		ref  string

		secret    string
		wantError string
		data      map[string]any
		rawData   map[string]any
		unwired   bool
		encoded   bool
		missing   bool
		field     string
	}
}

func testFetcherSecretReferencesCaseCallback(
	_, _ string,
	testCaseInput testFetcherSecretReferencesCaseCallbackInputs,
) *manifest.Secret {
	if (*testCaseInput.tc).data == nil && (*testCaseInput.tc).rawData == nil {
		return nil
	}
	sec := &manifest.Secret{
		Name:       "metadata-secret",
		Namespace:  "elsewhere",
		StringData: (*testCaseInput.tc).data,
		Data:       (*testCaseInput.tc).rawData,
	}
	if (*testCaseInput.tc).encoded {
		sec.StringData, sec.Data = nil, map[string]any{}
		for k, v := range (*testCaseInput.tc).data {
			sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
		}
	}
	return sec
}
