package bucket

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/testutil"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type machineProvider struct {
	value credentials.Value
	calls atomic.Int32
	get   func(*credentials.CredContext) (credentials.Value, error)
}

func (p *machineProvider) RetrieveWithCredContext(cc *credentials.CredContext) (credentials.Value, error) {
	p.calls.Add(1)
	if p.get != nil {
		return p.get(cc)
	}
	return p.value, nil
}

func (p *machineProvider) Retrieve() (credentials.Value, error) {
	return p.RetrieveWithCredContext(nil)
}

func (p *machineProvider) IsExpired() bool { return false }

func (f *operation) fetchMachine(
	ctx context.Context, requestValue *bucketAcquisition,
	providers []credentials.Provider,
	policy *cache.Policy,
) (*store.SourceArtifact, error) {
	request := *requestValue
	timeout := request.bucket.OperationTimeout()
	policy.Timeout = &timeout
	prepared, err := source.AcquireMachine(
		ctx,
		policy,
		func(ctx context.Context) error { return request.machineCredentials(ctx, providers) },
		func(ctx context.Context, policy *cache.Policy, _ *store.SourceArtifact) (*store.SourceArtifact, error) {
			return f.fetch(ctx, &request, policy)
		},
	)
	if err != nil {
		return nil, err
	}
	return prepared.Fetch(ctx, nil)
}

type bucketTransport func(*http.Request) (*http.Response, error)

func (f bucketTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func bucketReply(r *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Status:        strconv.Itoa(status),
		Header:        http.Header{"Content-Type": {"application/xml"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       r,
	}
}

func ambientBucket() *manifest.Bucket {
	b := &manifest.Bucket{
		Name:       "source",
		Namespace:  "apps",
		Endpoint:   "https://bucket.invalid",
		BucketName: "data",
		Region:     "us-east-1",
		SecretRef:  &manifest.LocalObjectReference{Name: "auth"},
	}
	b.Interval.Duration = time.Hour
	return b
}

func TestFetcher_MachineContinuationEligibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, provider string
		sts            bool
		auth, cert     *manifest.Secret
		certRef, proxy bool
		wantAmbient    bool
		wantInput      bool
	}{
		{name: "unavailable auth", wantAmbient: true},
		{name: "unavailable AWS auth", provider: sourcev1.BucketProviderAmazon, wantAmbient: true},
		{name: "unsupported provider auth", provider: sourcev1.BucketProviderGoogle},
		{name: "STS auth", sts: true},
		{name: "empty auth", auth: &manifest.Secret{StringData: map[string]any{"accesskey": "", "secretkey": "key"}}, wantAmbient: true},
		{name: "malformed auth", auth: &manifest.Secret{Data: map[string]any{"accesskey": "credential-canary!"}}, wantInput: true},
		{name: "missing TLS", certRef: true},
		{name: "malformed TLS", certRef: true, cert: &manifest.Secret{StringData: map[string]any{"ca.crt": "credential-canary"}}, wantInput: true},
		{name: "missing proxy", proxy: true},
		{name: "malformed proxy", proxy: true, cert: &manifest.Secret{StringData: map[string]any{"address": "://credential-canary"}}, wantInput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherMachineContinuationEligibilityCase1(t, &tc)
		})
	}
}

func testFetcherMachineContinuationEligibilityCase1(t *testing.T, tc *struct {
	name        string
	provider    string
	sts         bool
	auth        *manifest.Secret
	cert        *manifest.Secret
	certRef     bool
	proxy       bool
	wantAmbient bool
	wantInput   bool
},
) {
	t.Helper()

	b := ambientBucket()
	b.Provider = tc.provider
	if tc.sts {
		b.STS = &sourcev1.BucketSTSSpec{
			Provider: sourcev1.BucketProviderAmazon,
			Endpoint: "https://sts.invalid",
		}
	}
	if tc.certRef {
		b.CertSecretRef = &manifest.LocalObjectReference{Name: "transport"}
	}
	if tc.proxy {
		b.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
	}
	inputs := testInputs{secrets: func(_, name string) *manifest.Secret {
		if name == "auth" {
			return tc.auth
		}
		return tc.cert
	}}
	f := &Fetcher{Network: newTestNetwork(t)}
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, inputs.readSet(b), &cache.Policy{}, nil)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, err == nil || artifact != nil || (ok && missing.RetryAmbient != nil) != tc.wantAmbient ||
		errors.Is(err, manifest.ErrInput) != tc.wantInput ||
		strings.Contains(err.Error(), "credential-canary"),

		"ambient=%t input=%t: %v",
		ok && missing.RetryAmbient != nil,
		errors.Is(err, manifest.ErrInput),
		err)
	assert.FatalIf(
		t,
		(tc.sts || tc.provider == sourcev1.BucketProviderGoogle) && (!ok || missing.Secret.Name != "auth"),
		"provider availability lost the missing Secret identity: %v",
		err,
	)
}

func TestFetcher_MachineProviders(t *testing.T) {
	t.Parallel()
	failure := &fs.PathError{Op: "open", Path: "credential-canary", Err: fs.ErrPermission}
	for _, tc := range []struct {
		name         string
		first, next  credentials.Value
		firstErr     error
		nextErr      error
		wantNext     int32
		wantComplete bool
		wantMissing  bool
	}{
		{name: "environment", first: credentials.Value{AccessKeyID: "environment", SecretAccessKey: "secret", SignerType: credentials.SignatureV4}, wantComplete: true},
		{name: "IAM", next: credentials.Value{AccessKeyID: "IAM", SecretAccessKey: "secret", SignerType: credentials.SignatureV4}, wantNext: 1, wantComplete: true},
		{name: "partial environment", first: credentials.Value{AccessKeyID: "partial", SignerType: credentials.SignatureAnonymous}, next: credentials.Value{AccessKeyID: "IAM", SecretAccessKey: "secret", SignerType: credentials.SignatureV4}, wantNext: 1, wantComplete: true},
		{name: "anonymous", first: credentials.Value{AccessKeyID: "anonymous", SecretAccessKey: "secret", SignerType: credentials.SignatureAnonymous}, wantNext: 1, wantComplete: true},
		{name: "incomplete signed value", first: credentials.Value{AccessKeyID: "partial", SignerType: credentials.SignatureV4}},
		{name: "exhausted", wantNext: 1, wantComplete: true},
		{name: "environment failure", firstErr: failure},
		{name: "IAM failure", nextErr: failure, wantNext: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherMachineProvidersCase(t, &failure, &tc)
		},
		)
	}
}

func TestFetcher_MachineAcquisitionFailures(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"listing", "download", "publication"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			testFetcherMachineAcquisitionFailuresCase(t, &stage)
		},
		)
	}
}

func clearMachineEnvironment(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(root, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(root, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"} {
		t.Setenv(key, "")
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
func TestFetcher_MachineProfiles(t *testing.T) {
	for _, shape := range []string{"process", "static before process", "environment before files", "partial", "missing executable", "failing process", "missing profile", "no default profile", "selected profile", "shared credentials", "malformed INI", "no files", "absent SSO token"} {
		t.Run(shape, func(t *testing.T) {
			testFetcherMachineProfilesCase(t, &shape)
		},
		)
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_MachineIAMFailures(t *testing.T) {
	clearMachineEnvironment(t)
	for _, tc := range []struct {
		name      string
		container bool
		cause     error
		missing   bool
	}{
		{name: "metadata dial", cause: &net.OpError{Op: "dial", Err: fs.ErrNotExist}, missing: true},
		{name: "metadata deadline", cause: context.DeadlineExceeded, missing: true},
		{name: "metadata read", cause: &net.OpError{Op: "read", Err: io.ErrUnexpectedEOF}},
		{name: "metadata HTTP"},
		{name: "container dial", container: true, cause: &net.OpError{Op: "dial", Err: fs.ErrNotExist}},
		{name: "container deadline", container: true, cause: context.DeadlineExceeded},
		{name: "container HTTP", container: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := ambientBucket()
			f := &operation{
				Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)},
				secrets: func(string, string) *manifest.Secret { return nil },
			}
			request, err := f.preparedBucket(b)
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			providers := []credentials.Provider{
				&machineProvider{get: func(*credentials.CredContext) (credentials.Value, error) {
					endpoint := credentials.DefaultIAMRoleEndpoint
					if tc.container {
						endpoint = credentials.DefaultECSRoleEndpoint
					}
					cause := tc.cause
					if cause == nil {
						cause = errors.New("403 credential-canary")
					}
					return credentials.Value{}, &url.Error{Op: "Get", URL: endpoint + "/credentials", Err: cause}
				}},
			}
			request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
				return bucketReply(r, http.StatusForbidden, `<Error><Code>AccessDenied</Code></Error>`), nil
			})
			artifact, err := f.fetchMachine(t.Context(), &request, providers, &cache.Policy{})
			assert.FatalIf(
				t,
				artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing ||
					strings.Contains(err.Error(), "canary"),
				"missing=%t, acquisition lost its outcome or exposed credentials: %v",
				tc.missing,
				err,
			)

			assert.FatalIf(
				t,
				!tc.missing && tc.cause != nil && !errors.Is(err, tc.cause),
				"lost native IAM failure cause: %v",
				err,
			)
		})
	}
}

func bucketPipe(t *testing.T, handle func(*http.Request) *http.Response) {
	t.Helper()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		t.Setenv(key, "")
	}
	old := http.DefaultTransport
	transport := old.(*http.Transport).Clone()
	transport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			request, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			defer request.Body.Close()
			response := handle(request)
			response.Close = true
			_ = response.Write(server)
		}()
		return client, nil
	}
	http.DefaultTransport = transport //nolint:reassign // The test replaces the process transport to exercise offline acquisition and restores it afterward.
	t.Cleanup(
		func() {
			transport.CloseIdleConnections()
			http.DefaultTransport = old //nolint:reassign // The test must restore the process HTTP transport after offline acquisition.
		},
	)
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_MachineContinuation(t *testing.T) {
	clearMachineEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "machine-canary")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "machine-secret-canary")
	var requests atomic.Int32
	access := "machine-canary"
	rejected := false
	bucketPipe(t, func(r *http.Request) *http.Response {
		requests.Add(1)
		if r.Host == "sts.amazonaws.com" {
			return bucketReply(
				r,
				403,
				`<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Code>AccessDenied</Code><Message>machine-canary</Message></Error></ErrorResponse>`,
			)
		}
		assert.ErrorIf(
			t,
			access != "" && !strings.Contains(r.Header.Get("Authorization"), "Credential="+access+"/"),
			"%s",
			"credential precedence selected a different signer",
		)
		assert.ErrorIf(
			t,
			access == "" && r.Header.Get("Authorization") != "",
			"%s",
			"public source consulted machine credentials",
		)

		if rejected {
			return bucketReply(r, 403, `<Error><Code>AccessDenied</Code><Message>rejected</Message></Error>`)
		}
		if r.URL.Query().Get("list-type") != "" {
			return bucketReply(
				r,
				200,
				`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>file.yaml</Key><Size>4</Size><ETag>"etag"</ETag></Contents></ListBucketResult>`,
			)
		}
		response := bucketReply(r, 200, "data")
		response.Header.Set("ETag", `"etag"`)
		response.Header.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		return response
	})
	for _, tier := range []string{"repository", "supply", "partial supply", "machine", "public", "rejected", "STS rejected"} {
		t.Run(tier, func(t *testing.T) {
			testFetcherMachineContinuationCase1(
				t,
				&testFetcherMachineContinuationCase1Inputs{
					rejected: &rejected,
					tier:     &tier,
					access:   &access,
					requests: &requests,
				},
			)
		})
	}
}

func testFetcherMachineContinuationCase1(
	t *testing.T, inputs *testFetcherMachineContinuationCase1Inputs,
) {
	t.Helper()
	b := ambientBucket()
	st := store.New()
	(*inputs.rejected) = (*inputs.tier) == "rejected"
	if (*inputs.tier) == "STS rejected" {
		clearMachineEnvironment(t)
		token := filepath.Join(t.TempDir(), "web-identity")
		testutil.WriteFileAt(t, token, "synthetic-token")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture")
	}
	switch {
	case (*inputs.tier) == "public":
		b.SecretRef = nil
		(*inputs.access) = ""
	case (*inputs.tier) != "machine" && (*inputs.tier) != "STS rejected":
		testFetcherMachineContinuationCase1Part1(&inputs.access, &inputs.tier, &st)
	default:

		(*inputs.access) = "machine-canary"
	}

	st.AddObject(b)
	var reads store.ReadSet
	reads.Init(st, b.Named(), nil)
	defer reads.Close()
	root := ""
	if (*inputs.tier) == "machine" {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)}
	initialRequests := inputs.requests.Load()
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, &reads, &cache.Policy{MaxAge: time.Hour}, nil)
	if (*inputs.tier) == "rejected" {
		assert.FatalIf(
			t,
			err == nil || artifact != nil || !errors.Is(err, manifest.ErrMissingSecret) || inputs.requests.
				Load() != initialRequests+1,
			"explicit rejection advanced credential tiers: %v",
			err,
		)

		return
	}
	if (*inputs.tier) == "STS rejected" {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.RetryAmbient == nil,
			"missing continuation: %v", err)

		artifact, err = fetchtest.Retry(t.Context(), missing.RetryAmbient)
		rejection, rejected := errors.AsType[credentials.ErrorResponse](err)
		assert.FatalIf(t, artifact != nil || !rejected || rejection.STSError.Code != "AccessDenied" ||
			errors.Is(err, manifest.ErrMissingSecret) ||
			strings.Contains(err.Error(), "canary") || inputs.requests.
			Load() == initialRequests,
			"machine STS rejection lost its cause or exposed credentials: %v", err)

		return
	}
	if (*inputs.tier) == "machine" {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.RetryAmbient == nil || artifact != nil,
			"missing auth did not retain continuation: %v", err)

		before, prepErr := (&operation{Fetcher: f, secrets: source.SecretLookup(&reads)}).preparedBucket(b)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-before-first-acquisition")
		after, prepErr := (&operation{Fetcher: f, secrets: source.SecretLookup(&reads)}).preparedBucket(b)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		assert.FatalIf(t, before.material != nil || after.material != nil ||
			before.options.Transport != after.options.Transport, "%s", "machine values changed transport identity")

		artifact, err = fetchtest.Retry(t.Context(), missing.RetryAmbient)
	}
	testFetcherMachineContinuationCase1Tail(
		t,
		&testFetcherMachineContinuationCase1TailInputs{
			err:      &err,
			artifact: &artifact,
			requests: &inputs.requests,
			f:        &f,
			b:        &b,
			reads:    &reads,
			tier:     &inputs.tier,
			access:   &inputs.access,
			root:     &root,
		},
	)
}

func testFetcherMachineContinuationCase1Part1(access, tier **string, st **store.Store) {
	(*(*access)) = "repository"
	sec := &manifest.Secret{
		Name:       "auth",
		Namespace:  "apps",
		StringData: map[string]any{"accesskey": (*(*access)), "secretkey": "explicit-secret"},
	}
	if (*(*tier)) != "supply" {
		if (*(*tier)) == "partial supply" {
			delete(sec.StringData, "secretkey")
		}
		(*st).
			AddObject(sec)
	}
	if (*(*tier)) == "supply" || (*(*tier)) == "partial supply" {
		supplied := []store.Declaration{
			{
				Doc: testutil.ObjectDocument(
					&manifest.Secret{
						Name:       "auth",
						Namespace:  "apps",
						StringData: map[string]any{"accesskey": "supply", "secretkey": "supply-secret"},
					},
				),
				Obj: &manifest.Secret{
					Name:       "auth",
					Namespace:  "apps",
					StringData: map[string]any{"accesskey": "supply", "secretkey": "supply-secret"},
				},
				Origin: manifest.SuppliedDeclaration,
			},
		}
		for _, declaration := range supplied {
			(*st).
				AddDeclaration(declaration)
		}
		if (*(*tier)) == "supply" {
			(*(*access)) = "supply"
		}
	}
}

func bucketCredentialRoute(t *testing.T, hostname string, handle func(*http.Request) *http.Response) {
	t.Helper()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		t.Setenv(key, "")
	}
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	transport.DialTLSContext = func(_ context.Context, _, address string) (net.Conn, error) {
		if address != hostname+":443" {
			return nil, fmt.Errorf("unexpected credential destination %s", address)
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			request, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			defer request.Body.Close()
			response := handle(request)
			response.Close = true
			_ = response.Write(server)
		}()
		return client, nil
	}
	http.DefaultTransport = transport //nolint:reassign // The test replaces the process transport to exercise offline acquisition and restores it afterward.
	t.Cleanup(
		func() {
			transport.CloseIdleConnections()
			http.DefaultTransport = previous //nolint:reassign // The test must restore the process HTTP transport after offline acquisition.
		},
	)
}

func bucketSourceProxy(t *testing.T, target string, observe func(*http.Request)) string {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observe(r)
		if r.Method != http.MethodConnect || r.Host != target {
			http.Error(w, "unexpected proxy destination", http.StatusBadGateway)
			return
		}
		remote, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", target)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer remote.Close()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		go func() { _, _ = io.Copy(remote, conn); _ = remote.Close() }()
		_, _ = io.Copy(conn, remote)
	}))
	t.Cleanup(proxy.Close)
	return proxy.URL
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_MachineSSOTransport(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(strconv.FormatBool(canceled), func(t *testing.T) {
			testFetcherMachineSSOTransportCase(t, &canceled)
		},
		)
	}
}

func testFetcherMachineProvidersCase(t *testing.T, failure **fs.PathError,
	tc *struct {
		name  string
		first credentials.Value
		next  credentials.
			Value
		firstErr     error
		nextErr      error
		wantNext     int32
		wantComplete bool
		wantMissing  bool
	},
) {
	t.Helper()

	b := ambientBucket()
	f := &operation{
		Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)},
		secrets: func(string, string) *manifest.Secret { return nil },
	}
	request, err := f.preparedBucket(b)
	providers := []credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.FileAWSCredentials{},
		&credentials.IAM{},
	}
	assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
	assert.FatalIf(t, len(providers) != 3, "%s", "machine chain must contain EnvAWS, FileAWSCredentials and IAM")
	{
		_, ok := providers[0].(*credentials.EnvAWS)
		assert.FatalIf(t, !ok, "%s", "first native provider is not EnvAWS")
	}
	{
		_, ok := providers[2].(*credentials.IAM)
		assert.FatalIf(t, !ok, "%s", "second native provider is not IAM")
	}
	assert.FatalIf(
		t,
		request.options.Creds != nil || request.material != nil,
		"%s",
		"unavailable auth became identity material",
	)

	first := &machineProvider{value: tc.first}
	next := &machineProvider{value: tc.next}
	first.get = func(cc *credentials.CredContext) (credentials.Value, error) {
		assert.ErrorIf(
			t,
			cc == nil || cc.Context == nil || cc.Client == nil || cc.Client.Transport != f.Network,
			"%s",
			"provider lost the producer context or guarded client",
		)

		if _, ok := cc.Context.Deadline(); !ok {
			t.Error("provider ran without the bounded acquisition deadline")
		}
		return tc.first, tc.firstErr
	}
	next.get = func(cc *credentials.CredContext) (credentials.Value, error) {
		assert.ErrorIf(t, cc.Client.Transport != f.Network, "%s", "fallback provider bypassed the run transport")

		return tc.next, tc.nextErr
	}
	providers = []credentials.Provider{first, next}
	var signed atomic.Int32
	request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
		assert.ErrorIf(t, r.Header.Get("Authorization") == "" && tc.name != "anonymous" && tc.
			name != "exhausted", "%s", "machine request is unsigned")

		signed.Add(1)
		return bucketReply(
			r,
			200,
			`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`,
		), nil
	})
	artifact, err := f.fetchMachine(t.Context(), &request, providers, &cache.Policy{MaxAge: time.Hour})
	assert.FatalIf(t, (err == nil) != tc.wantComplete || (artifact != nil) != tc.wantComplete ||
		first.calls.Load() != 1 ||
		next.calls.Load() != tc.wantNext,
		"complete=%t calls=%d/%d: %v", artifact != nil, first.calls.Load(), next.calls.Load(), err)

	assert.FatalIf(
		t,
		errors.Is(err, manifest.ErrMissingSecret) != tc.wantMissing,
		"only anonymous exhaustion is unavailable: %v",
		err,
	)
	if tc.firstErr != nil || tc.nextErr != nil {
		assert.FatalIf(
			t,
			!errors.Is(err, (*failure)) || strings.Contains(err.Error(), "canary"),
			"provider failure lost its cause or exposed credentials: %v",
			err,
		)
	}
	assert.FatalIf(t, !tc.wantComplete && signed.Load() != 0, "%s", "unusable credentials reached the bucket")
	assert.FatalIf(
		t,
		tc.wantComplete && (signed.Load() != 1 || len(artifact.Warnings) != 0),
		"%s",
		"successful signed acquisition emitted warnings or was unsigned",
	)
}

func testFetcherMachineAcquisitionFailuresCase(
	t *testing.T, stage *string,
) {
	t.Helper()

	b := ambientBucket()
	root := t.TempDir()
	c := cachetest.New(t, root, cache.New)
	f := &operation{
		Fetcher: &Fetcher{Cache: c, Network: newTestNetwork(t)},
		secrets: func(string, string) *manifest.Secret { return nil },
	}
	request, err := f.preparedBucket(b)
	assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
	providers := []credentials.Provider{
		&machineProvider{
			value: credentials.Value{
				AccessKeyID:     "credential-canary",
				SecretAccessKey: "secret-canary",
				SignerType:      credentials.SignatureV4,
			},
		},
	}
	request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
		if (*stage) == "listing" || r.URL.Query().Get("list-type") == "" {
			return bucketReply(
				r,
				403,
				`<Error><Code>AccessDenied</Code><Message>credential-canary secret-canary</Message></Error>`,
			), nil
		}
		if (*stage) == "publication" {
			assert.NoError(t, c.Close())
			return bucketReply(
				r,
				200,
				`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`,
			), nil
		}
		return bucketReply(
			r,
			200,
			`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>file.yaml</Key><Size>4</Size><ETag>"etag"</ETag></Contents></ListBucketResult>`,
		), nil
	})
	artifact, err := f.fetchMachine(t.Context(), &request, providers, &cache.Policy{})
	assert.FatalIf(
		t,
		err == nil || artifact != nil || strings.Contains(err.Error(), "canary"),
		"failed acquisition published or exposed credentials: %v",
		err,
	)
	if (*stage) != "publication" {
		response, ok := errors.AsType[*fetcherr.ResponseError](err)
		assert.FatalIf(
			t,
			!ok || response.StatusCode != http.StatusForbidden,
			"lost typed remote failure: %v",
			err,
		)
	}
	assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		assert.ErrorIf(
			t,
			strings.Contains(string(data), "canary"),
			"%s",
			"failed machine acquisition persisted credential material",
		)

		return err
	}))
}
