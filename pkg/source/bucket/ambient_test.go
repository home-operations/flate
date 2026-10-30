package bucket

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
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

func (f *operation) fetchMachine(ctx context.Context, request bucketAcquisition, providers []credentials.Provider, policy cache.Policy) (*store.SourceArtifact, error) {
	timeout := request.bucket.OperationTimeout()
	policy.Timeout = &timeout
	return source.AcquireMachine(ctx, policy, func(ctx context.Context) error { return request.machineCredentials(ctx, providers) }, func(ctx context.Context, policy cache.Policy) (*store.SourceArtifact, error) {
		return f.fetch(ctx, request, policy)
	})
}

type bucketTransport func(*http.Request) (*http.Response, error)

func (f bucketTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func bucketReply(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprint(status), Header: http.Header{"Content-Type": {"application/xml"}}, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r}
}

func ambientBucket() *manifest.Bucket {
	b := &manifest.Bucket{Name: "source", Namespace: "apps", Endpoint: "https://bucket.invalid", BucketName: "data", Region: "us-east-1", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
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
		{name: "wiped auth", auth: &manifest.Secret{StringData: map[string]any{"accesskey": manifest.ValuePlaceholder("accesskey"), "secretkey": "key"}}, wantAmbient: true},
		{name: "malformed auth", auth: &manifest.Secret{Data: map[string]any{"accesskey": "credential-canary!"}}, wantInput: true},
		{name: "missing TLS", certRef: true},
		{name: "malformed TLS", certRef: true, cert: &manifest.Secret{StringData: map[string]any{"ca.crt": "credential-canary"}}, wantInput: true},
		{name: "missing proxy", proxy: true},
		{name: "malformed proxy", proxy: true, cert: &manifest.Secret{StringData: map[string]any{"address": "://credential-canary"}}, wantInput: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ambientBucket()
			b.Provider = tc.provider
			if tc.sts {
				b.STS = &sourcev1.BucketSTSSpec{Provider: sourcev1.BucketProviderAmazon, Endpoint: "https://sts.invalid"}
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
			artifact, err := f.Fetch(t.Context(), b, inputs.readSet(b), cache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if err == nil || artifact != nil || (ok && missing.RetryAmbient != nil) != tc.wantAmbient || errors.Is(err, manifest.ErrInput) != tc.wantInput || strings.Contains(err.Error(), "credential-canary") {
				t.Fatalf("ambient=%t input=%t: %v", ok && missing.RetryAmbient != nil, errors.Is(err, manifest.ErrInput), err)
			}
			if (tc.sts || tc.provider == sourcev1.BucketProviderGoogle) && (!ok || missing.Secret.Name != "auth") {
				t.Fatalf("provider availability lost the missing Secret identity: %v", err)
			}
		})
	}
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
			b := ambientBucket()
			f := &operation{Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
			request, err := f.prepare(b)
			providers := []credentials.Provider{&credentials.EnvAWS{}, &credentials.FileAWSCredentials{}, &credentials.IAM{}}
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			if len(providers) != 3 {
				t.Fatal("machine chain must contain EnvAWS, FileAWSCredentials and IAM")
			}
			if _, ok := providers[0].(*credentials.EnvAWS); !ok {
				t.Fatal("first native provider is not EnvAWS")
			}
			if _, ok := providers[2].(*credentials.IAM); !ok {
				t.Fatal("second native provider is not IAM")
			}
			if request.options.Creds != nil || request.material != nil {
				t.Fatal("unavailable auth became identity material")
			}
			first := &machineProvider{value: tc.first}
			next := &machineProvider{value: tc.next}
			first.get = func(cc *credentials.CredContext) (credentials.Value, error) {
				if cc == nil || cc.Context == nil || cc.Client == nil || cc.Client.Transport != f.Network {
					t.Error("provider lost the producer context or guarded client")
				}
				if _, ok := cc.Context.Deadline(); !ok {
					t.Error("provider ran without the bounded acquisition deadline")
				}
				return tc.first, tc.firstErr
			}
			next.get = func(cc *credentials.CredContext) (credentials.Value, error) {
				if cc.Client.Transport != f.Network {
					t.Error("fallback provider bypassed the run transport")
				}
				return tc.next, tc.nextErr
			}
			providers = []credentials.Provider{first, next}
			var signed atomic.Int32
			request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") == "" && tc.name != "anonymous" && tc.name != "exhausted" {
					t.Error("machine request is unsigned")
				}
				signed.Add(1)
				return bucketReply(r, 200, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`), nil
			})
			artifact, err := f.fetchMachine(t.Context(), request, providers, cache.Policy{MaxAge: time.Hour})
			if (err == nil) != tc.wantComplete || (artifact != nil) != tc.wantComplete || first.calls.Load() != 1 || next.calls.Load() != tc.wantNext {
				t.Fatalf("complete=%t calls=%d/%d: %v", artifact != nil, first.calls.Load(), next.calls.Load(), err)
			}
			if errors.Is(err, manifest.ErrMissingSecret) != tc.wantMissing {
				t.Fatalf("only anonymous exhaustion is unavailable: %v", err)
			}
			if tc.firstErr != nil || tc.nextErr != nil {
				if !errors.Is(err, failure) || strings.Contains(err.Error(), "canary") {
					t.Fatalf("provider failure lost its cause or exposed credentials: %v", err)
				}
			}
			if !tc.wantComplete && signed.Load() != 0 {
				t.Fatal("unusable credentials reached the bucket")
			}
			if tc.wantComplete && (signed.Load() != 1 || len(artifact.Warnings) != 0) {
				t.Fatal("successful signed acquisition emitted warnings or was unsigned")
			}
		})
	}
}

func TestFetcher_MachineAcquisitionFailures(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"listing", "download", "publication"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			b := ambientBucket()
			root := t.TempDir()
			c := cachetest.New(t, root, cache.New)
			f := &operation{Fetcher: &Fetcher{Cache: c, Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
			request, err := f.prepare(b)
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			providers := []credentials.Provider{&machineProvider{value: credentials.Value{AccessKeyID: "credential-canary", SecretAccessKey: "secret-canary", SignerType: credentials.SignatureV4}}}
			request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
				if stage == "listing" || r.URL.Query().Get("list-type") == "" {
					return bucketReply(r, 403, `<Error><Code>AccessDenied</Code><Message>credential-canary secret-canary</Message></Error>`), nil
				}
				if stage == "publication" {
					assert.NoError(t, c.Close())
					return bucketReply(r, 200, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`), nil
				}
				return bucketReply(r, 200, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>file.yaml</Key><Size>4</Size><ETag>"etag"</ETag></Contents></ListBucketResult>`), nil
			})
			artifact, err := f.fetchMachine(t.Context(), request, providers, cache.Policy{})
			if err == nil || artifact != nil || strings.Contains(err.Error(), "canary") {
				t.Fatalf("failed acquisition published or exposed credentials: %v", err)
			}
			if stage != "publication" {
				response, ok := errors.AsType[*fetcherr.ResponseError](err)
				if !ok || response.StatusCode != http.StatusForbidden {
					t.Fatalf("lost typed remote failure: %v", err)
				}
			}
			assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := os.ReadFile(path)
				if strings.Contains(string(data), "canary") {
					t.Error("failed machine acquisition persisted credential material")
				}
				return err
			}))
		})
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

func TestFetcher_MachineProfiles(t *testing.T) {
	for _, shape := range []string{"process", "static before process", "environment before files", "partial", "missing executable", "failing process", "missing profile", "no default profile", "selected profile", "shared credentials", "malformed INI", "no files", "absent SSO token"} {
		t.Run(shape, func(t *testing.T) {
			clearMachineEnvironment(t)
			root := t.TempDir()
			marker := filepath.Join(root, "executed")
			program := filepath.Join(root, "credential-process")
			script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + filepath.Join(root, "args") + "'\n/bin/cat > '" + filepath.Join(root, "stdin") + "'\n/usr/bin/env > '" + filepath.Join(root, "environment") + "'\nprintf called > '" + marker + "'\nprintf '%s' '{\"Version\":1,\"AccessKeyId\":\"file-canary\",\"SecretAccessKey\":\"file-secret\"}'\n"
			if shape == "failing process" {
				script = "#!/bin/sh\nprintf '%s' 'credential-canary' >&2\nexit 1\n"
			}
			testutil.WriteFileAt(t, program, script)
			assert.NoError(t, os.Chmod(program, 0o700))
			profile := "[default]\ncredential_process = " + program + " argument\n"
			switch shape {
			case "static before process":
				profile += "aws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
			case "environment before files":
				t.Setenv("AWS_ACCESS_KEY_ID", "environment")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
				profile = "invalid INI ["
			case "partial":
				profile += "aws_access_key_id = partial\n"
			case "missing executable":
				profile = "[default]\ncredential_process = " + filepath.Join(root, "absent") + " argument\n"
			case "missing profile":
				t.Setenv("AWS_PROFILE", "absent")
			case "no default profile":
				profile = "[profile other]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
			case "selected profile":
				t.Setenv("AWS_PROFILE", "selected")
				profile = "[profile selected]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
			case "shared credentials":
				profile = "[default]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n"
			case "malformed INI":
				profile = "[unterminated"
			case "absent SSO token":
				profile = "[default]\nsso_role_name = role\nsso_account_id = 123456789012\nsso_start_url = https://sso.invalid/start\nsso_region = us-east-1\n"
			}
			path := os.Getenv("AWS_CONFIG_FILE")
			if shape == "shared credentials" {
				path = os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
			}
			if shape != "no files" {
				testutil.WriteFileAt(t, path, profile)
			}
			var remote, iam atomic.Int32
			metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				iam.Add(1)
				_, _ = io.WriteString(w, `{"Code":"Success","Expiration":"2099-01-01T00:00:00Z"}`)
			}))
			t.Cleanup(metadata.Close)
			t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", metadata.URL)
			var signer atomic.Value
			signer.Store("file-canary")
			switch shape {
			case "environment before files":
				signer.Store("environment")
			case "no files":
				signer.Store("")
			}
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				remote.Add(1)
				access := signer.Load().(string)
				if access == "" && r.Header.Get("Authorization") != "" || access != "" && !strings.Contains(r.Header.Get("Authorization"), "Credential="+access+"/") {
					t.Error("profile acquisition used a different signer")
				}
				_, _ = io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
			}))
			t.Cleanup(endpoint.Close)
			b := ambientBucket()
			b.Endpoint, b.Insecure = endpoint.URL, true
			inputs := testInputs{}
			ca, proxyAddress := "", ""
			if shape == "process" {
				t.Setenv("FLATE_PROCESS_MACHINE", "machine-environment")
				ca = testutil.SelfSignedCA(t)
				proxy := httptest.NewServer(endpoint.Config.Handler)
				t.Cleanup(proxy.Close)
				proxyAddress = proxy.URL
				b.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
				b.CertSecretRef = b.ProxySecretRef
				inputs.secrets = func(string, string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{
						"address": proxy.URL, "username": "repository-proxy-user", "password": "repository-proxy-password", "ca.crt": ca,
					}}
				}
			}
			reads := inputs.readSet(b)
			defer reads.Close()
			f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
			artifact, err := f.Fetch(t.Context(), b, reads, cache.Policy{MaxAge: time.Hour})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil || artifact != nil || remote.Load() != 0 || iam.Load() != 0 {
				t.Fatalf("profile lookup ran before continuation: %v", err)
			}
			artifact, err = missing.RetryMachine(t.Context(), reads)
			success := shape == "process" || shape == "static before process" || shape == "environment before files" || shape == "selected profile" || shape == "shared credentials"
			unavailable := shape == "no files"
			var wantRemote, wantIAM int32
			if success || unavailable {
				wantRemote = 1
			}
			if unavailable {
				wantIAM = 1
			}
			if (err == nil) != (success || unavailable) || (artifact != nil) != (success || unavailable) || errors.Is(err, manifest.ErrMissingSecret) || remote.Load() != wantRemote || iam.Load() != wantIAM {
				t.Fatalf("profile selection: %v remote=%d IAM=%d", err, remote.Load(), iam.Load())
			}
			_, markerErr := os.Stat(marker)
			if (markerErr == nil) != (shape == "process") {
				t.Fatalf("credential_process precedence: %v", markerErr)
			}
			if shape == "process" {
				args, err := os.ReadFile(filepath.Join(root, "args"))
				assert.NoError(t, err)
				assert.Equal(t, string(args), "argument\n")
				stdin, err := os.ReadFile(filepath.Join(root, "stdin"))
				assert.NoError(t, err)
				assert.Equal(t, string(stdin), "")
				environment, err := os.ReadFile(filepath.Join(root, "environment"))
				assert.NoError(t, err)
				if !strings.Contains(string(environment), "FLATE_PROCESS_MACHINE=machine-environment") {
					t.Fatal("credential process lost the machine environment")
				}
				for _, canary := range []string{proxyAddress, "repository-proxy-user", "repository-proxy-password", ca, "-----BEGIN CERTIFICATE-----"} {
					if strings.Contains(string(args)+string(stdin)+string(environment), canary) {
						t.Fatal("repository transport material reached credential process inputs")
					}
				}
			}
			if success {
				warm, err := missing.RetryMachine(t.Context(), reads)
				assert.NoError(t, err)
				if warm.Key != artifact.Key || remote.Load() != 1 {
					t.Fatal("unchanged profile missed warm content")
				}
				signer.Store("rotated-canary")
				if shape == "environment before files" {
					t.Setenv("AWS_ACCESS_KEY_ID", "rotated-canary")
				} else {
					testutil.WriteFileAt(t, path, strings.ReplaceAll(profile, "file-canary", "rotated-canary"))
					testutil.WriteFileAt(t, program, strings.ReplaceAll(script, "file-canary", "rotated-canary"))
				}
				rotated, err := missing.RetryMachine(t.Context(), reads)
				assert.NoError(t, err)
				if rotated.Key == warm.Key || remote.Load() != 2 || iam.Load() != 0 {
					t.Fatal("rotated profile reused credential identity or skipped signing")
				}
			}
		})
	}
}

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
			f := &operation{Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
			request, err := f.prepare(b)
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			providers := []credentials.Provider{&machineProvider{get: func(*credentials.CredContext) (credentials.Value, error) {
				endpoint := credentials.DefaultIAMRoleEndpoint
				if tc.container {
					endpoint = credentials.DefaultECSRoleEndpoint
				}
				cause := tc.cause
				if cause == nil {
					cause = errors.New("403 credential-canary")
				}
				return credentials.Value{}, &url.Error{Op: "Get", URL: endpoint + "/credentials", Err: cause}
			}}}
			request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
				return bucketReply(r, http.StatusForbidden, `<Error><Code>AccessDenied</Code></Error>`), nil
			})
			artifact, err := f.fetchMachine(t.Context(), request, providers, cache.Policy{})
			if artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) != tc.missing || strings.Contains(err.Error(), "canary") {
				t.Fatalf("missing=%t, acquisition lost its outcome or exposed credentials: %v", tc.missing, err)
			}
			if !tc.missing && tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("lost native IAM failure cause: %v", err)
			}
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
	http.DefaultTransport = transport
	t.Cleanup(func() { transport.CloseIdleConnections(); http.DefaultTransport = old })
}

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
			return bucketReply(r, 403, `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Code>AccessDenied</Code><Message>machine-canary</Message></Error></ErrorResponse>`)
		}
		if access != "" && !strings.Contains(r.Header.Get("Authorization"), "Credential="+access+"/") {
			t.Error("credential precedence selected a different signer")
		}
		if access == "" && r.Header.Get("Authorization") != "" {
			t.Error("public source consulted machine credentials")
		}
		if rejected {
			return bucketReply(r, 403, `<Error><Code>AccessDenied</Code><Message>rejected</Message></Error>`)
		}
		if r.URL.Query().Get("list-type") != "" {
			return bucketReply(r, 200, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>file.yaml</Key><Size>4</Size><ETag>"etag"</ETag></Contents></ListBucketResult>`)
		}
		response := bucketReply(r, 200, "data")
		response.Header.Set("ETag", `"etag"`)
		response.Header.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		return response
	})
	for _, tier := range []string{"repository", "supply", "partial supply", "machine", "public", "rejected", "STS rejected"} {
		t.Run(tier, func(t *testing.T) {
			b := ambientBucket()
			st := store.New()
			rejected = tier == "rejected"
			if tier == "STS rejected" {
				clearMachineEnvironment(t)
				token := filepath.Join(t.TempDir(), "web-identity")
				testutil.WriteFileAt(t, token, "synthetic-token")
				t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
				t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture")
			}
			if tier == "public" {
				b.SecretRef = nil
				access = ""
			} else if tier != "machine" && tier != "STS rejected" {
				access = "repository"
				sec := &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"accesskey": access, "secretkey": "explicit-secret"}}
				if tier != "supply" {
					if tier == "partial supply" {
						sec.StringData["secretkey"] = manifest.ValuePlaceholder("secretkey")
					}
					st.AddObject(sec)
				}
				if tier == "supply" || tier == "partial supply" {
					supplied, err := store.NewSupplied([]store.SuppliedObject{{Object: &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{"accesskey": "supply", "secretkey": "supply-secret"}}}})
					assert.NoError(t, err)
					st.SetSupplied(supplied)
					if tier == "supply" {
						access = "supply"
					}
				}
			} else {
				access = "machine-canary"
			}
			st.AddObject(b)
			var reads store.ReadSet
			reads.Init(st, b.Named(), nil)
			defer reads.Close()
			root := ""
			if tier == "machine" {
				root = t.TempDir()
			}
			f := &Fetcher{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)}
			initialRequests := requests.Load()
			artifact, err := f.Fetch(t.Context(), b, &reads, cache.Policy{MaxAge: time.Hour})
			if tier == "rejected" {
				if err == nil || artifact != nil || !errors.Is(err, manifest.ErrMissingSecret) || requests.Load() != initialRequests+1 {
					t.Fatalf("explicit rejection advanced credential tiers: %v", err)
				}
				return
			}
			if tier == "STS rejected" {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryAmbient == nil {
					t.Fatalf("missing continuation: %v", err)
				}
				artifact, err = missing.RetryAmbient(t.Context())
				rejection, rejected := errors.AsType[credentials.ErrorResponse](err)
				if artifact != nil || !rejected || rejection.STSError.Code != "AccessDenied" || errors.Is(err, manifest.ErrMissingSecret) || strings.Contains(err.Error(), "canary") || requests.Load() == initialRequests {
					t.Fatalf("machine STS rejection lost its cause or exposed credentials: %v", err)
				}
				return
			}
			if tier == "machine" {
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok || missing.RetryAmbient == nil || artifact != nil {
					t.Fatalf("missing auth did not retain continuation: %v", err)
				}
				before, prepErr := (&operation{Fetcher: f, secrets: source.SecretLookup(&reads)}).prepare(b)
				assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
				t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-before-first-acquisition")
				after, prepErr := (&operation{Fetcher: f, secrets: source.SecretLookup(&reads)}).prepare(b)
				assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
				if before.material != nil || after.material != nil || before.options.Transport != after.options.Transport {
					t.Fatal("machine values changed transport identity")
				}
				artifact, err = missing.RetryAmbient(t.Context())
			}
			assert.NoError(t, err)
			if artifact == nil {
				t.Fatal("acquisition returned no source")
			}
			if len(artifact.Warnings) != 0 {
				t.Fatalf("advisory count=%d, want %d", len(artifact.Warnings), 0)
			}
			data, err := fs.ReadFile(artifact.FS, "file.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "data")
			if strings.Contains(fmt.Sprint(artifact.Key, artifact.Content, artifact.Metadata, artifact.Warnings), "canary") {
				t.Fatal("machine credentials entered artifact identity or output")
			}
			before := requests.Load()

			warm, err := f.Fetch(t.Context(), b, &reads, cache.Policy{MaxAge: time.Hour})
			if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
				warm, err = missing.RetryAmbient(t.Context())
			}
			assert.NoError(t, err)
			if warm.Key != artifact.Key || warm.Content != artifact.Content || len(warm.Warnings) != 0 || requests.Load() != before {
				t.Fatal("warm acquisition consulted credentials, changed identity or repeated success information")
			}

			if tier == "machine" {
				access = "rotated-canary"
				t.Setenv("AWS_ACCESS_KEY_ID", access)
				t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret-canary")
				_, err := f.Fetch(t.Context(), b, &reads, cache.Policy{MaxAge: time.Hour})
				missing, ok := errors.AsType[*source.MissingSecretError](err)
				if !ok {
					t.Fatal(err)
				}
				rotated, err := missing.RetryMachine(t.Context(), &reads)
				assert.NoError(t, err)
				if rotated.Key == warm.Key || requests.Load() == before {
					t.Fatal("rotation reused credential identity or skipped signing")
				}
				t.Setenv("AWS_ACCESS_KEY_ID", "")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "")
				testutil.WriteFileAt(t, os.Getenv("AWS_CONFIG_FILE"), "[default]\naws_access_key_id = partial\n")
				beforeFailure := requests.Load()
				failed, err := missing.RetryMachine(t.Context(), &reads)
				if failed != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) || requests.Load() != beforeFailure {
					t.Fatalf("failed preparation reused warm content: %v", err)
				}
			}
			warmData, err := fs.ReadFile(warm.FS, "file.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(warmData), string(data))
			if root != "" {
				assert.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
					if err != nil || entry.IsDir() {
						return err
					}
					payload, err := os.ReadFile(path)
					if strings.Contains(string(payload), "canary") {
						t.Error("machine credentials or success provenance entered persistent cache data")
					}
					return err
				}))
			}
		})
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
	http.DefaultTransport = transport
	t.Cleanup(func() { transport.CloseIdleConnections(); http.DefaultTransport = previous })
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

func TestFetcher_MachineSSOTransport(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			clearMachineEnvironment(t)
			testutil.WriteFileAt(t, os.Getenv("AWS_CONFIG_FILE"), "[default]\nsso_session = fixture\nsso_role_name = role\nsso_account_id = 123456789012\n[sso-session fixture]\nsso_region = us-east-1\nsso_start_url = https://sso.invalid/start\n")
			hash := sha1.Sum([]byte("fixture"))
			testutil.WriteFile(t, os.Getenv("HOME"), fmt.Sprintf(".aws/sso/cache/%x.json", hash), `{"accessToken":"token-canary","expiresAt":"2099-01-01T00:00:00Z"}`)
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			defer cancel()
			const hostname = "portal.sso.us-east-1.amazonaws.com"
			var calls, traced, dataCalls atomic.Int32
			ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GetConn: func(address string) {
				if address == hostname+":443" {
					traced.Add(1)
				}
			}})
			bucketCredentialRoute(t, hostname, func(r *http.Request) *http.Response {
				calls.Add(1)
				if r.Host != hostname || r.Header.Get("x-amz-sso_bearer_token") != "token-canary" {
					t.Error("SSO lost its destination or fixture token")
				}
				if canceled {
					cancel()
				}
				return bucketReply(r, http.StatusOK, `{"roleCredentials":{"accessKeyId":"sso-canary","secretAccessKey":"secret-canary","sessionToken":"session-canary","expiration":4102444800000}}`)
			})
			endpoint := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				dataCalls.Add(1)
				if !strings.Contains(r.Header.Get("Authorization"), "Credential=sso-canary/") || strings.Contains(fmt.Sprint(r.Header, r.URL), "token-canary") {
					t.Error("S3 lost its SSO signing credentials or received the bearer token")
				}
				if r.URL.Query().Get("list-type") != "" {
					_, _ = io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>file.yaml</Key><Size>4</Size><ETag>"etag"</ETag></Contents></ListBucketResult>`)
					return
				}
				w.Header().Set("ETag", `"etag"`)
				w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
				_, _ = io.WriteString(w, "data")
			}))
			t.Cleanup(endpoint.Close)
			target := strings.TrimPrefix(endpoint.URL, "https://")
			var destinations sync.Map
			proxy := bucketSourceProxy(t, target, func(r *http.Request) {
				destinations.Store(r.Host, true)
				if strings.Contains(fmt.Sprint(r.Header, r.URL), "token-canary") {
					t.Error("SSO bearer token reached repository proxy")
				}
			})
			ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: endpoint.Certificate().Raw}))
			_, err := endpoint.Certificate().Verify(x509.VerifyOptions{DNSName: "127.0.0.1"})
			if err == nil {
				t.Fatal("S3 fixture is trusted without the source CA")
			}
			b := ambientBucket()
			b.Endpoint = endpoint.URL
			b.ProxySecretRef = &manifest.LocalObjectReference{Name: "route"}
			b.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
			inputs := testInputs{secrets: func(_, name string) *manifest.Secret {
				switch name {
				case "route":
					return &manifest.Secret{StringData: map[string]any{"address": proxy}}
				case "tls":
					return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
				default:
					return nil
				}
			}}
			reads := inputs.readSet(b)
			defer reads.Close()
			f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
			artifact, err := f.Fetch(ctx, b, reads, cache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil || artifact != nil {
				t.Fatalf("SSO continuation unavailable: %v", err)
			}
			artifact, err = missing.RetryMachine(ctx, reads)
			destinations.Range(func(destination, _ any) bool {
				if destination != target {
					t.Errorf("credential request reached repository proxy: %s", destination)
				}
				return true
			})
			if calls.Load() != 1 || traced.Load() != 1 || canceled && !errors.Is(err, context.Canceled) || !canceled && err != nil {
				t.Fatalf("SSO outcome: run calls=%d traced=%d: %v", calls.Load(), traced.Load(), err)
			}
			if canceled {
				assert.Equal(t, dataCalls.Load(), int32(0))
				return
			}
			_, proxied := destinations.Load(target)
			if !proxied || dataCalls.Load() < 2 || artifact == nil {
				t.Fatalf("S3 did not list and download through the source proxy: proxied=%t calls=%d", proxied, dataCalls.Load())
			}
			data, err := fs.ReadFile(artifact.FS, "file.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "data")
		})
	}
}
