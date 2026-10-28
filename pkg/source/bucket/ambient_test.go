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
	"os"
	"path/filepath"
	"strings"
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
		{name: "anonymous", first: credentials.Value{AccessKeyID: "anonymous", SecretAccessKey: "secret", SignerType: credentials.SignatureAnonymous}, wantNext: 1, wantMissing: true},
		{name: "incomplete signed value", first: credentials.Value{AccessKeyID: "partial", SignerType: credentials.SignatureV4}},
		{name: "exhausted", wantNext: 1, wantMissing: true},
		{name: "environment failure", firstErr: failure},
		{name: "IAM failure", nextErr: failure, wantNext: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ambientBucket()
			f := &operation{Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
			request, err := f.prepare(b)
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			if len(request.providers) != 2 {
				t.Fatal("machine chain must contain only EnvAWS and IAM")
			}
			if _, ok := request.providers[0].(*credentials.EnvAWS); !ok {
				t.Fatal("first native provider is not EnvAWS")
			}
			if _, ok := request.providers[1].(*credentials.IAM); !ok {
				t.Fatal("second native provider is not IAM")
			}
			if request.options.Creds != nil || request.material != nil {
				t.Fatal("unavailable auth became identity material")
			}
			first := &machineProvider{value: tc.first}
			next := &machineProvider{value: tc.next}
			first.get = func(cc *credentials.CredContext) (credentials.Value, error) {
				if cc == nil || cc.Context == nil || cc.Client == nil {
					t.Error("provider lost the producer context or guarded client")
				}
				if _, ok := cc.Context.Deadline(); !ok {
					t.Error("provider ran without the bounded acquisition deadline")
				}
				return tc.first, tc.firstErr
			}
			next.get = func(*credentials.CredContext) (credentials.Value, error) { return tc.next, tc.nextErr }
			request.providers = []credentials.Provider{first, next}
			var signed atomic.Int32
			request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") == "" {
					t.Error("machine request is unsigned")
				}
				signed.Add(1)
				return bucketReply(r, 200, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`), nil
			})
			artifact, err := f.fetch(t.Context(), request, cache.Policy{MaxAge: time.Hour})
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
			request.providers = []credentials.Provider{&machineProvider{value: credentials.Value{AccessKeyID: "credential-canary", SecretAccessKey: "secret-canary", SignerType: credentials.SignatureV4}}}
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
			artifact, err := f.fetch(t.Context(), request, cache.Policy{})
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
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"} {
		t.Setenv(key, "")
	}
}

func TestFetcher_MachineProfilesIgnored(t *testing.T) {
	clearMachineEnvironment(t)
	for _, profile := range []string{"static", "process"} {
		t.Run(profile, func(t *testing.T) {
			root := t.TempDir()
			marker := filepath.Join(root, "executed")
			program := filepath.Join(root, "credential-process")
			testutil.WriteFileAt(t, program, "#!/bin/sh\nprintf called > '"+marker+"'\n")
			assert.NoError(t, os.Chmod(program, 0o700))
			path := filepath.Join(root, "credentials")
			testutil.WriteFileAt(t, path, "[static]\naws_access_key_id = file-canary\naws_secret_access_key = file-secret\n[process]\ncredential_process = "+program+"\n")
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", path)
			t.Setenv("AWS_CONFIG_FILE", path)
			t.Setenv("AWS_PROFILE", profile)
			b := ambientBucket()
			f := &operation{Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
			request, err := f.prepare(b)
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			iam := &machineProvider{}
			request.providers[1] = iam
			artifact, err := f.fetch(t.Context(), request, cache.Policy{})
			if artifact != nil || err == nil || iam.calls.Load() != 1 || strings.Contains(err.Error(), "file-canary") {
				t.Fatalf("profile affected native machine selection: %v", err)
			}
			if _, err := os.Stat(marker); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("credential process executed")
			}
		})
	}
}

func TestFetcher_MachineIAMCancellation(t *testing.T) {
	clearMachineEnvironment(t)
	b := ambientBucket()
	f := &operation{Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
	request, err := f.prepare(b)
	assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
		select {
		case <-ctx.Done():
		default:
			close(entered)
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	request.providers[1].(*credentials.IAM).Endpoint = "http://iam.invalid"
	finished := make(chan error, 1)
	go func() {
		_, err := f.fetch(ctx, request, cache.Policy{})
		finished <- err
	}()
	<-entered
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("native IAM lost operation cancellation: %v", err)
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
			if tc.container {
				t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/credentials")
			}
			b := ambientBucket()
			f := &operation{Fetcher: &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}, secrets: func(string, string) *manifest.Secret { return nil }}
			request, err := f.prepare(b)
			assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), true)
			request.options.Transport = bucketTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host == "bucket.invalid" {
					t.Error("failed IAM credentials reached the bucket")
				}
				if tc.cause != nil {
					return nil, tc.cause
				}
				response := bucketReply(r, http.StatusForbidden, "credential-canary")
				response.Status = "403 credential-canary"
				return response, nil
			})
			artifact, err := f.fetch(t.Context(), request, cache.Policy{})
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
				if err == nil || artifact != nil || errors.Is(err, manifest.ErrMissingSecret) || requests.Load() != initialRequests+1 {
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
			if tier == "machine" {
				t.Setenv("AWS_ACCESS_KEY_ID", "rotated-canary")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret-canary")
			}
			warm, err := f.Fetch(t.Context(), b, &reads, cache.Policy{MaxAge: time.Hour})
			if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
				warm, err = missing.RetryAmbient(t.Context())
			}
			assert.NoError(t, err)
			if warm.Key != artifact.Key || warm.Content != artifact.Content || len(warm.Warnings) != 0 || requests.Load() != before {
				t.Fatal("warm acquisition consulted credentials, changed identity or repeated success information")
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
