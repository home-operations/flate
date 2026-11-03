package bucket

import (
	"context"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func testFetcherMachineProfilesCase(t *testing.T, shape *string,
) {
	t.Helper()
	clearMachineEnvironment(t)
	root := t.TempDir()
	marker := filepath.Join(root, "executed")
	program := filepath.Join(root, "credential-process")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > '" + filepath.Join(
		root,
		"args",
	) + "'\n/bin/cat > '" + filepath.Join(
		root,
		"stdin",
	) + "'\n/usr/bin/env > '" + filepath.Join(
		root,
		"environment",
	) + "'\nprintf called > '" + marker + "'\nprintf '%s' '{\"Version\":1,\"AccessKeyId\":\"file-canary\",\"SecretAccessKey\":\"file-secret\"}'\n"
	if (*shape) == "failing process" {
		script = "#!/bin/sh\nprintf '%s' 'credential-canary' >&2\nexit 1\n"
	}
	testutil.WriteFileAt(t, program, script)
	assert.NoError(t, os.Chmod(program, 0o700))
	profile := "[default]\ncredential_process = " + program + " argument\n"
	switch *shape {
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
	if (*shape) == "shared credentials" {
		path = os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	}
	if (*shape) != "no files" {
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
	switch *shape {
	case "environment before files":
		signer.Store("environment")
	case "no files":
		signer.Store("")
	}
	testFetcherMachineProfilesCaseCompletion(
		t,
		&testFetcherMachineProfilesCaseCompletionInputs{
			remote:  &remote,
			signer:  &signer,
			shape:   &shape,
			iam:     &iam,
			marker:  &marker,
			root:    &root,
			path:    &path,
			profile: &profile,
			program: &program,
			script:  &script,
		},
	)
}

func testFetcherMachineProfilesCaseCompletion(t *testing.T, captures *testFetcherMachineProfilesCaseCompletionInputs) {
	t.Helper()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captures.remote.
			Add(1)
		access := captures.signer.Load().(string)
		if access == "" && r.Header.Get("Authorization") != "" ||
			access != "" && !strings.Contains(r.Header.Get("Authorization"), "Credential="+access+"/") {
			t.Error("profile acquisition used a different signer")
		}
		_, _ = io.WriteString(
			w,
			`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`,
		)
	}))
	t.Cleanup(endpoint.Close)
	b := ambientBucket()
	b.Endpoint, b.Insecure = endpoint.URL, true
	inputs := testInputs{}
	ca, proxyAddress := "", ""
	if (*(*captures.shape)) == "process" {
		t.Setenv("FLATE_PROCESS_MACHINE", "machine-environment")
		ca = testutil.SelfSignedCA(t)
		proxy := httptest.NewServer(endpoint.Config.Handler)
		t.Cleanup(proxy.Close)
		proxyAddress = proxy.URL
		b.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
		b.CertSecretRef = b.ProxySecretRef
		inputs.secrets = func(string, string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{
				"address":  proxy.URL,
				"username": "repository-proxy-user",
				"password": "repository-proxy-password",
				"ca.crt":   ca,
			}}
		}
	}
	reads := inputs.readSet(b)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	artifact, err := f.Fetch(t.Context(), b, reads, cache.Policy{MaxAge: time.Hour})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(
		t,
		!ok || missing.RetryAmbient == nil || artifact != nil || captures.remote.Load() != 0 ||
			captures.iam.Load() != 0,
		"profile lookup ran before continuation: %v",
		err,
	)
	artifact, err = missing.RetryMachine(t.Context(), reads)
	success := (*(*captures.shape)) == "process" || (*(*captures.shape)) == "static before process" ||
		(*(*captures.shape)) == "environment before files" ||
		(*(*captures.shape)) ==
			"selected profile" ||
		(*(*captures.shape)) ==
			"shared credentials"
	unavailable := (*(*captures.shape)) == "no files"
	var wantRemote, wantIAM int32
	if success || unavailable {
		wantRemote = 1
	}
	if unavailable {
		wantIAM = 1
	}
	testFetcherMachineProfilesCaseCompletionTail(
		t,
		&testFetcherMachineProfilesCaseCompletionTailState{
			captures:     &captures,
			ca:           &ca,
			proxyAddress: &proxyAddress,
			reads:        &reads,
			artifact:     &artifact,
			err:          &err,
			missing:      &missing,
			success:      &success,
			unavailable:  &unavailable,
			wantRemote:   &wantRemote,
			wantIAM:      &wantIAM,
		},
	)
}

type testFetcherMachineContinuationCaseInputs struct {
	requests *atomic.Int32
	access   *string
	rejected *bool
	tier     *string
}

func testFetcherMachineContinuationCase(t *testing.T, testCaseInput testFetcherMachineContinuationCaseInputs) {
	t.Helper()
	b := ambientBucket()
	st := store.New()
	(*testCaseInput.rejected) = (*testCaseInput.tier) == "rejected"
	if (*testCaseInput.tier) == "STS rejected" {
		clearMachineEnvironment(t)
		token := filepath.Join(t.TempDir(), "web-identity")
		testutil.WriteFileAt(t, token, "synthetic-token")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture")
	}
	switch {
	case (*testCaseInput.tier) == "public":
		b.SecretRef = nil
		(*testCaseInput.access) = ""
	case (*testCaseInput.tier) != "machine" && (*testCaseInput.tier) != "STS rejected":
		(*testCaseInput.access) = "repository"
		sec := &manifest.Secret{
			Name:       "auth",
			Namespace:  "apps",
			StringData: map[string]any{"accesskey": (*testCaseInput.access), "secretkey": "explicit-secret"},
		}
		if (*testCaseInput.tier) != "supply" {
			if (*testCaseInput.tier) == "partial supply" {
				sec.StringData["secretkey"] = manifest.ValuePlaceholder("secretkey")
			}
			st.AddObject(sec)
		}
		if (*testCaseInput.tier) == "supply" || (*testCaseInput.tier) == "partial supply" {
			supplied, err := store.NewSupplied(
				[]store.SuppliedObject{
					{
						Object: &manifest.Secret{
							Name:       "auth",
							Namespace:  "apps",
							StringData: map[string]any{"accesskey": "supply", "secretkey": "supply-secret"},
						},
					},
				},
			)
			assert.NoError(t, err)
			st.SetSupplied(supplied)
			if (*testCaseInput.tier) == "supply" {
				(*testCaseInput.access) = "supply"
			}
		}
	default:
		(*testCaseInput.access) = "machine-canary"
	}
	st.AddObject(b)
	var reads store.ReadSet
	reads.Init(st, b.Named(), nil)
	defer reads.Close()
	root := ""
	if (*testCaseInput.tier) == "machine" {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, cache.New), Network: newTestNetwork(t)}
	initialRequests := testCaseInput.requests.Load()
	artifact, err := f.Fetch(t.Context(), b, &reads, cache.Policy{MaxAge: time.Hour})
	if (*testCaseInput.tier) == "rejected" {
		assert.FatalIf(
			t,
			err == nil || artifact != nil || !errors.Is(err, manifest.ErrMissingSecret) || testCaseInput.requests.
				Load() != initialRequests+1,
			"explicit rejection advanced credential tiers: %v",
			err,
		)
		return
	}
	if (*testCaseInput.tier) == "STS rejected" {
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
		artifact, err = missing.RetryAmbient(t.Context())
		rejection, rejected := errors.AsType[credentials.ErrorResponse](err)
		assert.FatalIf(t, artifact != nil || !rejected || rejection.STSError.Code != "AccessDenied" ||
			errors.Is(err, manifest.ErrMissingSecret) ||
			strings.Contains(err.Error(), "canary") || testCaseInput.requests.
			Load() == initialRequests, "machine STS rejection lost its cause or exposed credentials: %v", err)
		return
	}
	testFetcherMachineContinuationCaseCompletion(
		t,
		&testFetcherMachineContinuationCaseCompletionInputs{
			testCaseInput: &testCaseInput,
			err:           &err,
			artifact:      &artifact,
			f:             &f,
			reads:         &reads,
			b:             &b,
			root:          &root,
		},
	)
}

func testFetcherMachineContinuationCaseCompletion(
	t *testing.T,
	captures *testFetcherMachineContinuationCaseCompletionInputs,
) {
	t.Helper()
	if (*captures.testCaseInput.tier) == "machine" {
		missing, ok := errors.AsType[*source.MissingSecretError]((*captures.err))
		assert.FatalIf(
			t,
			!ok || missing.RetryAmbient == nil || (*captures.artifact) != nil,
			"missing auth did not retain continuation: %v", (*captures.err),
		)
		before, prepErr := (&operation{Fetcher: (*captures.f), secrets: source.SecretLookup(captures.reads)}).prepare(
			(*captures.b),
		)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-before-first-acquisition")
		after, prepErr := (&operation{Fetcher: (*captures.f), secrets: source.SecretLookup(captures.reads)}).prepare(
			(*captures.b),
		)
		assert.Equal(t, errors.Is(prepErr, manifest.ErrMissingSecret), true)
		assert.FatalIf(t, before.material != nil || after.material != nil ||
			before.options.Transport != after.options.Transport, "%s", "machine values changed transport identity")
		(*captures.artifact), (*captures.err) = missing.RetryAmbient(t.Context())
	}
	assert.NoError(t, (*captures.err))
	assert.FatalIf(t, (*captures.artifact) == nil, "%s", "acquisition returned no source")

	if len((*captures.artifact).Warnings) != 0 {
		t.Fatalf("advisory count=%d, want %d", len((*captures.artifact).Warnings), 0)
	}
	var data []byte

	data, (*captures.err) = fs.ReadFile((*captures.artifact).FS, "file.yaml")
	assert.NoError(t, (*captures.err))
	assert.Equal(t, string(data), "data")
	assert.FatalIf(t, strings.Contains(
		fmt.Sprint(
			(*captures.artifact).Key,
			(*captures.artifact).Content,
			(*captures.artifact).Metadata,
			(*captures.artifact).Warnings,
		),
		"canary",
	), "%s", "machine credentials entered artifact identity or output")

	before := captures.testCaseInput.requests.Load()
	var warm *store.SourceArtifact

	warm, (*captures.err) = (*captures.f).Fetch(
		t.Context(),
		(*captures.b),
		captures.reads,
		cache.Policy{MaxAge: time.Hour},
	)
	if missing, ok := errors.AsType[*source.MissingSecretError]((*captures.err)); ok && missing.RetryAmbient != nil {
		warm, (*captures.err) = missing.RetryAmbient(t.Context())
	}
	assert.NoError(t, (*captures.err))
	assert.FatalIf(
		t,
		warm.Key != (*captures.artifact).Key || warm.Content != (*captures.artifact).Content || len(warm.Warnings) != 0 ||
			captures.testCaseInput.
				requests.
				Load() != before,
		"%s",
		"warm acquisition consulted credentials, changed identity or repeated success information",
	)

	if (*captures.testCaseInput.tier) == "machine" {
		(*captures.testCaseInput.access) = "rotated-canary"
		t.Setenv("AWS_ACCESS_KEY_ID", (*captures.testCaseInput.access))
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret-canary")
		_, fetchErr := (*captures.f).Fetch(t.Context(), (*captures.b), captures.reads, cache.Policy{MaxAge: time.Hour})
		missing, ok := errors.AsType[*source.MissingSecretError](fetchErr)
		if !ok {
			t.Fatal(fetchErr)
		}
		rotated, fetchErr := missing.RetryMachine(t.Context(), captures.reads)
		assert.NoError(t, fetchErr)
		assert.FatalIf(
			t,
			rotated.Key == warm.Key || captures.testCaseInput.requests.Load() == before,
			"%s",
			"rotation reused credential identity or skipped signing",
		)

		t.Setenv("AWS_ACCESS_KEY_ID", "")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		testutil.WriteFileAt(t, os.Getenv("AWS_CONFIG_FILE"), "[default]\naws_access_key_id = partial\n")
		beforeFailure := captures.testCaseInput.requests.Load()
		failed, fetchErr := missing.RetryMachine(t.Context(), captures.reads)
		assert.FatalIf(
			t,
			failed != nil || fetchErr == nil || errors.Is(fetchErr, manifest.ErrMissingSecret) ||
				captures.testCaseInput.
					requests.
					Load() != beforeFailure,
			"failed preparation reused warm content: %v",
			fetchErr,
		)
	}
	var warmData []byte

	warmData, (*captures.err) = fs.ReadFile(warm.FS, "file.yaml")
	assert.NoError(t, (*captures.err))
	assert.Equal(t, string(warmData), string(data))
	if (*captures.root) != "" {
		assert.NoError(t, filepath.WalkDir((*captures.root), func(path string, entry fs.DirEntry, err error) error {
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
}

func testFetcherMachineSSOTransportCase(t *testing.T, canceled *bool,
) {
	t.Helper()
	clearMachineEnvironment(t)
	testutil.WriteFileAt(
		t,
		os.Getenv("AWS_CONFIG_FILE"),
		"[default]\nsso_session = fixture\nsso_role_name = role\nsso_account_id = 123456789012\n[sso-session fixture]\nsso_region = us-east-1\nsso_start_url = https://sso.invalid/start\n",
	)
	hash := sha1.Sum([]byte("fixture"))
	testutil.WriteFile(
		t,
		os.Getenv("HOME"),
		fmt.Sprintf(".aws/sso/cache/%x.json", hash),
		`{"accessToken":"token-canary","expiresAt":"2099-01-01T00:00:00Z"}`,
	)
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
		if r.Host != hostname || r.Header.Get("X-Amz-Sso_bearer_token") != "token-canary" {
			t.Error("SSO lost its destination or fixture token")
		}
		if *canceled {
			cancel()
		}
		return bucketReply(
			r,
			http.StatusOK,
			`{"roleCredentials":{"accessKeyId":"sso-canary","secretAccessKey":"secret-canary","sessionToken":"session-canary","expiration":4102444800000}}`,
		)
	})
	endpoint := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataCalls.Add(1)
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=sso-canary/") ||
			strings.Contains(fmt.Sprint(r.Header, r.URL), "token-canary") {
			t.Error("S3 lost its SSO signing credentials or received the bearer token")
		}
		if r.URL.Query().Get("list-type") != "" {
			_, _ = io.WriteString(
				w,
				`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>file.yaml</Key><Size>4</Size><ETag>"etag"</ETag></Contents></ListBucketResult>`,
			)
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
	shouldReturn := testFetcherMachineSSOTransportCaseCompletion(
		t,
		&testFetcherMachineSSOTransportCaseCompletionInputs{
			ctx:          &ctx,
			endpoint:     &endpoint,
			proxy:        &proxy,
			destinations: &destinations,
			target:       &target,
			calls:        &calls,
			traced:       &traced,
			canceled:     &canceled,
			dataCalls:    &dataCalls,
		},
	)
	if shouldReturn {
		return
	}
}

func testFetcherMachineSSOTransportCaseCompletion(
	t *testing.T,
	captures *testFetcherMachineSSOTransportCaseCompletionInputs,
) bool {
	t.Helper()
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: (*captures.endpoint).Certificate().Raw}))
	_, err := (*captures.endpoint).Certificate().Verify(x509.VerifyOptions{DNSName: "127.0.0.1"})
	assert.FatalIf(t, err == nil, "%s", "S3 fixture is trusted without the source CA")

	b := ambientBucket()
	b.Endpoint = (*captures.endpoint).URL
	b.ProxySecretRef = &manifest.LocalObjectReference{Name: "route"}
	b.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	inputs := testInputs{
		secrets: func(callbackArg1, name string) *manifest.Secret {
			return testFetcherMachineSSOTransportCaseCallback(
				callbackArg1,
				name,
				testFetcherMachineSSOTransportCaseCallbackInputs{proxy: captures.proxy, ca: &ca},
			)
		},
	}
	reads := inputs.readSet(b)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	artifact, err := f.Fetch((*captures.ctx), b, reads, cache.Policy{})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(
		t,
		!ok || missing.RetryAmbient == nil || artifact != nil,
		"SSO continuation unavailable: %v",
		err,
	)
	artifact, err = missing.RetryMachine((*captures.ctx), reads)
	captures.destinations.
		Range(func(destination, _ any) bool {
			assert.ErrorIf(
				t,
				destination != (*captures.target),
				"credential request reached repository proxy: %s",
				destination,
			)
			return true
		})
	if captures.calls.Load() != 1 || captures.traced.Load() != 1 ||
		(*(*captures.canceled)) && !errors.Is(err, context.Canceled) ||
		!(*(*captures.canceled)) && err != nil {
		t.Fatalf("SSO outcome: run calls=%d traced=%d: %v", captures.calls.Load(), captures.traced.Load(), err)
	}
	if *(*captures.canceled) {
		assert.Equal(t, captures.dataCalls.Load(), int32(0))
		return true
	}
	_, proxied := captures.destinations.Load((*captures.target))
	if !proxied || captures.dataCalls.Load() < 2 || artifact == nil {
		t.Fatalf(
			"S3 did not list and download through the source proxy: proxied=%t calls=%d",
			proxied, captures.dataCalls.
				Load(),
		)
	}
	data, err := fs.ReadFile(artifact.FS, "file.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "data")
	return false
}

type testFetcherMachineSSOTransportCaseCallbackInputs struct {
	proxy *string
	ca    *string
}

func testFetcherMachineSSOTransportCaseCallback(
	_, name string,
	testCaseInput testFetcherMachineSSOTransportCaseCallbackInputs,
) *manifest.Secret {
	switch name {
	case "route":
		return &manifest.Secret{StringData: map[string]any{"address": (*testCaseInput.proxy)}}
	case "tls":
		return &manifest.Secret{StringData: map[string]any{"ca.crt": (*testCaseInput.ca)}}
	default:
		return nil
	}
}

type testFetcherMachineProfilesCaseCompletionInputs struct {
	remote  *atomic.Int32
	signer  *atomic.Value
	shape   **string
	iam     *atomic.Int32
	marker  *string
	root    *string
	path    *string
	profile *string
	program *string
	script  *string
}
type testFetcherMachineContinuationCaseCompletionInputs struct {
	testCaseInput *testFetcherMachineContinuationCaseInputs
	err           *error
	artifact      **store.SourceArtifact
	f             **Fetcher
	reads         *store.ReadSet
	b             **manifest.Bucket
	root          *string
}
type testFetcherMachineSSOTransportCaseCompletionInputs struct {
	ctx          *context.Context
	endpoint     **httptest.Server
	proxy        *string
	destinations *sync.Map
	target       *string
	calls        *atomic.Int32
	traced       *atomic.Int32
	canceled     **bool
	dataCalls    *atomic.Int32
}

type testFetcherMachineProfilesCaseCompletionTailState struct {
	captures     **testFetcherMachineProfilesCaseCompletionInputs
	ca           *string
	proxyAddress *string
	reads        **store.ReadSet
	artifact     **store.SourceArtifact
	err          *error
	missing      **source.MissingSecretError
	success      *bool
	unavailable  *bool
	wantRemote   *int32
	wantIAM      *int32
}

func testFetcherMachineProfilesCaseCompletionTail(
	t *testing.T,
	scope *testFetcherMachineProfilesCaseCompletionTailState,
) {
	t.Helper()
	if ((*scope.err) == nil) != ((*scope.success) || (*scope.unavailable)) ||
		((*scope.artifact) != nil) != ((*scope.success) || (*scope.unavailable)) ||
		errors.Is((*scope.err), manifest.ErrMissingSecret) ||
		(*scope.captures).remote.
			Load() != (*scope.wantRemote) ||
		(*scope.captures).iam.
			Load() != (*scope.wantIAM) {
		t.Fatalf(
			"profile selection: %v remote=%d IAM=%d",
			(*scope.err), (*scope.captures).remote.Load(), (*scope.captures).iam.Load(),
		)
	}
	_, markerErr := os.Stat((*(*scope.captures).marker))
	assert.FatalIf(
		t,
		(markerErr == nil) != ((*(*(*scope.captures).shape)) == "process"),
		"credential_process precedence: %v",
		markerErr,
	)
	if (*(*(*scope.captures).shape)) == "process" {
		args, err := os.ReadFile(filepath.Join((*(*scope.captures).root), "args"))
		assert.NoError(t, err)
		assert.Equal(t, string(args), "argument\n")
		stdin, err := os.ReadFile(filepath.Join((*(*scope.captures).root), "stdin"))
		assert.NoError(t, err)
		assert.Equal(t, string(stdin), "")
		environment, err := os.ReadFile(filepath.Join((*(*scope.captures).root), "environment"))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			!strings.Contains(string(environment), "FLATE_PROCESS_MACHINE=machine-environment"),
			"%s",
			"credential process lost the machine environment",
		)

		for _, canary := range []string{(*scope.proxyAddress), "repository-proxy-user", "repository-proxy-password", (*scope.ca), "-----BEGIN CERTIFICATE-----"} {
			assert.FatalIf(
				t,
				strings.Contains(string(args)+string(stdin)+string(environment), canary),
				"%s",
				"repository transport material reached credential process inputs",
			)
		}
	}
	if *scope.success {
		warm, err := (*scope.missing).RetryMachine(t.Context(), (*scope.reads))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			warm.Key != (*scope.artifact).Key || (*scope.captures).remote.Load() != 1,
			"%s",
			"unchanged profile missed warm content",
		)
		(*scope.captures).signer.
			Store("rotated-canary")
		if (*(*(*scope.captures).shape)) == "environment before files" {
			t.Setenv("AWS_ACCESS_KEY_ID", "rotated-canary")
		} else {
			testutil.WriteFileAt(
				t,
				(*(*scope.captures).path),
				strings.ReplaceAll((*(*scope.captures).profile), "file-canary", "rotated-canary"),
			)
			testutil.WriteFileAt(
				t,
				(*(*scope.captures).program),
				strings.ReplaceAll((*(*scope.captures).script), "file-canary", "rotated-canary"),
			)
		}
		rotated, err := (*scope.missing).RetryMachine(t.Context(), (*scope.reads))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			rotated.Key == warm.Key || (*scope.captures).remote.Load() != 2 || (*scope.captures).iam.Load() != 0,
			"%s",
			"rotated profile reused credential identity or skipped signing",
		)
	}
}
