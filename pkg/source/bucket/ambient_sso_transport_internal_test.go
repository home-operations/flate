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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
)

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
		assert.ErrorIf(
			t,
			r.Host != hostname || r.Header.Get("X-Amz-Sso_bearer_token") != "token-canary",
			"%s",
			"SSO lost its destination or fixture token",
		)

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
		assert.ErrorIf(t, !strings.Contains(r.Header.Get("Authorization"), "Credential=sso-canary/") ||
			strings.Contains(
				fmt.Sprint(r.Header, r.URL),
				"token-canary",
			), "%s", "S3 lost its SSO signing credentials or received the bearer token")

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
		assert.ErrorIf(
			t,
			strings.Contains(fmt.Sprint(r.Header, r.URL), "token-canary"),
			"%s",
			"SSO bearer token reached repository proxy",
		)
	})
	testFetcherMachineSSOTransportCaseCompletion(
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
}

func testFetcherMachineSSOTransportCaseCompletion(
	t *testing.T,
	captures *testFetcherMachineSSOTransportCaseCompletionInputs,
) {
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
	assert.FatalIf(t, captures.calls.Load() != 1 || captures.traced.Load() != 1 ||
		(*(*captures.canceled)) && !errors.Is(err, context.Canceled) ||
		!(*(*captures.canceled)) && err != nil,
		"SSO outcome: run calls=%d traced=%d: %v", captures.calls.Load(), captures.traced.Load(), err)

	if *(*captures.canceled) {
		assert.Equal(t, captures.dataCalls.Load(), int32(0))
		return
	}
	_, proxied := captures.destinations.Load((*captures.target))
	assert.FatalIf(t, !proxied || captures.dataCalls.Load() < 2 || artifact == nil,

		"S3 did not list and download through the source proxy: proxied=%t calls=%d",
		proxied, captures.dataCalls.
			Load())

	data, err := fs.ReadFile(artifact.FS, "file.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "data")
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
