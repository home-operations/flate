package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/ssrfguard"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // Credential fixture helpers change the process environment.
func TestFetcher_AnonymousPreparation(t *testing.T) {
	clearMachineEnvironment(t)
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"Code":"Success","Expiration":"2099-01-01T00:00:00Z"}`))
	}))
	t.Cleanup(metadata.Close)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", metadata.URL)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(server.Close)
	for _, authored := range []bool{false, true} {
		t.Run(strconv.FormatBool(authored), func(t *testing.T) {
			f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", cache.New)}
			b := ambientBucket()
			b.Endpoint, b.Insecure, b.Provider = server.URL, true, "aws"
			if !authored {
				b.SecretRef = nil
			}
			reads := (testInputs{}).readSet(b)
			defer reads.Close()
			before := requests.Load()
			prepared, err := f.Fetch(t.Context(), b, reads, &cache.Policy{})
			var missing *source.MissingSecretError
			if authored {
				var ok bool
				missing, ok = errors.AsType[*source.MissingSecretError](err)
				assert.FatalIf(t, !ok, "missing credentials: %v", err)
				prepared, err = missing.RetryMachine(t.Context(), reads)
			}
			assert.NoError(t, err)
			assert.Equal(t, prepared.LocalUnavailable, missing)
			assert.Equal(t, requests.Load(), before)
			artifact, err := prepared.Fetch(t.Context(), nil)
			assert.NoError(t, err)
			assert.Equal(t, artifact != nil, true)
			assert.Equal(t, requests.Load(), before+1)
		})
	}
}

func TestFetcher_AnonymousBucketsShareIAM(t *testing.T) {
	clearMachineEnvironment(t)
	network := newTestNetwork(t)
	var probes, acquisitions atomic.Int32
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		_, _ = w.Write([]byte(`{"Code":"Success","Expiration":"2099-01-01T00:00:00Z"}`))
	}))
	t.Cleanup(metadata.Close)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", metadata.URL)
	bucket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("anonymous Bucket sent credentials")
		}
		acquisitions.Add(1)
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(bucket.Close)
	var workers sync.WaitGroup
	start := make(chan struct{})
	closers := make([]func(), 0, 10)
	defer func() {
		for _, closeResource := range slices.Backward(closers) {
			closeResource()
		}
	}()
	for i := range 10 {
		f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: network}
		b := ambientBucket()
		b.Name = strconv.Itoa(i)
		b.Provider = "aws"
		b.SecretRef = nil
		b.Endpoint = bucket.URL
		b.Insecure = true
		reads := (testInputs{}).readSet(b)
		closers = append(closers, reads.Close)
		workers.Go(func() {
			<-start
			artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, reads, &cache.Policy{}, nil)
			if err != nil || artifact == nil {
				t.Errorf("Bucket %s: %v", b.Name, err)
			}
		})
	}
	close(start)
	workers.Wait()
	if probes.Load() != 1 || acquisitions.Load() != 10 {
		t.Fatalf("IAM probes=%d Bucket acquisitions=%d", probes.Load(), acquisitions.Load())
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_MachineIAMCancellation(t *testing.T) {
	for _, after := range []string{"live borrower", "owner close"} {
		t.Run(after, func(t *testing.T) {
			testFetcherMachineIAMCancellationCase(t, &after)
		},
		)
	}
}

func TestFetcher_SharedIAMTransport(t *testing.T) {
	clearMachineEnvironment(t)
	var probes, leaked, acquisitions atomic.Int32
	metadata := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		_, _ = io.WriteString(
			w,
			`{"Code":"Success","AccessKeyId":"iam-canary","SecretAccessKey":"secret","Expiration":"2099-01-01T00:00:00Z"}`,
		)
	}))
	t.Cleanup(metadata.Close)
	previous := http.DefaultTransport
	transport := previous.(*http.Transport).Clone()
	transport.TLSClientConfig = metadata.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	http.DefaultTransport = transport //nolint:reassign // The test replaces the process transport to exercise offline acquisition and restores it afterward.
	t.Cleanup(
		func() {
			transport.CloseIdleConnections()
			http.DefaultTransport = previous //nolint:reassign // The test must restore the process HTTP transport after offline acquisition.
		},
	)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", metadata.URL)
	network := newTestNetwork(t)
	var workers sync.WaitGroup
	start := make(chan struct{})
	closers := make([]func(), 0, 2)
	defer func() {
		for _, closeResource := range slices.Backward(closers) {
			closeResource()
		}
	}()
	for i := range 2 {
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Host != "bucket.invalid" {
				leaked.Add(1)
				_, _ = io.WriteString(
					w,
					`{"Code":"Success","AccessKeyId":"proxy-canary","SecretAccessKey":"secret","Expiration":"2099-01-01T00:00:00Z"}`,
				)
				return
			}
			acquisitions.Add(1)
			if !strings.Contains(r.Header.Get("Authorization"), "Credential=iam-canary/") {
				t.Error("source proxy supplied the run's shared identity")
			}
			_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
		}))
		t.Cleanup(proxy.Close)
		b := ambientBucket()
		b.Name, b.Provider, b.SecretRef = strconv.Itoa(i), "aws", nil
		b.Endpoint, b.Insecure = "http://bucket.invalid", true
		b.ProxySecretRef = &manifest.LocalObjectReference{Name: "route"}
		b.CertSecretRef = b.ProxySecretRef
		ca := testutil.SelfSignedCA(t)
		inputs := testInputs{secrets: func(string, string) *manifest.Secret {
			return &manifest.Secret{StringData: map[string]any{"address": proxy.URL, "ca.crt": ca}}
		}}
		reads := inputs.readSet(b)
		closers = append(closers, reads.Close)
		f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: network}
		workers.Go(func() {
			<-start
			artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, reads, &cache.Policy{}, nil)
			if err != nil || artifact == nil {
				t.Errorf("Bucket %s: %v", b.Name, err)
			}
		})
	}
	close(start)
	workers.Wait()
	if probes.Load() != 1 || leaked.Load() != 0 || acquisitions.Load() != 2 {
		t.Fatalf(
			"IAM probes=%d through source proxy=%d Bucket acquisitions=%d",
			probes.Load(),
			leaked.Load(),
			acquisitions.Load(),
		)
	}
	t.Run("restricted metadata dial", func(t *testing.T) {
		testFetcherSharedIAMTransportRestrictedMetadataDial(

			t, &probes, &leaked,
			&acquisitions)
	},
	)
}

func TestFetcher_MachineWebIdentityTransport(t *testing.T) {
	clearMachineEnvironment(t)
	token := filepath.Join(t.TempDir(), "web-identity")
	testutil.WriteFileAt(t, token, "identity-token-canary")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture")
	t.Setenv("AWS_REGION", "us-east-1")
	var probes, acquisitions, leaked atomic.Int32
	bucketCredentialRoute(t, "sts.us-east-1.amazonaws.com", func(r *http.Request) *http.Response {
		probes.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("WebIdentityToken") != "identity-token-canary" ||
			r.Form.Get("RoleArn") != os.Getenv("AWS_ROLE_ARN") {
			t.Error("STS lost its identity token or role")
		}
		return bucketReply(
			r,
			http.StatusOK,
			`<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>sts-canary</AccessKeyId><SecretAccessKey>sts-secret</SecretAccessKey><SessionToken>sts-session</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`,
		)
	})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect || r.URL.Host != "bucket.invalid" ||
			strings.Contains(fmt.Sprint(r.URL, r.Header), "identity-token-canary") {
			leaked.Add(1)
			http.Error(w, "unexpected proxy destination", http.StatusBadGateway)
			return
		}
		acquisitions.Add(1)
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=sts-canary/") ||
			r.Header.Get("X-Amz-Security-Token") != "sts-session" {
			t.Error("S3 lost the STS signing credentials")
		}
		_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	t.Cleanup(proxy.Close)
	b := ambientBucket()
	b.Provider, b.SecretRef, b.Endpoint, b.Insecure = "aws", nil, "http://bucket.invalid", true
	b.ProxySecretRef = &manifest.LocalObjectReference{Name: "transport"}
	b.CertSecretRef = b.ProxySecretRef
	ca := testutil.SelfSignedCA(t)
	inputs := testInputs{secrets: func(string, string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"address": proxy.URL, "ca.crt": ca}}
	}}
	reads := inputs.readSet(b)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, reads, &cache.Policy{}, nil)
	if err != nil || artifact == nil || probes.Load() != 1 || acquisitions.Load() != 1 || leaked.Load() != 0 {
		t.Fatalf(
			"web identity: STS=%d S3=%d proxy leaks=%d error=%v",
			probes.Load(),
			acquisitions.Load(),
			leaked.Load(),
			err,
		)
	}
}

func testFetcherMachineIAMCancellationCase(t *testing.T, after *string,
) {
	t.Helper()
	clearMachineEnvironment(t)
	var probes, acquisitions atomic.Int32
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseProbe := sync.OnceFunc(func() { close(release) })
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probes.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-r.Context().Done():
			close(stopped)
			return
		case <-release:
		}
		_, _ = io.WriteString(
			w,
			`{"Code":"Success","AccessKeyId":"iam-canary","SecretAccessKey":"secret","Expiration":"2099-01-01T00:00:00Z"}`,
		)
	}))
	t.Cleanup(metadata.Close)
	t.Cleanup(releaseProbe)
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", metadata.URL)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acquisitions.Add(1)
		if !strings.Contains(r.Header.Get("Authorization"), "Credential=iam-canary/") {
			t.Error("live Bucket lost the shared IAM credential")
		}
		_, _ = io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	t.Cleanup(endpoint.Close)
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	b := ambientBucket()
	b.Provider, b.SecretRef, b.Endpoint, b.Insecure = "aws", nil, endpoint.URL, true
	reads := (testInputs{}).readSet(b)
	defer reads.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := fetchtest.Fetch(ctx, f.Fetch, b, reads, &cache.Policy{}, nil)
		finished <- err
	}()
	<-entered
	cancel()
	{
		err := <-finished
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "initiating caller lost cancellation: %v", err)
	}
	if (*after) == "owner close" {
		f.Network.Close()
		<-stopped
		assert.FatalIf(
			t,
			probes.Load() != 1 || acquisitions.Load() != 0,
			"%s",
			"closed discovery reached source acquisition",
		)

		return
	}
	releaseProbe()
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, reads, &cache.Policy{}, nil)
	if err != nil || artifact == nil || probes.Load() != 1 || acquisitions.Load() != 1 {
		t.Fatalf(
			"cancelled caller poisoned shared discovery: %v probes=%d acquisitions=%d",
			err,
			probes.Load(),
			acquisitions.Load(),
		)
	}
}

func testFetcherSharedIAMTransportRestrictedMetadataDial(
	t *testing.T, probes *atomic.Int32,
	leaked *atomic.Int32,
	acquisitions *atomic.Int32,
) {
	t.Helper()
	ssrfguard.Restrict(true)
	t.Cleanup(func() { ssrfguard.Restrict(false) })
	b := ambientBucket()
	b.Provider, b.SecretRef = "aws", nil
	reads := (testInputs{}).readSet(b)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New), Network: newTestNetwork(t)}
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, reads, &cache.Policy{}, nil)
	dial, ok := errors.AsType[*net.OpError](err)
	if artifact != nil || !ok || dial.Op != "dial" || probes.Load() != 1 || acquisitions.
		Load() != 2 || leaked.
		Load() != 0 {
		t.Fatalf(
			"metadata guard: probes=%d acquisitions=%d error=%v", probes.
				Load(), acquisitions.
				Load(), err,
		)
	}
}
