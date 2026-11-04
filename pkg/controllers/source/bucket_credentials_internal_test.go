package source

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestController_BucketMachineFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		dial, absent bool
	}{
		{name: "STS rejection"},
		{name: "STS dial failure", dial: true},
		{name: "missing token file", absent: true},
	} {
		for _, allow := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allow-%t", tc.name, allow), func(t *testing.T) {
				testControllerBucketMachineFailuresCase(
					t,
					testControllerBucketMachineFailuresCaseInputs{tc: &tc, allow: &allow},
				)
			},
			)
		}
	}
}

type testControllerBucketMachineFailuresCaseInputs struct {
	tc *struct {
		name string

		dial   bool
		absent bool
	}
	allow *bool
}

func testControllerBucketMachineFailuresCase(
	t *testing.T,
	testCaseInput testControllerBucketMachineFailuresCaseInputs,
) {
	t.Helper()
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		t.Setenv(key, "")
	}
	token := filepath.Join(t.TempDir(), "token-canary")
	if !testCaseInput.tc.absent {
		testutil.WriteFileAt(t, token, "credential-canary")
	}
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/fixture")
	old := http.DefaultTransport
	transport := old.(*http.Transport).Clone()
	dial := func(_ context.Context, network, _ string) (net.Conn, error) {
		if testCaseInput.tc.dial {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
		}
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			request, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			defer request.Body.Close()
			{
				assert.ErrorIf(
					t,
					request.Host == "bucket.invalid",
					"%v",
					"failed machine credentials reached the bucket",
				)
			}
			body := `<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><Error><Code>AccessDenied</Code><Message>credential-canary</Message></Error></ErrorResponse>`
			response := &http.Response{
				StatusCode:    http.StatusForbidden,
				Status:        "403 credential-canary",
				Header:        make(http.Header),
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: int64(len(body)),
				Close:         true,
			}
			_ = response.Write(server)
		}()
		return client, nil
	}
	transport.DialTLSContext = dial
	http.DefaultTransport = transport //nolint:reassign // The test intercepts AWS credential traffic through the default transport.
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		http.DefaultTransport = old //nolint:reassign // Restore the default transport after the test.
	})
	network := new(src.Network)
	t.Cleanup(network.Close)
	f := &bucket.Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: network}
	c, st := newConfiguredController(
		t,
		map[string]src.Fetcher{manifest.KindBucket: src.Wrap(manifest.KindBucket, f)},
		FetchOptions{AllowMissingSecrets: (*testCaseInput.allow)},
	)
	b := &manifest.Bucket{
		Name:       "bucket",
		Namespace:  "ns",
		Endpoint:   "https://bucket.invalid",
		BucketName: "data",
		Region:     "us-east-1",
		SecretRef:  &manifest.LocalObjectReference{Name: "auth"},
	}
	st.AddObject(b)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: b.Named(), ReconcileNode: c.ReconcileNode},
	)
	{
		assert.FatalIf(t, info.Status != store.StatusFailed || info.Outcome() == store.OutcomeSkipped ||
			strings.Contains(
				info.Message,
				"canary",
			), "status=%+v, want failure with no credential material", info)
	}
	{
		assert.FatalIf(
			t,
			st.GetArtifact(b.Named(), store.ArtifactContent) != nil,
			"%v",
			"failed machine acquisition published content",
		)
	}
}
