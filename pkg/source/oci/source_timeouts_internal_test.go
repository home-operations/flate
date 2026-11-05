package oci

import (
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_SourceTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "default"},
		{name: "shorter", timeout: 10 * time.Second},
		{name: "longer", timeout: 180 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherSourceTimeoutCase(t, &tc)
		},
		)
	}
}

func testFetcherSourceTimeoutCase(t *testing.T, tc *struct {
	name    string
	timeout time.Duration
},
) {
	t.Helper()

	want := tc.timeout
	if want == 0 {
		want = time.Minute
	}
	r := &manifest.OCIRepository{
		URL:       "oci://registry.invalid/repository",
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	if tc.timeout != 0 {
		r.Timeout = &metav1.Duration{Duration: tc.timeout}
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", sourcecache.New)}
	request, err := (&operation{Fetcher: f}).resolvedRepository(t.Context(), new(r.RegistryAccess()))
	assert.NoError(t, err)

	stop := errors.New("controlled registry request")
	var calls atomic.Int32
	request.client.Client = &http.Client{
		Transport: registryRoundTripper(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			deadline, ok := req.Context().Deadline()
			remaining := time.Until(deadline)
			assert.ErrorIf(
				t,
				!ok || remaining <= want-time.Second || remaining > want,
				"registry request deadline: present=%t remaining=%v, want %v",
				ok,
				remaining,
				want,
			)
			return nil, stop
		}),
	}
	_, err = (&operation{Fetcher: f}).fetchLayer(t.Context(), r, new(request), &sourcecache.Policy{})
	if calls.Load() == 0 || !errors.Is(err, stop) {
		t.Fatalf("controlled registry request not reached: calls=%d error=%v", calls.Load(), err)
	}
}
