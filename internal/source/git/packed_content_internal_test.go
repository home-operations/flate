package git

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type testFetcherPackedContentCaseInputs struct {
	files    *map[string]string
	src      *string
	mode     *string
	attempts *int
}

func testFetcherPackedContentCase(t *testing.T, testCaseInput testFetcherPackedContentCaseInputs) {
	t.Helper()

	var acquisitions atomic.Int32
	handler := gitHTTPHandler(t, (*testCaseInput.src))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && acquisitions.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, "001e# service=git-upload-pack\n00000040abc")
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	root := ""
	if (*testCaseInput.mode) == "disk" {
		root = t.TempDir()
	}
	f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
	art, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		&manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: srv.URL},
		(testInputs{}).readSet(
			&manifest.GitRepository{Name: "packed", Namespace: "flux-system", URL: srv.URL},
		),
		&sourcecache.Policy{
			Retry: sourcecache.RetryConfig{
				Attempts: (*testCaseInput.attempts),
				MinWait:  time.Nanosecond,
				MaxWait:  time.Nanosecond,
			},
		},
		nil,
	)
	if (*testCaseInput.attempts) == 1 {
		if err == nil || art != nil || acquisitions.Load() != 1 {
			t.Fatalf("retry-off artifact=%v, error=%v, acquisitions=%d", art, err, acquisitions.Load())
		}
		return
	}
	if err != nil || acquisitions.Load() != 2 {
		t.Fatalf("original caller error=%v, acquisitions=%d, want 2", err, acquisitions.Load())
	}
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for file, want := range *testCaseInput.files {
				got, err := fs.ReadFile(art.FS, file)
				assert.ErrorIf(
					t,
					err != nil || string(got) != want,
					"packed %s content differs: bytes=%d, err=%v",
					file,
					len(got),
					err,
				)
			}
		})
	}
	readers.Wait()
}
