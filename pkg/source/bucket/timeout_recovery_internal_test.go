package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // The fixture replaces the process HTTP transport and proxy environment.
func TestFetcher_TimeoutRecovery(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopen=%t", reopen), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				testFetcherTimeoutRecoveryCase(t, &reopen)
			})
		},
		)
	}
}

func testFetcherTimeoutRecoveryCase(t *testing.T, reopen *bool,
) {
	t.Helper()

	var blocked atomic.Bool
	blocked.Store(true)
	var listings, objects atomic.Int32
	bodyStarted, bodyCanceled := make(chan struct{}), make(chan struct{})
	f := timeoutBucketFetcher(t, func(r *http.Request) (*http.Response, error) {
		data := "hi"
		if r.URL.Query().Get("list-type") != "" {
			listings.Add(1)
			data = `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>hello.txt</Key><Size>2</Size><ETag>"a"</ETag></Contents></ListBucketResult>`
		} else {
			objects.Add(1)
		}
		body := io.NopCloser(strings.NewReader(data))
		if r.URL.Query().Get("list-type") == "" && blocked.Load() {
			reader, writer := io.Pipe()
			body = reader
			go func() {
				defer writer.Close()
				_, _ = io.WriteString(writer, "h")
				close(bodyStarted)
				<-r.Context().Done()
				close(bodyCanceled)
			}()
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Length": {strconv.Itoa(len(data))},
				"Etag":           {`"a"`},
				"Last-Modified":  {"Mon, 02 Jan 2006 15:04:05 GMT"},
			},
			Body:          body,
			ContentLength: int64(len(data)),
			Request:       r,
		}, nil
	})
	root := t.TempDir()
	f.Cache = cachetest.New(t, root, cache.New)
	b := &manifest.Bucket{
		Name: "timeout", Namespace: "test", BucketName: "data", Endpoint: "https://bucket.invalid",
		Insecure: true, Region: "us-east-1", Timeout: &metav1.Duration{Duration: time.Second},
	}
	policy := cache.Policy{MaxAge: time.Hour}
	artifact, err := fetchtest.Fetch(t.Context(), f.Fetch, b, (testInputs{}).readSet(b), &policy, nil)
	assert.FatalIf(
		t,
		!errors.Is(err, context.DeadlineExceeded) || artifact != nil,
		"body timeout: artifact=%v error=%v",
		artifact,
		err,
	)
	select {
	case <-bodyStarted:
	default:
		t.Fatal("source timed out before the object body was blocked")
	}
	<-bodyCanceled
	if *reopen {
		{
			closeErr := f.Cache.Close()
			assert.NoError(t, closeErr)
		}

		f.Cache = cachetest.New(t, root, cache.New)
	}
	blocked.Store(false)
	longer := *b
	longer.Timeout = &metav1.Duration{Duration: 10 * time.Second}
	artifact, err = fetchtest.Fetch(t.Context(), f.Fetch, &longer, (testInputs{}).readSet(&longer), &policy, nil)
	assert.FatalIf(t, err != nil, "longer policy reused timeout: %v", err)
	{
		data, err := fs.ReadFile(artifact.FS, "hello.txt")
		assert.FatalIf(t, err != nil || string(data) != "hi", "recovered content=%q error=%v", data, err)
	}
	if listings.Load() != 2 || objects.Load() != 2 {
		t.Fatalf("recovery listings=%d objects=%d, want 2 each", listings.Load(), objects.Load())
	}
}
