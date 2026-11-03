package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func testWalkBucketPhaseTimeoutsCase(t *testing.T, tc *struct {
	name    string
	objects int
},
) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		testWalkBucketPhaseTimeoutsCaseCallback(t, testWalkBucketPhaseTimeoutsCaseCallbackInputs{tc: &tc})
	},
	)
}

type testWalkBucketPhaseTimeoutsCaseCallbackInputs struct {
	tc **struct {
		name    string
		objects int
	}
}

func testWalkBucketPhaseTimeoutsCaseCallback(
	t *testing.T,
	testCaseInput testWalkBucketPhaseTimeoutsCaseCallbackInputs,
) {
	t.Helper()
	start := time.Now()
	b := &manifest.Bucket{
		BucketName: "data",
		Endpoint:   "https://bucket.invalid",
		Region:     "us-east-1",
		Timeout:    &metav1.Duration{Duration: 5 * time.Second},
	}
	f := timeoutBucketFetcher(t, func(req *http.Request) (*http.Response, error) {
		listing := req.URL.Query().Get("list-type") != ""
		data := "x"
		if listing {
			data = "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>"
			var dataSb1088 strings.Builder
			for i := range (*testCaseInput.tc).objects {
				fmt.Fprintf(&dataSb1088,
					"<Contents><Key>file%02d</Key><Size>1</Size><ETag>\"x\"</ETag></Contents>",
					i,
				)
			}
			data += dataSb1088.String()
			data += "</ListBucketResult>"
		}
		if listing || (*testCaseInput.tc).name != "download body" {
			select {
			case <-time.After(3 * time.Second):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		body := io.NopCloser(strings.NewReader(data))
		if listing && (*testCaseInput.tc).name == "listing body" ||
			!listing && (*testCaseInput.tc).name == "download body" {
			body = bucketTimeoutBody{req.Context()}
		}
		header := http.Header{
			"Content-Length": {strconv.Itoa(len(data))},
			"Etag":           {"\"x\""},
			"Last-Modified":  {"Mon, 02 Jan 2006 15:04:05 GMT"},
		}
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        header,
			Body:          body,
			ContentLength: int64(len(data)),
			Request:       req,
		}, nil
	})
	artifact, err := f.Fetch(t.Context(), b, (testInputs{}).readSet(b), cache.Policy{})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 5*time.Second {
		t.Fatalf("phase result: elapsed=%v error=%v", time.Since(start), err)
	}
	assert.FatalIf(t, artifact != nil, "%s", "expired operation published an artifact")
}
