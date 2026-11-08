package bucket

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testWalkBucketArchiveNamesCase(t *testing.T, tc *struct {
	name    string
	prefix  string
	key     string
	want    string
	invalid bool
},
) {
	t.Helper()

	var gets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			if got := r.URL.Query().Get("prefix"); got != tc.prefix {
				t.Errorf("listing prefix=%q, want %q", got, tc.prefix)
			}
			_, _ = fmt.Fprintf(
				w,
				`<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><EncodingType>url</EncodingType><Contents><Key>%s</Key><Size>1</Size><ETag>"x"</ETag></Contents></ListBucketResult>`,
				url.QueryEscape(tc.key),
			)
			return
		}
		gets.Add(1)
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", `"x"`)
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		_, _ = io.WriteString(w, "x")
	}))
	defer srv.Close()
	client, err := minio.New(
		strings.TrimPrefix(srv.URL, "http://"),
		&minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath},
	)
	assert.NoError(t, err)

	var data bytes.Buffer
	_, err = walkBucket(
		t.Context(),
		client,
		&manifest.Bucket{BucketName: "data", Prefix: tc.prefix},
		bucketOutput{Writer: &data},
	)
	if tc.invalid {
		if !errors.Is(err, fs.ErrInvalid) || gets.Load() != 0 || data.Len() != 0 {
			t.Fatalf(
				"err=%v gets=%d bytes=%d, want invalid name before reads or headers",
				err,
				gets.Load(),
				data.Len(),
			)
		}
		return
	}
	assert.NoError(t, err)

	tr := tar.NewReader(&data)
	if tc.want != "" {
		h, err := tr.Next()
		if err != nil || h.Name != tc.want {
			t.Fatalf("header=%v err=%v, want %q", h, err, tc.want)
		}
		body, err := io.ReadAll(tr)
		if err != nil || string(body) != "x" || gets.Load() != 1 {
			t.Fatalf("body=%q err=%v gets=%d", body, err, gets.Load())
		}
	} else if gets.Load() != 0 {
		t.Fatalf("directory marker triggered %d reads", gets.Load())
	}
	{
		_, err := tr.Next()
		assert.FatalIf(t, !errors.Is(err, io.EOF), "archive end: %v", err)
	}
}
