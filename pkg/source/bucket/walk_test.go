package bucket

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/minio/minio-go/v7"
)

func delayedBucket(b testing.TB) (*minio.Client, *atomic.Int32) {
	b.Helper()
	var active, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
			for i := 15; i >= 0; i-- {
				_, _ = fmt.Fprintf(w, "<Contents><Key>file%02d</Key><Size>1</Size><ETag>\"x\"</ETag></Contents>", i)
			}
			_, _ = fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", "\"x\"")
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		_, _ = io.WriteString(w, "x")
	}))
	b.Cleanup(srv.Close)
	client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		b.Fatal(err)
	}
	return client, &peak
}

func TestWalkBucket_BoundedParallelOrder(t *testing.T) {
	client, peak := delayedBucket(t)
	var data bytes.Buffer
	if _, err := walkBucket(t.Context(), client, &manifest.Bucket{BucketName: "data"}, &data); err != nil {
		t.Fatal(err)
	}
	if got := peak.Load(); got < 2 || got > 8 {
		t.Fatalf("parallel downloads=%d, want 2..8", got)
	}
	tr := tar.NewReader(&data)
	for i := range 16 {
		h, err := tr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("file%02d", i); h.Name != want {
			t.Fatalf("entry=%s, want %s", h.Name, want)
		}
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Fatalf("archive end: %v", err)
	}
}

func BenchmarkWalkBucket_Delayed(b *testing.B) {
	client, _ := delayedBucket(b)
	bucket := &manifest.Bucket{BucketName: "data"}
	for b.Loop() {
		if _, err := walkBucket(b.Context(), client, bucket, io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}
