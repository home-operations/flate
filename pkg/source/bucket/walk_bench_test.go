package bucket

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/minio/minio-go/v7"
)

type bucketWorkload struct {
	n        int
	ttfb     func(i int) time.Duration
	bodySize int
	chunk    int
	pace     time.Duration
}

func benchmarkBucket(b testing.TB, s bucketWorkload) *minio.Client {
	b.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
			for i := range s.n {
				_, _ = fmt.Fprintf(w, "<Contents><Key>file%03d</Key><Size>%d</Size><ETag>\"x\"</ETag></Contents>", i, s.bodySize)
			}
			_, _ = fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		idx, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], "file"))
		time.Sleep(s.ttfb(idx))
		size := s.bodySize
		if value := r.Header.Get("Range"); value != "" {
			var start, end int
			if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil {
				b.Error(err)
				return
			}
			size = end - start + 1
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, s.bodySize))
		}
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.Header().Set("ETag", "\"x\"")
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		}
		buf := make([]byte, s.chunk)
		for sent := 0; sent < size; sent += s.chunk {
			n := min(s.chunk, size-sent)
			if _, err := w.Write(buf[:n]); err != nil {
				return
			}
			if s.pace > 0 {
				w.(http.Flusher).Flush()
				time.Sleep(s.pace)
			}
		}
	}))
	b.Cleanup(srv.Close)
	client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		b.Fatal(err)
	}
	return client
}

var scenarios = map[string]bucketWorkload{
	"UnevenTTFB": {n: 64, ttfb: func(i int) time.Duration {
		if i%8 == 0 {
			return 100 * time.Millisecond
		}
		return 5 * time.Millisecond
	}, bodySize: 1, chunk: 1},
	"PacedBodies": {n: 16, ttfb: func(int) time.Duration { return 5 * time.Millisecond }, bodySize: 8 << 20, chunk: 64 << 10, pace: time.Millisecond},
	"EvenSmall":   {n: 64, ttfb: func(int) time.Duration { return 10 * time.Millisecond }, bodySize: 1, chunk: 1},
}

func BenchmarkWalkBucket(b *testing.B) {
	for _, name := range []string{"UnevenTTFB", "PacedBodies", "EvenSmall"} {
		s := scenarios[name]
		b.Run(name, func(b *testing.B) {
			client := benchmarkBucket(b, s)
			bk := &manifest.Bucket{BucketName: "data"}
			for b.Loop() {
				if _, err := walkBucket(b.Context(), client, bk, io.Discard); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
