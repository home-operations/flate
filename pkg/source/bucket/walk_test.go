package bucket

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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

type blockedArchiveWriter struct{ release <-chan struct{} }

func (w blockedArchiveWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func TestWalkBucket_ReadAheadByteBound(t *testing.T) {
	old := readAheadBytes
	readAheadBytes = 4
	defer func() { readAheadBytes = old }()
	for _, size := range []int{1, 6} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			fetched := make(chan struct{}, 16)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") != "" {
					_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
					for i := range 16 {
						_, _ = fmt.Fprintf(w, "<Contents><Key>file%02d</Key><Size>%d</Size><ETag>\"x\"</ETag></Contents>", i, size)
					}
					_, _ = fmt.Fprint(w, "</ListBucketResult>")
					return
				}
				fetched <- struct{}{}
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				w.Header().Set("ETag", "\"x\"")
				w.Header().Set("Content-Length", strconv.Itoa(size))
				_, _ = io.WriteString(w, strings.Repeat("x", size))
			}))
			defer srv.Close()
			client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := walkBucket(ctx, client, &manifest.Bucket{BucketName: "data"}, blockedArchiveWriter{release})
				done <- err
			}()
			want := max(1, int(readAheadBytes)/size)
			for range want {
				select {
				case <-fetched:
				case <-ctx.Done():
					t.Fatal("downloads stalled before filling window")
				}
			}
			select {
			case <-fetched:
				t.Fatal("download exceeded byte window")
			case <-time.After(30 * time.Millisecond):
			}
			unblock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWalkBucket_Faults(t *testing.T) {
	for _, failure := range []string{"missing", "server", "hostile", "short", "long", "hang"} {
		t.Run(failure, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") != "" {
					_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
					for i := range 16 {
						key := fmt.Sprintf("file%02d", i)
						if failure == "hostile" && i == 3 {
							key = "../escape"
						}
						_, _ = fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>4</Size><ETag>\"x\"</ETag></Contents>", key)
					}
					_, _ = fmt.Fprint(w, "</ListBucketResult>")
					return
				}
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				w.Header().Set("ETag", "\"x\"")
				if strings.HasSuffix(r.URL.Path, "file05") {
					switch failure {
					case "server":
						http.Error(w, "failure", http.StatusInternalServerError)
						return
					case "long":
						w.Header().Set("Content-Length", "5")
						_, _ = io.WriteString(w, "abcde")
						return
					case "missing":
						http.NotFound(w, r)
						return
					case "hang":
						<-r.Context().Done()
						return
					case "short":
						w.Header().Set("Content-Length", "4")
						_, _ = io.WriteString(w, "ab")
						return
					}
				}
				w.Header().Set("Content-Length", "4")
				_, _ = io.WriteString(w, "abcd")
			}))
			defer srv.Close()
			client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			_, err = walkBucket(ctx, client, &manifest.Bucket{BucketName: "data"}, io.Discard)
			if err == nil {
				t.Fatal("fault accepted")
			}
			if failure == "hang" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline identity: %v", err)
			}
		})
	}
}

// A listing is untrusted: an object larger than the archive cap, or a negative size, must fail before
// any allocation or download.
func TestWalkBucket_RejectsImplausibleListedSize(t *testing.T) {
	for _, size := range []int64{-1, 1<<30 + 1, 1 << 40} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var gets atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "" {
					gets.Add(1)
					return
				}
				_, _ = fmt.Fprintf(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>big</Key><Size>%d</Size><ETag>\"x\"</ETag></Contents></ListBucketResult>", size)
			}))
			defer srv.Close()
			client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := walkBucket(t.Context(), client, &manifest.Bucket{BucketName: "data"}, io.Discard); err == nil || gets.Load() != 0 {
				t.Fatalf("err=%v gets=%d, want a listing-time error and no download", err, gets.Load())
			}
		})
	}
}
