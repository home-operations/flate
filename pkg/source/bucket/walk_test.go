package bucket

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
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

func TestWalkBucket_ChunkBound(t *testing.T) {
	const size = 8 << 20
	requested := make(chan struct{}, 16)
	completed := make(chan struct{}, 16)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "" {
			_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
			for i := range 16 {
				_, _ = fmt.Fprintf(w, "<Contents><Key>file%02d</Key><Size>%d</Size><ETag>\"x\"</ETag></Contents>", i, size)
			}
			_, _ = fmt.Fprint(w, "</ListBucketResult>")
			return
		}
		requested <- struct{}{}
		w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
		w.Header().Set("ETag", "\"x\"")
		w.Header().Set("Content-Length", strconv.Itoa(size))
		buf := []byte(strings.Repeat("x", min(size, 64<<10)))
		for sent := 0; sent < size; sent += len(buf) {
			if _, err := w.Write(buf[:min(len(buf), size-sent)]); err != nil {
				return
			}
		}
		w.(http.Flusher).Flush()
		completed <- struct{}{}
	}))
	defer srv.Close()
	client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := walkBucket(ctx, client, &manifest.Bucket{BucketName: "data"}, blockedArchiveWriter{release})
		done <- err
	}()
	for range 8 {
		select {
		case <-completed:
		case <-ctx.Done():
			t.Fatal("downloads stalled before filling window")
		}
	}
	for range 8 {
		<-requested
	}
	select {
	case <-requested:
		t.Fatal("download exceeded chunk window")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
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
			client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_, err = walkBucket(ctx, client, &manifest.Bucket{BucketName: "data"}, io.Discard)
			if err == nil {
				t.Fatal("fault accepted")
			}
			if failure != "hang" && errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("fault timed out: %v", err)
			}
			switch failure {
			case "missing", "server":
				want := http.StatusNotFound
				if failure == "server" {
					want = http.StatusInternalServerError
				}
				if response, ok := errors.AsType[minio.ErrorResponse](err); !ok || response.StatusCode != want {
					t.Fatalf("HTTP fault: %v, want status %d", err, want)
				}
			case "hostile":
				if !strings.Contains(err.Error(), "escapes target directory") {
					t.Fatalf("path fault: %v", err)
				}
			case "short":
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("short body: %v", err)
				}
			case "long":
				if err.Error() != `bucket object "file05" exceeds chunk size` {
					t.Fatalf("long body: %v", err)
				}
			case "hang":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline identity: %v", err)
				}
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
			if _, err := walkBucket(t.Context(), client, &manifest.Bucket{BucketName: "data"}, io.Discard); err == nil || err.Error() != `bucket object "big" exceeds 1 GiB extract limit` || gets.Load() != 0 {
				t.Fatalf("err=%v gets=%d, want a listing-time error and no download", err, gets.Load())
			}
		})
	}
}

// Children MUST use AfterFunc so the gate holds cancellation propagation
// until the writer's body error reaches errgroup.
type gatedCancelContext struct {
	context.Context
	gate <-chan struct{}
}

func (c gatedCancelContext) Value(any) any { return nil }

func (c gatedCancelContext) AfterFunc(f func()) func() bool {
	return context.AfterFunc(c.Context, func() {
		<-c.gate
		f()
	})
}

type cancelArchiveWriter struct {
	cancel context.CancelFunc
	header bool
}

func (w *cancelArchiveWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.header {
		w.cancel()
	}
	w.header = true
	return len(p), nil
}

func TestWalkBucket_Ranges(t *testing.T) {
	const part = 8 << 20
	for _, behavior := range []string{"ordered", "ignore", "changed", "wrong_range", "short", "long", "cancel", "ignore_short", "ignore_long", "ignore_cancel", "ignore_changed", "ignore_missing", "ignore_server", "ignore_big", "ignore_cancel_ordered"} {
		run := func(t *testing.T) {
			copyCtx, cancelCopy := context.WithCancel(t.Context())
			defer cancelCopy()
			var gets atomic.Int32
			later := make(chan struct{})
			streamed := make(chan struct{})
			completeLater := sync.OnceFunc(func() { close(later) })
			sizes := []int64{0, 2*part + 17, 2*part + 17}
			fault := 0
			switch behavior {
			case "ignore_changed":
				fault = http.StatusPreconditionFailed
			case "ignore_missing":
				fault = http.StatusNotFound
			case "ignore_server":
				fault = http.StatusInternalServerError
			case "ignore_cancel_ordered":
				sizes = []int64{9 << 20}
			case "ignore_big":
				sizes = []int64{downloads*part + 1, 0, 0}
			}
			if fault != 0 {
				sizes = []int64{9 << 20, 1}
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") != "" {
					_, _ = fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
					for i, size := range slices.Backward(sizes) {
						_, _ = fmt.Fprintf(w, `<Contents><Key>prefix/file%02d</Key><Size>%d</Size><ETag>"v1"</ETag></Contents>`, i, size)
					}
					_, _ = fmt.Fprint(w, "</ListBucketResult>")
					return
				}
				gets.Add(1)
				if got := r.Header.Get("If-Match"); got != `"v1"` {
					t.Errorf("If-Match=%q, want listed ETag", got)
				}
				i, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/data/prefix/file"))
				if fault != 0 && i == 1 {
					select {
					case <-streamed:
					case <-r.Context().Done():
						return
					}
					http.Error(w, "sibling failed", fault)
					return
				}
				start, end := int64(0), sizes[i]-1
				status := http.StatusOK
				if sizes[i] > 0 && !strings.HasPrefix(behavior, "ignore") {
					if n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); n != 2 || err != nil || end-start+1 > part {
						t.Errorf("invalid chunk range: %q", r.Header.Get("Range"))
						http.Error(w, "bad range", http.StatusBadRequest)
						return
					}
					status = http.StatusPartialContent
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, sizes[i]))
				}
				if behavior == "wrong_range" && start == part {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start+1, end+1, sizes[i]))
				}
				if behavior == "changed" && start >= part {
					http.Error(w, "changed object", http.StatusPreconditionFailed)
					return
				}
				length := end - start + 1
				if i == 1 && (start == part || strings.HasPrefix(behavior, "ignore_")) {
					switch strings.TrimPrefix(behavior, "ignore_") {
					case "short":
						length--
					case "long":
						length++
					}
				}
				w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
				w.Header().Set("ETag", `"v1"`)
				w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
				w.WriteHeader(status)
				w.(http.Flusher).Flush()
				if behavior == "ignore_cancel_ordered" {
					_, _ = w.Write(make([]byte, 64<<10))
					w.(http.Flusher).Flush()
					if r.Header.Get("Range") != "" {
						<-r.Context().Done()
						return
					}
					<-copyCtx.Done()
					return
				}
				if i == 1 && (behavior == "ignore_cancel" || behavior == "cancel" && start == part) {
					_, _ = w.Write(make([]byte, 64<<10))
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				if fault != 0 && i == 0 && r.Header.Get("Range") == "" {
					_, _ = w.Write(make([]byte, 64<<10))
					w.(http.Flusher).Flush()
					close(streamed)
					<-r.Context().Done()
					return
				}
				if behavior == "ordered" && i == 1 && start == 0 {
					select {
					case <-later:
					case <-r.Context().Done():
						return
					}
				}
				for sent := int64(0); sent < length; {
					n := min(int64(64<<10), length-sent)
					buf := bytes.Repeat([]byte{byte((start+sent)/(1<<20) + int64(i))}, int(n))
					if _, err := w.Write(buf); err != nil {
						return
					}
					sent += n
				}
				if i == 1 && start == part {
					completeLater()
				}
			}))
			t.Cleanup(srv.Close)
			client, err := minio.New(strings.TrimPrefix(srv.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1})
			if err != nil {
				t.Fatal(err)
			}
			timeout := 2 * time.Second
			if behavior == "ignore_big" {
				timeout = 10 * time.Second
			} else if strings.HasSuffix(behavior, "cancel") {
				timeout = 200 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			var data bytes.Buffer
			var archive io.Writer = &data
			if behavior == "ignore_cancel_ordered" {
				gate := make(chan struct{})
				defer close(gate)
				ctx = gatedCancelContext{Context: copyCtx, gate: gate}
				archive = &cancelArchiveWriter{cancel: cancelCopy}
			}
			desc, err := walkBucket(ctx, client, &manifest.Bucket{BucketName: "data", Prefix: "prefix/"}, archive)
			if fault != 0 {
				if response, ok := errors.AsType[minio.ErrorResponse](err); !ok || response.StatusCode != fault {
					t.Fatalf("streamed sibling failure: %v, want status %d", err, fault)
				}
				return
			}
			if behavior != "ordered" && behavior != "ignore" && behavior != "ignore_big" {
				if err == nil {
					t.Fatal("invalid range accepted")
				}
				if !strings.HasSuffix(behavior, "cancel") && errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("range fault timed out: %v", err)
				}
				switch behavior {
				case "ignore_cancel_ordered":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("context identity: %v", err)
					}
				case "changed":
					if response, ok := errors.AsType[minio.ErrorResponse](err); !ok || response.StatusCode != http.StatusPreconditionFailed {
						t.Fatalf("ETag mismatch: %v", err)
					}
				case "wrong_range":
					if !strings.Contains(err.Error(), "Content-Range") {
						t.Fatalf("wrong range: %v", err)
					}
				case "short":
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("short range: %v", err)
					}
				case "long":
					if err.Error() != `bucket object "prefix/file01" exceeds chunk size` {
						t.Fatalf("long range: %v", err)
					}
				case "ignore_short":
					if !strings.Contains(err.Error(), "missed writing 1 bytes") {
						t.Fatalf("short stream: %v", err)
					}
				case "ignore_long":
					if !errors.Is(err, tar.ErrWriteTooLong) {
						t.Fatalf("long stream: %v", err)
					}
				case "cancel", "ignore_cancel":
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("context identity: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantGets := int32(8)
			if behavior == "ignore" || behavior == "ignore_big" {
				wantGets = 4
			}
			if gets.Load() != wantGets {
				t.Fatalf("GETs=%d, want %d", gets.Load(), wantGets)
			}
			h := sha256.New()
			tw := tar.NewWriter(h)
			for i, size := range sizes {
				if err := tw.WriteHeader(&tar.Header{Name: fmt.Sprintf("file%02d", i), Mode: 0o600, Size: size}); err != nil {
					t.Fatal(err)
				}
				for offset := int64(0); offset < size; offset += 1 << 20 {
					_, _ = tw.Write(bytes.Repeat([]byte{byte(offset/(1<<20) + int64(i))}, int(min(1<<20, size-offset))))
				}
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			if got, want := fmt.Sprintf("%x", sha256.Sum256(data.Bytes())), fmt.Sprintf("%x", h.Sum(nil)); got != want {
				t.Fatalf("tar digest=%s, want %s", got, want)
			}
			revision := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("prefix/file00 v1\nprefix/file01 v1\nprefix/file02 v1\n")))
			if desc.Annotations["flate.revision"] != revision || desc.Annotations["flate.object-count"] != "3" {
				t.Fatalf("metadata=%v, want revision=%s count=3", desc.Annotations, revision)
			}
		}
		if behavior == "ignore_changed" || behavior == "ignore_missing" || behavior == "ignore_server" {
			t.Run(behavior, func(t *testing.T) {
				for i := range 20 {
					t.Run(strconv.Itoa(i), run)
				}
			})
		} else {
			t.Run(behavior, run)
		}
	}
}
