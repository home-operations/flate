package bucket_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/bucket"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_NonGenericProviderFailsLoud(t *testing.T) {
	f := &bucket.Fetcher{}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderAmazon,
		BucketName: "x", Endpoint: "s3.amazonaws.com",
	}
	_, err := f.Fetch(context.Background(), b, cache.RetryConfig{})
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestFetcher_SecretRefWithoutGetter(t *testing.T) {
	f := &bucket.Fetcher{} // no Secrets
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderGeneric,
		BucketName: "x", Endpoint: "minio:9000",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b, cache.RetryConfig{})
	if err == nil {
		t.Fatalf("expected error when SecretRef set but no SecretGetter")
	}
	if !strings.Contains(err.Error(), "SecretGetter") {
		t.Errorf("error should mention SecretGetter; got %v", err)
	}
}

func TestFetcher_SecretRefMissingKeys(t *testing.T) {
	f := &bucket.Fetcher{
		Secrets: func(ns, name string) *manifest.Secret {
			return &manifest.Secret{
				Name: name, Namespace: ns,
				StringData: map[string]any{"accesskey": "k"}, // secretkey missing
			}
		},
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderGeneric,
		BucketName: "x", Endpoint: "minio:9000",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b, cache.RetryConfig{})
	if err == nil {
		t.Fatalf("expected error when accesskey/secretkey missing")
	}
	if !strings.Contains(err.Error(), "missing accesskey/secretkey") {
		t.Errorf("error should say missing accesskey/secretkey; got %v", err)
	}
}

func TestFetcher_WipedCredentials(t *testing.T) {
	for _, field := range []string{"accesskey", "secretkey"} {
		for _, encoded := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encoded=%t", field, encoded), func(t *testing.T) {
				data := map[string]any{"accesskey": "access", "secretkey": "secret", field: "..PLACEHOLDER_" + field + ".."}
				sec := &manifest.Secret{StringData: data}
				if encoded {
					sec.StringData, sec.Data = nil, map[string]any{}
					for k, v := range data {
						sec.Data[k] = base64.StdEncoding.EncodeToString([]byte(v.(string)))
					}
				}
				f := &bucket.Fetcher{Secrets: func(_, _ string) *manifest.Secret { return sec }}
				b := &manifest.Bucket{Name: "b", Namespace: "ns", BucketName: "fixture", Endpoint: "127.0.0.1:1", Insecure: true, SecretRef: &manifest.LocalObjectReference{Name: "creds"}}
				if _, err := f.Fetch(t.Context(), b, cache.RetryConfig{}); !errors.Is(err, manifest.ErrMissingSecret) {
					t.Fatalf("got %v, want ErrMissingSecret", err)
				}
			})
		}
	}
}

func TestFetcher_SecretRefNotFound(t *testing.T) {
	f := &bucket.Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderGeneric,
		BucketName: "x", Endpoint: "minio:9000",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b, cache.RetryConfig{})
	if err == nil {
		t.Fatalf("expected error when SecretRef not resolvable")
	}
	if !strings.Contains(err.Error(), "secret ns/creds not found") {
		t.Errorf("error should name the missing secret; got %v", err)
	}
}

func TestFetcher_ForceGenericProvider(t *testing.T) {
	// Gate bypassed: the generic path runs and fails on the absent secret.
	f := &bucket.Fetcher{
		ForceGeneric: true,
		Secrets:      func(_, _ string) *manifest.Secret { return nil },
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderAmazon,
		BucketName: "x", Endpoint: "s3.amazonaws.com",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b, cache.RetryConfig{})
	if err == nil {
		t.Fatalf("expected missing-secret error")
	}
	if strings.Contains(err.Error(), "not implemented") {
		t.Errorf("provider gate should be bypassed with ForceGeneric; got %v", err)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Errorf("want ErrMissingSecret from the generic path; got %v", err)
	}
}

func TestFetcher_ArchiveIsolationAndWarmHits(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		for _, attempts := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/attempts-%d", backing, attempts), func(t *testing.T) {
				var listings atomic.Int32
				sequences := map[string]*atomic.Int32{"first": {}, "second": {}, "canceled": {}}
				streaming, canceled := make(chan struct{}), make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					namespace := "first"
					for _, name := range []string{"second", "canceled"} {
						if strings.Contains(r.Header.Get("Authorization"), "Credential="+name+"/") {
							namespace = name
						}
					}
					sequence := sequences[namespace]
					files := map[string]string{"dir/file.yaml": "first", "dir/drop.yaml": "drop", ".sourceignore": "dir/drop.yaml\n", "image.png": "image", ".git/config": "git"}
					if namespace == "second" {
						files["dir/file.yaml"] = "other"
					}
					listing := r.URL.Query().Get("list-type") != ""
					if listing {
						listings.Add(1)
						sequence.Add(1)
					}
					if sequence.Load() == 1 {
						files["attempt-only.yaml"] = "discard"
					}
					if listing {
						names := []string{"dir/drop.yaml", "image.png", "dir/file.yaml", ".sourceignore", ".git/config"}
						if sequence.Load() == 1 {
							names = append(names, "attempt-only.yaml")
						}
						_, _ = fmt.Fprint(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>`)
						for _, name := range names {
							data := files[name]
							_, _ = fmt.Fprintf(w, `<Contents><Key>prefix/%s</Key><Size>%d</Size><ETag>"%x"</ETag></Contents>`, name, len(data), sha256.Sum256([]byte(data)))
						}
						_, _ = fmt.Fprint(w, `</ListBucketResult>`)
						return
					}
					name := strings.TrimPrefix(r.URL.Path, "/data/prefix/")
					data, ok := files[name]
					if !ok {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
					w.Header().Set("ETag", fmt.Sprintf(`"%x"`, sha256.Sum256([]byte(data))))
					w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
					if name == "image.png" {
						if namespace == "canceled" {
							_, _ = fmt.Fprint(w, data[:2])
							w.(http.Flusher).Flush()
							close(streaming)
							<-r.Context().Done()
							close(canceled)
							return
						}
						if sequence.Load() == 1 {
							_, _ = fmt.Fprint(w, data[:2])
							return
						}
					}
					_, _ = fmt.Fprint(w, data)
				}))
				t.Cleanup(srv.Close)
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c, err := cache.New(t.Context(), root)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() })
				f := &bucket.Fetcher{Cache: c, Secrets: func(ns, _ string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{"accesskey": ns, "secretkey": "test"}}
				}}
				var wg sync.WaitGroup
				for _, namespace := range []string{"first", "second"} {
					wg.Go(func() {
						b := &manifest.Bucket{Name: "source", Namespace: namespace, BucketName: "data", Prefix: "prefix/",
							Endpoint: srv.URL, Insecure: true, Region: "us-east-1", Interval: metav1.Duration{Duration: time.Hour},
							SecretRef: &manifest.LocalObjectReference{Name: "creds"}}
						for range 2 {
							art, err := f.Fetch(t.Context(), b, cache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond})
							if attempts == 1 {
								if err == nil || art != nil {
									t.Errorf("%s retry-off artifact=%v, error=%v", namespace, art, err)
								}
								return
							}
							if err != nil {
								t.Error(err)
								return
							}
							want := namespace
							if namespace == "second" {
								want = "other"
							}
							if data, err := fs.ReadFile(art.FS, "dir/file.yaml"); err != nil || string(data) != want {
								t.Errorf("%s content=%q err=%v", namespace, data, err)
							}
							if _, err := fs.Stat(art.FS, "attempt-only.yaml"); !errors.Is(err, fs.ErrNotExist) {
								t.Errorf("failed acquisition leaked a file: %v", err)
							}
							if _, err := fs.Stat(art.FS, "dir/drop.yaml"); !errors.Is(err, fs.ErrNotExist) {
								t.Errorf("ignored file: %v", err)
							}
							if data, err := fs.ReadFile(art.FS, "image.png"); err != nil || string(data) != "image" {
								t.Errorf("complete image=%q, error=%v", data, err)
							}
							if data, err := fs.ReadFile(art.FS, ".git/config"); err != nil || string(data) != "git" {
								t.Errorf("bucket flavor excluded git content: %q %v", data, err)
							}
							if art.Metadata["objectCount"] != "5" || art.Revision == "" {
								t.Errorf("bucket metadata: %v", art.Metadata)
							}
						}
					})
				}
				wg.Wait()
				want := int32(2 * min(attempts, 2))
				if listings.Load() != want {
					t.Fatalf("producer sequences=%d, want %d", listings.Load(), want)
				}
				if attempts == 1 {
					return
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					b := &manifest.Bucket{Name: "source", Namespace: "canceled", BucketName: "data", Prefix: "prefix/",
						Endpoint: srv.URL, Insecure: true, Region: "us-east-1", SecretRef: &manifest.LocalObjectReference{Name: "creds"}}
					_, err := f.Fetch(ctx, b, cache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond})
					done <- err
				}()
				select {
				case <-streaming:
				case err := <-done:
					t.Fatalf("fetch ended before cancellation: %v", err)
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled fetch: %v", err)
				}
				<-canceled
				if sequences["canceled"].Load() != 1 {
					t.Fatalf("canceled producer sequences=%d, want 1", sequences["canceled"].Load())
				}
			})
		}
	}
}
