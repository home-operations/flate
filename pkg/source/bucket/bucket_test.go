package bucket_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	"github.com/home-operations/flate/pkg/source/cache"
)

func newTestNetwork(tb testing.TB) *source.Network {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	return network
}

func TestFetcher_NonGenericProviderFailsLoud(t *testing.T) {
	t.Parallel()
	f := &bucket.Fetcher{Network: newTestNetwork(t)}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderAmazon,
		BucketName: "x", Endpoint: "s3.amazonaws.com",
	}
	_, err := f.Fetch(context.Background(), b, (testInputs{}).readSet(b), cache.Policy{MaxAge: b.SourceInterval()})
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestFetcher_ForceGenericProvider(t *testing.T) {
	t.Parallel()
	// Gate bypassed: the generic path runs and fails on the absent secret.
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret { return nil }}
	f := &bucket.Fetcher{Network: newTestNetwork(t),
		ForceGeneric: true,
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider:   sourcev1.BucketProviderAmazon,
		BucketName: "x", Endpoint: "s3.amazonaws.com",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b, fInputs.readSet(b), cache.Policy{MaxAge: b.SourceInterval()})
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
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, attempts := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/attempts-%d", backing, attempts), func(t *testing.T) {
				t.Parallel()
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
				c := cachetest.New(t, root, cache.New)
				fInputs := testInputs{secrets: func(ns, _ string) *manifest.Secret {
					return &manifest.Secret{StringData: map[string]any{"accesskey": ns, "secretkey": "test"}}
				}}
				f := &bucket.Fetcher{Network: newTestNetwork(t), Cache: c}
				var wg sync.WaitGroup
				for _, namespace := range []string{"first", "second"} {
					wg.Go(func() {
						b := &manifest.Bucket{Name: "source", Namespace: namespace, BucketName: "data", Prefix: "prefix/",
							Endpoint: srv.URL, Insecure: true, Region: "us-east-1", Interval: metav1.Duration{Duration: time.Hour},
							SecretRef: &manifest.LocalObjectReference{Name: "creds"}}
						var family string
						for range 2 {
							art, err := f.Fetch(t.Context(), b, fInputs.readSet(b), cache.Policy{MaxAge: b.SourceInterval(), Retry: cache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}})
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
							if art.Key == "" || family != "" && art.Key != family {
								t.Errorf("warm acquisition family = %q, first = %q", art.Key, family)
							}
							family = art.Key
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
					_, err := f.Fetch(ctx, b, fInputs.readSet(b), cache.Policy{MaxAge: b.SourceInterval(), Retry: cache.RetryConfig{Attempts: attempts, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}})
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

func TestFetcher_TransientStatusRecovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status        int
		code, message string
	}{
		{http.StatusTooManyRequests, "SlowDown", "temporary failure"},
		{http.StatusServiceUnavailable, "SlowDown", "temporary failure"},
		{http.StatusForbidden, "AccessDenied", "access denied"},
	} {
		t.Run(fmt.Sprintf("status-%d", tc.status), func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				if calls.Add(1) == 1 {
					w.WriteHeader(tc.status)
					_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", tc.code, tc.message)
					return
				}
				_, _ = io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated></ListBucketResult>`)
			}))
			t.Cleanup(srv.Close)
			c := cachetest.New(t, "", cache.New)
			f := &bucket.Fetcher{Network: newTestNetwork(t), Cache: c}
			_, err := f.Fetch(t.Context(), &manifest.Bucket{
				Name: "retry", Namespace: "test", BucketName: "data",
				Endpoint: srv.URL, Insecure: true, Region: "us-east-1",
			}, (testInputs{}).readSet(&manifest.Bucket{
				Name: "retry", Namespace: "test", BucketName: "data",
				Endpoint: srv.URL, Insecure: true, Region: "us-east-1",
			}), cache.Policy{Retry: cache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond}})
			if tc.status == http.StatusForbidden {
				cause, ok := errors.AsType[minio.ErrorResponse](err)
				response, typed := errors.AsType[*fetcherr.ResponseError](err)
				if !ok || !typed || response.StatusCode != tc.status || response.Op != http.MethodGet || !errors.Is(response.Err, cause) ||
					cause.StatusCode != tc.status || cause.Code != tc.code || cause.Message != tc.message ||
					err.Error() != "Bucket test/retry: "+cause.Error() || calls.Load() != 1 {
					t.Fatalf("permanent failure: %v, cause=%+v, requests=%d", err, cause, calls.Load())
				}
				return
			}
			if err != nil || calls.Load() != 2 {
				t.Fatalf("recovery: %v, requests=%d; want success after 2 requests", err, calls.Load())
			}
		})
	}
}

func TestFetcher_TerminalStatusReplay(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `<Error><Code>SlowDown</Code><Message>temporary failure fixture-secret</Message></Error>`)
			}))
			t.Cleanup(srv.Close)
			root := t.TempDir()
			c, err := cache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			f := &bucket.Fetcher{Cache: c, Network: newTestNetwork(t)}
			b := &manifest.Bucket{
				Name: "terminal", Namespace: "test", BucketName: "data",
				Endpoint: srv.URL, Insecure: true, Region: "us-east-1",
			}
			retry := cache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond}
			_, err = f.Fetch(t.Context(), b, (testInputs{}).readSet(b), cache.Policy{MaxAge: b.SourceInterval(), Retry: retry})
			terminal, ok := errors.AsType[*cache.TerminalError](err)
			cause, typed := errors.AsType[*fetcherr.ResponseError](err)
			if !ok || !terminal.Attempted || !typed || cause.StatusCode != status || cause.Op != http.MethodGet || cause.Error() != "temporary failure fixture-secret" || calls.Load() != 3 {
				t.Fatalf("terminal failure: %v, requests=%d", err, calls.Load())
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			readRecord := func() (ocispec.Descriptor, []byte) {
				t.Helper()
				data, err := os.ReadFile(filepath.Join(root, "oci", "index.json"))
				if err != nil {
					t.Fatal(err)
				}
				var index ocispec.Index
				if err := json.Unmarshal(data, &index); err != nil || len(index.Manifests) != 1 {
					t.Fatalf("persisted outcome index: %s, %v", data, err)
				}
				desc := index.Manifests[0]
				data, err = os.ReadFile(filepath.Join(root, "oci", "blobs", desc.Digest.Algorithm().String(), desc.Digest.Encoded()))
				if err != nil {
					t.Fatal(err)
				}
				var m ocispec.Manifest
				if err := json.Unmarshal(data, &m); err != nil || m.ArtifactType != "application/vnd.flate.failure.v1" {
					t.Fatalf("persisted outcome manifest: %s, %v", data, err)
				}
				data, err = os.ReadFile(filepath.Join(root, "oci", "blobs", m.Config.Digest.Algorithm().String(), m.Config.Digest.Encoded()))
				if err != nil {
					t.Fatal(err)
				}
				return desc, data
			}
			initial, record := readRecord()
			if strings.Contains(string(record), "fixture-secret") || strings.Contains(string(record), "temporary failure") {
				t.Fatalf("persisted upstream diagnostic: %s", record)
			}
			var timing struct {
				Completed time.Time `json:"completed"`
				Count     uint32    `json:"count"`
			}
			if err := json.Unmarshal(record, &timing); err != nil || timing.Completed.IsZero() || timing.Count != 1 {
				t.Fatalf("persisted timing: %s, %v", record, err)
			}
			deadline := timing.Completed.Add(750 * time.Millisecond)
			reopened, err := cache.New(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			f.Cache = reopened
			_, err = f.Fetch(t.Context(), b, (testInputs{}).readSet(b), cache.Policy{MaxAge: b.SourceInterval(), Retry: retry})
			terminal, ok = errors.AsType[*cache.TerminalError](err)
			cause, typed = errors.AsType[*fetcherr.ResponseError](err)
			if !ok || terminal.Attempted || !typed || cause.StatusCode != status || cause.Op != http.MethodGet || cause.Err != nil || cause.Message != "" || calls.Load() != 3 {
				t.Fatalf("replayed failure: %v, requests=%d", err, calls.Load())
			}
			if !time.Now().Before(deadline) {
				t.Fatal("replay did not finish before the recorded deadline")
			}
			unchanged, replayed := readRecord()
			if unchanged.Digest != initial.Digest || unchanged.Annotations[ocispec.AnnotationRefName] != initial.Annotations[ocispec.AnnotationRefName] || string(replayed) != string(record) {
				t.Fatalf("replay changed persisted record or deadline: %s -> %s", record, replayed)
			}
		})
	}
}

func TestFetcher_NetworkRoutes(t *testing.T) {

	for _, route := range []string{"list", "object"} {
		t.Run(route, func(t *testing.T) {

			var network source.Network
			var lists, objects, dials atomic.Int32
			var cause error
			var failing atomic.Bool
			testutil.StubDNS(t, func(context.Context, string) error {
				if failing.Load() {
					dials.Add(1)
					return os.ErrDeadlineExceeded
				}
				return nil
			})
			var target string
			latch := func() error {
				failing.Store(true)
				tr, err := network.HTTPTransport(source.HTTPTransportConfig{Insecure: true})
				if err != nil {
					return err
				}
				req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
				_, err = (&http.Client{Transport: tr}).Do(req)
				failing.Store(false)
				terminal, ok := errors.AsType[*cache.TerminalError](err)
				if !ok || !terminal.Attempted {
					return fmt.Errorf("seed: %w", err)
				}
				cause = terminal.Err
				return nil
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "" {
					objects.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				lists.Add(1)
				if err := latch(); err != nil {
					t.Error(err)
				}
				_, _ = io.WriteString(w, `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated><Contents><Key>app.yaml</Key><Size>1</Size><ETag>"a"</ETag></Contents></ListBucketResult>`)
			}))
			t.Cleanup(srv.Close)
			target = strings.Replace(srv.URL, "127.0.0.1", "example.com", 1)
			c := cachetest.New(t, "", cache.New)
			f := &bucket.Fetcher{Cache: c, Network: &network}
			if route == "list" {
				if err := latch(); err != nil {
					t.Fatal(err)
				}
			}
			_, err := f.Fetch(t.Context(), &manifest.Bucket{Name: "network", Namespace: "test", BucketName: "data", Endpoint: target, Insecure: true, Region: "us-east-1"}, (testInputs{}).readSet(&manifest.Bucket{Name: "network", Namespace: "test", BucketName: "data", Endpoint: target, Insecure: true, Region: "us-east-1"}), cache.Policy{Retry: cache.RetryConfig{Attempts: 3, MinWait: time.Hour}})
			terminal, ok := errors.AsType[*cache.TerminalError](err)
			wantLists := int32(0)
			if route == "object" {
				wantLists = 1
			}
			if !ok || terminal.Attempted || !errors.Is(err, cause) || dials.Load() == 0 || lists.Load() != wantLists || objects.Load() != 0 {
				t.Fatalf("error=%v terminal=%+v dials=%d lists=%d objects=%d", err, terminal, dials.Load(), lists.Load(), objects.Load())
			}
		})
	}
}
