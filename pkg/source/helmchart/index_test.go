package helmchart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	repo "helm.sh/helm/v4/pkg/repo/v1"
)

func TestFetchIndex_WithoutTemporaryStorage(t *testing.T) {
	for _, backing := range []string{"memory", "disk"} {
		for _, restored := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restored=%t", backing, restored), func(t *testing.T) {
				root := t.TempDir()
				cacheRoot := ""
				if backing == "disk" {
					cacheRoot = filepath.Join(root, "cache")
				}
				owner := cachetest.New(t, cacheRoot, sourcecache.New)
				cfg := Config{Cache: owner, Concurrency: 512}
				f := New(nil, cfg)
				r := &httpRepository{HelmRepository: httpRepo("https://charts.test/owner"), client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(helmRepoIndex("")))}, nil
				})}}
				if restored {
					if _, err := f.fetchIndex(t.Context(), r, sourcecache.Policy{}); err != nil {
						t.Fatal(err)
					}
					f = New(nil, cfg)
				}
				testutil.WriteFile(t, root, "blocked-tmp", "not a directory")
				t.Setenv("TMPDIR", filepath.Join(root, "blocked-tmp"))
				idx, err := f.fetchIndex(t.Context(), r, sourcecache.Policy{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := idx.Get("app-template", "1.0.0"); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestFetchIndex_Diagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		invalid    bool
	}{
		{"malformed", "apiVersion: [", true},
		{"partial", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n  - version: 2.0.0\n  - null\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var previous string
			for _, restored := range []bool{false, true} {
				t.Run(fmt.Sprintf("restored=%t", restored), func(t *testing.T) {
					for range 2 {
						owner := cachetest.New(t, t.TempDir(), sourcecache.New)
						r := &httpRepository{HelmRepository: httpRepo("https://charts.test/diagnostics"), client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
							return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(tc.data))}, nil
						})}}
						if restored {
							key := sourcecache.BlobKey{Content: struct{ Repository, Transport any }{r.Content(), r.material}}
							_, _, _, err := owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
								_, err := io.WriteString(w, tc.data)
								return sourcecache.ProducerResult{Digest: digest.FromString(tc.data), Size: int64(len(tc.data))}, err
							})
							if err != nil {
								t.Fatal(err)
							}
						}
						var output bytes.Buffer
						saved := slog.Default()
						slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
						idx, err := New(nil, Config{Cache: owner, Concurrency: 512}).fetchIndex(t.Context(), r, sourcecache.Policy{})
						slog.SetDefault(saved)
						if tc.invalid {
							if !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), "parse https://charts.test/diagnostics/index.yaml") || strings.Contains(err.Error(), "flate-index-") {
								t.Fatalf("parse diagnostic: %v", err)
							}
							if previous != "" && err.Error() != previous {
								t.Fatalf("unstable parse diagnostic: %q, previously %q", err, previous)
							}
							previous = err.Error()
						} else if err != nil || len(idx.Entries["chart"]) != 1 {
							t.Fatalf("partial index: %v, %v", idx, err)
						}
						if output.Len() != 0 {
							t.Fatalf("index loader emitted diagnostics: %s", output.String())
						}
					}
				})
			}
		})
	}
}

func TestLoadIndex_FluxSemantics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		err        error
	}{
		{"entries", `apiVersion: v1
entries:
  chart:
    - name: chart
      version: 1.0.0
      urls: [chart.tgz]
    - version: 2.0.0
    - name: chart
      version: invalid
    - null
    - name: chart
      version: 3.0.0
      dependencies:
        - name: dependency
          version: 1.0.0
        - name: dependency
          version: 1.0.0
`, nil},
		{"json", `{"apiVersion":"v1","entries":{"chart":[{"name":"chart","version":"1.0.0"},null]}}`, nil},
		{"empty", "", repo.ErrEmptyIndexYaml},
		{"no api version", "entries: {}\n", repo.ErrNoAPIVersion},
		{"ordinary metadata", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    apiVersion: v2\n    appVersion: app-v1\n    version: 1.0.0\n    urls: [chart.tgz]\n", nil},
		{"scalar coercion", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    digest: 1234\n    urls: [yes]\n", nil},
		{"case insensitive fields", "APIVERSION: v1\nentries: {}\n", nil},
		{"unknown field", "apiVersion: v1\nunknown: true\nentries: {}\n", nil},
		{"duplicate fields", "apiVersion: v1\napiVersion: v2\nentries: {}\n", nil},
		{"empty and null entries", "apiVersion: v1\nentries:\n  chart: [null, {}]\n", nil},
		{"numeric annotation", "apiVersion: v1\nentries:\n  chart:\n  - name: chart\n    version: 1.0.0\n    annotations: {number: 1234}\n", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "index.yaml", tc.data)
			want, wantErr := repo.LoadIndexFile(filepath.Join(root, "index.yaml"))
			f := New(nil, Config{Concurrency: 256})
			got, _, gotErr := f.loadIndex(t.Context(), "https://charts.test/index.yaml", func(w io.Writer) (ocispec.Descriptor, error) {
				_, err := io.WriteString(w, tc.data)
				return ocispec.Descriptor{Digest: digest.FromString(tc.data), Size: int64(len(tc.data))}, err
			})
			if (wantErr == nil) != (gotErr == nil) || tc.err != nil && (!errors.Is(wantErr, tc.err) || !errors.Is(gotErr, tc.err)) {
				t.Fatalf("upstream=%v FS=%v, want %v", wantErr, gotErr, tc.err)
			}
			if wantErr == nil {
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("index (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestFetchIndex_DecodeBudget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		restored, canceled bool
	}{
		{"download", false, false},
		{"restored", true, false},
		{"canceled download", false, true},
		{"canceled restored", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				owner := cachetest.New(t, "", sourcecache.New)
				cfg := Config{Cache: owner, Concurrency: 256}
				f := New(nil, cfg)
				data := map[string]string{
					"/one/index.yaml": helmRepoIndex(""),
					"/two/index.yaml": strings.ReplaceAll(helmRepoIndex(""), "1.0.0", "2.0.0"),
				}
				var calls atomic.Int32
				transfer := make(chan struct{})
				client := &http.Client{Transport: chartTransport(func(req *http.Request) (*http.Response, error) {
					body, index := data[req.URL.Path]
					if index {
						calls.Add(1)
						if !tc.restored {
							select {
							case <-transfer:
							case <-req.Context().Done():
								return nil, req.Context().Err()
							}
						}
					} else {
						body = "chart"
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}
				repositories := []*httpRepository{
					{HelmRepository: httpRepo("https://charts.test/one"), client: client},
					{HelmRepository: httpRepo("https://charts.test/two"), client: client},
				}
				if tc.restored {
					for _, r := range repositories {
						if _, err := f.fetchIndex(t.Context(), r, sourcecache.Policy{}); err != nil {
							t.Fatal(err)
						}
					}
					f = New(nil, cfg)
				}
				calls.Store(0)
				if cap(f.indexDecodes) != 2 {
					t.Fatalf("decode budget = %d, want 2 from 256 task slots", cap(f.indexDecodes))
				}
				for range cap(f.indexDecodes) {
					f.indexDecodes <- struct{}{}
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, len(repositories))
				for i, r := range repositories {
					go func() {
						_, err := f.fetchHTTPChart(ctx, r, helmChart(r.Name, "app-template", fmt.Sprintf("%d.0.0", i+1)), sourcecache.Policy{})
						done <- err
					}()
				}
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("index decoded while all permits were borrowed: %v", err)
				default:
				}
				if calls.Load() != 0 {
					t.Fatal("waiting indexes acquired raw responses outside the decode budget")
				}
				if tc.canceled {
					cancel()
					synctest.Wait()
					for range repositories {
						if err := <-done; !errors.Is(err, context.Canceled) {
							t.Fatalf("blocked decode cancellation: %v", err)
						}
					}
					for _, body := range data {
						if _, ok := f.indexCache.Load("sha256:" + chartDigest([]byte(body))); ok {
							t.Fatal("canceled decode published an index")
						}
					}
				}
				for range cap(f.indexDecodes) {
					<-f.indexDecodes
				}
				if tc.canceled {
					close(transfer)
					for i, r := range repositories {
						if _, err := f.fetchHTTPChart(t.Context(), r, helmChart(r.Name, "app-template", fmt.Sprintf("%d.0.0", i+1)), sourcecache.Policy{}); err != nil {
							t.Fatal(err)
						}
					}
				} else {
					synctest.Wait()
					if !tc.restored && (calls.Load() != 2 || len(f.indexDecodes) != 2) {
						t.Errorf("two permits did not admit two indexes: calls=%d permits=%d", calls.Load(), len(f.indexDecodes))
					}
					close(transfer)
					for range repositories {
						if err := <-done; err != nil {
							t.Fatal(err)
						}
					}
				}
				if len(f.indexDecodes) != 0 {
					t.Fatal("decode did not return its borrowed permit")
				}
			})
		})
	}
}
