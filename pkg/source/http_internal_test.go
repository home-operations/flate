package source

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDownloadHTTP_ResponseRules(t *testing.T) {
	t.Parallel()
	for _, policy := range []HTTPPolicy{HTTPInputPolicy, HTTPHelmPolicy} {
		for _, status := range []int{200, 201, 204, 206, 299, 300, 401, 500, 600} {
			t.Run(fmt.Sprintf("%d/%d", policy, status), func(t *testing.T) {
				t.Parallel()
				const target = "https://example.invalid/content"
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, http.NoBody)
				assert.NoError(t, err)
				closes := 0
				client := &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: status,
						Header:     http.Header{"Content-Type": {"application/yaml"}, "Etag": {"one"}},
						Body: remoteBody{Reader: strings.NewReader("content"), close: func() error {
							closes++
							return nil
						}},
					}, nil
				})}
				var body bytes.Buffer
				result, err := DownloadHTTP(req, HTTPDownload{Client: client, Writer: &body, Policy: policy})
				assert.Equal(t, closes, 1)
				if status == http.StatusOK || policy == HTTPInputPolicy && status >= 200 && status < 300 {
					assert.NoError(t, err)
					assert.Equal(t, body.String(), "content")
					assert.Equal(t, result.Size, int64(len("content")))
					assert.Equal(t, result.MediaType, "application/yaml")
					assert.Equal(t, result.Annotations["flate.http-etag"], "one")
					assert.Equal(t, result.Annotations["flate.http-original-url"], "true")
					assert.Equal(t, result.Digest, digest.Digest(""))
					assert.Equal(t, result.Unchanged, false)
					return
				}
				response, ok := errors.AsType[*fetcherr.ResponseError](err)
				assert.FatalIf(t, !ok, "response error: %v", err)
				assert.Equal(t, response.StatusCode, status)
				assert.Equal(t, response.Op, http.MethodGet)
				assert.Equal(
					t,
					err.Error(),
					fmt.Sprintf("failed to fetch %s : %d %s", target, status, http.StatusText(status)),
				)
				assert.Equal(t, body.Len(), 0)
				assert.Equal(t, result.Unchanged, false)
			})
		}
	}
}

func TestDownloadHTTP_RedactsURLPassword(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		status int
		want   string
	}{
		{
			name:   "rejected status",
			status: http.StatusNotFound,
			want:   "failed to fetch https://user:xxxxx@example.invalid/content : 404 Not Found",
		},
		{
			name:   "copy failure",
			status: http.StatusOK,
			want:   `Get "https://user:xxxxx@example.invalid/content": body read`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("body read")
			client := &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: tt.status,
					Body: io.NopCloser(remoteRead(func([]byte) (int, error) {
						return 0, cause
					})),
				}, nil
			})}
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				"https://user:s3cret@example.invalid/content",
				http.NoBody,
			)
			assert.NoError(t, err)
			_, err = DownloadHTTP(req, HTTPDownload{Client: client, Writer: io.Discard, Policy: HTTPInputPolicy})
			assert.FatalIf(t, err == nil, "expected download failure")
			assert.Equal(t, strings.Contains(err.Error(), "s3cret"), false)
			assert.Equal(t, err.Error(), tt.want)
			if tt.status == http.StatusOK {
				copyErr, ok := errors.AsType[*url.Error](err)
				assert.FatalIf(t, !ok, "copy error: %v", err)
				assert.Equal(t, copyErr.Op, "Get")
				assert.Equal(t, copyErr.URL, "https://user:xxxxx@example.invalid/content")
				assert.FatalIf(t, !errors.Is(err, cause), "copy cause: %v", err)
				return
			}
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			assert.FatalIf(t, !ok, "response error: %v", err)
			assert.Equal(t, response.StatusCode, tt.status)
			assert.Equal(t, response.Op, http.MethodGet)
		})
	}
}

func TestDownloadHTTP_ConditionalMetadata(t *testing.T) {
	t.Parallel()
	for _, policy := range []HTTPPolicy{HTTPInputPolicy, HTTPHelmPolicy} {
		for _, scenario := range []string{"valid", "missing", "unvalidated", "stripped"} {
			t.Run(strconv.Itoa(int(policy))+"/"+scenario, func(t *testing.T) {
				t.Parallel()
				previous := &ocispec.Descriptor{
					Digest:    digest.FromString("original"),
					Size:      8,
					MediaType: "application/yaml",
					Annotations: map[string]string{
						"flate.http-original-url": "true",
						"flate.http-etag":         "one",
						"retained":                "value",
					},
				}
				want := *previous
				want.Annotations = maps.Clone(previous.Annotations)
				want.Annotations["flate.http-etag"] = "two"
				switch scenario {
				case "missing":
					previous = nil
				case "unvalidated":
					delete(previous.Annotations, "flate.http-etag")
				}
				req, err := http.NewRequestWithContext(
					t.Context(),
					http.MethodGet,
					"https://example.invalid/content",
					http.NoBody,
				)
				assert.NoError(t, err)
				client := &http.Client{Transport: remoteRoundTrip(func(final *http.Request) (*http.Response, error) {
					if scenario == "stripped" {
						assert.Equal(t, final.Header.Get("If-None-Match"), "one")
						final = final.Clone(final.Context())
						final.Header.Del("If-None-Match")
					}
					return &http.Response{
						Request: final, StatusCode: http.StatusNotModified,
						Header: http.Header{"Etag": {"two"}, "Content-Type": {"ignored/type"}},
						Body:   io.NopCloser(strings.NewReader("must not be copied")),
					}, nil
				})}
				var body bytes.Buffer
				result, err := DownloadHTTP(
					req,
					HTTPDownload{Client: client, Writer: &body, Previous: previous, Policy: policy},
				)
				assert.Equal(t, body.Len(), 0)
				if scenario == "valid" {
					assert.NoError(t, err)
					assert.Equal(t, result.Unchanged, true)
					assert.FatalIf(
						t,
						cmp.Diff(want, result.Descriptor) != "",
						"descriptor (-want +got):\n%s",
						cmp.Diff(want, result.Descriptor),
					)
					return
				}
				response, ok := errors.AsType[*fetcherr.ResponseError](err)
				assert.FatalIf(t, !ok, "response error: %v", err)
				assert.Equal(t, response.StatusCode, http.StatusNotModified)
				assert.Equal(t, response.Op, http.MethodGet)
				assert.Equal(t, fetcherr.Classes(err), fetcherr.Terminal)
				cause, ok := errors.AsType[*fetcherr.ResponseError](response.Err)
				assert.FatalIf(t, !ok || cause.StatusCode != http.StatusNotModified, "metadata cause: %v", response.Err)
				assert.Equal(t, result.Unchanged, false)
			})
		}
	}
}

func TestDownloadHTTP_CleanupAccounting(t *testing.T) {
	t.Parallel()
	for _, policy := range []HTTPPolicy{HTTPInputPolicy, HTTPHelmPolicy} {
		t.Run(strconv.Itoa(int(policy)), func(t *testing.T) {
			t.Parallel()
			closeErr := errors.New("body close")
			recorder := manifest.WorkRecorder{
				Key: manifest.WorkKey{Tree: manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}},
			}
			work := &manifest.AcquisitionWork{Scope: recorder.Scope()}
			validation := work.Validate()
			previous := &ocispec.Descriptor{
				Annotations: map[string]string{"flate.http-original-url": "true", "flate.http-etag": "one"},
			}
			closes := 0
			client := &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotModified,
					Body: remoteBody{Reader: strings.NewReader(""), close: func() error {
						closes++
						assert.Equal(t, validation.Counts, manifest.WorkCounts{Attempts: 1})
						return closeErr
					}},
				}, nil
			})}
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				"https://example.invalid/content",
				http.NoBody,
			)
			assert.NoError(t, err)
			_, err = DownloadHTTP(
				req,
				HTTPDownload{Client: client, Writer: io.Discard, Previous: previous, Work: work, Policy: policy},
			)
			assert.Equal(t, closes, 1)
			want := manifest.WorkCounts{Attempts: 1, Completed: 1}
			if policy == HTTPInputPolicy {
				assert.FatalIf(t, !errors.Is(err, closeErr), "cleanup error: %v", err)
				want = manifest.WorkCounts{Attempts: 1, Failures: 1}
			} else {
				assert.NoError(t, err)
			}
			records := recorder.Scope().Snapshot(0)
			assert.FatalIf(t, len(records) != 1, "validation records: %+v", records)
			assert.Equal(t, records[0].Key.Phase, manifest.WorkValidation)
			assert.Equal(t, records[0].Counts, want)
		})
	}
}

func TestDownloadHTTP_PanicClosesBody(t *testing.T) {
	t.Parallel()
	for _, policy := range []HTTPPolicy{HTTPInputPolicy, HTTPHelmPolicy} {
		t.Run(strconv.Itoa(int(policy)), func(t *testing.T) {
			t.Parallel()
			cause := errors.New("copy panic")
			closes := 0
			client := &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: remoteBody{
					Reader: remoteRead(func([]byte) (int, error) { panic(cause) }),
					close:  func() error { closes++; return nil },
				}}, nil
			})}
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				"https://example.invalid/content",
				http.NoBody,
			)
			assert.NoError(t, err)
			defer func() {
				assert.Equal(t, recover(), any(cause))
				assert.Equal(t, closes, 1)
			}()
			_, _ = DownloadHTTP(req, HTTPDownload{Client: client, Writer: io.Discard, Policy: policy})
			t.Fatal("copy did not panic")
		})
	}
}
