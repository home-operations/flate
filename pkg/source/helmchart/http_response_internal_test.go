package helmchart

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
)

type chartResponseBody struct {
	io.Reader

	close func() error
}

func (b chartResponseBody) Close() error { return b.close() }

func TestHTTPRepositoryGet_BodyCloseErrors(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"success", "read", "write"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			copyErr, closeErr := errors.New("body copy"), errors.New("body close")
			closes := 0
			r := &httpRepository{
				HelmRepository: httpRepo("https://charts.invalid"),
				client: &http.Client{Transport: chartTransport(func(*http.Request) (*http.Response, error) {
					var reader io.Reader = strings.NewReader("content")
					if outcome == "read" {
						reader = io.MultiReader(reader, iotest.ErrReader(copyErr))
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": {"application/gzip"}},
						Body: chartResponseBody{Reader: reader, close: func() error {
							closes++
							return closeErr
						}},
					}, nil
				})},
			}
			writer := io.Discard
			if outcome == "write" {
				reader, pipe := io.Pipe()
				assert.NoError(t, reader.CloseWithError(copyErr))
				defer pipe.Close()
				writer = pipe
			}
			const target = "https://charts.invalid/chart.tgz"
			result, err := r.get(t.Context(), target, httpDownload{Writer: writer})
			assert.Equal(t, closes, 1)
			assert.FatalIf(t, errors.Is(err, closeErr), "cleanup replaced primary outcome: %v", err)
			if outcome == "success" {
				assert.NoError(t, err)
				assert.Equal(t, result.Size, int64(len("content")))
				assert.Equal(t, result.Digest, digest.FromString("content"))
				assert.Equal(t, result.MediaType, "application/gzip")
				return
			}
			assert.FatalIf(t, !errors.Is(err, copyErr), "copy cause: %v", err)
			operation, ok := errors.AsType[*url.Error](err)
			assert.FatalIf(t, !ok || operation.Op != "Get" || operation.URL != target, "copy operation: %v", err)
		})
	}
}

func TestHTTPRepositoryGet_ConditionalMetadata(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"valid", "stripped"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			previous := &ocispec.Descriptor{
				Digest:    digest.FromString("content"),
				Size:      7,
				MediaType: "application/gzip",
				Annotations: map[string]string{
					"flate.http-original-url": "true",
					"flate.http-etag":         "one",
					"flate.chart-url":         "retained",
				},
			}
			r := &httpRepository{
				HelmRepository: httpRepo("https://charts.invalid"),
				client: &http.Client{Transport: chartTransport(func(final *http.Request) (*http.Response, error) {
					assert.Equal(t, final.Header.Get("If-None-Match"), "one")
					if scenario == "stripped" {
						final = final.Clone(final.Context())
						final.Header.Del("If-None-Match")
					}
					return &http.Response{
						Request: final, StatusCode: http.StatusNotModified,
						Header: http.Header{"Content-Type": {"ignored/type"}},
						Body:   io.NopCloser(strings.NewReader("must not be copied")),
					}, nil
				})},
			}
			var body bytes.Buffer
			result, err := r.get(
				t.Context(),
				"https://charts.invalid/chart.tgz",
				httpDownload{Writer: &body, previous: previous},
			)
			assert.Equal(t, body.Len(), 0)
			if scenario == "valid" {
				assert.NoError(t, err)
				assert.Equal(t, result.Unchanged, true)
				assert.FatalIf(
					t,
					cmp.Diff(*previous, result.Descriptor) != "",
					"descriptor (-want +got):\n%s",
					cmp.Diff(*previous, result.Descriptor),
				)
				return
			}
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			assert.FatalIf(
				t,
				!ok || response.StatusCode != http.StatusNotModified || response.Op != http.MethodGet,
				"response facts: %v",
				err,
			)
			assert.Equal(t, fetcherr.Classes(err), fetcherr.Terminal)
			assert.Equal(t, result.Unchanged, false)
			cause, ok := errors.AsType[*fetcherr.ResponseError](response.Err)
			assert.FatalIf(t, !ok || cause.StatusCode != http.StatusNotModified, "metadata cause: %v", response.Err)
		})
	}
}
