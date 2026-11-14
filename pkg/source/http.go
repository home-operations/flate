package source

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/httpconditional"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

// HTTPPolicy preserves each upstream caller's status and cleanup contract.
type HTTPPolicy uint8

// HTTPInputPolicy follows Kustomize's 2xx contract; HTTPHelmPolicy follows Helm's 200-only contract.
const (
	HTTPInputPolicy HTTPPolicy = iota
	HTTPHelmPolicy
)

// HTTPDownload borrows its client, writer and work counters for the request lifetime.
type HTTPDownload struct {
	Client   *http.Client
	Writer   io.Writer
	Previous *ocispec.Descriptor
	Work     *manifest.AcquisitionWork
	Policy   HTTPPolicy
}

// DownloadHTTP leaves publication and content digests with the cache and caller.
//
//nolint:nonamedreturns // Cleanup must join the returned error before validation finishes, including during panic unwinding.
func DownloadHTTP(req *http.Request, download HTTPDownload) (result cache.ProducerResult, resultErr error) {
	if httpconditional.Apply(req, download.Previous) {
		work := download.Work.Validate()
		work.Add(&manifest.WorkCounts{Attempts: 1})
		defer func() { work.Finish(resultErr) }()
	}
	//nolint:gosec // The supplied transport owns egress restrictions for caller-selected source URLs.
	response, err := download.Client.Do(req)
	if err != nil {
		return result, err //nolint:wrapcheck // The HTTP client already identifies the method, target and cancellation cause.
	}
	defer func() {
		closeErr := response.Body.Close()
		if download.Policy == HTTPInputPolicy {
			resultErr = errors.Join(resultErr, closeErr)
		}
	}()
	inputSuccess := download.Policy == HTTPInputPolicy &&
		response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
	validStatus := response.StatusCode == http.StatusOK || response.StatusCode == http.StatusNotModified || inputSuccess
	if validStatus {
		result.Descriptor, result.Unchanged, err = httpconditional.Metadata(req, response, download.Previous)
	}
	if !validStatus || err != nil {
		return result, &fetcherr.ResponseError{
			StatusCode: response.StatusCode,
			Op:         http.MethodGet,
			Err:        err,
			Message: fmt.Sprintf(
				"failed to fetch %s : %d %s",
				req.URL.Redacted(),
				response.StatusCode,
				http.StatusText(response.StatusCode),
			),
		}
	}
	if result.Unchanged {
		return result, nil
	}
	download.Work.Physical().Add(&manifest.WorkCounts{RemoteTransfers: 1})
	result.Size, err = io.Copy(download.Writer, response.Body)
	result.MediaType = response.Header.Get("Content-Type")
	if err != nil {
		err = &url.Error{Op: "Get", URL: req.URL.Redacted(), Err: err}
	}
	return result, err
}
