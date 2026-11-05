package helmchart

import (
	"net/http"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func preparedHTTP(t *testing.T, f *Fetcher, fInputs testInputs, r *manifest.HelmRepository) *httpRepository {
	t.Helper()
	prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).resolvedHTTP(r)
	assert.NoError(t, err)

	return prepared
}

type chartTransport func(*http.Request) (*http.Response, error)

func (f chartTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (f *operation) resolvedHTTP(r *manifest.HelmRepository) (*httpRepository, error) {
	prepared, err := f.prepareHTTP(r)
	if prepared != nil {
		if connectErr := f.connectHTTP(prepared); err == nil {
			err = connectErr
		}
	}
	return prepared, err
}
