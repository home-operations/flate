package helmchart

import (
	"net/http"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func preparedHTTP(t *testing.T, f *Fetcher, fInputs testInputs, r *manifest.HelmRepository) *httpRepository {
	t.Helper()
	prepared, err := (&operation{Fetcher: f, secrets: fInputs.lookup(r)}).prepareHTTP(r)
	assert.NoError(t, err)

	return prepared
}

type chartTransport func(*http.Request) (*http.Response, error)

func (f chartTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
