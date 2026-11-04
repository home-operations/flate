package oci

import (
	"net/http"
)

type registryRoundTripper func(*http.Request) (*http.Response, error)

func (rt registryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt(req)
}
