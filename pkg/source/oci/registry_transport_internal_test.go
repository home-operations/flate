package oci

import (
	"context"
	"net/http"

	"github.com/home-operations/flate/pkg/manifest"
)

type registryRoundTripper func(*http.Request) (*http.Response, error)

func (rt registryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt(req)
}

func (f *operation) resolvedRepository(
	ctx context.Context,
	access *manifest.RegistryAccess,
) (registryAcquisition, error) {
	request, err := f.newRepoClient(ctx, access)
	if err == nil {
		err = f.configure(access, &request, request.credential)
	}
	return request, err
}
