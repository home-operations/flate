// Package httpconditional keeps validators scoped to the original request URL.
package httpconditional

import (
	"errors"
	"net/http"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/fetcherr"
)

const (
	originalURLAnnotation  = "flate.http-original-url"
	etagAnnotation         = "flate.http-etag"
	lastModifiedAnnotation = "flate.http-last-modified"
)

// Apply only sends validators admitted for the original request URL.
func Apply(req *http.Request, previous *ocispec.Descriptor) bool {
	if previous != nil && previous.Annotations[originalURLAnnotation] == "true" {
		if etag := previous.Annotations[etagAnnotation]; etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		if modified := previous.Annotations[lastModifiedAnnotation]; modified != "" {
			req.Header.Set("If-Modified-Since", modified)
		}
	}
	return req.Header.Get("If-None-Match") != "" || req.Header.Get("If-Modified-Since") != ""
}

// Metadata requires usable prior content and a conditional final request for HTTP 304.
// The shared source executor MUST enforce its success-status policy for body responses.
func Metadata(req *http.Request, resp *http.Response, previous *ocispec.Descriptor) (ocispec.Descriptor, bool, error) {
	var desc ocispec.Descriptor
	unchanged := resp.StatusCode == http.StatusNotModified
	if unchanged {
		if previous == nil ||
			resp.Request.Header.Get("If-None-Match") == "" && resp.Request.Header.Get("If-Modified-Since") == "" {
			return desc, false, &fetcherr.ResponseError{StatusCode: resp.StatusCode, Op: http.MethodGet}
		}
		desc = *previous
	} else {
		desc.Annotations = make(map[string]string)
		requestURL, responseURL := *req.URL, *resp.Request.URL
		if requestURL.User != nil && responseURL.User != nil && *requestURL.User == *responseURL.User {
			responseURL.User = requestURL.User
		}
		if requestURL == responseURL {
			desc.Annotations[originalURLAnnotation] = "true"
		}
	}

	for _, validator := range [...]struct{ header, annotation string }{{"ETag", etagAnnotation}, {"Last-Modified", lastModifiedAnnotation}} {
		if _, supplied := resp.Header[http.CanonicalHeaderKey(validator.header)]; supplied {
			desc.Annotations[validator.annotation] = resp.Header.Get(validator.header)
		}
	}
	return desc, unchanged, nil
}

// Redirect MUST remove validators before a new URL receives the request.
func Redirect(req *http.Request, via []*http.Request) error {
	const maxRedirects = 10
	if len(via) >= maxRedirects {
		return errors.New("stopped after 10 redirects")
	}
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Modified-Since")
	return nil
}
