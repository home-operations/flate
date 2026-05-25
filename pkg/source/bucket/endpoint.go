package bucket

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// normalizeEndpoint splits a Flux-style endpoint into the
// host[:port] form minio-go expects and a tls flag.
func normalizeEndpoint(endpoint string, insecure bool) (host string, secure bool, err error) {
	switch {
	case strings.HasPrefix(endpoint, "https://"):
		host = strings.TrimPrefix(endpoint, "https://")
		secure = true
	case strings.HasPrefix(endpoint, "http://"):
		host = strings.TrimPrefix(endpoint, "http://")
		secure = false
	default:
		host = endpoint
		secure = !insecure
	}
	host = strings.TrimRight(host, "/")
	if host == "" {
		return "", false, errors.New("bucket endpoint is empty")
	}
	if _, perr := url.Parse(schemeFor(secure) + "://" + host); perr != nil {
		return "", false, fmt.Errorf("parse Bucket endpoint %q: %w", endpoint, perr)
	}
	return host, secure, nil
}

func schemeFor(secure bool) string {
	if secure {
		return "https"
	}
	return "http"
}
