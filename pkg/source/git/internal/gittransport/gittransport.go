// Package gittransport installs go-git's bounded HTTPS client at initialization.
// Its concrete *http.Transport preserves the SSRF dial hook
// when go-git clones it for per-operation TLS and proxy settings.
package gittransport

import (
	"net/http"

	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/pkg/source"
)

var boundedHTTPSTransport = source.NewHTTPTransport(source.TransportMaterial{})

var boundedHTTPSClient = githttp.NewClient(&http.Client{Transport: boundedHTTPSTransport})

func init() {
	client.InstallProtocol("https", boundedHTTPSClient)
}
