// Package gittransport installs go-git's bounded HTTPS client at initialization.
// Its concrete *http.Transport preserves the header timeout and SSRF dial hook
// when go-git clones it for per-operation TLS and proxy settings.
package gittransport

import (
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/pkg/source"
)

var boundedHTTPSTransport = source.NewHTTPTransport(source.TransportMaterial{})

var boundedHTTPSClient = githttp.NewClient(source.NewHTTPClient(boundedHTTPSTransport, source.HTTPStream))

func init() {
	client.InstallProtocol("https", boundedHTTPSClient)
}
