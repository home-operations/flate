// Package gittransport installs go-git's bounded HTTPS client at initialization.
// Its concrete *[http.Transport] preserves the SSRF dial hook
// when go-git clones it for per-operation TLS and proxy settings.
package gittransport

import (
	"net/http"

	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/internal/source"
)

//nolint:gochecknoglobals // The installed go-git protocol client shares this concrete guarded transport.
var boundedHTTPSTransport = source.NewHTTPTransport(source.TransportMaterial{})

//nolint:gochecknoglobals // go-git retains one registered HTTPS client for the process.
var boundedHTTPSClient = githttp.NewClient(&http.Client{Transport: boundedHTTPSTransport})

//nolint:gochecknoinits // Importing this package must install the guarded HTTPS protocol before any go-git operation.
func init() {
	client.InstallProtocol("https", boundedHTTPSClient)
}
