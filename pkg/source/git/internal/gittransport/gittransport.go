// Package gittransport installs go-git's bounded HTTPS client at initialization.
// Its concrete *http.Transport preserves the header timeout and SSRF dial hook
// when go-git clones it for per-operation TLS and proxy settings.
package gittransport

import (
	"net/http"

	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/pkg/source"
)

// boundedHTTPSTransport is the ResponseHeaderTimeout-bounded transport behind
// the go-git default HTTPS client; kept as a package var for test introspection.
var boundedHTTPSTransport = func() *http.Transport {
	tr, _ := source.NewHTTPTransport(nil, nil) // nil/nil never errors
	return tr
}()

var boundedHTTPSClient = githttp.NewClient(&http.Client{Transport: boundedHTTPSTransport})

func init() {
	client.InstallProtocol("https", boundedHTTPSClient)
}
