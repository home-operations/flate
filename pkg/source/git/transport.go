package git

import (
	"context"
	"io"
	"net/url"
)

// Transports keys MUST be exact lowercase schemes. Missing keys use upstream transports.
// Route behavior MUST remain stable while reusing a cache.
type Transports map[string]Transport

// Transport callbacks MUST support concurrent calls and honor cancellation.
// Their resources MUST outlive the invocation, including lazy baseline reads.
// A custom connector owns connection security.
type Transport func(context.Context, TransportRequest) (Connection, error)

// TransportRequest callbacks MUST NOT mutate URL after returning.
type TransportRequest struct {
	URL      *url.URL
	Command  string
	Protocol int
}

// Connection requires Writer().Close() to preserve unread output. The operation owns the connection.
type Connection interface {
	io.Closer
	Reader() io.Reader
	Writer() io.WriteCloser
}
