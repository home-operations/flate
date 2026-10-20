package source

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/idna"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

// Network owns compatible HTTP transports and first connection timeouts for one invocation.
// Its zero value is usable; independent invocations MUST NOT share it through a cache owner.
type Network struct {
	endpoints   atomic.Pointer[networkEndpoints]
	transports  atomic.Pointer[networkTransports]
	defaultOnce sync.Once
	defaultHTTP http.RoundTripper
}

// RoundTrip sends a request through the lazily selected default transport.
func (n *Network) RoundTrip(r *http.Request) (*http.Response, error) {
	n.defaultOnce.Do(func() {
		n.defaultHTTP, _ = n.HTTPTransport(HTTPTransportConfig{})
	})
	return n.defaultHTTP.RoundTrip(r)
}

type networkTransports map[string]*networkTransport

type networkEndpoint struct {
	authority  string
	proxy      string
	credential string
}

type networkEndpoints map[networkEndpoint]*atomic.Pointer[error]

type networkRequest struct {
	proxy    *url.URL
	endpoint networkEndpoint
	done     <-chan struct{}
	deadline time.Time
}

type networkRequestKey struct{}

type networkTransport struct {
	auth.Cache
	network   *Network
	transport *http.Transport
	proxy     func(*http.Request) (*url.URL, error)
}

func (n *Network) wrapHTTPTransport(tr *http.Transport) http.RoundTripper {
	t := &networkTransport{Cache: auth.NewCache(), network: n, transport: tr, proxy: tr.Proxy}
	tr.Proxy = func(r *http.Request) (*url.URL, error) {
		return r.Context().Value(networkRequestKey{}).(*networkRequest).proxy, nil
	}
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		request := ctx.Value(networkRequestKey{}).(*networkRequest)
		key := request.endpoint
		key.authority = networkAuthority(address)
		cause := n.endpoint(key)
		if err := cause.Load(); err != nil {
			return nil, &cache.TerminalError{Err: *err}
		}
		conn, err := dial(ctx, network, address)
		if fetcherr.DialTimeout(err) &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && request.live() {
			cause.CompareAndSwap(nil, &err)
			return conn, &cache.TerminalError{Err: err, Attempted: true}
		}
		return conn, err
	}
	return t
}

func (t *networkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	request := &networkRequest{done: r.Context().Done()}
	request.deadline, _ = r.Context().Deadline()
	if t.proxy != nil {
		proxy, err := t.proxy(r)
		if err != nil {
			return nil, err
		}
		if proxy != nil {
			selected := *proxy
			request.proxy = &selected
		}
	}
	endpoint := r.URL
	if request.proxy != nil {
		endpoint = request.proxy
		request.endpoint.proxy = "proxy:" + strings.ToLower(endpoint.Scheme)
		if endpoint.User != nil {
			request.endpoint.credential = manifest.Fingerprint(endpoint.User.String())
		}
	}
	port := endpoint.Port()
	if port == "" {
		switch strings.ToLower(endpoint.Scheme) {
		case "http":
			port = "80"
		case "https":
			port = "443"
		case "socks5", "socks5h":
			port = "1080"
		}
	}
	request.endpoint.authority = networkAuthority(net.JoinHostPort(endpoint.Hostname(), port))
	if err := t.network.endpoint(request.endpoint).Load(); err != nil {
		return nil, &cache.TerminalError{Err: *err}
	}
	return t.transport.RoundTrip(r.WithContext(context.WithValue(r.Context(), networkRequestKey{}, request)))
}

func (n *Network) endpoint(key networkEndpoint) *atomic.Pointer[error] {
	for {
		current := n.endpoints.Load()
		var next networkEndpoints
		if current != nil {
			if cause := (*current)[key]; cause != nil {
				return cause
			}
			next = maps.Clone(*current)
		} else {
			next = make(networkEndpoints)
		}
		cause := new(atomic.Pointer[error])
		next[key] = cause
		if n.endpoints.CompareAndSwap(current, &next) {
			return cause
		}
	}
}

func (r *networkRequest) live() bool {
	select {
	case <-r.done:
		return false
	default:
		return r.deadline.IsZero() || time.Now().Before(r.deadline)
	}
}

func networkAuthority(address string) string {
	host, port, _ := net.SplitHostPort(address)
	if value, err := strconv.ParseUint(port, 10, 16); err == nil {
		port = strconv.FormatUint(value, 10)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		host = ip.String()
	} else {
		host = strings.ToLower(host)
		if ascii, err := idna.Lookup.ToASCII(host); err == nil {
			host = ascii
		}
	}
	return net.JoinHostPort(host, port)
}

// Close releases idle HTTP connections after all users have joined.
// Borrowers MUST leave cleanup to the invocation owner.
func (n *Network) Close() {
	if n == nil {
		return
	}
	if transports := n.transports.Load(); transports != nil {
		for _, tr := range *transports {
			tr.transport.CloseIdleConnections()
		}
	}
}

// CloseIdleConnections releases this transport's idle connections.
func (t *networkTransport) CloseIdleConnections() { t.transport.CloseIdleConnections() }
