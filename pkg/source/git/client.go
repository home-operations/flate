package git

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/memory"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// ClientConfig requires a Network that outlives every operation.
type ClientConfig struct {
	Transports Transports
	// Network MUST outlive every operation that uses this client.
	Network  *source.Network
	Material source.TransportMaterial
}

// Client retains invocation policy for deferred baseline operations.
type Client struct {
	options []client.Option
	network *source.Network
}

// NewClient requires a caller-owned network, including for local transport overrides.
//
//nolint:gocritic // The SDK configuration MUST remain a value snapshot.
func NewClient(
	cfg ClientConfig,
) (*Client, error) {
	return newClient(&cfg, nil)
}

func newClient(cfg *ClientConfig, auth any) (*Client, error) {
	routes := maps.Clone(cfg.Transports)
	keys := slices.Sorted(maps.Keys(routes))
	for _, scheme := range keys {
		if scheme == "" || scheme != strings.ToLower(scheme) || routes[scheme] == nil {
			return nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf("invalid Git transport for scheme %q", scheme),
			)
		}
	}
	if cfg.Network == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "Git invocation network is required")
	}
	c := &Client{network: cfg.Network}
	httpTransport, err := c.network.HTTPTransport(source.HTTPTransportConfig{Material: cfg.Material})
	if err != nil {
		return nil, err
	}
	c.options = []client.Option{client.WithHTTPClient(&http.Client{Transport: httpTransport})}
	for _, scheme := range keys {
		c.options = append(c.options, client.WithTransport(scheme, streamTransport{routes[scheme]}))
	}
	switch credentials := auth.(type) {
	case client.HTTPAuth:
		c.options = append(c.options, client.WithHTTPAuth(credentials))
	case client.SSHAuth:
		c.options = append(c.options, client.WithSSHAuth(credentials))
	}
	return c, nil
}

// List preserves upstream error identity for baseline classification.
func (c *Client) List(ctx context.Context, remoteURL string) ([]*plumbing.Reference, error) {
	remote := git.NewRemote(
		memory.NewStorage(),
		&config.RemoteConfig{Name: git.DefaultRemoteName, URLs: []string{remoteURL}},
	)
	return remote.ListContext( //nolint:wrapcheck // Baseline classification requires upstream error identity.
		ctx,
		&git.ListOptions{ClientOptions: c.options},
	)
}

// Fetch preserves the caller's fetch policy and upstream error identity.
func (c *Client) Fetch(ctx context.Context, st storage.Storer, remoteURL string, options *git.FetchOptions) error {
	opts := *options
	opts.ClientOptions = c.options
	remote := git.NewRemote(st, &config.RemoteConfig{Name: git.DefaultRemoteName, URLs: []string{remoteURL}})
	return remote.FetchContext(ctx, &opts) //nolint:wrapcheck // Upstream errors retain their protocol identity.
}

type streamTransport struct{ connect Transport }

//nolint:ireturn // The upstream transport contract requires transport.Session.
func (t streamTransport) Handshake(ctx context.Context, req *transport.Request) (transport.Session, error) {
	conn, err := t.connect(ctx, TransportRequest{URL: req.URL, Command: req.Command, Protocol: int(req.Protocol)})
	if err != nil {
		return nil, err
	}
	return transport.NewStreamSession( //nolint:wrapcheck // Upstream owns negotiation and connection cleanup.
		conn,
		req.Command,
	)
}
