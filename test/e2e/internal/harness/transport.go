package harness

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/testfixture"
)

type fixtureTransport struct {
	fixture   *testfixture.Fixture
	responses atomic.Pointer[[]testfixture.Response]
}

func (f *fixtureTransport) update(t *testing.T, changes []testfixture.Response) {
	t.Helper()
	var responses []testfixture.Response
	if changes == nil {
		changes = f.fixture.Responses
	} else if current := f.responses.Load(); current != nil {
		responses = slices.Clone(*current)
	}
	for _, response := range changes {
		if response.Error == "" {
			response.Body = f.fixture.Read(t, response.Body, "")
		}
		i := slices.IndexFunc(responses, func(r testfixture.Response) bool {
			return r.Host == response.Host && r.Method == response.Method && r.Path == response.Path
		})
		if i < 0 {
			responses = append(responses, response)
		} else {
			responses[i] = response
		}
	}
	f.responses.Store(&responses)
}

type fixtureConnection struct {
	net.Conn

	reset atomic.Bool
}

func (c *fixtureConnection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil && c.reset.Load() {
		return n, syscall.ECONNRESET
	}
	return n, err //nolint:wrapcheck // net.Conn.Read must preserve EOF and concrete transport error identity.
}

func serve(t *testing.T, f *testfixture.Fixture) *fixtureTransport {
	t.Helper()
	git := server.NewClient(f)
	for protocol, previous := range client.Protocols {
		client.InstallProtocol(protocol, git)
		t.Cleanup(func() { client.InstallProtocol(protocol, previous) })
	}
	fixture := &fixtureTransport{fixture: f}
	fixture.update(t, nil)
	old := http.DefaultTransport
	original, ok := old.(*http.Transport)
	if !ok {
		t.Fatal("fixture requires a concrete HTTP transport")
	}
	transport := original.Clone()
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	transport.Proxy = nil
	transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, fmt.Errorf("fixture forbids plain HTTP and live endpoints")
	}
	// CLI transports replace DialContext but capture HTTP_PROXY before fetching.
	// Loopback HTTP bypasses environment proxies and remains outside this guarantee.
	t.Setenv("HTTP_PROXY", "http://plain-http.fixture.invalid:1")
	resolver := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{ //nolint:reassign // CLI transports must resolve the proxy without opening a socket.
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			return nil, fmt.Errorf("fixture forbids plain HTTP and live endpoints")
		},
	}
	transport.DialTLSContext = func(_ context.Context, _, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("fixture host: %w", err)
		}
		allowed := false
		for _, r := range *fixture.responses.Load() {
			if r.Host == host {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, fmt.Errorf("undeclared fixture host %q", host)
		}
		pipe, server := net.Pipe()
		client := &fixtureConnection{Conn: pipe}
		mu.Lock()
		connections = append(connections, client, server)
		mu.Unlock()
		workers.Go(func() {
			defer server.Close()
			request, err := http.ReadRequest(bufio.NewReader(server))
			if err != nil {
				return
			}
			defer request.Body.Close()
			response, err := reply(t, *fixture.responses.Load(), request)
			if errors.Is(err, syscall.ECONNRESET) {
				client.reset.Store(true)
				return
			}
			defer response.Body.Close()
			_ = response.Write(server)
		})
		return client, nil
	}
	http.DefaultTransport = transport //nolint:reassign // CLI acquisition clones the concrete process transport.
	t.Cleanup(func() {
		transport.CloseIdleConnections()
		mu.Lock()
		for _, connection := range connections {
			_ = connection.Close()
		}
		mu.Unlock()
		workers.Wait()
		http.DefaultTransport = old    //nolint:reassign // Restore the process transport after joining fixture workers.
		net.DefaultResolver = resolver //nolint:reassign // Restore the process resolver after joining fixture workers.
	})
	return fixture
}

func reply(t *testing.T, responses []testfixture.Response, request *http.Request) (*http.Response, error) {
	t.Helper()
	response := &http.Response{
		StatusCode: http.StatusNotFound,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("undeclared fixture request")),
		Close:      true,
	}
	for _, r := range responses {
		if r.Host != request.Host || r.Method != request.Method || r.Path != request.URL.RequestURI() {
			continue
		}
		if r.Error == "connection-reset" {
			return nil, syscall.ECONNRESET
		}
		body := []byte(r.Body)
		response.StatusCode = r.Status
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		for key, value := range r.Headers {
			response.Header.Set(key, value)
		}
		return response, nil
	}
	t.Errorf("undeclared fixture request: %s %s%s", request.Method, request.Host, request.URL.RequestURI())
	return response, nil
}
