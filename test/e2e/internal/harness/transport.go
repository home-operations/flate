package harness

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/plumbing/transport/server"

	"github.com/home-operations/flate/internal/testfixture"
)

func serve(t *testing.T, f *testfixture.Fixture) {
	t.Helper()
	git := server.NewClient(f)
	for protocol, previous := range client.Protocols {
		client.InstallProtocol(protocol, git)
		t.Cleanup(func() { client.InstallProtocol(protocol, previous) })
	}
	responses := slices.Clone(f.Responses)
	for i := range responses {
		responses[i].Body = f.Read(t, responses[i].Body, "")
	}
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
		for _, r := range responses {
			if r.Host == host {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, fmt.Errorf("undeclared fixture host %q", host)
		}
		client, server := net.Pipe()
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
			response := reply(t, responses, request)
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
}

func reply(t *testing.T, responses []testfixture.Response, request *http.Request) *http.Response {
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
		body := []byte(r.Body)
		response.StatusCode = r.Status
		response.Body = io.NopCloser(bytes.NewReader(body))
		response.ContentLength = int64(len(body))
		for key, value := range r.Headers {
			response.Header.Set(key, value)
		}
		return response
	}
	t.Errorf("undeclared fixture request: %s %s%s", request.Method, request.Host, request.URL.RequestURI())
	return response
}
