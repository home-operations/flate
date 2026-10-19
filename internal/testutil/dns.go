package testutil

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// StubDNS bypasses environment proxies and resolves fixture hosts to loopback or a lookup error.
// It changes process-global state, so callers MUST remain serial.
func StubDNS(t *testing.T, lookup func(context.Context, string) error) {
	t.Helper()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		t.Setenv(name, "")
	}
	previous := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return &dnsConn{lookup: func(name string) error { return lookup(ctx, name) }}, nil
	}}
	t.Cleanup(func() { net.DefaultResolver = previous })
}

type dnsConn struct {
	lookup   func(string) error
	response *bytes.Reader
}

func (c *dnsConn) Write(p []byte) (int, error) {
	var query dnsmessage.Message
	if err := query.Unpack(p[2:]); err != nil {
		return 0, err
	}
	if err := c.lookup(query.Questions[0].Name.String()); err != nil {
		return 0, err
	}
	response := dnsmessage.Message{ID: query.ID, Response: true, RecursionAvailable: true, Questions: query.Questions}
	if q := query.Questions[0]; q.Type == dnsmessage.TypeA {
		response.Answers = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: q.Name, Type: q.Type, Class: q.Class}, Body: &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}}}
	}
	data, err := response.Pack()
	if err != nil {
		return 0, err
	}
	size := len(data)
	if size > 1<<16-1 {
		return 0, fmt.Errorf("fixture DNS response exceeds TCP length")
	}
	packet := binary.BigEndian.AppendUint16(nil, uint16(size))
	c.response = bytes.NewReader(append(packet, data...))
	return len(p), nil
}

func (c *dnsConn) Read(p []byte) (int, error) { return c.response.Read(p) }

func (*dnsConn) Close() error                     { return nil }
func (*dnsConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*dnsConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*dnsConn) SetDeadline(time.Time) error      { return nil }
func (*dnsConn) SetReadDeadline(time.Time) error  { return nil }
func (*dnsConn) SetWriteDeadline(time.Time) error { return nil }
