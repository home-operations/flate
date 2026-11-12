package testutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
	"golang.org/x/crypto/ssh"
)

// GitSSH permits only the configured identity and repository roots.
type GitSSH struct {
	Host          ssh.Signer
	Allowed       ssh.PublicKey
	Roots         []string
	Authenticated atomic.Int32
	AfterAuth     func() error
}

// LocalGitSSH uses a local socket to exercise real SSH authentication.
func LocalGitSSH(tb testing.TB, fixture *GitSSH) string {
	tb.Helper()
	cfg := &ssh.ServerConfig{
		PublicKeyCallback: func(metadata ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if metadata.User() != "git" || !bytes.Equal(key.Marshal(), fixture.Allowed.Marshal()) {
				return nil, transport.ErrAuthorizationFailed
			}
			return &ssh.Permissions{}, nil
		},
	}
	cfg.AddHostKey(fixture.Host)
	listener, err := new(net.ListenConfig).Listen(tb.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() { fixture.serve(tb, conn, cfg, &workers) })
		}
	})
	tb.Cleanup(func() { _ = listener.Close(); workers.Wait() })
	return listener.Addr().String()
}

func (f *GitSSH) serve(tb testing.TB, conn net.Conn, cfg *ssh.ServerConfig, workers *sync.WaitGroup) {
	tb.Helper()
	defer conn.Close()
	connection, channels, requests, err := ssh.NewServerConn(conn, cfg)
	if err != nil {
		return
	}
	defer connection.Close()
	f.Authenticated.Add(1)
	workers.Go(func() { ssh.DiscardRequests(requests) })
	var failure error
	if f.AfterAuth != nil {
		failure = f.AfterAuth()
	}
	for incoming := range channels {
		if incoming.ChannelType() != "session" {
			_ = incoming.Reject(ssh.UnknownChannelType, "session required")
			continue
		}
		channel, requests, err := incoming.Accept()
		if err != nil {
			return
		}
		workers.Go(func() { f.serveSession(tb, channel, requests, failure) })
	}
}

//nolint:gocognit // The f keeps service admission and failure injection in one SSH session.
func (f *GitSSH) serveSession(
	tb testing.TB,
	channel ssh.Channel,
	requests <-chan *ssh.Request,
	failure error,
) {
	tb.Helper()
	defer channel.Close()
	var root string
	for request := range requests {
		var command struct{ Command string }
		path, valid := "", false
		if request.Type == "exec" && ssh.Unmarshal(request.Payload, &command) == nil {
			path, valid = strings.CutPrefix(command.Command, "git-upload-pack '")
			valid = valid && strings.HasSuffix(path, "'")
			path = strings.TrimSuffix(path, "'")
		}
		if !valid || !slices.Contains(f.Roots, path) {
			_ = request.Reply(false, nil)
			continue
		}
		root = path
		_ = request.Reply(true, nil)
		break
	}
	if root == "" {
		return
	}
	endpoint, err := transport.ParseURL(root)
	if err != nil {
		tb.Error(err)
		return
	}
	st, err := transport.DefaultLoader.Load(endpoint)
	if err != nil {
		tb.Error(err)
		return
	}
	if closer, ok := st.(io.Closer); ok {
		defer closer.Close()
	}
	if failure != nil {
		if errors.Is(failure, syscall.ECONNRESET) {
			var advertisement bytes.Buffer
			if advertiseErr := transport.AdvertiseRefs(
				tb.Context(),
				st,
				&advertisement,
				transport.UploadPackService,
				false,
				protocol.V0,
			); advertiseErr != nil {
				tb.Error(advertiseErr)
				return
			}
			_, _ = channel.Write(advertisement.Bytes()[:advertisement.Len()-pktline.LenSize-1])
		} else {
			_, _ = pktline.WriteError(channel, failure)
		}
		return
	}
	err = transport.UploadPack(
		tb.Context(),
		st,
		io.NopCloser(channel),
		gitio.WriteNopCloser(channel), //nolint:forbidigo // Go-git supplies this writer adapter; io has no equivalent.
		&transport.UploadPackRequest{SkipDeltaCompression: true},
	)
	// Test cancellation precedes cleanup, so a finishing server MUST ignore that cancellation.
	if errors.Is(err, context.Canceled) && tb.Context().Err() != nil {
		return
	}
	if err != nil && !errors.Is(err, io.EOF) {
		tb.Error(err)
		return
	}
	_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
}
