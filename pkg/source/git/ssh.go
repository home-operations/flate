package git

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// insecureIgnoreHostKey is the default SSH host-key callback when a
// GitRepository SecretRef does NOT include a known_hosts entry. It
// matches the pre-existing ssh-with-agent behavior; users who want
// strict host-key checking provide known_hosts in the Secret.
func insecureIgnoreHostKey(_ string, _ net.Addr, _ ssh.PublicKey) error {
	return nil
}

// knownHostsCallback returns an SSH HostKeyCallback that validates
// against the provided known_hosts data. It feeds the data through an
// os.Pipe so no temp file is written to disk; the write side is closed
// before New returns so New reads all data synchronously.
func knownHostsCallback(data []byte) (ssh.HostKeyCallback, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pipe known_hosts: %w", err)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, werr := w.Write(data)
		writeErr <- werr
		_ = w.Close() // write side closed after data is sent
	}()
	cb, parseErr := knownhosts.New(fmt.Sprintf("/dev/fd/%d", r.Fd()))
	_ = r.Close() // read side no longer needed after New returns
	if werr := <-writeErr; werr != nil && parseErr == nil {
		return nil, fmt.Errorf("write known_hosts pipe: %w", werr)
	}
	return cb, parseErr
}
