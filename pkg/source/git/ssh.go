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
// against the provided known_hosts data. knownhosts.New only accepts
// file paths, so we materialize a temp file; New reads and parses it
// synchronously and returns a callback that holds the parsed data in
// memory, so the file is safe to remove immediately after New returns.
func knownHostsCallback(data []byte) (ssh.HostKeyCallback, error) {
	f, err := os.CreateTemp("", "flate-known-hosts-*")
	if err != nil {
		return nil, fmt.Errorf("temp known_hosts: %w", err)
	}
	name := f.Name()
	defer os.Remove(name) //nolint:errcheck // best-effort cleanup of temp file
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write known_hosts: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("close known_hosts: %w", err)
	}
	return knownhosts.New(name)
}
