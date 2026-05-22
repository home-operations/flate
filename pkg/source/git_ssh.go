package source

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
// against the provided known_hosts data. The data is materialized to
// a temp file because golang.org/x/crypto/ssh/knownhosts only exposes
// a file-based New constructor.
func knownHostsCallback(data []byte) (ssh.HostKeyCallback, error) {
	f, err := os.CreateTemp("", "flate-known-hosts-*")
	if err != nil {
		return nil, fmt.Errorf("temp known_hosts: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("write known_hosts: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("close known_hosts: %w", err)
	}
	// Note: the file leaks until process exit. Acceptable for a CLI
	// run; revisit if flate ever grows a watch mode.
	return knownhosts.New(f.Name())
}
