package source

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"oras.land/oras-go/v2/registry/remote/auth"
)

// errStore mimics a broken credential store (missing credential-helper
// binary, unreadable config): Get fails.
type errStore struct{}

func (errStore) Get(context.Context, string) (auth.Credential, error) {
	return auth.EmptyCredential, errors.New("credential helper broken")
}
func (errStore) Put(context.Context, string, auth.Credential) error { return nil }
func (errStore) Delete(context.Context, string) error               { return nil }

func TestCredentialForHost_StoreErrorCountsAsNoCredential(t *testing.T) {
	if _, ok := CredentialForHost(context.Background(), errStore{}, "ghcr.io"); ok {
		t.Errorf("a failing store must count as no credential")
	}
}

func TestRegistryCredentialStore_CorruptConfigFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"auths":`), 0o600); err != nil {
		t.Fatalf("write corrupt config: %v", err)
	}
	if _, err := RegistryCredentialStore(path); err == nil {
		t.Fatal("expected a corrupt config file to fail loudly")
	}
}

// The docker-default arm either succeeds or gracefully returns a nil
// store when no default is configured — never errors.
func TestRegistryCredentialStore_EmptyPathNeverErrors(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	if _, err := RegistryCredentialStore(""); err != nil {
		t.Errorf("empty path should never error; got %v", err)
	}
}

// The docker config's "auth" field is base64(user:password); the probe
// must surface it decoded, and a host the config doesn't cover must
// report no credential.
func TestCredentialForHost_DecodesAuthField(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("alice:hunter2"))
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"auths":{"ghcr.io":{"auth":"`+encoded+`"}}}`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	store, err := RegistryCredentialStore(path)
	if err != nil {
		t.Fatalf("RegistryCredentialStore: %v", err)
	}
	cred, ok := CredentialForHost(context.Background(), store, "ghcr.io")
	if !ok || cred.Username != "alice" || cred.Password != "hunter2" {
		t.Errorf("CredentialForHost(ghcr.io) = %q/%q, %v; want alice/hunter2, true", cred.Username, cred.Password, ok)
	}
	if _, ok := CredentialForHost(context.Background(), store, "other.registry.io"); ok {
		t.Errorf("CredentialForHost(other.registry.io) = found; want not found")
	}
}
