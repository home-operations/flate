package source

import (
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

// MissingSecretErr returns a structured *MissingSecretError that (a) renders
// the same string as the prior fmt.Errorf form (byte-equivalence of the skip
// reason), (b) still satisfies errors.Is(ErrMissingSecret), and (c) exposes the
// missing Secret's identity via errors.As — even through an outer wrap — so the
// source controller can consult the producer index.
func TestMissingSecretErr(t *testing.T) {
	err := MissingSecretErr("OCIRepository", "ns", "r", "ghcr-creds", "not found")

	const want = "flux error: missing secret: OCIRepository ns/r: secret ns/ghcr-creds not found"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, manifest.ErrMissingSecret) {
		t.Error("errors.Is(err, ErrMissingSecret) = false, want true")
	}

	var mse *MissingSecretError
	if !errors.As(fmt.Errorf("git clone: %w", err), &mse) {
		t.Fatal("errors.As did not recover *MissingSecretError through a wrap")
	}
	wantSecret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "ghcr-creds"}
	if mse.Secret != wantSecret {
		t.Errorf("recovered Secret = %v, want %v", mse.Secret, wantSecret)
	}
	wantOwner := manifest.NamedResource{Kind: "OCIRepository", Namespace: "ns", Name: "r"}
	if mse.Owner != wantOwner {
		t.Errorf("recovered Owner = %v, want %v", mse.Owner, wantOwner)
	}
}

func TestBasicAuthFromSecret_UnavailableMaterial(t *testing.T) {
	for _, value := range []string{"", "..PLACEHOLDER_username..", "invalid-base64!"} {
		sec := &manifest.Secret{Data: map[string]any{"username": value}, StringData: map[string]any{"password": "plain"}}
		if _, _, err := BasicAuthFromSecret(sec, "GitRepository", "ns", "repo", "creds"); !errors.Is(err, manifest.ErrMissingSecret) {
			t.Fatalf("value %q: %v", value, err)
		}
	}
}
