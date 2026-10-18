package source

import (
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestMissingSecretErr(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, name, want string }{
		{manifest.KindOCIRepository, "r", "flux error: missing secret: OCIRepository ns/r: secret ns/ghcr-creds not found"},
		{manifest.KindOCIRepository, "ns/r", "flux error: missing secret: OCIRepository ns/ns/r: secret ns/ghcr-creds not found"},
		{manifest.KindHelmRepository, "HelmRepository ns/r", "flux error: missing secret: HelmRepository ns/HelmRepository ns/r: secret ns/ghcr-creds not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := manifest.NamedResource{Group: "input.example", Kind: tc.kind, Namespace: "ns", Name: tc.name}
			err := MissingSecretErr(owner, "ghcr-creds", "not found")
			if got := err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
			if !errors.Is(err, manifest.ErrMissingSecret) {
				t.Error("errors.Is(err, ErrMissingSecret) = false, want true")
			}
			mse, ok := errors.AsType[*MissingSecretError](fmt.Errorf("git clone: %w", err))
			if !ok {
				t.Fatal("errors.AsType did not recover *MissingSecretError through a wrap")
			}
			wantSecret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "ghcr-creds"}
			if mse.Secret != wantSecret {
				t.Errorf("recovered Secret = %v, want %v", mse.Secret, wantSecret)
			}
			wantOwner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Namespace: "ns", Name: tc.name}
			if mse.Owner != wantOwner || owner.Group != "input.example" {
				t.Errorf("recovered Owner = %v, want %v; input = %v", mse.Owner, wantOwner, owner)
			}
		})
	}
}

func TestBasicAuthFromSecret_UnavailableMaterial(t *testing.T) {
	t.Parallel()
	owner := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "ns", Name: "repo"}
	for _, value := range []string{"", "..PLACEHOLDER_username..", base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_username..")), "invalid-base64!"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			sec := &manifest.Secret{Name: "metadata-secret", Namespace: "elsewhere", Data: map[string]any{"username": value}, StringData: map[string]any{"password": "plain"}}
			username, password, err := BasicAuthFromSecret(sec, owner, "creds")
			missing, ok := errors.AsType[*MissingSecretError](err)
			if !ok || !errors.Is(err, manifest.ErrMissingSecret) || username != "" || password != "" {
				t.Fatalf("credentials = %q/%q, error = %v", username, password, err)
			}
			wantSecret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}
			const want = "flux error: missing secret: GitRepository ns/repo: secret ns/creds missing username/password"
			if missing.Owner != owner || missing.Secret != wantSecret || err.Error() != want {
				t.Fatalf("credential identities = %+v, error = %v", missing, err)
			}
		})
	}
	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		sec := &manifest.Secret{StringData: map[string]any{"username": "user", "password": "plain"}}
		username, password, err := BasicAuthFromSecret(sec, owner, "creds")
		if err != nil || username != "user" || password != "plain" {
			t.Fatalf("credentials = %q/%q, error = %v", username, password, err)
		}
	})
}
