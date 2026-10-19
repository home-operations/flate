package source

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
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
	for _, tc := range []struct {
		name               string
		data, stringData   map[string]any
		field              string
		username, password string
	}{
		{name: "absent"},
		{name: "empty username", stringData: map[string]any{"username": "", "password": "plain"}},
		{name: "empty password", stringData: map[string]any{"username": "user", "password": ""}},
		{name: "wiped username", stringData: map[string]any{"username": "..PLACEHOLDER_username..", "password": "plain"}},
		{name: "wiped password", stringData: map[string]any{"username": "user", "password": "..PLACEHOLDER_password.."}},
		{name: "encoded wiped username", data: map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_username.."))}, stringData: map[string]any{"password": "plain"}},
		{name: "encoded wiped password", data: map[string]any{"password": base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_password.."))}, stringData: map[string]any{"username": "user"}},
		{name: "malformed username", data: map[string]any{"username": "credential-payload!"}, stringData: map[string]any{"password": "plain"}, field: "username"},
		{name: "malformed password", data: map[string]any{"password": "credential-payload!"}, stringData: map[string]any{"username": "user"}, field: "password"},
		{name: "nonscalar username", stringData: map[string]any{"username": []any{"credential-payload"}, "password": "plain"}, field: "username"},
		{name: "nonscalar password", stringData: map[string]any{"username": "user", "password": map[string]any{"secret": "credential-payload"}}, field: "password"},
		{name: "missing username malformed password", data: map[string]any{"password": "credential-payload!"}, field: "password"},
		{name: "wiped username malformed password", data: map[string]any{"password": "credential-payload!"}, stringData: map[string]any{"username": "..PLACEHOLDER_username.."}, field: "password"},
		{name: "malformed username wiped password", data: map[string]any{"username": "credential-payload!"}, stringData: map[string]any{"password": "..PLACEHOLDER_password.."}, field: "username"},
		{name: "both malformed choose username", data: map[string]any{"username": "credential-payload!", "password": "credential-payload!"}, field: "username"},
		{name: "raw placeholder is malformed", data: map[string]any{"username": "..PLACEHOLDER_username.."}, stringData: map[string]any{"password": "plain"}, field: "username"},
		{name: "valid", stringData: map[string]any{"username": "user", "password": "plain"}, username: "user", password: "plain"},
		{name: "valid encoded", data: map[string]any{"username": base64.StdEncoding.EncodeToString([]byte("user")), "password": base64.StdEncoding.EncodeToString([]byte("plain"))}, username: "user", password: "plain"},
		{name: "shadowed malformed data", data: map[string]any{"username": "credential-payload!"}, stringData: map[string]any{"username": "user", "password": "plain"}, username: "user", password: "plain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sec := &manifest.Secret{Name: "metadata-secret", Namespace: "elsewhere", Data: tc.data, StringData: tc.stringData}
			username, password, err := BasicAuthFromSecret(sec, owner, "creds")
			if tc.username != "" {
				if err != nil || username != tc.username || password != tc.password {
					t.Fatal("valid credentials were not preserved")
				}
				return
			}
			if username != "" || password != "" {
				t.Fatal("credential failure returned partial credentials")
			}
			if tc.field != "" {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, manifest.ErrMissingSecret) {
					t.Fatalf("expected hard input failure, got %v", err)
				}
				for _, context := range []string{"GitRepository ns/repo", "secret ns/creds", "[" + tc.field + "]"} {
					if !strings.Contains(err.Error(), context) {
						t.Errorf("input error missing context %q: %v", context, err)
					}
				}
				if strings.Contains(err.Error(), "credential-payload") {
					t.Error("input error exposed credential payload")
				}
				return
			}
			missing, ok := errors.AsType[*MissingSecretError](err)
			if !ok || !errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("expected MissingSecretError, got %v", err)
			}
			wantSecret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}
			if missing.Owner != owner || missing.Secret != wantSecret {
				t.Fatalf("credential identities = %+v", missing)
			}
		})
	}
}
