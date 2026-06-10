package source

import (
	"encoding/base64"
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

// StringFromSecret prefers StringData over Data and base64-decodes
// Data values (per k8s Secret semantics). Both the wipe-placeholder
// and a non-base64 Data value surface as the empty string.
func TestStringFromSecret(t *testing.T) {
	pem := "-----BEGIN PUBLIC KEY-----\nblob\n-----END PUBLIC KEY-----\n"
	b64 := base64.StdEncoding.EncodeToString([]byte(pem))

	cases := []struct {
		name string
		sec  *manifest.Secret
		key  string
		want string
	}{
		{
			name: "StringData wins over Data",
			sec: &manifest.Secret{
				StringData: map[string]any{"k": "plain"},
				Data:       map[string]any{"k": base64.StdEncoding.EncodeToString([]byte("from-data"))},
			},
			key: "k", want: "plain",
		},
		{
			name: "Data is base64-decoded",
			sec:  &manifest.Secret{Data: map[string]any{"cosign.pub": b64}},
			key:  "cosign.pub", want: pem,
		},
		{
			name: "wiped StringData → empty",
			sec:  &manifest.Secret{StringData: map[string]any{"k": "..PLACEHOLDER_k.."}},
			key:  "k", want: "",
		},
		{
			name: "wiped Data → empty (placeholder lands inside the base64 envelope)",
			sec: &manifest.Secret{Data: map[string]any{
				"k": base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_k..")),
			}},
			key: "k", want: "",
		},
		{
			name: "Data that isn't valid base64 → empty",
			sec:  &manifest.Secret{Data: map[string]any{"k": "not-base64!@#$"}},
			key:  "k", want: "",
		},
		{
			name: "missing key → empty",
			sec:  &manifest.Secret{},
			key:  "absent", want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := StringFromSecret(tc.sec, tc.key); got != tc.want {
				t.Errorf("StringFromSecret = %q, want %q", got, tc.want)
			}
		})
	}
}
