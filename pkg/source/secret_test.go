package source

import (
	"encoding/base64"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

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
