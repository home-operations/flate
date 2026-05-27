package verify

import (
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestSignatures_NilVerifyIsNoOp(t *testing.T) {
	// Empty mode — neither HEAD nor tag — should be a no-op.
	if err := Signatures(nil, "ns", "r", "", "", "", nil, plumbing.ZeroHash); err != nil {
		t.Errorf("empty mode should be a no-op, got %v", err)
	}
}

func TestSignatures_RequiresSecretRef(t *testing.T) {
	err := Signatures(nil, "ns", "r", "", manifest.GitVerifyModeHEAD, "", nil, plumbing.ZeroHash)
	if err == nil || !strings.Contains(err.Error(), "secretRef is required") {
		t.Errorf("expected secretRef-required error; got %v", err)
	}
}

func TestSignatures_RequiresSecretGetter(t *testing.T) {
	err := Signatures(nil, "ns", "r", "keys", manifest.GitVerifyModeHEAD, "", nil, plumbing.ZeroHash)
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestSignatures_SecretNotFound(t *testing.T) {
	getter := func(_, _ string) *manifest.Secret { return nil }
	err := Signatures(getter, "ns", "r", "missing", manifest.GitVerifyModeHEAD, "", nil, plumbing.ZeroHash)
	if err == nil || !strings.Contains(err.Error(), "verify secret") || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected not-found error; got %v", err)
	}
}

func TestBuildPGPKeyring(t *testing.T) {
	cases := []struct {
		name    string
		sec     *manifest.Secret
		want    string
		wantErr bool
	}{
		{
			name: "single armored key",
			sec: &manifest.Secret{StringData: map[string]any{
				"alice.asc": "-----BEGIN PGP PUBLIC KEY BLOCK-----\nfake\n-----END PGP PUBLIC KEY BLOCK-----\n",
			}},
			want: "-----BEGIN PGP PUBLIC KEY BLOCK-----\nfake\n-----END PGP PUBLIC KEY BLOCK-----\n",
		},
		{
			name:    "empty secret",
			sec:     &manifest.Secret{StringData: map[string]any{}},
			wantErr: true,
		},
		{
			name: "placeholder-wiped keys treated as missing",
			sec: &manifest.Secret{StringData: map[string]any{
				"alice.asc": "..PLACEHOLDER_alice.asc..",
			}},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := buildPGPKeyring(tc.sec)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got out=%q", out)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out != tc.want {
				t.Errorf("keyring = %q, want %q", out, tc.want)
			}
		})
	}
}

func TestMatchesHEAD_Tag_HelperMatrix(t *testing.T) {
	cases := []struct {
		mode    GitVerificationMode
		wantH   bool
		wantTag bool
	}{
		{manifest.GitVerifyModeHEAD, true, false},
		{manifest.GitVerifyModeTag, false, true},
		{manifest.GitVerifyModeTagAndHEAD, true, true},
		{"", false, false},
		{"unknown", false, false},
	}
	for _, tc := range cases {
		if got := matchesHEAD(tc.mode); got != tc.wantH {
			t.Errorf("matchesHEAD(%q) = %v, want %v", tc.mode, got, tc.wantH)
		}
		if got := matchesTag(tc.mode); got != tc.wantTag {
			t.Errorf("matchesTag(%q) = %v, want %v", tc.mode, got, tc.wantTag)
		}
	}
}
