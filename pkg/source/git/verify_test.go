package git

import (
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestVerifySignatures_NilVerifyIsNoOp(t *testing.T) {
	repo := &manifest.GitRepository{Name: "r", Namespace: "ns"}
	if err := verifySignatures(nil, repo, nil, plumbing.ZeroHash); err != nil {
		t.Errorf("nil Verify should be a no-op, got %v", err)
	}
}

func TestVerifySignatures_RequiresSecretRef(t *testing.T) {
	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		Verify: &manifest.GitRepositoryVerify{Mode: manifest.GitVerifyModeHEAD},
	}
	err := verifySignatures(nil, repo, nil, plumbing.ZeroHash)
	if err == nil || !strings.Contains(err.Error(), "secretRef is required") {
		t.Errorf("expected secretRef-required error; got %v", err)
	}
}

func TestVerifySignatures_RequiresSecretGetter(t *testing.T) {
	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		Verify: &manifest.GitRepositoryVerify{
			Mode:      manifest.GitVerifyModeHEAD,
			SecretRef: &manifest.LocalObjectReference{Name: "keys"},
		},
	}
	err := verifySignatures(nil, repo, nil, plumbing.ZeroHash)
	if err == nil || !strings.Contains(err.Error(), "source.SecretGetter") {
		t.Errorf("expected SecretGetter error; got %v", err)
	}
}

func TestVerifySignatures_SecretNotFound(t *testing.T) {
	repo := &manifest.GitRepository{
		Name: "r", Namespace: "ns",
		Verify: &manifest.GitRepositoryVerify{
			Mode:      manifest.GitVerifyModeHEAD,
			SecretRef: &manifest.LocalObjectReference{Name: "missing"},
		},
	}
	getter := func(_, _ string) *manifest.Secret { return nil }
	err := verifySignatures(getter, repo, nil, plumbing.ZeroHash)
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

func TestVerifyHEAD_Tag_HelperMatrix(t *testing.T) {
	cases := []struct {
		mode    string
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
		if got := verifyHEAD(tc.mode); got != tc.wantH {
			t.Errorf("verifyHEAD(%q) = %v, want %v", tc.mode, got, tc.wantH)
		}
		if got := verifyTag(tc.mode); got != tc.wantTag {
			t.Errorf("verifyTag(%q) = %v, want %v", tc.mode, got, tc.wantTag)
		}
	}
}
