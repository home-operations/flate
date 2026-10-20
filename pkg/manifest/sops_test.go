package manifest

import (
	"encoding/base64"
	"testing"
)

func TestEncryptedSecret_Classification(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		doc  map[string]any
		want bool
	}{
		{
			name: "encrypted Secret with mac",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"metadata": map[string]any{"name": "s", "namespace": "ns"},
				"data":     map[string]any{"key": "ENC[AES256_GCM,data:...]"},
				"sops": map[string]any{
					"mac":     "ENC[AES256_GCM,data:...]",
					"version": "3.7.3",
				},
			},
			want: true,
		},
		{
			name: "encrypted with version but no mac",
			doc: map[string]any{
				"kind": "Secret",
				"sops": map[string]any{"version": "3.7.3"},
			},
			want: true,
		},
		{
			name: "plain Secret (no sops block)",
			doc: map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"data": map[string]any{"key": "Zm9v"},
			},
			want: false,
		},
		{
			name: "user-authored top-level 'sops' without mac/version",
			doc: map[string]any{
				"kind": "ConfigMap",
				"sops": map[string]any{"description": "not encrypted"},
			},
			want: false,
		},
		{
			name: "non-map 'sops' field is ignored",
			doc: map[string]any{
				"kind": "Secret",
				"sops": "stringly",
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isEncryptedSecret(tc.doc); got != tc.want {
				t.Errorf("isEncryptedSecret = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWipeValue_Ciphertext(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"encrypted str scalar", "ENC[AES256_GCM,data:abc=,iv:def=,tag:ghi=,type:str]", true},
		{"encrypted comment scalar", "ENC[AES256_GCM,data:abc=,iv:def=,tag:ghi=,type:comment]", true},
		{"encoded plaintext field", base64.StdEncoding.EncodeToString([]byte("ENC[AES256_GCM,data:abc]")), false},
		{"cleartext value", "example.com", false},
		{"empty", "", false},
		{"prefix only, no close bracket", "ENC[AES256_GCM,data:abc", true},
		{"different algorithm", "ENC[CHACHA20_POLY1305,data:abc]", false},
		{"substring not at start", "host=ENC[AES256_GCM,data:abc]", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WipeValue("key", tc.in, nil, false) != tc.in; got != tc.want {
				t.Errorf("WipeValue(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name, value, want string
		encoding          *base64.Encoding
		encrypted         bool
	}{
		{"plaintext ciphertext", "ENC[AES256_GCM,data:abc]", "..PLACEHOLDER_key..", nil, false},
		{"encoded ciphertext", "RU5DW0FFUzI1Nl9HQ00sZGF0YTphYmNd", "Li5QTEFDRUhPTERFUl9rZXkuLg==", base64.StdEncoding, false},
		{"encoded malformed", "malformed!", "malformed!", base64.StdEncoding, false},
		{"marked plaintext", "short", "..PLACEHOLDER_key..", nil, true},
		{"marked encoded", "short", "Li5QTEFDRUhPTERFUl9rZXkuLg==", base64.StdEncoding, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WipeValue("key", tc.value, tc.encoding, tc.encrypted); got != tc.want {
				t.Fatalf("wiped = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHasSubstituteDisabled(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		doc  map[string]any
		want bool
	}{
		{
			name: "annotation set to disabled",
			doc: map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{
						"kustomize.toolkit.fluxcd.io/substitute": "disabled",
					},
				},
			},
			want: true,
		},
		{
			name: "label set to disabled",
			doc: map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{
						"kustomize.toolkit.fluxcd.io/substitute": "disabled",
					},
				},
			},
			want: true,
		},
		{
			name: "annotation set to anything else",
			doc: map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{
						"kustomize.toolkit.fluxcd.io/substitute": "enabled",
					},
				},
			},
			want: false,
		},
		{
			name: "no metadata",
			doc:  map[string]any{"kind": "ConfigMap"},
			want: false,
		},
		{
			name: "metadata without labels or annotations",
			doc: map[string]any{
				"metadata": map[string]any{"name": "x"},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := HasSubstituteDisabled(tc.doc); got != tc.want {
				t.Errorf("HasSubstituteDisabled = %v, want %v", got, tc.want)
			}
		})
	}
}
