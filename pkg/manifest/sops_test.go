package manifest

import "testing"

func TestIsEncryptedSecret(t *testing.T) {
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
			if got := IsEncryptedSecret(tc.doc); got != tc.want {
				t.Errorf("IsEncryptedSecret = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsSopsCiphertext(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"encrypted str scalar", "ENC[AES256_GCM,data:abc=,iv:def=,tag:ghi=,type:str]", true},
		{"encrypted comment scalar", "ENC[AES256_GCM,data:abc=,iv:def=,tag:ghi=,type:comment]", true},
		{"cleartext value", "example.com", false},
		{"empty", "", false},
		{"prefix only, no close bracket", "ENC[AES256_GCM,data:abc", false},
		{"different algorithm", "ENC[CHACHA20_POLY1305,data:abc]", false},
		{"substring not at start", "host=ENC[AES256_GCM,data:abc]", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSopsCiphertext(tc.in); got != tc.want {
				t.Errorf("IsSopsCiphertext(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestHasSubstituteDisabled(t *testing.T) {
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
			if got := HasSubstituteDisabled(tc.doc); got != tc.want {
				t.Errorf("HasSubstituteDisabled = %v, want %v", got, tc.want)
			}
		})
	}
}
