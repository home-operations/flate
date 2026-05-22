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
