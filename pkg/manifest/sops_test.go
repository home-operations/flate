package manifest

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSanitizeData_Ciphertext(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	for _, tc := range []struct {
		name     string
		metadata map[string]any
		cipher   bool
	}{
		{name: "absent metadata", cipher: true},
		{name: "present metadata", metadata: map[string]any{"mac": "metadata", "version": "3.13.3"}, cipher: true},
		{name: "altered metadata", metadata: map[string]any{"mac": nil, "version": false, "unencrypted_suffix": "_unencrypted"}, cipher: true},
		{name: "clear metadata document", metadata: map[string]any{"version": "3.13.3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := map[string]any{
				"CLEAR":             "readable",
				"CLEAR_unencrypted": "visible",
				"LITERAL":           "documentation: " + cipher,
				"PARTIAL":           "ENC[AES256_GCM,data:fixture]",
				"COUNT":             int64(3),
				"FLAG":              false,
				"UNSUPPORTED":       map[string]any{"nested": true},
			}
			if tc.cipher {
				data["ENCRYPTED"] = cipher
			}
			doc := map[string]any{"apiVersion": "v1", "kind": KindConfigMap, "data": data}
			if tc.metadata != nil {
				doc["sops"] = tc.metadata
			}
			before := DeepCopyMap(doc)
			want := DeepCopyMap(doc)
			if tc.cipher {
				want["data"].(map[string]any)["ENCRYPTED"] = "..PLACEHOLDER_ENCRYPTED.."
			}
			got, changed := sanitizeData(doc, KindConfigMap)
			assert.Equal(t, changed, tc.cipher)
			assert.Diff(t, got, want)
			assert.Diff(t, doc, before)
			replay, replayChanged := sanitizeData(got, KindConfigMap)
			assert.Equal(t, replayChanged, false)
			assert.Diff(t, replay, got)
		})
	}
}

func TestWipeValue_Ciphertext(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	const token = "..PLACEHOLDER_key.."
	const encodedToken = "Li5QTEFDRUhPTERFUl9rZXkuLg=="
	encoded := base64.StdEncoding.EncodeToString([]byte(cipher))
	embedded := "enabled: " + cipher + "\n"
	encodedEmbedded := base64.StdEncoding.EncodeToString([]byte(embedded))
	doubleEncoded := base64.StdEncoding.EncodeToString([]byte(encoded))
	for _, tc := range []struct {
		name, value, want string
		encoding          *base64.Encoding
		secretData        bool
	}{
		{"complete scalar", cipher, token, nil, false},
		{"comment scalar", "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:comment]", token, nil, false},
		{"empty", "", "", nil, false},
		{"short", "short", "short", nil, false},
		{"clear", "example.com", "example.com", nil, false},
		{"partial framing", "ENC[AES256_GCM,data:fixture]", "ENC[AES256_GCM,data:fixture]", nil, false},
		{"missing close", "ENC[AES256_GCM,data:a,iv:b,tag:c,type:str", "ENC[AES256_GCM,data:a,iv:b,tag:c,type:str", nil, false},
		{"empty data", "ENC[AES256_GCM,data:,iv:b,tag:c,type:str]", "ENC[AES256_GCM,data:,iv:b,tag:c,type:str]", nil, false},
		{"empty iv", "ENC[AES256_GCM,data:a,iv:,tag:c,type:str]", "ENC[AES256_GCM,data:a,iv:,tag:c,type:str]", nil, false},
		{"empty tag", "ENC[AES256_GCM,data:a,iv:b,tag:,type:str]", "ENC[AES256_GCM,data:a,iv:b,tag:,type:str]", nil, false},
		{"empty type", "ENC[AES256_GCM,data:a,iv:b,tag:c,type:]", "ENC[AES256_GCM,data:a,iv:b,tag:c,type:]", nil, false},
		{"other algorithm", "ENC[CHACHA20_POLY1305,data:a,iv:b,tag:c,type:str]", "ENC[CHACHA20_POLY1305,data:a,iv:b,tag:c,type:str]", nil, false},
		{"literal prefix", "host=" + cipher, "host=" + cipher, nil, false},
		{"leading whitespace", " " + cipher, " " + cipher, nil, false},
		{"component newline", "ENC[AES256_GCM,data:a\nb,iv:c,tag:d,type:str]", "ENC[AES256_GCM,data:a\nb,iv:c,tag:d,type:str]", nil, false},
		{"trailing text", cipher + " trailing", token, nil, false},
		{"trailing newline", cipher + "\ntrailing", token, nil, false},
		{"embedded yaml", embedded, embedded, nil, false},
		{"base64-looking plaintext", encoded, encoded, nil, false},
		{"encoded complete scalar", encoded, encodedToken, base64.StdEncoding, true},
		{"encoded embedded yaml", encodedEmbedded, encodedEmbedded, base64.StdEncoding, true},
		{"raw ciphertext in encoded field", cipher, encodedToken, base64.StdEncoding, true},
		{"double encoded", doubleEncoded, doubleEncoded, base64.StdEncoding, true},
		{"encoded yaml document", base64.StdEncoding.EncodeToString([]byte("sops:\n  mac: ENC[fixture]\n")), encodedToken, base64.StdEncoding, true},
		{"encoded json document", base64.StdEncoding.EncodeToString([]byte(`{"sops": {"mac": "ENC[fixture]"}}`)), encodedToken, base64.StdEncoding, true},
		{"encoded dotenv document", base64.StdEncoding.EncodeToString([]byte("sops_mac=ENC[fixture]")), encodedToken, base64.StdEncoding, true},
		{"encoded shortest ini document", base64.StdEncoding.EncodeToString([]byte("[sops]")), encodedToken, base64.StdEncoding, true},
		{"plaintext document marker", "sops_mac=ENC[fixture]", "sops_mac=ENC[fixture]", nil, false},
		{"encoded token outside secret data", encoded, encoded, base64.StdEncoding, false},
		{"encoded document outside secret data", base64.StdEncoding.EncodeToString([]byte("[sops]")), base64.StdEncoding.EncodeToString([]byte("[sops]")), base64.StdEncoding, false},
		{"encoded incomplete token", base64.StdEncoding.EncodeToString([]byte("ENC[AES256_GCM,data:fixture]")), base64.StdEncoding.EncodeToString([]byte("ENC[AES256_GCM,data:fixture]")), base64.StdEncoding, true},
		{"encoded prefixed token", base64.StdEncoding.EncodeToString([]byte("prefix " + cipher)), base64.StdEncoding.EncodeToString([]byte("prefix " + cipher)), base64.StdEncoding, true},
		{"encoded trailing text", base64.StdEncoding.EncodeToString([]byte(cipher + " trailing")), encodedToken, base64.StdEncoding, true},
		{"double encoded document", base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString([]byte("[sops]")))), base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString([]byte("[sops]")))), base64.StdEncoding, true},
		{"partial document decode with error", "W3NvcHNd!", "W3NvcHNd!", base64.StdEncoding, true},
		{"large late marker", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 8192) + "[sops]")), encodedToken, base64.StdEncoding, true},
		{"large clear", base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 8192))), base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 8192))), base64.StdEncoding, true},
		{"malformed encoded", "malformed!", "malformed!", base64.StdEncoding, true},
		{"partial decode with error", encoded + "!", encoded + "!", base64.StdEncoding, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := WipeValue("key", tc.value, tc.encoding, tc.secretData); got != tc.want {
				t.Fatalf("WipeValue(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestWipeValue_ClearEncodedAllocationFree(t *testing.T) {
	value := base64.StdEncoding.EncodeToString([]byte("a readable clear scalar that fits in the decoding scratch buffer"))
	var got string
	allocs := testing.AllocsPerRun(100, func() {
		got = WipeValue("key", value, base64.StdEncoding, true)
	})
	assert.Diff(t, value, got)
	if allocs != 0 {
		t.Fatalf("WipeValue allocated %g times, want 0", allocs)
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
