package manifest

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSanitizeDocument_Ciphertext(t *testing.T) {
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
				delete(want["data"].(map[string]any), "ENCRYPTED")
			}
			delete(want, "sops")
			admitted, changed := SanitizeDocument(DocOccurrence{Document: doc})
			got := admitted.Document
			assert.Equal(t, changed, tc.cipher || tc.metadata != nil)
			assert.Diff(t, got, want)
			assert.Diff(t, doc, before)
			if tc.cipher {
				assert.FatalIf(t, len(admitted.Warnings) != 1,
					"admission warnings: got %d, want 1", len(admitted.Warnings))

				assert.Diff(t, admitted.Warnings[0].Detail, []string{"ENCRYPTED"})
			} else {
				assert.Equal(t, len(admitted.Warnings), 0)
			}
			replay, replayChanged := SanitizeDocument(admitted)
			assert.Equal(t, replayChanged, false)
			assert.Diff(t, replay, admitted)
		})
	}
}

func TestEncryptedValue_Ciphertext(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	const token = "removed"
	const encodedToken = token
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
			if got := encryptedValue(tc.value, tc.encoding, tc.secretData); got != (tc.want == token) {
				t.Fatalf("encryptedValue(%q) = %t, want %t", tc.value, got, tc.want == token)
			}
		})
	}
}

//nolint:paralleltest // The allocation counter changes process-global state.
func TestEncryptedValue_ClearEncodedAllocationFree(t *testing.T) {
	value := base64.StdEncoding.EncodeToString(
		[]byte("a readable clear scalar that fits in the decoding scratch buffer"),
	)
	var got bool
	allocs := testing.AllocsPerRun(100, func() {
		got = encryptedValue(value, base64.StdEncoding, true)
	})
	assert.Equal(t, got, false)
	assert.FatalIf(t, allocs != 0,
		"encryptedValue allocated %g times, want 0", allocs)
}

func TestSanitizeDocument_OverlappingFieldsAndDiagnostics(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       KindSecret,
		"metadata":   map[string]any{"name": "input", "namespace": "apps"},
		"data": map[string]any{
			"EMPTY":    cipher,
			"READABLE": base64.StdEncoding.EncodeToString([]byte("repository")),
		},
		"stringData": map[string]any{"EMPTY": "", "READABLE": cipher},
		"sops":       map[string]any{"mac": "encrypted"},
	}
	before := DeepCopyMap(doc)
	o, changed := SanitizeDocument(DocOccurrence{Document: doc, Ordinal: 7})
	assert.Equal(t, changed, true)
	assert.Equal(t, o.Ordinal, 7)
	assert.Equal(t, len(o.Warnings), 1)
	assert.Diff(t, o.Warnings[0].Detail, []string{"EMPTY", "READABLE"})
	result1, err := ParseDoc(o.Document, ParseDocOptions{})
	obj := result1.Object
	assert.NoError(t, err)
	for key, want := range map[string]string{"EMPTY": "", "READABLE": "repository"} {
		result2, err := obj.(Data).Value(key)
		got, found := result2.Value, result2.Found
		assert.NoError(t, err)
		assert.Equal(t, found, true)
		assert.Equal(t, got, want)
	}
	again, changed := SanitizeDocument(o)
	assert.Equal(t, changed, false)
	assert.Diff(t, again, o)
	assert.Diff(t, doc, before)
	without := o
	without.Warnings = nil
	assert.Equal(t, Fingerprint(o), Fingerprint(without))
}
