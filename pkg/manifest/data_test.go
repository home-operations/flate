package manifest

import (
	"encoding/base64"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSecret_Value_ShadowedAndUnrequestedKeys(t *testing.T) {
	t.Parallel()
	s := &Secret{StringData: map[string]any{"VALUE": ""}, Data: map[string]any{"VALUE": "malformed!", "UNUSED": "malformed!"}}
	value, found, err := s.Value("VALUE")
	if value != "" || !found || err != nil {
		t.Fatalf("value=%q found=%t err=%v", value, found, err)
	}
	keys := slices.Collect(s.Keys())
	slices.Sort(keys)
	assert.Diff(t, keys, []string{"UNUSED", "VALUE"})
	if _, _, err := s.Value("UNUSED"); err == nil {
		t.Fatal("requested malformed value succeeded")
	}
}

func TestParseDoc_AuthoritativeSanitation(t *testing.T) {
	t.Parallel()
	const cipher = "enabled: ENC[AES256_GCM,data:fixture,type:str]\n"
	for _, tc := range []struct {
		kind, field, raw   string
		encoded, encrypted bool
	}{
		{KindConfigMap, "data", cipher, false, false}, {KindConfigMap, "binaryData", cipher, true, false},
		{KindSecret, "stringData", cipher, false, false}, {KindSecret, "data", cipher, true, false},
		{KindConfigMap, "data", "short", false, true}, {KindSecret, "data", "short", true, true},
	} {
		name := tc.kind + "/" + tc.field
		if tc.encrypted {
			name += "/marked"
		}
		for _, valid := range []bool{false, true} {
			caseName := name + "/missing-metadata"
			if valid {
				caseName = name + "/defaulted-namespace"
			}
			t.Run(caseName, func(t *testing.T) {
				t.Parallel()
				raw := tc.raw
				want := "..PLACEHOLDER_VALUE.."
				if tc.encoded {
					raw = base64.StdEncoding.EncodeToString([]byte(raw))
					want = base64.StdEncoding.EncodeToString([]byte(want))
				}
				doc := map[string]any{"apiVersion": "v1", "kind": tc.kind, tc.field: map[string]any{"VALUE": raw}}
				if tc.encrypted {
					doc["sops"] = map[string]any{"version": "1"}
				}
				if valid {
					doc["metadata"] = map[string]any{"name": "app"}
				}
				before := DeepCopyMap(doc)
				obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"})
				assert.Equal(t, changed, true)
				if valid {
					assert.NoError(t, err)
					assert.Equal(t, obj.Named().Namespace, "apps")
					assert.Equal(t, authoritative["metadata"].(map[string]any)["namespace"], any("apps"))
				} else if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
					t.Fatalf("metadata error = %v, want ErrInput/ErrFlux", err)
				}
				if authoritative[tc.field].(map[string]any)["VALUE"] != want || doc[tc.field].(map[string]any)["VALUE"] != raw {
					t.Fatal("authoritative sanitation changed borrowed input or retained ciphertext")
				}
				_, replay, changed, _ := ParseDoc(authoritative, ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"})
				if changed {
					t.Fatal("replay reported sanitation changes")
				}
				assert.Diff(t, replay, authoritative)
				assert.Diff(t, doc, before)
				retained := DeepCopyMap(authoritative)
				ReleaseDoc(doc)
				next, err := DecodeDocs(strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {VALUE: next}\n"))
				assert.NoError(t, err)
				ReleaseDoc(next[0])
				assert.Diff(t, authoritative, retained)
			})
		}
	}
}

func TestVarsMap_Newlines(t *testing.T) {
	t.Parallel()
	assert.Diff(t, VarsMap(map[string]string{"TEXT": "one\ntwo", "EMPTY": ""}), map[string]string{"TEXT": "onetwo", "EMPTY": ""})
}

func TestDataValue_FieldErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, field string
		data        Data
	}{
		{"configmap data", "data[VALUE]", &ConfigMap{Data: map[string]any{"VALUE": map[string]any{"bad": true}}}},
		{"secret data", "data[VALUE]", &Secret{Data: map[string]any{"VALUE": []any{"bad"}}}},
		{"secret decode", "decode data[VALUE]", &Secret{Data: map[string]any{"VALUE": "malformed!"}}},
		{"secret stringData", "stringData[VALUE]", &Secret{StringData: map[string]any{"VALUE": map[string]any{"bad": true}}, Data: map[string]any{"VALUE": "malformed!"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value, found, err := tc.data.Value("VALUE")
			if value != "" || found || !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
				t.Fatalf("value=%q found=%t err=%v", value, found, err)
			}
			if !strings.HasPrefix(TrimSentinelPrefix(err.Error()), tc.field+": ") {
				t.Fatalf("error = %v, want field %s", err, tc.field)
			}
		})
	}
}
