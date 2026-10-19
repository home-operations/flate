package manifest

import (
	"encoding/base64"
	"slices"
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
		t.Run(name, func(t *testing.T) {
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
			_, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"})
			if err == nil || !changed {
				t.Fatal("metadata-invalid data object parsed")
			}
			if authoritative[tc.field].(map[string]any)["VALUE"] != want || doc[tc.field].(map[string]any)["VALUE"] != raw {
				t.Fatal("authoritative sanitation changed borrowed input or retained ciphertext")
			}
			_, replay, changed, _ := ParseDoc(authoritative, ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"})
			if changed {
				t.Fatal("replay reported sanitation changes")
			}
			assert.Diff(t, replay, authoritative)
		})
	}
}

func TestVarsMap_Newlines(t *testing.T) {
	t.Parallel()
	assert.Diff(t, VarsMap(map[string]string{"TEXT": "one\ntwo", "EMPTY": ""}), map[string]string{"TEXT": "onetwo", "EMPTY": ""})
}
