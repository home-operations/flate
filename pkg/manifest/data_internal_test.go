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
	s := &Secret{
		StringData: map[string]any{"VALUE": ""},
		Data:       map[string]any{"VALUE": "malformed!", "UNUSED": "malformed!"},
	}
	result1, err := s.Value("VALUE")
	value, found := result1.Value, result1.Found

	assert.FatalIf(t, value != "" || !found || err != nil, "value=%q found=%t err=%v", value, found, err)

	keys := slices.Collect(s.Keys())
	slices.Sort(keys)
	assert.Diff(t, keys, []string{"UNUSED", "VALUE"})
	{
		_, err := s.Value("UNUSED")
		assert.FatalIf(t, err == nil, "%s", "requested malformed value succeeded")
	}
}

func TestParseDoc_AuthoritativeSanitation(t *testing.T) {
	t.Parallel()
	const cipher = "ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	const embedded = "enabled: " + cipher + "\n"
	for _, tc := range []struct {
		name, kind, field, wantRead             string
		raw                                     any
		encode, metadata, unreadable, readError bool
	}{
		{name: "configmap data", kind: KindConfigMap, field: "data", raw: cipher, unreadable: true},
		{name: "configmap binary", kind: KindConfigMap, field: "binaryData", raw: cipher, unreadable: true},
		{name: "secret stringData", kind: KindSecret, field: "stringData", raw: cipher, unreadable: true},
		{name: "secret data", kind: KindSecret, field: "data", raw: cipher, metadata: true, unreadable: true},
		{name: "embedded plaintext", kind: KindConfigMap, field: "data", raw: embedded, wantRead: embedded},
		{name: "embedded encoded", kind: KindSecret, field: "data", raw: embedded, encode: true, metadata: true, wantRead: embedded},
		{name: "clear metadata", kind: KindConfigMap, field: "data", raw: "short", metadata: true, wantRead: "short"},
		{name: "clear encoded metadata", kind: KindSecret, field: "data", raw: "short", encode: true, metadata: true, wantRead: "short"},
		{name: "malformed encoded metadata", kind: KindSecret, field: "data", raw: "malformed!", metadata: true, readError: true},
		{name: "base64 ciphertext encoded field", kind: KindSecret, field: "data", raw: cipher, encode: true, metadata: true, unreadable: true},
		{name: "yaml document", kind: KindSecret, field: "data", raw: "sops:\n  mac: ENC[fixture]\n", encode: true, unreadable: true},
		{name: "json document", kind: KindSecret, field: "data", raw: `{"sops": {"mac": "ENC[fixture]"}}`, encode: true, unreadable: true},
		{name: "dotenv document", kind: KindSecret, field: "data", raw: "sops_mac=ENC[fixture]", encode: true, unreadable: true},
		{name: "shortest ini document", kind: KindSecret, field: "data", raw: "[sops]", encode: true, unreadable: true},
		{name: "plaintext document", kind: KindSecret, field: "stringData", raw: "sops_mac=ENC[fixture]", wantRead: "sops_mac=ENC[fixture]"},
		{name: "configmap encoded token", kind: KindConfigMap, field: "binaryData", raw: cipher, encode: true},
		{name: "configmap encoded document", kind: KindConfigMap, field: "binaryData", raw: "[sops]", encode: true},
		{name: "partial document decode", kind: KindSecret, field: "data", raw: "W3NvcHNd!", readError: true},
		{name: "unsupported scalar metadata", kind: KindConfigMap, field: "data", raw: []any{"unsupported"}, metadata: true, readError: true},
		{name: "integer metadata", kind: KindConfigMap, field: "data", raw: int64(3), metadata: true, wantRead: "3"},
	} {
		for _, valid := range []bool{false, true} {
			name := tc.name + "/missing-metadata"
			if valid {
				name = tc.name + "/defaulted-namespace"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				testParseDocAuthoritativeSanitationCase1(t, &tc, &valid)
			})
		}
	}
}

func testParseDocAuthoritativeSanitationCase1(t *testing.T, tc *struct {
	name       string
	kind       string
	field      string
	wantRead   string
	raw        any
	encode     bool
	metadata   bool
	unreadable bool
	readError  bool
}, valid *bool,
) {
	t.Helper()

	raw := tc.raw
	if tc.encode {
		raw = base64.StdEncoding.EncodeToString([]byte(raw.(string)))
	}
	want := raw
	if tc.unreadable {
		want = nil
	}
	clearValue := "readable"
	if tc.field == "binaryData" || (tc.kind == KindSecret && tc.field == "data") {
		clearValue = base64.StdEncoding.EncodeToString([]byte(clearValue))
	}
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       tc.kind, tc.
				field: map[string]any{"VALUE": raw, "CLEAR": clearValue},
	}
	if tc.metadata {
		doc["sops"] = map[string]any{"version": "3.13.3"}
	}
	if *valid {
		doc["metadata"] = map[string]any{"name": "app"}
	}
	before := DeepCopyMap(doc)
	result3, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
	obj, authoritative, changed := result3.Object, result3.Document, result3.Changed
	assert.Equal(t, changed, tc.unreadable || tc.metadata || (*valid))
	if *valid {
		testParseDocAuthoritativeSanitationCase1Part1(
			t,
			&testParseDocAuthoritativeSanitationCase1Part1Inputs{
				err:           err,
				obj:           obj,
				authoritative: authoritative,
				tc:            &tc,
			},
		)
	} else if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
		t.Fatalf("metadata error = %v, want ErrInput/ErrFlux", err)
	}
	assert.Diff(t, authoritative[tc.field].(map[string]any)["VALUE"], want)
	assert.Diff(t, authoritative[tc.field].(map[string]any)["CLEAR"], any(clearValue))
	result4, _ := ParseDoc(authoritative, ParseDocOptions{DefaultNamespace: "apps"})
	replay, replayChanged := result4.Document, result4.Changed
	assert.FatalIf(t, replayChanged, "%s", "replay reported sanitation changes")

	assert.Diff(t, replay, authoritative)
	assert.Diff(t, doc, before)
	if changed {
		retained := DeepCopyMap(authoritative)
		ReleaseDoc(doc)
		next, err := decodeDocMaps(
			strings.NewReader(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {VALUE: next}\n",
			),
		)
		assert.NoError(t, err)
		ReleaseDoc(next[0])
		assert.Diff(t, authoritative, retained)
	}
}

func testParseDocAuthoritativeSanitationCase1Part1(
	t *testing.T, inputs *testParseDocAuthoritativeSanitationCase1Part1Inputs,
) {
	t.Helper()
	assert.NoError(t, inputs.err)
	assert.Equal(t, inputs.obj.Named().Namespace, "apps")
	assert.Equal(t, inputs.authoritative["metadata"].(map[string]any)["namespace"], any("apps"))
	if (*inputs.tc).field != "binaryData" {
		result5, valueErr := inputs.obj.(Data).Value("VALUE")
		value, found := result5.Value, result5.Found
		if (*inputs.tc).readError {
			assert.FatalIf(t, value != "" || found || !errors.Is(valueErr, ErrInput) || !errors.Is(valueErr, ErrFlux),
				"value=%q found=%t err=%v", value, found, valueErr)
		} else {
			assert.NoError(t, valueErr)
			assert.Equal(t, found, !(*inputs.tc).unreadable)
			assert.Equal(t, value, (*inputs.tc).wantRead)
		}
	}
}

func TestVarsMap_Newlines(t *testing.T) {
	t.Parallel()
	assert.Diff(
		t,
		VarsMap(map[string]string{"TEXT": "one\ntwo", "EMPTY": ""}),
		map[string]string{"TEXT": "onetwo", "EMPTY": ""},
	)
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
			result6, err := tc.data.Value("VALUE")
			value, found := result6.Value, result6.Found

			assert.FatalIf(
				t,
				value != "" || found || !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux),
				"value=%q found=%t err=%v",
				value,
				found,
				err,
			)

			assert.FatalIf(
				t,
				!strings.HasPrefix(err.Error(), tc.field+": "),
				"error = %v, want field %s",
				err,
				tc.field,
			)
		})
	}
}

type testParseDocAuthoritativeSanitationCase1Part1Inputs struct {
	err           error
	obj           BaseManifest
	authoritative map[string]any
	tc            **struct {
		name       string
		kind       string
		field      string
		wantRead   string
		raw        any
		encode     bool
		metadata   bool
		unreadable bool
		readError  bool
	}
}
