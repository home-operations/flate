package manifest

import (
	"encoding/base64"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseSecret(t *testing.T) {
	t.Parallel()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"
	yamlDoc := `
apiVersion: v1
kind: Secret
metadata: {name: creds, namespace: flux-system}
data:
  password: dGVzdA==
  cosign.pub: dGhpcy1pcy1ub3QtYS1zZWNyZXQ=
  enc-blob: ` + sops + `
stringData:
  apiKey: real-key
  enc-key: ` + sops + `
`
	t.Run("unreadable absent, plaintext preserved", func(t *testing.T) {
		t.Parallel()
		result1, err := ParseDoc(mustYAML(t, yamlDoc), ParseDocOptions{DefaultNamespace: ""})
		sobj := result1.Object
		s := sobj.(*Secret)
		assert.FatalIf(t, err != nil,
			"parseSecret: %v", err)
		assert.ErrorIf(t, s.Data["password"] != "dGVzdA==",
			"plaintext .data removed: %v", s.Data["password"])
		assert.ErrorIf(t, s.Data["cosign.pub"] != "dGhpcy1pcy1ub3QtYS1zZWNyZXQ=",
			"public key removed: %v", s.Data["cosign.pub"])
		assert.ErrorIf(t, s.StringData["apiKey"] != "real-key",
			"plaintext .stringData removed: %v", s.StringData["apiKey"])

		if _, present := s.Data["enc-blob"]; present {
			t.Errorf("SOPS .data not removed: %v", s.Data["enc-blob"])
		}
		if _, present := s.StringData["enc-key"]; present {
			t.Errorf("SOPS .stringData not removed: %v", s.StringData["enc-key"])
		}
	})

	t.Run("base64-embedded YAML preserved", func(t *testing.T) {
		t.Parallel()
		valuesYAML := "mariadb:\n  enabled: " + sops + "\nwordpressSkipInstall: " + sops + "\n"
		doc := mustYAML(t, `
apiVersion: v1
kind: Secret
metadata: {name: wordpress-values, namespace: default}
data:
  values.yaml: `+base64.StdEncoding.EncodeToString([]byte(valuesYAML))+`
`)
		result2, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: ""})
		sobj := result2.Object
		s := sobj.(*Secret)
		assert.FatalIf(t, err != nil,
			"parseSecret: %v", err)

		wantB64 := base64.StdEncoding.EncodeToString([]byte(valuesYAML))
		assert.ErrorIf(t, s.Data["values.yaml"] != wantB64,
			"embedded YAML changed: %v", s.Data["values.yaml"])
	})
}

func TestParseSecret_Idempotent(t *testing.T) {
	t.Parallel()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"
	yamlDoc := `
apiVersion: v1
kind: Secret
metadata: {name: s, namespace: ns}
data:
  token: c2VjcmV0
  enc: ` + sops + `
stringData:
  apiKey: plaintext
`
	doc := mustYAML(t, yamlDoc)
	result3, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: ""})
	s1obj := result3.Object
	s1 := s1obj.(*Secret)

	assert.FatalIf(t, err != nil, "first parse: %v", err)

	result4, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: ""})
	s2obj := result4.Object
	s2 := s2obj.(*Secret)

	assert.FatalIf(t, err != nil, "second parse: %v", err)
	assert.ErrorIf(t, s1.Data["enc"] != s2.Data["enc"] || s1.Data["token"] != s2.Data["token"],
		"data not idempotent: first=%v second=%v", s1.Data, s2.Data)
	assert.ErrorIf(t, s1.StringData["apiKey"] != s2.StringData["apiKey"],
		"stringData not idempotent: first=%q second=%q", s1.StringData["apiKey"], s2.StringData["apiKey"])

	gotData := doc["data"].(map[string]any)

	assert.ErrorIf(
		t,
		gotData["token"] != "c2VjcmV0" || gotData["enc"] != sops,
		"parseSecret mutated doc[\"data\"]: %v",
		gotData,
	)

	{
		got := doc["stringData"].(map[string]any)["apiKey"]
		assert.ErrorIf(
			t,
			got != "plaintext",
			"parseSecret mutated doc[\"stringData\"]: got %q want %q",
			got,
			"plaintext",
		)
	}
}
