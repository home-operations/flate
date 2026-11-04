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
	t.Run("sops wiped, plaintext preserved", func(t *testing.T) {
		t.Parallel()
		testParseSecretSopsWipedPlaintextPreserved(
			t,
			&yamlDoc,
		)
	},
	)

	t.Run("preserved with wipeSecrets=false", func(t *testing.T) {
		t.Parallel()
		testParseSecretPreservedWithWipeSecretsFalse(
			t,
			&yamlDoc,
		)
	},
	)

	t.Run(
		"base64-embedded YAML preserved",
		func(t *testing.T) { t.Parallel(); testParseSecretBase64EmbeddedYAMLPreserved(t) },
	)
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

	parsed, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	s1obj := parsed.Object
	s1 := s1obj.(*Secret)
	assert.FatalIf(t, err != nil, "first parse: %v", err)
	parsed2, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	s2obj := parsed2.Object
	s2 := s2obj.(*Secret)
	assert.FatalIf(t, err != nil, "second parse: %v", err)

	if s1.Data["enc"] != s2.Data["enc"] || s1.Data["token"] != s2.Data["token"] {
		t.Errorf("data not idempotent: first=%v second=%v", s1.Data, s2.Data)
	}
	if s1.StringData["apiKey"] != s2.StringData["apiKey"] {
		t.Errorf("stringData not idempotent: first=%q second=%q", s1.StringData["apiKey"], s2.StringData["apiKey"])
	}

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

func testParseSecretSopsWipedPlaintextPreserved(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	parsed3, err := ParseDoc(
		mustYAML(t, (*testCaseInput)),
		ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""},
	)
	sobj := parsed3.Object
	s := sobj.(*Secret)
	assert.FatalIf(t, err != nil, "parseSecret: %v", err)
	if s.Data["password"] != "dGVzdA==" {
		t.Errorf("plaintext .data wiped: %v", s.Data["password"])
	}
	if s.Data["cosign.pub"] != "dGhpcy1pcy1ub3QtYS1zZWNyZXQ=" {
		t.Errorf("public key wiped: %v", s.Data["cosign.pub"])
	}
	if s.StringData["apiKey"] != "real-key" {
		t.Errorf("plaintext .stringData wiped: %v", s.StringData["apiKey"])
	}
	wantB64 := base64.StdEncoding.EncodeToString([]byte(ValuePlaceholder("enc-blob")))
	if s.Data["enc-blob"] != wantB64 {
		t.Errorf("SOPS .data not wiped: %v", s.Data["enc-blob"])
	}
	if s.StringData["enc-key"] != ValuePlaceholder("enc-key") {
		t.Errorf("SOPS .stringData not wiped: %v", s.StringData["enc-key"])
	}
}

func testParseSecretPreservedWithWipeSecretsFalse(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"

	parsed4, err := ParseDoc(
		mustYAML(t, (*testCaseInput)),
		ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""},
	)
	sobj := parsed4.Object
	s := sobj.(*Secret)
	assert.FatalIf(t, err != nil, "parseSecret: %v", err)
	if s.Data["password"] != "dGVzdA==" || s.Data["enc-blob"] != sops {
		t.Errorf("value changed despite wipeSecrets=false: %v", s.Data)
	}
}

func testParseSecretBase64EmbeddedYAMLPreserved(t *testing.T) {
	t.Helper()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"

	valuesYAML := "mariadb:\n  enabled: " + sops + "\nwordpressSkipInstall: " + sops + "\n"
	doc := mustYAML(t, `
apiVersion: v1
kind: Secret
metadata: {name: wordpress-values, namespace: default}
data:
  values.yaml: `+base64.StdEncoding.EncodeToString([]byte(valuesYAML))+`
`)
	parsed5, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	sobj := parsed5.Object
	s := sobj.(*Secret)
	assert.FatalIf(t, err != nil, "parseSecret: %v", err)
	wantB64 := base64.StdEncoding.EncodeToString([]byte(valuesYAML))
	if s.Data["values.yaml"] != wantB64 {
		t.Errorf("embedded YAML changed: %v", s.Data["values.yaml"])
	}
}
