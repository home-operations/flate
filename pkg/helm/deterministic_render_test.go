package helm

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestTemplate_Deterministic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
	testutil.WriteFile(t, dir, "mychart/templates/_helpers.tpl", `{{- define "mychart.deterministic" -}}
{{- tpl .Values.deterministic . -}}
{{- end -}}`)
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
  annotations:
    checksum/secret: {{ randAlphaNum 24 | sha256sum | quote }}
    checksum/shuffled-secret: {{ printf "%s%s" (randAlphaNum 8) (randAscii 12) | shuffle | sha256sum | quote }}
data:
  stamp: {{ now | date "20060102150405.000000000" | quote }}
  token: {{ randAlphaNum 24 | quote }}
  id: {{ uuidv4 | quote }}
  bcrypt: {{ bcrypt "password" | quote }}
  htpasswd: {{ htpasswd "user" "password" | quote }}
  aes: {{ encryptAES "key" "plaintext" | quote }}
  pw: {{ "abcdefghijklmnopqrstuvwxyz" | shuffle | quote }}
  {{- $ca := genCA "my-ca" 3650 }}
  {{- $cert := genSignedCert "svc" nil (list "svc.default.svc") 365 $ca }}
  ca.crt: {{ $ca.Cert | b64enc | quote }}
  tls.crt: {{ $cert.Cert | b64enc | quote }}
  tls.key: {{ $cert.Key | b64enc | quote }}
  port: {{ get .Values.ports (keys .Values.ports | first) | quote }}
  parsed-date: {{ toDate "2006-01-02" "2020-01-01" | unixEpoch | quote }}
  parsed-must-date: {{ mustToDate "2006-01-02" "2020-01-01" | unixEpoch | quote }}
  rounded-now: {{ now | durationRound | quote }}
  rounded-past: {{ now | dateModify "-2h" | durationRound | quote }}
  ordered-values: {{ values (dict "z" "first" "a" "last" "m" "middle") | join "," | quote }}
  nested: {{ include "mychart.deterministic" . | quote }}
`)

	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)

	hr := newHR()
	values := map[string]any{
		"ports": map[string]any{"http": 80, "bittorrent": 50413},
		"deterministic": `{{ toDate "2006-01-02" "2020-01-01" | unixEpoch }}|` +
			`{{ mustToDate "2006-01-02" "2020-01-01" | unixEpoch }}|` +
			`{{ now | durationRound }}|{{ now | dateModify "-2h" | durationRound }}|` +
			`{{ values (dict "z" "first" "a" "last" "m" "middle") | join "," }}`,
	}
	first, err := cli.Template(t.Context(), hr, values, RenderOptions{Options: Options{}, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
	assert.NoError(t, err)
	second, err := cli.Template(t.Context(), hr, values, RenderOptions{Options: Options{}, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
	assert.NoError(t, err)
	assert.Diff(t, second, first)
	for _, docs := range [][]map[string]any{first, second} {
		if len(docs) != 1 {
			t.Fatalf("rendered %d documents, want 1", len(docs))
		}
		data := docs[0]["data"].(map[string]any)
		assert.Equal(t, data["stamp"], any("20200101000000.000000000"))
		assert.Equal(t, data["port"], any("50413"))
		assert.Equal(t, data["parsed-date"], any("1577836800"))
		assert.Equal(t, data["parsed-must-date"], any("1577836800"))
		assert.Equal(t, data["rounded-now"], any("0s"))
		assert.Equal(t, data["rounded-past"], any("2h"))
		assert.Equal(t, data["ordered-values"], any("last,middle,first"))
		assert.Equal(t, data["nested"], any("1577836800|1577836800|0s|2h|last,middle,first"))
	}
}

func TestTemplate_InvalidMustToDate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
data:
  parsed: {{ mustToDate "2006-01-02" "invalid" | unixEpoch | quote }}
`)
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)

	out, err := cli.Template(t.Context(), newHR(), nil, RenderOptions{Options: Options{}, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
	if err == nil {
		t.Error("invalid mustToDate rendered successfully")
	}
	assert.Diff(t, out, []map[string]any(nil))
}

func TestTemplate_EncryptAESDecryptRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm\ndata:\n  cleartext: {{ encryptAES \"key\" .Values.plaintext | decryptAES \"key\" | quote }}\n")
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatal(err)
	}

	for _, plaintext := range []string{"", "hello", strings.Repeat("a", 16), "Unicode: βé"} {
		t.Run(plaintext, func(t *testing.T) {
			t.Parallel()
			out, err := cli.Template(t.Context(), newHR(), map[string]any{"plaintext": plaintext}, RenderOptions{Options: Options{}, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
			if err != nil {
				t.Fatal(err)
			}
			if !hasRenderField(out, "cleartext", plaintext) {
				t.Fatalf("decryptAES round trip differs: %s", out)
			}
		})
	}
}
