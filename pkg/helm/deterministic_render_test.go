package helm

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestTemplate_Deterministic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml",
		"apiVersion: v2\nname: mychart\nversion: 0.1.0\ndescription: t\n")
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
`)

	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	hr := newHR()
	values := map[string]any{"ports": map[string]any{"http": 80, "bittorrent": 50413}}
	first, err := cli.Template(t.Context(), hr, values, Options{})
	assert.NoError(t, err)
	second, err := cli.Template(t.Context(), hr, values, Options{})
	assert.NoError(t, err)
	assert.Equal(t, second, first)
	docs, err := manifest.SplitDocs([]byte(first))
	assert.NoError(t, err)
	if len(docs) != 1 {
		t.Fatalf("rendered %d documents, want 1", len(docs))
	}
	data := docs[0]["data"].(map[string]any)
	assert.Equal(t, data["stamp"], any("20200101000000.000000000"))
	assert.Equal(t, data["port"], any("50413"))
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
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	for _, plaintext := range []string{"", "hello", strings.Repeat("a", 16), "Unicode: βé"} {
		t.Run(plaintext, func(t *testing.T) {
			t.Parallel()
			out, err := cli.Template(t.Context(), newHR(), map[string]any{"plaintext": plaintext}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out, fmt.Sprintf("cleartext: %q", plaintext)) {
				t.Fatalf("decryptAES round trip differs: %s", out)
			}
		})
	}
}
