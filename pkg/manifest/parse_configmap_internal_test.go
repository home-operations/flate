package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseConfigMap(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-vars
  namespace: flux-system
data:
  DOMAIN: example.com
`)
	cmobj, _, _, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	cm := cmobj.(*ConfigMap)
	{
		assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)
	}
	assert.Diff(t, cm.Data, map[string]any{"DOMAIN": "example.com"})
}

// TestParseConfigMap_WipesSopsCiphertext pins the offline-SOPS fix: a
// SOPS-encrypted ConfigMap (commonly a postBuild.substituteFrom source)
// must surface placeholders, not raw ciphertext, when wipeSecrets is set
// (the default). flate can't decrypt, and the `:` inside ENC[...]
// otherwise leaks into envsubst and trips chart validation (Ingress
// hosts, cert-manager dnsNames). Cleartext entries are left untouched,
// and wipeSecrets=false preserves the ciphertext to track Secret wiping.
func TestParseConfigMap_WipesSopsCiphertext(t *testing.T) {
	t.Parallel()
	const ciphertext = "ENC[AES256_GCM,data:NWahjtvi/hiAX9sVkctDGcmWOn0=,iv:2fxQddhm7pwpnO23VkFFgPPEXudfa7TgI0oxUkAjZtA=,tag:rS3MK2jGptzB3RyWOj+Amg==,type:str]"
	src := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-config
  namespace: flux-system
data:
  BASE_DOMAIN: ` + ciphertext + `
  PLAINTEXT: keep-me
`
	t.Run("wiped", func(t *testing.T) {
		t.Parallel()
		cmobj, _, _, err := ParseDoc(mustYAML(t, src), ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
		cm := cmobj.(*ConfigMap)
		{
			assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": ValuePlaceholder("BASE_DOMAIN"),
			"PLAINTEXT":   "keep-me",
		})
	})
	t.Run("preserved when wipeSecrets=false", func(t *testing.T) {
		t.Parallel()
		cmobj, _, _, err := ParseDoc(mustYAML(t, src), ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""})
		cm := cmobj.(*ConfigMap)
		{
			assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": ciphertext,
			"PLAINTEXT":   "keep-me",
		})
	})
}
