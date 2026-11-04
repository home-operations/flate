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
	result1, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: ""})
	cmobj := result1.Object
	cm := cmobj.(*ConfigMap)

	assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)

	assert.Diff(t, cm.Data, map[string]any{"DOMAIN": "example.com"})
}

// TestParseConfigMap_WipesSopsCiphertext pins the offline-SOPS fix: a
// SOPS-encrypted ConfigMap (commonly a postBuild.substituteFrom source)
// must surface placeholders, not raw ciphertext, when wipeSecrets is set
// (the default). flate can't decrypt, and the `:` inside ENC[...]
// otherwise leaks into envsubst and trips chart validation (Ingress
// hosts, cert-manager dnsNames). Cleartext entries are left untouched,
// and wipeSecrets=false preserves the ciphertext to track Secret wiping.
