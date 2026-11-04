package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseConfigMap_UnreadableCiphertext(t *testing.T) {
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
	t.Run("unreadable", func(t *testing.T) {
		t.Parallel()
		result1, err := ParseDoc(mustYAML(t, src), ParseDocOptions{DefaultNamespace: ""})
		cmobj := result1.Object
		cm := cmobj.(*ConfigMap)
		assert.FatalIf(t, err != nil,
			"parseConfigMap: %v", err)

		assert.Diff(t, cm.Data, map[string]any{
			"PLAINTEXT": "keep-me",
		})
	})
}
