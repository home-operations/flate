package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_DataReader_GeneratorEmbeddedCiphertext(t *testing.T) {
	for _, kind := range []string{"secretGenerator", "configMapGenerator"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			refKind := "Secret"
			if kind == "configMapGenerator" {
				refKind = "ConfigMap"
			}
			testutil.WriteFile(t, root, "flux/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: ./apps
  postBuild: {substituteFrom: [{kind: %s, name: creds}]}
`, refKind))
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux]\n"+kind+":\n- name: creds\n  namespace: flux-system\n  files: [VALUE=values.yaml]\n")
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "values.yaml", "enabled: ENC[AES256_GCM,data:fixture,type:str]\n")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE}'}\n")
			for _, workers := range []string{"2", "8"} {
				stdout, stderr, code := runCLIBuffers("build", "ks", "consumer", "--path", root, "--concurrency", workers, "--cache-dir", t.TempDir(), "--no-progress")
				if code != 0 || !strings.Contains(stdout, "..PLACEHOLDER_VALUE..") || strings.Contains(stdout, "ENC[") {
					t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			}
		})
	}
}

func TestE2E_DataReader_StringDataPrecedence(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: ./apps
  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}
`)
	testutil.WriteFile(t, root, "secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: creds, namespace: flux-system}\ndata: {VALUE: QQ==, EMPTY: invalid-base64!}\nstringData: {VALUE: B, EMPTY: ''}\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE}', empty: '${EMPTY:=fallback}'}\n")
	stdout, stderr, code := runCLIBuffers("build", "ks", "consumer", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
	if code != 0 || !strings.Contains(stdout, "value: B") || !strings.Contains(stdout, "empty: fallback") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}
