package substitution

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_DataReader_GeneratorEmbeddedCiphertext(t *testing.T) {
	const value = "enabled: ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]"
	for _, kind := range []string{"secretGenerator", "configMapGenerator"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			refKind := "Secret"
			if kind == "configMapGenerator" {
				refKind = "ConfigMap"
			}
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps\n  postBuild: {substituteFrom: [{kind: "+refKind+", name: creds}]}\n"))
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux]\ngeneratorOptions: {disableNameSuffixHash: true}\n"+kind+":\n- name: creds\n  namespace: flux-system\n  files: [VALUE=values.yaml]\n")
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
			testutil.WriteFile(t, root, "values.yaml", value+"\n")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata:\n  value: |\n    ${VALUE}\n")
			var first string
			for i := range 2 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "ks", "consumer", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
				if code != 0 || strings.Contains(stdout, "..PLACEHOLDER_VALUE..") {
					t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				docs, err := manifest.DecodeDocs(strings.NewReader(stdout))
				if err != nil {
					t.Fatal(err)
				}
				found := 0
				for _, doc := range docs {
					name, _ := manifest.DocMetadata(doc)
					if manifest.DocKind(doc) != "ConfigMap" || name != "result" {
						continue
					}
					found++
					data, _ := doc["data"].(map[string]any)
					if data["value"] != value+"\n" {
						t.Fatalf("value=%v, want %q", data["value"], value+"\n")
					}
				}
				if found != 1 {
					t.Fatalf("result ConfigMaps=%d, want 1", found)
				}
				if i == 1 && stdout != first {
					t.Fatal("repeated build changed stdout")
				}
				first = stdout
			}
		})
	}
}

func TestE2E_DataReader_StringDataPrecedence(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps\n  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}\n"))
	testutil.WriteFile(t, root, "secret.yaml", "apiVersion: v1\nkind: Secret\nmetadata: {name: creds, namespace: flux-system}\ndata: {VALUE: QQ==, EMPTY: invalid-base64!}\nstringData: {VALUE: B, EMPTY: ''}\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {value: '${VALUE}', empty: '${EMPTY:=fallback}'}\n")
	stdout, stderr, code := harness.RunCLIBuffers("build", "ks", "consumer", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
	if code != 0 || !strings.Contains(stdout, "value: B") || !strings.Contains(stdout, "empty: fallback") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}
