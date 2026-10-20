// Package crds tests resource expansion and CRD availability.
package crds

import (
	"encoding/base64"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ResourceSetExpandsIntoChildKustomizations(t *testing.T) {
	// The resourceset/ fixture has one ResourceSet that templates a
	// Namespace + a Kustomization per tenant (frontend, backend). After
	// loading, flate should expose the rendered child Kustomizations
	// via `get ks` alongside the static parent — confirming the
	// load-time expansion pass plumbed each rendered doc through the
	// store.
	out := harness.RunCLIOutputOnly(t, "get", "ks", "--path", harness.TestdataPath(t, "resourceset"))
	for _, want := range []string{
		"cluster-tenants", // parent KS from cluster/flux-system.yaml
		"apps-frontend",   // emitted by ResourceSet for tenant=frontend
		"apps-backend",    // emitted by ResourceSet for tenant=backend
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in ks listing:\n%s", want, out)
		}
	}
}

func TestE2E_ResourceSetGeneratedData(t *testing.T) {
	const (
		cm         = "ConfigMap/default/cm-one"
		rs         = "ResourceSet/flux-system/demo"
		sa         = "ServiceAccount/default/sa-one"
		secret     = "Secret/default/secret-one"
		ciphertext = "ENC[AES256_GCM,data:fixture,type:str]"
	)
	for _, tc := range []struct {
		name      string
		flags     []string
		want      []string
		encrypted bool
	}{
		{name: "all", flags: []string{"--skip-kinds="}, want: []string{cm, rs, secret, sa}},
		{name: "only-explicit-skips", flags: []string{"--skip-kinds=", "--only-kinds=ConfigMap,Secret"}, want: []string{cm, secret}},
		{name: "only-implicit-skips", flags: []string{"--only-kinds=ConfigMap,Secret"}, want: []string{cm, secret}},
		{name: "defaults", want: []string{cm, rs, sa}},
		{name: "skip-data", flags: []string{"--skip-kinds=ConfigMap,Secret"}, want: []string{rs, sa}},
		{name: "encrypted", flags: []string{"--skip-kinds="}, want: []string{cm, rs, secret, sa}, encrypted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [root.yaml]\n")
			testutil.WriteFile(t, root, "entry/root.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {interval: 1m, url: https://example.invalid/repo.git, ref: {branch: main}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  interval: 1m
  prune: true
  path: ./app
  sourceRef: {kind: GitRepository, name: flux-system}
`)
			testutil.WriteFile(t, root, "app/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: [resourceset.yaml]\n")
			fields, secretData := "{key: value}", ""
			wantFields := map[string]any{"key": "value"}
			if tc.encrypted {
				fields = fmt.Sprintf("{key: value, encrypted: '%s'}", ciphertext)
				secretData = fmt.Sprintf("      data: {plain: dmFsdWU=, encoded: %s}\n", base64.StdEncoding.EncodeToString([]byte(ciphertext)))
				wantFields["encrypted"] = "..PLACEHOLDER_encrypted.."
			}
			testutil.WriteFile(t, root, "app/resourceset.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: demo, namespace: flux-system}
spec:
  inputs:
    - name: one
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>, namespace: default}
      data: %s
    - apiVersion: v1
      kind: Secret
      metadata: {name: secret-<< inputs.name >>, namespace: default}
%s      stringData: %s
    - apiVersion: v1
      kind: ServiceAccount
      metadata: {name: sa-<< inputs.name >>, namespace: default}
`, fields, secretData, fields))
			args := append([]string{"build", "all", "--path", filepath.Join(root, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", t.TempDir()}, tc.flags...)
			stdout, _ := harness.RequireCLIOK(t, args...)
			if tc.name == "all" {
				again, _ := harness.RequireCLIOK(t, args...)
				assert.Equal(t, again, stdout)
			}
			docs, err := manifest.DecodeDocs(strings.NewReader(stdout))
			assert.NoError(t, err)
			identities := make([]string, 0, len(docs))
			for _, doc := range docs {
				name, namespace := manifest.DocMetadata(doc)
				identity := manifest.DocKind(doc) + "/" + namespace + "/" + name
				identities = append(identities, identity)
				switch identity {
				case cm:
					assert.Diff(t, doc["data"], wantFields)
				case secret:
					assert.Diff(t, doc["stringData"], wantFields)
					if tc.encrypted {
						assert.Diff(t, doc["data"], map[string]any{
							"plain":   "dmFsdWU=",
							"encoded": base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_encoded..")),
						})
					}
				}
			}
			slices.Sort(identities)
			assert.Diff(t, identities, tc.want)
		})
	}
}
