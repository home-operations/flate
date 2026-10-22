// Package crds tests resource expansion and CRD availability.
package crds

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ResourceSetExpandsIntoChildKustomizations(t *testing.T) {
	root := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "resourceset"), root)
	inputFile := filepath.Join(root, "tenants", "apps.yaml")
	body, err := os.ReadFile(inputFile)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "tenants/apps.yaml", strings.Replace(string(body), "  inputs:", "  inputsFrom: [{name: branches}]\n  inputs:", 1))
	testutil.WriteFile(t, root, "tenants/provider.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: branches, namespace: flux-system}\nspec: {type: GitHubBranch, url: https://example.invalid/branches}\n")
	testutil.WriteFile(t, root, "tenants/kustomization.yaml", "resources: [apps.yaml, provider.yaml]\n")
	var out string
	for _, verb := range []string{"get", "build", "test"} {
		for range 2 {
			args := []string{verb, "all", "--path", root, "--concurrency=2", "--allow-missing-crds"}
			if verb == "get" {
				args[1] = "ks"
			}
			stdout, stderr := harness.RequireCLIOK(t, args...)
			if strings.Count(stderr, "[OfflineInputProvider]") != 1 || !strings.Contains(stderr, "branches") || strings.Contains(stdout, "warnings (") {
				t.Fatalf("provider warning: stdout=%s stderr=%s", stdout, stderr)
			}
			if verb == "get" {
				out = stdout
			}
			if verb == "build" && (!strings.Contains(stdout, "frontend") || !strings.Contains(stdout, "backend")) || verb == "test" && !strings.Contains(stdout, "passed") {
				t.Fatalf("provider product: %s", stdout)
			}
		}
	}
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

func TestE2E_ResourceSetDocumentBoundary(t *testing.T) {
	path := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "resourceset-documents"), path)
	args := []string{"build", "all", "--path", path, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	var previous string
	for range 2 {
		out, _ := harness.RequireCLIOK(t, args...)
		counts := map[string]int{}
		decoder := yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 2048)
		for {
			var doc map[string]any
			err := decoder.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
			name, ns := manifest.DocMetadata(doc)
			if name == "raw" || name == "duplicate" {
				assert.Equal(t, ns, "")
			}
			if name == "explicit" {
				assert.Equal(t, ns, "other")
			}
			if name == "sanitized" {
				assert.Diff(t, doc["data"], map[string]any{"encrypted": "..PLACEHOLDER_encrypted.."})
			}
			if name == "kindless-list" {
				assert.Equal(t, manifest.DocKind(doc), "")
				assert.Equal(t, manifest.DocAPIVersion(doc), "")
			}
			counts[name]++
			if data, ok := doc["data"].(map[string]any); ok {
				counts[fmt.Sprint(data["value"])]++
			}
			if spec, ok := doc["spec"].(map[string]any); ok {
				counts[fmt.Sprint(spec["value"])]++
			}
			if manifest.DocKind(doc) == "List" {
				counts["wrapper"]++
			}
			if manifest.DocAPIVersion(doc) == "kustomize.config.k8s.io/v1beta1" {
				counts["directive"]++
			}
		}
		for key, want := range map[string]int{"raw": 1, "duplicate": 2, "nameless-json": 1, "nameless-list": 1, "kindless-list": 1, "wrapper": 2, "directive": 2, "explicit": 1, "sanitized": 1, "nested": 0, "item-directive": 1, "child-result": 1, "rejected-yaml": 0, "rejected-directive": 0, "child": 0} {
			if counts[key] != want {
				t.Errorf("%s count=%d, want %d; all=%v", key, counts[key], want, counts)
			}
		}
		if previous != "" {
			assert.Equal(t, out, previous)
		}
		previous = out
	}

	original := t.TempDir()
	testutil.CopyTree(t, path, original)
	body, err := os.ReadFile(filepath.Join(path, "apps/rs.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, path, "apps/rs.yaml", strings.ReplaceAll(string(body), "metadata: {name: duplicate, annotations: {fluxcd.controlplane.io/reconcile: disabled}}", "metadata: {name: duplicate, annotations: {fluxcd.controlplane.io/reconcile: disabled}}\n      spec: {replicas: 2}"))
	var previousDiff, previousWarnings string
	for run := range 2 {
		product, diagnostics := harness.RequireCLIOK(t, "diff", "all", "--path", path, "--path-orig", original, "--output=json", "--concurrency=2")
		if strings.Count(diagnostics, "[DuplicateDocument]") != 2 || !strings.Contains(diagnostics, "duplicate") || !strings.Contains(diagnostics, "orig") || !strings.Contains(diagnostics, "current") || strings.Contains(product, "[DuplicateDocument]") {
			t.Fatalf("duplicate diff: stdout=%s stderr=%s", product, diagnostics)
		}
		var changes struct {
			Changes    []map[string]any
			Suppressed []any
		}
		assert.NoError(t, json.Unmarshal([]byte(product), &changes))
		if len(changes.Changes) == 0 || len(changes.Suppressed) != 0 {
			t.Fatalf("duplicate product changed: %s", product)
		}
		if run == 0 {
			previousDiff, previousWarnings = product, diagnostics
		} else {
			assert.Equal(t, product, previousDiff)
			assert.Equal(t, diagnostics, previousWarnings)
		}
	}

	testutil.WriteFile(t, path, "apps/rs.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: documents, namespace: flux-system}\nspec:\n resourcesTemplate: |\n  items: [null]\n")
	out, code := harness.RunCLIExpectErr(t, args...)
	assert.Equal(t, code, 1)
	if !strings.Contains(out, "read rendered list") {
		t.Fatalf("malformed member failure missing: %s", out)
	}
}
