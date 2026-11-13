package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SkipKindsPolicy(t *testing.T) {
	for _, kind := range []string{"helm", "kustomize"} {
		t.Run(kind, func(t *testing.T) {
			deployment := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: retained, namespace: apps}\nspec: {replicas: 2}\n"
			omitted := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: omitted, namespace: apps}\n---\napiVersion: v1\nkind: Secret\nmetadata: {name: omitted, namespace: apps}\n---\napiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: omitted.example.test}\nspec: {group: example.test, names: {kind: Omitted, plural: omitted}, scope: Namespaced, versions: [{name: v1, served: true, storage: true, schema: {openAPIV3Schema: {type: object}}}]}\n"
			var root, scan, changed string
			if kind == "helm" {
				root = writeHelmOptionsFixture(t, deployment+"---\n"+omitted)
				scan, changed = "cluster", "charts/app/templates/main.yaml"
				release, readErr := os.ReadFile(filepath.Join(root, "cluster", "hr.yaml"))
				assert.NoError(t, readErr)
				testutil.WriteFile(
					t,
					root,
					"cluster/hr.yaml",
					strings.Replace(string(release), "  values: {tag: after}\n", "", 1),
				)
			} else {
				root = writeFixture(t)
				testutil.GitInit(t, root)
				scan, changed = "flux", "apps/deployment.yaml"
				testutil.WriteFile(
					t,
					root,
					"apps/kustomization.yaml",
					"resources: [cm.yaml, deployment.yaml, omitted.yaml]\n",
				)
				testutil.WriteFile(t, root, changed, deployment)
				testutil.WriteFile(t, root, "apps/omitted.yaml", omitted)
			}
			original := t.TempDir()
			testutil.CopyTree(t, root, original)
			data, err := os.ReadFile(filepath.Join(original, changed))
			assert.NoError(t, err)
			testutil.WriteFile(t, original, changed, strings.Replace(string(data), "replicas: 2", "replicas: 1", 1))
			fixture := skipKindsPolicyFixture{kind: kind, head: root, base: original, scan: scan}
			for _, mode := range []string{"full", "changed", "diff", "reversed diff"} {
				t.Run(mode, func(t *testing.T) {
					fixture.run(t, mode)
				})
			}
		})
	}
}

type skipKindsPolicyFixture struct{ kind, head, base, scan string }

func (f *skipKindsPolicyFixture) run(t *testing.T, mode string) {
	t.Helper()
	head, base := f.head, f.base
	if mode == "reversed diff" {
		head, base = base, head
	}
	args := []string{
		"build",
		"all",
		"--path",
		filepath.Join(head, f.scan),
		"--concurrency=2",
		"-o",
		"json",
	}
	if mode != "full" {
		args = append(args, "--path-orig", filepath.Join(base, f.scan))
	}
	if strings.Contains(mode, "diff") {
		args[0] = "diff"
	}
	domain, parentKind, parentName, parentNamespace, parentPath := "helm.toolkit.fluxcd.io", "HelmRelease", "app", "apps", ""
	if f.kind == "kustomize" {
		domain, parentKind, parentName, parentNamespace, parentPath = "kustomize.toolkit.fluxcd.io", "Kustomization", "apps", "flux-system", "apps"
	}
	var documents [2]string
	for side, replicas := range []int{1, 2} {
		documents[side] = fmt.Sprintf(`{
  "apiVersion": "apps/v1",
  "kind": "Deployment",
  "metadata": {
    "labels": {
      %q: %q,
      %q: %q
    },
    "name": "retained",
    "namespace": "apps"
  },
  "spec": {
    "replicas": %d
  }
}`, domain+"/name", parentName, domain+"/namespace", parentNamespace, replicas)
	}
	expected := cliResult{stdout: "[\n  " + strings.ReplaceAll(documents[1], "\n", "\n  ") + "\n]\n"}
	if strings.Contains(mode, "diff") {
		if mode == "reversed diff" {
			documents[0], documents[1] = documents[1], documents[0]
		}
		expected.stdout = fmt.Sprintf(`{
  "changes": [
    {
      "parent": {
        "group": %q,
        "kind": %q,
        "namespace": %q,
        "name": %q,
        "path": %q
      },
      "apiVersion": "apps/v1",
      "kind": "Deployment",
      "namespace": "apps",
      "name": "retained",
      "status": "changed",
      "old": %s,
      "new": %s,
      "oldChart": "",
      "newChart": ""
    }
  ],
  "suppressed": []
}
`, domain, parentKind, parentNamespace, parentName, parentPath, strings.ReplaceAll(documents[0], "\n", "\n      "), strings.ReplaceAll(documents[1], "\n", "\n      "))
	}
	if renderPersistenceDisabledReason != "" {
		expected.stderr = "\n  ⚠ warnings (1)\n    [RenderPersistenceDisabled] render persistence is disabled: " + renderPersistenceDisabledReason + "; source caching remains available\n"
	}
	for _, policy := range []string{"ConfigMap,CustomResourceDefinition,Secret", "Secret,ConfigMap,CustomResourceDefinition", "Secret,CustomResourceDefinition,ConfigMap,Secret,CustomResourceDefinition"} {
		cacheDir := t.TempDir()
		command := append(slices.Clone(args), "--skip-kinds="+policy, "--cache-dir", cacheDir)
		fresh := runCLI(t, command...)
		assert.Equal(t, fresh.code, 0)
		assert.FatalIf(
			t,
			fresh.stdout == "" || strings.Contains(fresh.stdout, "omitted"),
			"policy result: %+v",
			fresh,
		)

		assert.Equal(t, fresh, expected)
		populated := runCLI(t, command...)
		assert.Equal(t, populated, fresh)
	}
	t.Logf("complete policy output: %q", expected.stdout)
	t.Logf("diagnostic: %q", expected.stderr)
}
