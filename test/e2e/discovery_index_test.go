package e2e

import (
	"fmt"
	"github.com/home-operations/flate/internal/testutil"
	"path/filepath"
	"strings"
	"testing"
)

func TestE2E_DiscoveryIndex_ScopedRawFileFallback(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}}
`)
			testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: raw, namespace: flux-system}
spec: {interval: 10m, path: ./shared, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: nsprod, namespace: flux-system}
spec: {interval: 10m, path: ./overlay, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: other, namespace: other}
spec:
  interval: 10m
  path: ./shared
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  patches:
    - target: {kind: ConfigMap}
      patch: |-
        - op: replace
          path: /does/not/exist
          value: x
`)
			testutil.WriteFile(t, root, "shared/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - settings.yaml
`)
			testutil.WriteFile(t, root, "shared/settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: settings}
data: {K: v}
`)
			testutil.WriteFile(t, root, "overlay/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: flux-system
resources:
  - ../shared
`)
			testutil.WriteFile(t, root, "consumer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`)
			testutil.WriteFile(t, root, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: out, namespace: default}
data: {v: "${K}"}
`)
			_, stderr, code := runCLIBuffers("build", "ks", "consumer", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if code != 1 || !strings.Contains(stderr, "other") {
				t.Fatalf("exit=%d stderr=%s", code, stderr)
			}
		})
	}
}

func TestE2E_DiscoveryIndex_ChangedOnlyFileBackedProducers(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := t.TempDir()
			orig := t.TempDir()
			for _, dir := range []string{root, orig} {
				testutil.WriteFile(t, dir, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}}
`)
				testutil.WriteFile(t, dir, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: othercm}]}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: producer, namespace: flux-system}
spec: {interval: 10m, path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}
`)
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: flux-system
resources:
  - other
`)
				testutil.WriteFile(t, dir, "apps/other/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`)
				testutil.WriteFile(t, dir, "apps/other/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: othercm, namespace: default}
data: {K: v}
`)
				testutil.WriteFile(t, dir, "consumer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`)
				testutil.WriteFile(t, dir, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: out, namespace: default}
data: {v: "${K}"}
`)
			}
			testutil.WriteFile(t, root, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out2, namespace: default}\ndata: {v: \"${K}\"}\n")
			stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--path-orig", orig, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if code != 0 || !strings.Contains(stdout, "name: out2") || !strings.Contains(stdout, "v: v") {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestE2E_BootstrapGeneratorFailureIsolation(t *testing.T) {
	for _, input := range []string{"files: [missing.txt]", "files: [OUT=../../../outside.txt]"} {
		t.Run(input, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "repo")
			testutil.WriteFile(t, parent, "outside.txt", "OUTSIDE-CONTENT")
			testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: good, namespace: flux-system}
spec: {path: ./apps/good}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: bad, namespace: flux-system}
spec: {path: ./apps/bad}
`)
			testutil.WriteFile(t, root, "apps/good/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/good/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: good-output}\n")
			testutil.WriteFile(t, root, "apps/bad/kustomization.yaml", "configMapGenerator:\n- name: bad\n  "+input+"\n")
			for _, workers := range []string{"2", "8"} {
				stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--concurrency", workers, "--cache-dir", t.TempDir(), "--no-progress")
				if code != 1 || !strings.Contains(stdout, "good-output") || !strings.Contains(stderr, "bad") || !strings.Contains(stderr, "file sources:") {
					t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				if strings.Contains(stdout, "OUTSIDE-CONTENT") {
					t.Fatal("generator exposed an outside file")
				}
			}
		})
	}
}

func TestE2E_BootstrapGeneratorSopsSubstitution(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: ./apps/b
  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}
`)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux, apps/a]\n")
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "secretGenerator:\n- name: creds\n  namespace: flux-system\n  envs: [creds.sops.env]\n")
	testutil.WriteFile(t, root, "apps/a/creds.sops.env", "API_KEY=ENC[AES256_GCM,data:abcd1234,iv:xyz,tag:tt,type:str]\nsops_version=3.8.1\nsops_mac=ENC[fixture]\n")
	testutil.WriteFile(t, root, "apps/b/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/b/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {key: '${API_KEY}'}\n")
	for _, workers := range []string{"2", "8"} {
		stdout := runCLIStdout(t, "build", "ks", "consumer", "--path", root, "--concurrency", workers, "--cache-dir", t.TempDir(), "--no-progress")
		if !strings.Contains(stdout, "..PLACEHOLDER_API_KEY..") || strings.Contains(stdout, "ENC[") {
			t.Fatalf("ciphertext substitution: %s", stdout)
		}
	}
}

func TestE2E_SourceRef_OCIArtifactRoot(t *testing.T) {
	for _, tc := range []struct {
		name, path, diagnostic, kind string
		later                        bool
	}{
		{name: "subtree", path: "./apps/x"},
		{name: "bucket_subtree", path: "./apps/x", kind: "Bucket"},
		{name: "unsupported", path: "kubernetes/apps/x", kind: "ExternalArtifact", diagnostic: "ExternalArtifact"},
		{name: "repository", path: "kubernetes/apps/x"},
		{name: "unknown", path: "./absent", diagnostic: "unknown OCI artifact root"},
		{name: "frozen", path: "./apps/x", diagnostic: "kustomization path is not a directory", later: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind := tc.kind
			if kind == "" {
				kind = "OCIRepository"
			}
			root := t.TempDir()
			repo := gitInit(t, root)
			testutil.WriteFile(t, root, "kubernetes/clusters/c0/ks.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: bootstrap, namespace: flux-system}
spec: {path: %s, sourceRef: {kind: %s, name: flux-system}}
`, tc.path, kind))
			if tc.diagnostic == "" {
				consumerPath := "./consumer"
				if tc.name == "repository" {
					consumerPath = "kubernetes/consumer"
				}
				testutil.WriteFile(t, root, "kubernetes/clusters/c0/consumer.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: %s
  sourceRef: {kind: %s, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: artifact-output}]}
`, consumerPath, kind))
				testutil.WriteFile(t, root, "kubernetes/consumer/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "kubernetes/consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output, namespace: flux-system}\ndata: {value: '${K}'}\n")
				testutil.WriteFile(t, root, "apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-sibling}\n")
			}
			resources := "resources: [cm.yaml]\n"
			if tc.later {
				resources = "resources: [cm.yaml, child.yaml]\n"
				testutil.WriteFile(t, root, "kubernetes/apps/x/child.yaml", fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system}
spec: {path: ./root-only, sourceRef: {kind: %s, name: flux-system}}
`, kind))
				testutil.WriteFile(t, root, "root-only/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "root-only/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-root-output}\n")
			}
			testutil.WriteFile(t, root, "kubernetes/apps/x/kustomization.yaml", resources)
			testutil.WriteFile(t, root, "kubernetes/apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output, namespace: flux-system}\ndata: {K: before}\n")
			gitCommitAll(t, repo)
			orig := copyTree(t, root)
			origScan := filepath.Join(orig, "kubernetes/clusters/c0")
			testutil.WriteFile(t, root, "kubernetes/apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output, namespace: flux-system}\ndata: {K: after}\n")
			scan := filepath.Join(root, "kubernetes/clusters/c0")
			var rendered string
			cache := t.TempDir()
			for _, workers := range []string{"2", "8"} {
				for _, mode := range []string{"build", "test", "diff"} {
					if tc.name == "unsupported" && mode != "diff" {
						continue
					}
					args := []string{mode, "all", "--path", scan, "--concurrency", workers, "--cache-dir", cache, "--no-progress"}
					if mode == "diff" {
						args = append(args, "--path-orig", origScan)
					}
					stdout, stderr, code := runCLIBuffers(args...)
					if tc.name == "unknown" && mode == "diff" {
						if code != 0 || stdout != "" || !strings.Contains(stderr, "outside the local tree; not rendered") {
							t.Fatalf("unknown diff exit=%d stdout=%s stderr=%s", code, stdout, stderr)
						}
						continue
					}
					if tc.diagnostic != "" {
						if code != 1 || !strings.Contains(stdout+stderr, tc.diagnostic) {
							t.Fatalf("%s exit=%d stdout=%s stderr=%s", mode, code, stdout, stderr)
						}
						if tc.later && mode == "build" && !strings.Contains(stdout, "artifact-output") {
							t.Fatalf("parent did not render: %s", stdout)
						}
						if strings.Contains(stdout, "unrelated-root-output") {
							t.Fatal("frozen root moved")
						}
						continue
					}
					if code != 0 {
						t.Fatalf("%s exit=%d stdout=%s stderr=%s", mode, code, stdout, stderr)
					}
					switch mode {
					case "build":
						if !strings.Contains(stdout, "artifact-output") || !strings.Contains(stdout, "after") {
							t.Fatalf("missing artifact bytes: %s", stdout)
						}
						if rendered != "" && rendered != stdout {
							t.Fatal("render changed with concurrency/cache")
						}
						rendered = stdout
					case "test":
						if !strings.Contains(stdout+stderr, "3 passed") {
							t.Fatalf("test did not evaluate content: %s %s", stdout, stderr)
						}
					case "diff":
						if strings.Count(stdout, "- before\n") != 2 || strings.Count(stdout, "+ after\n") != 2 {
							t.Fatalf("forward artifact/consumer values: %s", stdout)
						}
					}
				}
			}
			if tc.diagnostic == "" {
				args := []string{"build", "all", "--path", scan, "--path-orig", origScan, "--concurrency", "2", "--no-progress"}
				stdout, stderr, code := runCLIBuffers(args...)
				if code != 0 || !strings.Contains(stdout, "consumer-output") || !strings.Contains(stdout, "value: after") || strings.Contains(stdout, "unrelated-sibling") {
					t.Fatalf("changed subtree exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				stdout, stderr, code = runCLIBuffers("diff", "all", "--path", filepath.Join(orig, "kubernetes/clusters/c0"), "--path-orig", scan, "--concurrency", "2", "--no-progress")
				if code != 0 {
					t.Fatalf("reverse diff exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				if strings.Count(stdout, "- after\n") != 2 || strings.Count(stdout, "+ before\n") != 2 {
					t.Fatalf("reverse artifact/consumer values: %s", stdout)
				}
				testutil.WriteFile(t, orig, "apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty-unrelated-sibling}\n")
				clean := copyTree(t, orig)
				testutil.WriteFile(t, orig, "apps/x/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: another-unrelated-sibling}\n")
				stdout, stderr, code = runCLIBuffers("build", "all", "--path", filepath.Join(orig, "kubernetes/clusters/c0"), "--path-orig", filepath.Join(clean, "kubernetes/clusters/c0"), "--concurrency", "2", "--no-progress")
				if code != 0 || strings.Contains(stdout, "consumer-output") || strings.Contains(stdout, "artifact-output") || strings.Contains(stdout, "unrelated-sibling") {
					t.Fatalf("unrelated edit exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			}

		})
	}
}
