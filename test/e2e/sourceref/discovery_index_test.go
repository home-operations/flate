package sourceref

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
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
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}
`)+"---\n"+
				testutil.KustomizationYAML("raw", "flux-system", "  {interval: 10m, path: ./shared, sourceRef: {kind: GitRepository, name: flux-system}}\n")+"---\n"+
				testutil.KustomizationYAML("nsprod", "flux-system", "  {interval: 10m, path: ./overlay, sourceRef: {kind: GitRepository, name: flux-system}}\n")+"---\n"+
				testutil.KustomizationYAML("other", "other", `  interval: 10m
  path: ./shared
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  patches:
    - target: {kind: ConfigMap}
      patch: |-
        - op: replace
          path: /does/not/exist
          value: x
`))
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
			stdout, stderr, code := harness.RunCLIBuffers("build", "ks", "consumer", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if code != 0 || !strings.Contains(stdout, "name: out") || !strings.Contains(stdout, "v: v") || strings.Contains(stderr, "other") {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			_, fullErr, fullCode := harness.RunCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if fullCode != 1 || !strings.Contains(fullErr, "other") {
				t.Fatalf("full render omitted unrelated producer failure: exit=%d stderr=%s", fullCode, fullErr)
			}
		})
	}
}

func TestE2E_DiscoveryIndex_ChangedOnlyFileBackedProducers(t *testing.T) {
	for _, list := range []bool{false, true} {
		t.Run(fmt.Sprintf("list=%t", list), func(t *testing.T) {
			const workers = "2"
			root := t.TempDir()
			orig := t.TempDir()
			for _, dir := range []string{root, orig} {
				testutil.GitInit(t, dir)
				testutil.WriteFile(t, dir, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}, sparseCheckout: [./apps, ./consumer]}
`)
				testutil.WriteFile(t, dir, "flux/ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: othercm}]}
`)+"---\n"+
					testutil.KustomizationYAML("producer", "flux-system", "  {interval: 10m, path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}\n"))
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
				cm := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: othercm, namespace: default}\ndata: {K: v}\n"
				if list {
					cm = "apiVersion: v1\nkind: List\nitems:\n- " + strings.ReplaceAll(strings.TrimSpace(cm), "\n", "\n  ") + "\n"
				}
				testutil.WriteFile(t, dir, "apps/other/cm.yaml", cm)
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
			fullArgs := []string{"build", "all", "--path", filepath.Join(root, "flux"), "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn"}
			full, stderr := harness.RequireCLIOK(t, fullArgs...)
			if !strings.Contains(full, "name: out") || !strings.Contains(full, "v: v") {
				t.Fatalf("full list consumer missing: %s\n%s", full, stderr)
			}
			if strings.Contains(stderr, "[") && (strings.Count(stderr, "[") != 1 || !strings.Contains(stderr, "[RenderPersistenceDisabled]")) {
				t.Fatalf("unexpected full warnings: %s", stderr)
			}
			again, repeatedStderr := harness.RequireCLIOK(t, fullArgs...)
			if strings.Contains(repeatedStderr, "[") && (strings.Count(repeatedStderr, "[") != 1 || !strings.Contains(repeatedStderr, "[RenderPersistenceDisabled]")) {
				t.Fatalf("unexpected repeated warnings: %s", repeatedStderr)
			}
			if again != full {
				t.Fatal("identical list builds changed output")
			}
			testutil.WriteFile(t, root, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out2, namespace: default}\ndata: {v: \"${K}\"}\n")
			wantValue := "v: v"
			if list {
				harness.MutateFile(t, filepath.Join(root, "apps/other/cm.yaml"), "K: v", "K: changed")
				wantValue = "v: changed"
			}
			stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"), "--path-orig", filepath.Join(orig, "flux"), "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if strings.Contains(stderr, "[") && (strings.Count(stderr, "[") != 1 || !strings.Contains(stderr, "[RenderPersistenceDisabled]")) {
				t.Fatalf("unexpected changed-only warnings: %s", stderr)
			}
			if code != 0 || !strings.Contains(stdout, "name: out2") || !strings.Contains(stdout, wantValue) {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestE2E_DiscoveryIndex_ChangedOnlyGeneratorInputs(t *testing.T) {
	for _, tc := range []struct {
		name, kind, generator, input, before, after string
	}{
		{"secret_env", "Secret", "secretGenerator:\n- name: settings\n  envs: [values.sops.env]\nconfigMapGenerator:\n- name: stable-settings\n  literals: [STABLE=unchanged]\n", "values.sops.env", "VALUE=before\n", "VALUE=after\n"},
		{"configmap_file", "ConfigMap", "configMapGenerator:\n- name: settings\n  files: [VALUE=value.txt]\n- name: stable-settings\n  literals: [STABLE=unchanged]\n", "value.txt", "before", "after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig, root := t.TempDir(), t.TempDir()
			repo := testutil.GitInit(t, orig)
			testutil.WriteFile(t, orig, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}}
`)
			testutil.WriteFile(t, orig, "flux/ks.yaml", testutil.KustomizationYAML("producer", "flux-system", "  path: ./producer\n  sourceRef: {kind: GitRepository, name: flux-system}\n")+"---\n"+
				testutil.KustomizationYAML("consumer", "flux-system", "  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild: {substituteFrom: [{kind: "+tc.kind+", name: settings}]}\n")+"---\n"+
				testutil.KustomizationYAML("sibling", "flux-system", "  path: ./sibling\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: stable-settings}]}\n")+"---\n"+
				testutil.KustomizationYAML("unrelated", "flux-system", "  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			testutil.WriteFile(t, orig, "producer/kustomization.yaml", "namespace: flux-system\ngeneratorOptions: {disableNameSuffixHash: true}\n"+tc.generator)
			testutil.WriteFile(t, orig, filepath.Join("producer", tc.input), tc.before)
			for _, consumer := range []struct{ name, value string }{{"consumer", "${VALUE}"}, {"sibling", "${STABLE}"}, {"unrelated", "independent"}} {
				testutil.WriteFile(t, orig, consumer.name+"/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, orig, consumer.name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+consumer.name+", namespace: apps}\ndata: {value: '"+consumer.value+"'}\n")
			}
			testutil.GitCommitAll(t, repo, "add generators and consumers", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			testutil.CopyTree(t, orig, root)
			testutil.WriteFile(t, root, filepath.Join("producer", tc.input), tc.after)

			var full [2]string
			for side, dir := range []string{orig, root} {
				args := []string{"build", "all", "--path", filepath.Join(dir, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "--log-level", "warn", "-o", "json"}
				out, warnings := harness.RequireCLIOK(t, args...)
				again, againWarnings := harness.RequireCLIOK(t, args...)
				assert.Equal(t, again, out)
				assert.Equal(t, againWarnings, warnings)
				t.Logf("full side=%d stdout=%x stderr=%x", side, sha256.Sum256([]byte(out)), sha256.Sum256([]byte(warnings)))
				full[side] = out
				value := []string{"before", "after"}[side]
				consumer := generatorConsumer(t, out, "consumer")
				if !strings.Contains(consumer, `"value": "`+value+`"`) {
					t.Fatalf("expected public consumer with %s:\n%s", value, out)
				}
				var docs []struct{ Kind string }
				assert.NoError(t, json.Unmarshal([]byte(out), &docs))
				for _, doc := range docs {
					if doc.Kind == "Secret" {
						t.Fatalf("default output included a Secret:\n%s", out)
					}
				}
				if !strings.Contains(generatorConsumer(t, out, "sibling"), `"value": "unchanged"`) || !strings.Contains(generatorConsumer(t, out, "unrelated"), `"value": "independent"`) {
					t.Fatalf("missing stable full-side oracles:\n%s", out)
				}
			}
			assert.Equal(t, generatorConsumer(t, full[1], "sibling"), generatorConsumer(t, full[0], "sibling"))
			for side, dir := range []string{orig, root} {
				other := []string{root, orig}[side]
				args := []string{"build", "all", "--path", filepath.Join(dir, "flux"), "--path-orig", filepath.Join(other, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "--log-level", "warn", "-o", "json"}
				out, warnings := harness.RequireCLIOK(t, args...)
				again, againWarnings := harness.RequireCLIOK(t, args...)
				assert.Equal(t, again, out)
				assert.Equal(t, againWarnings, warnings)
				t.Logf("selected side=%d stdout=%x stderr=%x", side, sha256.Sum256([]byte(out)), sha256.Sum256([]byte(warnings)))
				assert.Equal(t, generatorConsumer(t, out, "consumer"), generatorConsumer(t, full[side], "consumer"))
				assert.Equal(t, generatorConsumer(t, out, "sibling"), generatorConsumer(t, full[side], "sibling"))
				var docs []struct {
					Kind     string
					Metadata struct{ Name string }
				}
				assert.NoError(t, json.Unmarshal([]byte(out), &docs))
				for _, doc := range docs {
					if doc.Kind == "ConfigMap" && doc.Metadata.Name == "unrelated" {
						t.Fatalf("independent owner selected:\n%s", out)
					}
				}
			}
		})
	}
}

func generatorConsumer(t *testing.T, output, name string) string {
	t.Helper()
	var docs []json.RawMessage
	assert.NoError(t, json.Unmarshal([]byte(output), &docs))
	var found []string
	for _, raw := range docs {
		var doc struct {
			APIVersion, Kind string
			Metadata         struct{ Name, Namespace string }
		}
		assert.NoError(t, json.Unmarshal(raw, &doc))
		if doc.APIVersion == "v1" && doc.Kind == "ConfigMap" && doc.Metadata.Namespace == "apps" && doc.Metadata.Name == name {
			found = append(found, string(raw))
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected one ConfigMap apps/%s, got %d:\n%s", name, len(found), output)
	}
	return found[0]
}

func TestE2E_BootstrapGeneratorFailureIsolation(t *testing.T) {
	for _, input := range []string{"files: [missing.txt]", "files: [OUT=../../../outside.txt]"} {
		t.Run(input, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "repo")
			testutil.GitInit(t, root)
			testutil.WriteFile(t, parent, "outside.txt", "OUTSIDE-CONTENT")
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("good", "flux-system", "  {path: ./apps/good}\n")+"---\n"+testutil.KustomizationYAML("bad", "flux-system", "  {path: ./apps/bad}\n"))
			testutil.WriteFile(t, root, "apps/good/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/good/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: good-output}\n")
			testutil.WriteFile(t, root, "apps/bad/kustomization.yaml", "configMapGenerator:\n- name: bad\n  "+input+"\n")
			for _, workers := range []string{"2", "8"} {
				stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"), "--concurrency", workers, "--cache-dir", t.TempDir(), "--no-progress")
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
	testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./apps/b\n  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}\n"))
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux, apps/a]\n")
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "generatorOptions: {disableNameSuffixHash: true}\nsecretGenerator:\n- name: creds\n  namespace: flux-system\n  envs: [creds.sops.env]\n")
	testutil.WriteFile(t, root, "apps/a/creds.sops.env", "API_KEY=ENC[AES256_GCM,data:abcd1234,iv:xyz,tag:tt,type:str]\nsops_version=3.8.1\nsops_mac=ENC[fixture]\n")
	testutil.WriteFile(t, root, "apps/b/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/b/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {key: '${API_KEY}'}\n")
	for _, workers := range []string{"2", "8"} {
		stdout := harness.RunCLIStdout(t, "build", "ks", "consumer", "--path", root, "--concurrency", workers, "--cache-dir", t.TempDir(), "--no-progress")
		if !strings.Contains(stdout, "..PLACEHOLDER_API_KEY..") || strings.Contains(stdout, "ENC[") {
			t.Fatalf("ciphertext substitution: %s", stdout)
		}
	}
}

func TestE2E_GeneratorSopsDocumentHash(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", `resources: [deployment.yaml]
secretGenerator:
  - name: settings
    files: [values.yaml=values.sops.yaml]
`)
	testutil.WriteFile(t, root, "deployment.yaml", `apiVersion: apps/v1
kind: Deployment
metadata: {name: app}
spec:
  selector: {matchLabels: {app: demo}}
  template:
    metadata: {labels: {app: demo}}
    spec:
      containers:
        - name: app
          image: example.invalid/app:latest
          envFrom:
            - secretRef: {name: settings}
`)
	testutil.WriteFile(t, root, "values.sops.yaml", `enabled: ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]
sops:
  mac: ENC[fixture]
`)
	args := []string{"build", "all", "--path", root, "--skip-kinds", "", "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress"}
	var previous string
	for run := range 2 {
		out, _ := harness.RequireCLIOK(t, args...)
		if strings.Count(out, "name: settings-8h75cgbfc5") != 2 || !strings.Contains(out, "secretRef:") ||
			!strings.Contains(out, "values.yaml: "+base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_values.yaml.."))) {
			t.Fatalf("generated name, reference or sanitized data changed:\n%s", out)
		}
		if run != 0 && out != previous {
			t.Fatal("identical generated builds changed output")
		}
		previous = out
	}
}

func TestE2E_SourceRef_MissingNonGit(t *testing.T) {
	for _, kind := range []string{"OCIRepository", "HelmRepository", "Bucket"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			consumer := func(interval string) string {
				if kind == "Bucket" {
					return testutil.KustomizationYAML("consumer", "apps", "  interval: "+interval+"\n  path: ./decoy\n  sourceRef: {kind: Bucket, name: missing, namespace: sources}\n")
				}
				chart := "  chartRef: {kind: OCIRepository, name: missing, namespace: sources}\n"
				if kind == "HelmRepository" {
					chart = "  chart:\n    spec:\n      chart: ./decoy\n      sourceRef: {kind: HelmRepository, name: missing, namespace: sources}\n"
				}
				return "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer, namespace: apps}\nspec:\n  interval: " + interval + "\n" + chart
			}
			testutil.WriteFile(t, root, "entry/consumer.yaml", consumer("1m"))
			testutil.WriteFile(t, root, "Chart.yaml", "apiVersion: v2\nname: decoy\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, ".editorconfig", "root = true\n")
			testutil.WriteFile(t, root, "templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-payload}\n")
			testutil.WriteFile(t, root, "decoy/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "decoy/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-payload}\n")
			testutil.GitCommitAll(t, repo, "initial", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			orig := t.TempDir()
			testutil.CopyTree(t, root, orig)
			testutil.WriteFile(t, root, "entry/consumer.yaml", consumer("2m"))
			for _, sides := range [][2]string{{root, orig}, {orig, root}} {
				for _, mode := range []string{"full", "changed", "diff"} {
					var previous [2]string
					for run := range 2 {
						args := []string{"build", "all", "--path", filepath.Join(sides[0], "entry"), "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress"}
						if mode != "full" {
							args = append(args, "--path-orig", filepath.Join(sides[1], "entry"))
						}
						if mode == "diff" {
							args[0] = "diff"
						}
						stdout, stderr, code := harness.RunCLIBuffers(args...)
						if code != 1 || !strings.Contains(stderr, kind) || !strings.Contains(stderr, "sources/missing") {
							t.Fatalf("%s %s exit=%d stdout=%s stderr=%s", kind, mode, code, stdout, stderr)
						}
						if strings.Contains(stdout, "consumer-payload") || strings.Contains(stderr, "chart illegally contains content") {
							t.Fatalf("missing source used decoy bytes: stdout=%s stderr=%s", stdout, stderr)
						}
						current := [2]string{stdout, stderr}
						if run == 1 && current != previous {
							t.Fatalf("%s %s output changed between two cold runs", kind, mode)
						}
						previous = current
					}
				}
			}
		})
	}
}
