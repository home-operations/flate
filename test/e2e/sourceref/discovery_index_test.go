package sourceref_test

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

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiscoveryIndex_ScopedRawFileFallback(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}}
`)
			testutil.WriteFile(
				t,
				root,
				"flux/ks.yaml",
				testutil.KustomizationYAML("consumer", "flux-system", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}
`)+"---\n"+
					testutil.KustomizationYAML(
						"raw",
						"flux-system",
						"  {interval: 10m, path: ./shared, sourceRef: {kind: GitRepository, name: flux-system}}\n",
					)+"---\n"+
					testutil.KustomizationYAML(
						"nsprod",
						"flux-system",
						"  {interval: 10m, path: ./overlay, sourceRef: {kind: GitRepository, name: flux-system}}\n",
					)+"---\n"+
					testutil.KustomizationYAML("other", "other", `  interval: 10m
  path: ./shared
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  patches:
    - target: {kind: ConfigMap}
      patch: |-
        - op: replace
          path: /does/not/exist
          value: x
`),
			)
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
			runCLIBuffersResult1 := harness.RunCLIBuffers(
				"build",
				"ks",
				"consumer",
				"--path",
				root,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				workers,
				"--no-progress",
				"--log-level",
				"warn",
			)
			stdout, stderr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(stdout, "name: out") || !strings.Contains(stdout, "v: v") ||
					strings.Contains(stderr, "other"),
				"exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)
			runCLIBuffersResult2 := harness.RunCLIBuffers(
				"build",
				"all",
				"--path",
				root,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				workers,
				"--no-progress",
				"--log-level",
				"warn",
			)
			_, fullErr, fullCode := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
			assert.FatalIf(
				t,
				fullCode != 1 || !strings.Contains(fullErr, "other"),
				"full render omitted unrelated producer failure: exit=%d stderr=%s",
				fullCode,
				fullErr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
				testutil.WriteFile(
					t,
					dir,
					"flux/ks.yaml",
					testutil.KustomizationYAML("consumer", "flux-system", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: othercm}]}
`)+"---\n"+
						testutil.KustomizationYAML(
							"producer",
							"flux-system",
							"  {interval: 10m, path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}\n",
						),
				)
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
					cm = "apiVersion: v1\nkind: List\nitems:\n- " + strings.ReplaceAll(
						strings.TrimSpace(cm),
						"\n",
						"\n  ",
					) + "\n"
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
			fullArgs := []string{
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				workers,
				"--no-progress",
				"--log-level",
				"warn",
			}
			requireCLIOKResult3 := harness.RequireCLIOK(t, fullArgs...)
			full, stderr := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
			assert.FatalIf(
				t,
				!strings.Contains(full, "name: out") || !strings.Contains(full, "v: v"),
				"full list consumer missing: %s\n%s",
				full,
				stderr,
			)
			assert.FatalIf(t, strings.Contains(stderr, "[") &&
				(strings.Count(stderr, "[") != 1 || !strings.Contains(stderr, "[RenderPersistenceDisabled]")), "unexpected full warnings: %s", stderr)
			requireCLIOKResult4 := harness.RequireCLIOK(t, fullArgs...)
			again, repeatedStderr := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
			assert.FatalIf(t, strings.Contains(repeatedStderr, "[") &&
				(strings.Count(repeatedStderr, "[") != 1 || !strings.Contains(repeatedStderr, "[RenderPersistenceDisabled]")), "unexpected repeated warnings: %s", repeatedStderr)
			assert.FatalIf(t, again != full, "%v", "identical list builds changed output")
			testutil.WriteFile(
				t,
				root,
				"consumer/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out2, namespace: default}\ndata: {v: \"${K}\"}\n",
			)
			wantValue := "v: v"
			if list {
				harness.MutateFile(t, filepath.Join(root, "apps", "other", "cm.yaml"), "K: v", "K: changed")
				wantValue = "v: changed"
			}
			runCLIBuffersResult5 := harness.RunCLIBuffers(
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--path-orig",
				filepath.Join(orig, "flux"),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				workers,
				"--no-progress",
				"--log-level",
				"warn",
			)
			stdout, stderr, code := runCLIBuffersResult5.Stdout, runCLIBuffersResult5.Stderr, runCLIBuffersResult5.Code
			assert.FatalIf(t, strings.Contains(stderr, "[") &&
				(strings.Count(stderr, "[") != 1 || !strings.Contains(stderr, "[RenderPersistenceDisabled]")), "unexpected changed-only warnings: %s", stderr)
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(stdout, "name: out2") || !strings.Contains(stdout, wantValue),
				"exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
			testutil.WriteFile(
				t,
				orig,
				"flux/ks.yaml",
				testutil.KustomizationYAML(
					"producer",
					"flux-system",
					"  path: ./producer\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				)+"---\n"+
					testutil.KustomizationYAML(
						"consumer",
						"flux-system",
						"  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild: {substituteFrom: [{kind: "+tc.kind+", name: settings}]}\n",
					)+"---\n"+
					testutil.KustomizationYAML(
						"sibling",
						"flux-system",
						"  path: ./sibling\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: stable-settings}]}\n",
					)+"---\n"+
					testutil.KustomizationYAML(
						"unrelated",
						"flux-system",
						"  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
					),
			)
			testutil.WriteFile(
				t,
				orig,
				"producer/kustomization.yaml",
				"namespace: flux-system\ngeneratorOptions: {disableNameSuffixHash: true}\n"+tc.generator,
			)
			testutil.WriteFile(t, orig, filepath.Join("producer", tc.input), tc.before)
			for _, consumer := range []struct{ name, value string }{{"consumer", "${VALUE}"}, {"sibling", "${STABLE}"}, {"unrelated", "independent"}} {
				testutil.WriteFile(t, orig, consumer.name+"/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(
					t,
					orig,
					consumer.name+"/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+consumer.name+", namespace: apps}\ndata: {value: '"+consumer.value+"'}\n",
				)
			}
			testutil.GitCommitAll(
				t,
				repo,
				"add generators and consumers",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			testutil.CopyTree(t, orig, root)
			testutil.WriteFile(t, root, filepath.Join("producer", tc.input), tc.after)

			var full [2]string
			for side, dir := range []string{orig, root} {
				args := []string{
					"build",
					"all",
					"--path",
					filepath.Join(dir, "flux"),
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
					"--no-progress",
					"--log-level",
					"warn",
					"-o",
					"json",
				}
				requireCLIOKResult6 := harness.RequireCLIOK(t, args...)
				out, warnings := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
				requireCLIOKResult7 := harness.RequireCLIOK(t, args...)
				again, againWarnings := requireCLIOKResult7.Stdout, requireCLIOKResult7.Stderr
				assert.Equal(t, again, out)
				assert.Equal(t, againWarnings, warnings)
				t.Logf(
					"full side=%d stdout=%x stderr=%x",
					side,
					sha256.Sum256([]byte(out)),
					sha256.Sum256([]byte(warnings)),
				)
				full[side] = out
				value := []string{"before", "after"}[side]
				consumer := generatorConsumer(t, out, "consumer")
				assert.FatalIf(
					t,
					!strings.Contains(consumer, `"value": "`+value+`"`),
					"expected public consumer with %s:\n%s",
					value,
					out,
				)
				var docs []struct {
					Kind string `json:"Kind"`
				}
				assert.NoError(t, json.Unmarshal([]byte(out), &docs))
				for _, doc := range docs {
					assert.FatalIf(t, doc.Kind == "Secret", "default output included a Secret:\n%s", out)
				}
				assert.FatalIf(t, !strings.Contains(generatorConsumer(t, out, "sibling"), `"value": "unchanged"`) ||
					!strings.Contains(
						generatorConsumer(t, out, "unrelated"),
						`"value": "independent"`,
					), "missing stable full-side oracles:\n%s", out)
			}
			assert.Equal(t, generatorConsumer(t, full[1], "sibling"), generatorConsumer(t, full[0], "sibling"))
			for side, dir := range []string{orig, root} {
				other := []string{root, orig}[side]
				args := []string{
					"build",
					"all",
					"--path",
					filepath.Join(dir, "flux"),
					"--path-orig",
					filepath.Join(other, "flux"),
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
					"--no-progress",
					"--log-level",
					"warn",
					"-o",
					"json",
				}
				requireCLIOKResult8 := harness.RequireCLIOK(t, args...)
				out, warnings := requireCLIOKResult8.Stdout, requireCLIOKResult8.Stderr
				requireCLIOKResult9 := harness.RequireCLIOK(t, args...)
				again, againWarnings := requireCLIOKResult9.Stdout, requireCLIOKResult9.Stderr
				assert.Equal(t, again, out)
				assert.Equal(t, againWarnings, warnings)
				t.Logf(
					"selected side=%d stdout=%x stderr=%x",
					side,
					sha256.Sum256([]byte(out)),
					sha256.Sum256([]byte(warnings)),
				)
				assert.Equal(t, generatorConsumer(t, out, "consumer"), generatorConsumer(t, full[side], "consumer"))
				assert.Equal(t, generatorConsumer(t, out, "sibling"), generatorConsumer(t, full[side], "sibling"))
				var docs []struct {
					Kind     string `json:"Kind"`
					Metadata struct {
						Name string `json:"Name"`
					} `json:"Metadata"`
				}
				assert.NoError(t, json.Unmarshal([]byte(out), &docs))
				for _, doc := range docs {
					assert.FatalIf(
						t,
						doc.Kind == "ConfigMap" && doc.Metadata.Name == "unrelated",
						"independent owner selected:\n%s",
						out,
					)
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
			APIVersion string `json:"APIVersion"`
			Kind       string `json:"Kind"`
			Metadata   struct {
				Name      string `json:"Name"`
				Namespace string `json:"Namespace"`
			} `json:"Metadata"`
		}
		assert.NoError(t, json.Unmarshal(raw, &doc))
		if doc.APIVersion == "v1" && doc.Kind == "ConfigMap" && doc.Metadata.Namespace == "apps" &&
			doc.Metadata.Name == name {
			found = append(found, string(raw))
		}
	}
	assert.FatalIf(t, len(found) != 1, "expected one ConfigMap apps/%s, got %d:\n%s", name, len(found), output)
	return found[0]
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_BootstrapGeneratorFailureIsolation(t *testing.T) {
	for _, input := range []string{"files: [missing.txt]", "files: [OUT=../../../outside.txt]"} {
		t.Run(input, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "repo")
			testutil.GitInit(t, root)
			testutil.WriteFile(t, parent, "outside.txt", "OUTSIDE-CONTENT")
			testutil.WriteFile(
				t,
				root,
				"flux/ks.yaml",
				testutil.KustomizationYAML(
					"good",
					"flux-system",
					"  {path: ./apps/good}\n",
				)+"---\n"+testutil.KustomizationYAML(
					"bad",
					"flux-system",
					"  {path: ./apps/bad}\n",
				),
			)
			testutil.WriteFile(t, root, "apps/good/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/good/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: good-output}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"apps/bad/kustomization.yaml",
				"configMapGenerator:\n- name: bad\n  "+input+"\n",
			)
			for _, workers := range []string{"2", "8"} {
				runCLIBuffersResult10 := harness.RunCLIBuffers(
					"build",
					"all",
					"--path",
					filepath.Join(root, "flux"),
					"--concurrency",
					workers,
					"--cache-dir",
					t.TempDir(),
					"--no-progress",
				)
				stdout, stderr, code := runCLIBuffersResult10.Stdout, runCLIBuffersResult10.Stderr, runCLIBuffersResult10.Code
				assert.FatalIf(
					t,
					code != 1 || !strings.Contains(stdout, "good-output") || !strings.Contains(stderr, "bad") ||
						!strings.Contains(stderr, "file sources:"),
					"exit=%d stdout=%s stderr=%s",
					code,
					stdout,
					stderr,
				)
				assert.FatalIf(
					t,
					strings.Contains(stdout, "OUTSIDE-CONTENT"),
					"%v",
					"generator exposed an outside file",
				)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_BootstrapGeneratorSopsSubstitution(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"flux-system",
			"  path: ./apps/b\n  postBuild: {substituteFrom: [{kind: Secret, name: creds}]}\n",
		),
	)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux, apps/a]\n")
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [ks.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/a/kustomization.yaml",
		"generatorOptions: {disableNameSuffixHash: true}\nsecretGenerator:\n- name: creds\n  namespace: flux-system\n  envs: [creds.sops.env]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/a/creds.sops.env",
		"API_KEY=ENC[AES256_GCM,data:abcd1234,iv:xyz,tag:tt,type:str]\nsops_version=3.8.1\nsops_mac=ENC[fixture]\n",
	)
	testutil.WriteFile(t, root, "apps/b/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/b/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\ndata: {key: '${API_KEY}'}\n",
	)
	for _, workers := range []string{"2", "8"} {
		stdout := harness.RunCLIStdout(
			t,
			"build",
			"ks",
			"consumer",
			"--path",
			root,
			"--concurrency",
			workers,
			"--cache-dir",
			t.TempDir(),
			"--no-progress",
		)
		assert.FatalIf(
			t,
			!strings.Contains(stdout, "..PLACEHOLDER_API_KEY..") || strings.Contains(stdout, "ENC["),
			"ciphertext substitution: %s",
			stdout,
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
	args := []string{
		"build",
		"all",
		"--path",
		root,
		"--skip-kinds",
		"",
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
		"--no-progress",
	}
	var previous string
	for run := range 2 {
		requireCLIOKResult11 := harness.RequireCLIOK(t, args...)
		out, _ := requireCLIOKResult11.Stdout, requireCLIOKResult11.Stderr
		assert.FatalIf(
			t,
			strings.Count(out, "name: settings-8h75cgbfc5") != 2 || !strings.Contains(out, "secretRef:") ||
				!strings.Contains(
					out,
					"values.yaml: "+base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_values.yaml..")),
				),
			"generated name, reference or sanitized data changed:\n%s",
			out,
		)
		assert.FatalIf(t, run != 0 && out != previous, "%v", "identical generated builds changed output")
		previous = out
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_MissingNonGit(t *testing.T) {
	for _, kind := range []string{"OCIRepository", "HelmRepository", "Bucket"} {
		t.Run(kind, func(t *testing.T) { e2ESourceRefMissingNonGitKind(t, &kind) })
	}
}
