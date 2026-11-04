package sourceref_test

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_SubstituteFromProducer(t *testing.T) {
	testPinnedProducerDiff(t, false)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_GeneratorInputs(t *testing.T) {
	for _, tc := range []struct {
		name, kind, generator, input, before, after string
	}{
		{"secret_env", "Secret", "secretGenerator:\n- name: settings\n  envs: [values.sops.env]\nconfigMapGenerator:\n- name: stable-settings\n  literals: [STABLE=unchanged]\n", "values.sops.env", "GENERATED=before\n", "GENERATED=after\n"},
		{"configmap_file", "ConfigMap", "configMapGenerator:\n- name: settings\n  files: [GENERATED=value.txt]\n- name: stable-settings\n  literals: [STABLE=unchanged]\n", "value.txt", "before", "after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig, repo, _, _ := sourceRefFixture(t)
			transport := installSourceRefTransport(t, repo.Storer)
			testutil.WriteFile(t, orig, "flux/consumer.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: working, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
---
`+testutil.KustomizationYAML("producer", "apps", "  path: ./producer\n  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}\n")+"---\n"+
				testutil.KustomizationYAML(
					"consumer",
					"apps",
					"  path: ./consumer\n  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}\n  postBuild:\n    substituteFrom:\n    - {kind: ConfigMap, name: hello}\n    - {kind: "+tc.kind+", name: settings}\n",
				)+"---\n"+
				testutil.KustomizationYAML(
					"sibling",
					"apps",
					"  path: ./sibling\n  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: stable-settings}]}\n",
				))
			testutil.WriteFile(
				t,
				orig,
				"producer/kustomization.yaml",
				"namespace: apps\ngeneratorOptions: {disableNameSuffixHash: true}\n"+tc.generator,
			)
			testutil.WriteFile(t, orig, filepath.Join("producer", tc.input), tc.before)
			testutil.WriteFile(t, orig, "consumer/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, orig, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer, namespace: apps}
data:
  generated: ${GENERATED}
  pinned: ${value}
`)
			testutil.WriteFile(t, orig, "sibling/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				orig,
				"sibling/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling, namespace: apps}\ndata: {value: '${STABLE}'}\n",
			)
			testutil.GitCommitAll(
				t,
				repo,
				"add generators and consumers",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
			)
			root := t.TempDir()
			testutil.CopyTree(t, orig, root)
			testutil.WriteFile(t, root, filepath.Join("producer", tc.input), tc.after)

			var full [2]string
			for side, dir := range []string{orig, root} {
				args := []string{
					"build",
					"all",
					"--path",
					filepath.Join(dir, "flux"),
					"--concurrency",
					"2",
					"--cache-dir",
					t.TempDir(),
					"--no-progress",
					"--log-level",
					"warn",
					"-o",
					"json",
				}
				out, warnings := harness.RequireCLIOK(t, args...)
				again, againWarnings := harness.RequireCLIOK(t, args...)
				assert.Equal(t, again, out)
				assert.Equal(t, againWarnings, warnings)
				t.Logf(
					"full side=%d stdout=%x stderr=%x",
					side,
					sha256.Sum256([]byte(out)),
					sha256.Sum256([]byte(warnings)),
				)
				full[side] = out
				consumer := generatorConsumer(t, out, "consumer")
				assert.FatalIf(t, !strings.Contains(consumer, `"generated": "`+[]string{"before", "after"}[side]+`"`) ||
					!strings.Contains(consumer, `"pinned": "v1.0.0"`) ||
					strings.Contains(out, "v2.0.0") ||
					strings.Contains(
						out,
						"${",
					), "expected changing generated and stable pinned consumer data:\n%s", out)
			}
			assert.Equal(t, generatorConsumer(t, full[1], "sibling"), generatorConsumer(t, full[0], "sibling"))
			args := []string{
				"diff",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--path-orig",
				filepath.Join(orig, "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--no-progress",
				"--log-level",
				"warn",
				"-o",
				"diff",
			}
			out, warnings := harness.RequireCLIOK(t, args...)
			again, againWarnings := harness.RequireCLIOK(t, args...)
			assert.Equal(t, again, out)
			assert.Equal(t, againWarnings, warnings)
			t.Logf("diff stdout=%x stderr=%x", sha256.Sum256([]byte(out)), sha256.Sum256([]byte(warnings)))
			assert.FatalIf(
				t,
				strings.Count(out, "-  generated: before") != 1 || strings.Count(out, "+  generated: after") != 1 ||
					!strings.Contains(out, "   pinned: v1.0.0") ||
					!strings.Contains(out, "--- ConfigMap apps/consumer (consumer)") ||
					strings.Contains(out, "sibling") ||
					strings.Contains(out, "v2.0.0") ||
					strings.Contains(
						out,
						"${",
					),
				"expected consumer diff with pinned substitution and no sibling change:\n%s\n%s",
				out,
				warnings,
			)
			assert.Equal(t, transport.calls.Load(), int64(0))
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_DirtyTransitiveSubstituteFrom(t *testing.T) {
	var roots [2]string
	for i, mode := range []string{"before", "after"} {
		root, repo, _, _ := sourceRefFixture(t)
		roots[i] = root
		testutil.WriteFile(t, root, "flux/consumer.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: working, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
---
`+testutil.KustomizationYAML("bridge", "apps", `  interval: 10m
  path: ./bridge
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: hello, optional: true}]
`)+"---\n"+testutil.KustomizationYAML("consumer", "apps", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: bridge-settings, optional: true}]
`))
		testutil.WriteFile(t, root, "bridge/kustomization.yaml", "resources: [settings.yaml]\n")
		testutil.WriteFile(t, root, "bridge/settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: bridge-settings, namespace: apps}
data:
  value: ${value}
`)
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [../shared/consumer.yaml]\n")
		testutil.WriteFile(t, root, "shared/consumer.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer, namespace: apps}
data:
  value: ${value}
  mode: before
`)
		testutil.GitCommitAll(
			t,
			repo,
			"add consumers",
			&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
		)
		if mode == "after" {
			harness.MutateFile(t, filepath.Join(root, "shared/consumer.yaml"), "mode: before", "mode: after")
		}
		harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty-pin")
	}
	var priorOut, priorWarnings string
	for run := range 2 {
		out, warnings := harness.RequireCLIOK(
			t,
			"diff",
			"all",
			"--path",
			filepath.Join(roots[1], "flux"),
			"--path-orig",
			filepath.Join(roots[0], "flux"),
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
			"--log-level",
			"warn",
			"-o",
			"diff",
		)
		assert.FatalIf(t, strings.Count(out, "-  mode: before") != 1 || strings.Count(out, "+  mode: after") != 1 ||
			strings.Count(out, "   value: v1.0.0") != 1 ||
			strings.Contains(out, "dirty-pin") ||
			strings.Contains(
				out,
				"${",
			), "expected dirty consumer changes with transitive pinned substitution on both sides:\n%s\n%s", out, warnings)
		assert.FatalIf(
			t,
			run != 0 && (out != priorOut || warnings != priorWarnings),
			"diff or warnings changed across runs:\n%s\n%s",
			out,
			warnings,
		)
		priorOut, priorWarnings = out, warnings
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_DeletedSubstituteFromProducer(t *testing.T) {
	testPinnedProducerDiff(t, true)
}

func testPinnedProducerDiff(t *testing.T, deleted bool) {
	t.Helper()
	var roots [2]string
	for i, mode := range []string{"before", "after"} {
		root, repo, older, head := sourceRefFixture(t)
		roots[i] = root
		wt, err := repo.Worktree()
		assert.NoError(t, err)
		for _, hash := range []plumbing.Hash{older, head} {
			assert.NoError(t, wt.Checkout(&gogit.CheckoutOptions{Hash: hash}))
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "name: hello, namespace: apps", "name: hello")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources:\n- cm.yaml\n")
			if hash == older {
				testutil.GitCommitAll(
					t,
					repo,
					"init",
					&gogit.CommitOptions{
						Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
					},
				)
				ref, err := repo.Head()
				assert.NoError(t, err)
				assert.NoError(t, repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash()),
				))
			}
		}
		if deleted {
			assert.NoError(t, os.Remove(filepath.Join(root, "apps/cm.yaml")))
		}
		testutil.WriteFile(t, root, "flux/consumer.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: working, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
`+"---\n"+
			testutil.KustomizationYAML("consumer", "apps", `  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: working, namespace: flux-system}
  postBuild:
    substitute: {mode: `+mode+`}
    substituteFrom:
      - kind: ConfigMap
        name: hello
`))
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(t, root, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: consumer, namespace: apps}
data:
  value: ${value}
  mode: ${mode}
`)
		out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
			"--concurrency", "2", "--cache-dir", t.TempDir())
		assert.FatalIf(
			t,
			strings.Count(out, "value: v1.0.0") != 2 || !strings.Contains(out, "mode: "+mode),
			"expected pinned substitution in full render:\n%s",
			out,
		)
	}
	out, _ := harness.RequireCLIOK(t, "diff", "all", "--path", filepath.Join(roots[1], "flux"),
		"--path-orig", filepath.Join(roots[0], "flux"), "--concurrency", "2", "--cache-dir", t.TempDir(), "-o", "diff")
	assert.FatalIf(t, strings.Count(out, "-  mode: before") != 1 || strings.Count(out, "+  mode: after") != 1 ||
		!strings.Contains(
			out,
			"   value: v1.0.0",
		) || strings.Contains(out, "${value}") || strings.Contains(out, "v2.0.0"), "expected consumer diff with pinned substitution on both sides:\n%s", out)
}
