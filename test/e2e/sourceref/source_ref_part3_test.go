package sourceref_test

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_NonHEADRendersPreserveCheckout(t *testing.T) {
	for _, kind := range []string{"tag", "commit", "semver", "head", "no-ref"} {
		t.Run(kind, func(t *testing.T) {
			root, repo, a, _ := sourceRefFixture(t)
			ref := "tag: v1.0.0"
			want := "v1.0.0"
			switch kind {
			case "commit":
				ref = "commit: " + a.String()
			case "semver":
				ref = "semver: '<2.0.0'"
			case "head":
				ref, want = "tag: v2.0.0", "dirty"
			case "no-ref":
				ref, want = "", "dirty"
			}
			setSourceRef(t, root, ref)
			harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
			wt, err := repo.Worktree()
			assert.NoError(t, err)
			beforeHead, err := repo.Head()
			assert.NoError(t, err)
			beforeStatus, err := wt.Status()
			assert.NoError(t, err)
			before := make(map[string][]byte)
			for _, path := range []string{".git/HEAD", ".git/index", "flux/entry.yaml", "apps/cm.yaml", "apps/kustomization.yaml"} {
				before[path], err = os.ReadFile(filepath.Join(root, path))
				assert.NoError(t, err)
			}
			out, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", "2", "--cache-dir", t.TempDir())
			assert.FatalIf(t, !strings.Contains(out, "value: "+want) ||
				kind != "head" && kind != "no-ref" &&
					strings.Contains(out, "dirty"), "expected %s content:\n%s", want, out)
			afterHead, err := repo.Head()
			assert.FatalIf(
				t,
				err != nil || afterHead.Hash() != beforeHead.Hash() || afterHead.Name() != beforeHead.Name(),
				"HEAD changed: %v, %v",
				afterHead,
				err,
			)
			afterStatus, err := wt.Status()
			{
				diff := cmp.Diff(beforeStatus, afterStatus)
				assert.FatalIf(t, err != nil || diff != "", "checkout status changed (-want +got):\n%s\n%v", diff, err)
			}
			for path, want := range before {
				got, err := os.ReadFile(filepath.Join(root, path))
				assert.FatalIf(t, err != nil || !bytes.Equal(got, want), "checkout file %s changed: %v", path, err)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_BaseAndCurrentRefs(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	setSourceRef(t, root, "tag: v2.0.0")
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--base",
				"HEAD",
				"--concurrency",
				concurrency,
				"--cache-dir",
				t.TempDir(),
				"--git-depth",
				"0",
				"--log-level",
				"warn",
				"-o",
				"diff",
			)
			assertSourceValueDiff(t, out, "v1.0.0", "v2.0.0")
			if loader.calls.Load() != 0 || strings.Contains(stderr, "declared source ref cannot be satisfied locally") {
				t.Fatalf("locally available side refs warned/fetched: calls=%d\n%s", loader.calls.Load(), stderr)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_StablePinIgnoresDirtyContent(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	local, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	exported := t.TempDir()
	testutil.CopyTree(t, root, exported)
	assert.NoError(t, os.RemoveAll(filepath.Join(exported, ".git")))
	fetched, _ := harness.RequireCLIOK(
		t,
		"build",
		"all",
		"--path",
		exported,
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
		"--git-depth",
		"0",
	)
	assert.FatalIf(
		t,
		fetched != local || loader.calls.Load() == 0,
		"local/fetched pin bytes differ or no fetch occurred:\nlocal:\n%s\nfetched:\n%s",
		local,
		fetched,
	)
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			before := loader.calls.Load()
			out, stderr := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--base",
				"HEAD",
				"--concurrency",
				concurrency,
				"--cache-dir",
				t.TempDir(),
				"--git-depth",
				"0",
				"--log-level",
				"warn",
				"-o",
				"diff",
			)
			if out != "" || loader.calls.Load() != before ||
				strings.Contains(stderr, "declared source ref cannot be satisfied locally") {
				t.Fatalf(
					"stable local pin changed, warned or fetched with dirty working files: calls=%d\n%s\nstderr:\n%s",
					loader.calls.Load()-before,
					out,
					stderr,
				)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_RootPin(t *testing.T) {
	current, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	for _, hash := range []plumbing.Hash{older, head} {
		assert.NoError(t, wt.Checkout(&gogit.CheckoutOptions{Hash: hash}))
		testutil.WriteFile(
			t,
			current,
			"flux/entry.yaml",
			testutil.KustomizationYAML(
				"apps",
				"flux-system",
				"  interval: 10m\n  path: ./\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(t, current, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
  ignore: '/ignored.txt'
`)
		testutil.WriteFile(t, current, "kustomization.yaml", "resources: [apps, flux/entry.yaml, flux/repo.yaml]\n")
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
		)
		if hash == older {
			ref, err := repo.Head()
			assert.NoError(t, err)
			assert.NoError(t, repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash()),
			))
		}
	}
	original := t.TempDir()
	testutil.CopyTree(t, current, original)
	transport := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(current, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	for _, dir := range []string{original, current} {
		full, _ := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			dir,
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
		)
		docs, err := testutil.DecodeDocs(strings.NewReader(full))
		assert.NoError(t, err)
		pinned := 0
		ids := map[string]int{}
		declarations := map[string]int{}
		e2EPinnedDiffRootPinLoop(t, struct {
			docs         *[]map[string]any
			ids          *map[string]int
			declarations *map[string]int
			pinned       *int
		}{docs: &docs, ids: &ids, declarations: &declarations, pinned: &pinned})
		assert.Equal(t, pinned, 1)
		assert.Diff(t, declarations, map[string]int{"": 1, "apps": 1})
		assert.Diff(
			t,
			ids,
			map[string]int{
				"ConfigMap/apps/hello":              1,
				"Kustomization/flux-system/apps":    2,
				"GitRepository/flux-system/cluster": 1,
			},
		)
	}
	for _, stage := range []string{"dirty-only", "tag-change"} {
		t.Run(stage, func(t *testing.T) {
			e2EPinnedDiffRootPinStage3(t, &stage, &current, struct {
				original  *string
				transport **sourceRefLoader
			}{original: &original, transport: &transport})
		})
	}
}

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

func e2EPinnedDiffRootPinLoop(t *testing.T, loopInputs struct {
	docs         *[]map[string]any
	ids          *map[string]int
	declarations *map[string]int
	pinned       *int
},
) {
	t.Helper()

	for _, doc := range *loopInputs.docs {
		name, namespace := manifest.DocMetadata(doc)
		(*loopInputs.ids)[manifest.DocKind(doc)+"/"+namespace+"/"+name]++
		if manifest.DocKind(doc) == manifest.KindKustomization {
			metadata := doc["metadata"].(map[string]any)
			labels, _ := metadata["labels"].(map[string]any)
			owner, _ := labels[manifest.FluxKustomizeDomain+"/name"].(string)
			(*loopInputs.declarations)[owner]++
			if owner != "" {
				assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/namespace"], "flux-system")
			}
			spec := doc["spec"].(map[string]any)
			assert.Equal(t, spec["path"], "./")
			assert.Diff(
				t,
				spec["sourceRef"],
				map[string]any{"kind": manifest.KindGitRepository, "name": "cluster", "namespace": "flux-system"},
			)
		}
		if manifest.DocKind(doc) != manifest.KindConfigMap || name != "hello" {
			continue
		}
		metadata := doc["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		if labels[manifest.FluxKustomizeDomain+"/name"] == "apps" {
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/namespace"], "flux-system")
			assert.Equal(t, doc["data"].(map[string]any)["value"], "v1.0.0")
			(*loopInputs.pinned)++
		}
	}
}
