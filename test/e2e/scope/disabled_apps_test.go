package scope_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DisabledAppAdmission(t *testing.T) {
	for _, declaration := range []string{"enabled", "absent", "commented"} {
		t.Run("nested cross source/"+declaration, func(t *testing.T) { e2EDisabledAppAdmissionCase(t, &declaration) })
	}
	for _, tc := range []struct{ name, body, message string }{
		{"api version", "apiVersion: source.toolkit.fluxcd.io/\nkind: GitRepository\nmetadata: {name: invalid, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n", "apiVersion"},
		{"yaml syntax", "apiVersion: [\n", "yaml"},
	} {
		t.Run("inactive invalid manifest/"+tc.name, func(t *testing.T) { e2EDisabledAppAdmissionCase2(t, &tc) })
	}

	t.Run(
		"shared active target and component",
		func(t *testing.T) { e2EDisabledAppAdmissionSharedActiveTargetAndComponent(t) },
	)
	t.Run("cross-package inclusion", func(t *testing.T) { e2EDisabledAppAdmissionCrossPackageInclusion(t) })

	for _, tc := range []struct{ name, entry, layout string }{
		{"single-root", ".", "single"},
		{"single-scoped", "flux", "single"},
		{"multi-root", ".", "multi"},
		{"multi-scoped", "flux/main", "multi"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EDisabledAppAdmissionName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DisabledAppDiffSides(t *testing.T) {
	t.Run(
		"membership namespace authority",
		func(t *testing.T) { e2EDisabledAppDiffSidesMembershipNamespaceAuthority(t) },
	)
	for _, scenario := range []string{"remove", "restore", "payload"} {
		t.Run("nested cross source/"+scenario, func(t *testing.T) { e2EDisabledAppDiffSidesCase(t, &scenario) })
	}
	for _, enabled := range []bool{false, true} {
		name := "remove"
		if enabled {
			name = "restore"
		}
		t.Run(name, func(t *testing.T) { e2EDisabledAppDiffSidesName(t, &enabled) })
	}
}

func disabledAppFixture(t *testing.T, root, layout string, enabled bool) {
	t.Helper()
	if layout == "nested" {
		const entry = "kube/clusters/main/flux"
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: ["+entry+"]\n")
		ref := "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
		testutil.WriteFile(
			t,
			root,
			entry+"/kustomization.yaml",
			"resources: [sync.yaml, external.yaml, ../../../deploy/app]\n",
		)
		testutil.WriteFile(
			t,
			root,
			entry+"/sync.yaml",
			testutil.KustomizationYAML("sync", "flux-system", "  path: ./"+entry+"\n"),
		)
		testutil.WriteFile(
			t,
			root,
			entry+"/external.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec:\n  provider: github\n  url: https://fixture.invalid/external\n  secretRef: {name: unavailable-auth}\n---\n"+testutil.KustomizationYAML(
				"external-sync",
				"flux-system",
				"  path: ./"+entry+"\n  sourceRef: {kind: GitRepository, name: external}\n",
			),
		)
		resources := "resources: []\n"
		if enabled {
			resources = "resources: [ks.yaml]\n# - commented.yaml\n"
		}
		testutil.WriteFile(t, root, "kube/deploy/app/kustomization.yaml", resources)
		for _, tc := range []struct{ file, name, target, payload string }{
			{"ks.yaml", "app", "payload", "reachable-cm"}, {"omitted.yaml", "excluded-app", "excluded", "excluded-cm"}, {"commented.yaml", "commented-app", "commented", "commented-cm"},
		} {
			testutil.WriteFile(
				t,
				root,
				"kube/deploy/app/"+tc.file,
				testutil.KustomizationYAML(tc.name, "flux-system", "  path: ./"+tc.target+"\n"+ref),
			)
			testutil.WriteFile(t, root, tc.target+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				tc.target+"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+tc.payload+", namespace: apps}\ndata: {value: original}\n",
			)
		}
		return
	}
	entry, games := "flux", "apps/games"
	if layout == "multi" {
		entry, games = "flux/main", "apps/main/games"
	}
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux]\n")
	ref := "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
	testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [parent.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		entry+"/parent.yaml",
		testutil.KustomizationYAML("parent", "flux-system", "  path: ./"+games+"\n"+ref),
	)
	resources := "namespace: games\nresources: []\n"
	if enabled {
		resources = "namespace: games\nresources: [disabled.yaml]\n"
	}
	testutil.WriteFile(t, root, games+"/kustomization.yaml", resources)
	testutil.WriteFile(
		t,
		root,
		games+"/disabled.yaml",
		testutil.KustomizationYAML("disabled", "games", "  path: ./targets/game\n  components: [../backup]\n"+ref),
	)
	testutil.WriteFile(t, root, "targets/game/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "targets/game/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: game-cm}\n")
	testutil.WriteFile(
		t,
		root,
		"targets/backup/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [cm.yaml]\n",
	)
	testutil.WriteFile(
		t,
		root,
		"targets/backup/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: backup-cm}\n",
	)
	if layout == "multi" {
		testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [main, test]\n")
		testutil.WriteFile(t, root, "flux/test/kustomization.yaml", "resources: [parent.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"flux/test/parent.yaml",
			testutil.KustomizationYAML("test-parent", "flux-system", "  path: ./targets/test\n"+ref),
		)
		testutil.WriteFile(t, root, "targets/test/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"targets/test/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: test-cm}\n",
		)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedEntrySelfOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, entry      string
		builder, foreign bool
		want             bool
	}{
		{"selected package", "flux", true, false, true},
		{"plain entry with ancestor owner", "flux", false, false, true},
		{"disabled foreign owner", ".", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			pkg, target := "flux", "./"
			if !tc.builder {
				pkg = "flux/system"
			}
			if tc.foreign {
				pkg, target = "target", "./target"
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [controls]\n")
				testutil.WriteFile(t, root, "controls/kustomization.yaml", "resources: []\n")
				testutil.WriteFile(
					t,
					root,
					"controls/disabled.yaml",
					testutil.KustomizationYAML("disabled", "flux-system", "  path: ./target\n"),
				)
				testutil.WriteFile(t, root, pkg+"/kustomization.yaml", "resources: [sync.yaml, cm.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					pkg+"/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: anchored}\n",
				)
			} else {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: ["+pkg+", app]\n")
				testutil.WriteFile(t, root, pkg+"/kustomization.yaml", "resources: [sync.yaml]\n")
				testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"app/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: anchored}\n",
				)
			}
			testutil.WriteFile(
				t,
				root,
				pkg+"/sync.yaml",
				testutil.KustomizationYAML("sync", "flux-system", "  path: "+target+"\n"),
			)
			runCLIBuffersResult1 := harness.RunCLIBuffers(
				"build",
				"all",
				"--path",
				filepath.Join(root, tc.entry),
				"--concurrency",
				"2",
				"--no-progress",
			)
			out, stderr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
			assert.FatalIf(
				t,
				code != 0 || strings.Contains(out, "name: anchored") != tc.want,
				"exit=%d want output=%t, stdout=%s stderr=%s",
				code,
				tc.want,
				out,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_EntryPrerequisiteDiffSides(t *testing.T) {
	before, after := t.TempDir(), t.TempDir()
	for _, side := range []struct{ root, dependency, value string }{
		{before, "", "original"},
		{after, "  dependsOn: [{name: sibling}]\n", "changed"},
	} {
		testutil.GitInit(t, side.root)
		testutil.WriteFile(t, side.root, "entry/kustomization.yaml", "resources: [owner.yaml, sibling.yaml, cm.yaml]\n")
		testutil.WriteFile(
			t,
			side.root,
			"entry/owner.yaml",
			testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n"+side.dependency),
		)
		testutil.WriteFile(
			t,
			side.root,
			"entry/sibling.yaml",
			testutil.KustomizationYAML("sibling", "apps", "  path: ./prerequisite\n"),
		)
		testutil.WriteFile(
			t,
			side.root,
			"entry/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: entry-output, namespace: apps}\ndata: {value: "+side.value+"}\n",
		)
		testutil.WriteFile(t, side.root, "prerequisite/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			side.root,
			"prerequisite/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: prerequisite-output, namespace: apps}\ndata: {value: "+side.value+"}\n",
		)
	}
	for _, direction := range []struct{ name, current, original, value string }{
		{"forward", after, before, "changed"},
		{"reverse", before, after, "original"},
	} {
		t.Run(direction.name, func(t *testing.T) { e2EEntryPrerequisiteDiffSidesName(t, &direction) })
	}
}

func e2EDisabledAppAdmissionCase(t *testing.T, declaration *string) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	enabled := (*declaration) == "enabled"
	disabledAppFixture(t, root, "nested", enabled)
	if (*declaration) == "commented" {
		testutil.WriteFile(t, root, "kube/deploy/app/kustomization.yaml", "resources: []\n# - ks.yaml\n")
	}
	for _, entry := range []string{".", "kube/clusters/main/flux"} {
		args := []string{
			"build",
			"all",
			"--path",
			filepath.Join(root, entry),
			"--allow-missing-kinds=Secret,ConfigMap",
			"--log-level",
			"info",
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		}
		runCLIBuffersResult2 := harness.RunCLIBuffers(args...)
		out, stderr, code := runCLIBuffersResult2.Stdout, runCLIBuffersResult2.Stderr, runCLIBuffersResult2.Code
		assert.FatalIf(
			t,
			code != 0 || strings.Contains(stderr, "dial tcp"),
			"nested build exit=%d output=%s stderr=%s",
			code,
			out,
			stderr,
		)
		docs, err := testutil.SplitDocs([]byte(out))
		assert.NoError(t, err)
		found := map[string]bool{}
		for _, doc := range docs {
			metadata := manifest.DocMetadata(doc)
			name := metadata.Name
			found[name] = true
		}
		assert.FatalIf(t, found["reachable-cm"] != enabled || found["excluded-cm"] || found["commented-cm"] ||
			found["excluded-app"] ||
			found["commented-app"], "nested admission enabled=%t output=%s stderr=%s", enabled, out, stderr)
		runCLIBuffersResult3 := harness.RunCLIBuffers(args...)
		again, diagnostic, code := runCLIBuffersResult3.Stdout, runCLIBuffersResult3.Stderr, runCLIBuffersResult3.Code
		assert.FatalIf(
			t,
			code != 0 || again != out,
			"nested render changed bytes: exit=%d stderr=%s",
			code,
			diagnostic,
		)
		if enabled {
			args[0] = "test"
			runCLIBuffersResult4 := harness.RunCLIBuffers(args...)
			status, diagnostic, code := runCLIBuffersResult4.Stdout, runCLIBuffersResult4.Stderr, runCLIBuffersResult4.Code
			assert.FatalIf(t, code != 0 || !strings.Contains(status, "flux-system/external") ||
				!strings.Contains(status, "flux-system/external-sync") ||
				!strings.Contains(status, "skipped") ||
				!strings.Contains(diagnostic, "base: credentials unavailable") ||
				!strings.Contains(
					diagnostic,
					"unavailable-auth",
				), "whole-source skip missing: exit=%d output=%s stderr=%s", code, status, diagnostic)
		}
	}
	if enabled {
		testutil.WriteFile(
			t,
			root,
			"payload/kustomization.yaml",
			"resources: [cm.yaml]\nconfigMapGenerator: [{name: broken, files: [missing.txt]}]\n",
		)
		runCLIBuffersResult5 := harness.RunCLIBuffers(
			"build",
			"all",
			"--path",
			root,
			"--allow-missing-kinds=Secret,ConfigMap",
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		)
		out, stderr, code := runCLIBuffersResult5.Stdout, runCLIBuffersResult5.Stderr, runCLIBuffersResult5.Code
		assert.FatalIf(
			t,
			code != 1 || !strings.Contains(stderr, "missing.txt") || strings.Contains(out, "reachable-cm"),
			"reachable input failure hidden: exit=%d output=%s stderr=%s",
			code,
			out,
			stderr,
		)
	}
}

func e2EDisabledAppAdmissionCase2(t *testing.T, tc *struct {
	name    string
	body    string
	message string
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	disabledAppFixture(t, root, "single", false)
	testutil.WriteFile(t, root, "targets/game/cm.yaml", tc.body)
	runCLIBuffersResult6 := harness.RunCLIBuffers(
		"build",
		"all",
		"--path",
		root,
		"--concurrency",
		"2",
		"--no-progress",
	)
	out, stderr, code := runCLIBuffersResult6.Stdout, runCLIBuffersResult6.Stderr, runCLIBuffersResult6.Code
	assert.FatalIf(
		t,
		code != 0 || strings.Contains(out, "name: invalid"),
		"disabled invalid manifest failed: exit=%d stderr=%s",
		code,
		stderr,
	)
	testutil.WriteFile(
		t,
		root,
		"apps/games/kustomization.yaml",
		"namespace: games\nresources: [disabled.yaml]\n",
	)
	{
		runCLIBuffersResult7 := harness.RunCLIBuffers(
			"build",
			"all",
			"--path",
			root,
			"--concurrency",
			"2",
			"--no-progress",
		)
		_, stderr, code := runCLIBuffersResult7.Stdout, runCLIBuffersResult7.Stderr, runCLIBuffersResult7.Code
		assert.FatalIf(t, code != 1 ||
			!strings.Contains(
				stderr,
				tc.message,
			), "enabled invalid manifest lost failure: exit=%d stderr=%s", code, stderr)
	}
}

func e2EDisabledAppAdmissionSharedActiveTargetAndComponent(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	disabledAppFixture(t, root, "single", false)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux, active.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"active.yaml",
		testutil.KustomizationYAML(
			"active",
			"games",
			"  path: ./targets/game\n  components: [../backup]\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	runCLIBuffersResult8 := harness.RunCLIBuffers(
		"build",
		"all",
		"--path",
		root,
		"--concurrency",
		"2",
		"--no-progress",
	)
	out, stderr, code := runCLIBuffersResult8.Stdout, runCLIBuffersResult8.Stderr, runCLIBuffersResult8.Code
	assert.FatalIf(t, code != 0, "shared active target exit=%d stderr=%s", code, stderr)
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	counts := map[string]int{}
	for _, doc := range docs {
		metadata2 := manifest.DocMetadata(doc)
		name := metadata2.Name
		counts[name]++
	}
	assert.FatalIf(
		t,
		counts["disabled"] != 0 || counts["game-cm"] != 1 || counts["backup-cm"] != 1,
		"shared output lost or disabled owner rendered: %v",
		counts,
	)
}

func e2EDisabledAppAdmissionCrossPackageInclusion(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	disabledAppFixture(t, root, "single", false)
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux, independent]\n")
	testutil.WriteFile(t, root, "independent/kustomization.yaml", "resources: [../apps/games/disabled.yaml]\n")
	runCLIBuffersResult9 := harness.RunCLIBuffers(
		"build",
		"all",
		"--path",
		root,
		"--concurrency",
		"2",
		"--no-progress",
	)
	out, stderr, code := runCLIBuffersResult9.Stdout, runCLIBuffersResult9.Stderr, runCLIBuffersResult9.Code
	assert.FatalIf(
		t,
		code != 0 || !strings.Contains(out, "name: game-cm") || !strings.Contains(out, "name: backup-cm"),
		"independent resource edge did not enable app: exit=%d stderr=%s output=%s",
		code,
		stderr,
		out,
	)
}

func e2EDisabledAppAdmissionName(t *testing.T, tc *struct {
	name   string
	entry  string
	layout string
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	for _, enabled := range []bool{false, true} {
		disabledAppFixture(t, root, tc.layout, enabled)
		args := []string{
			"build",
			"all",
			"--path",
			filepath.Join(root, tc.entry),
			"--concurrency",
			"2",
			"--no-progress",
		}
		runCLIBuffersResult10 := harness.RunCLIBuffers(args...)
		first, stderr, code := runCLIBuffersResult10.Stdout, runCLIBuffersResult10.Stderr, runCLIBuffersResult10.Code
		assert.FatalIf(
			t,
			code != 0 || strings.Contains(stderr, "[OrphanedResource]"),
			"enabled=%t exit=%d stderr=%s",
			enabled,
			code,
			stderr,
		)
		runCLIBuffersResult11 := harness.RunCLIBuffers(args...)
		second, _, code := runCLIBuffersResult11.Stdout, runCLIBuffersResult11.Stderr, runCLIBuffersResult11.Code
		assert.FatalIf(t, code != 0 || first != second, "%v", "identical inputs changed output bytes")
		docs, err := testutil.SplitDocs([]byte(first))
		assert.NoError(t, err)
		found := map[string]bool{}
		for _, doc := range docs {
			metadata3 := manifest.DocMetadata(doc)
			name := metadata3.Name
			found[name] = true
		}
		for _, name := range []string{"disabled", "game-cm", "backup-cm"} {
			if found[name] != enabled {
				t.Fatalf("%s present=%t want=%t; output=%s", name, found[name], enabled, first)
			}
		}
	}
	disabledAppFixture(t, root, tc.layout, false)
	testutil.WriteFile(
		t,
		root,
		"targets/game/kustomization.yaml",
		"resources: [cm.yaml]\nconfigMapGenerator:\n- name: broken\n  files: [missing.txt]\n",
	)
	args := []string{
		"build",
		"all",
		"--path",
		filepath.Join(root, tc.entry),
		"--concurrency",
		"2",
		"--no-progress",
	}
	{
		runCLIBuffersResult12 := harness.RunCLIBuffers(args...)
		_, stderr, code := runCLIBuffersResult12.Stdout, runCLIBuffersResult12.Stderr, runCLIBuffersResult12.Code
		assert.FatalIf(t, code != 0, "disabled broken target failed: exit=%d stderr=%s", code, stderr)
	}
	games := "apps/games"
	if tc.layout == "multi" {
		games = "apps/main/games"
	}
	testutil.WriteFile(t, root, games+"/kustomization.yaml", "namespace: games\nresources: [disabled.yaml]\n")
	{
		runCLIBuffersResult13 := harness.RunCLIBuffers(
			args...)
		_, stderr, code := runCLIBuffersResult13.Stdout, runCLIBuffersResult13.Stderr, runCLIBuffersResult13.Code
		assert.FatalIf(t, code != 1 ||
			!strings.Contains(
				stderr,
				"missing.txt",
			), "enabled broken target lost failure: exit=%d stderr=%s", code, stderr)
	}
}
