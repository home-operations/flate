// Package scope tests render selection, ownership and change scope.
package scope_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_GetKS_YAMLExposesProjectedFields(t *testing.T) {
	out := harness.RunCLIOutputOnly(t, "get", "ks", "--path", harness.TestdataPath(t, "simple"), "-o", "yaml")
	// Asserts the structured projection includes the new fields:
	// sourceRef block (kind/name/namespace), prune, wait.
	for _, want := range []string{"sourceRef:", "kind: GitRepository", "prune: true", "wait:"} {
		assert.ErrorIf(t, !strings.Contains(out, want), "missing %q in projected ks YAML:\n%s", want, out)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_MissingLocalInputDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, manifest, diagnostic string
	}{
		{
			name:       "resource",
			manifest:   testutil.KustomizationYAML("probe", "flux-system", "  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"),
			diagnostic: "resolve apps/missing.yaml: stat apps/missing.yaml: file does not exist",
		},
		{
			name: "chart",
			manifest: `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: probe, namespace: flux-system}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/nochart
      sourceRef: {kind: GitRepository, name: flux-system}
`,
			diagnostic: "chart not found at charts/nochart: stat charts/nochart: file does not exist",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://fixture.invalid/cluster.git}
`)
			testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [source.yaml, probe.yaml]\n")
			testutil.WriteFile(t, dir, "probe.yaml", tc.manifest)
			if tc.name == "resource" {
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
			}
			result1 := harness.RunCLIBuffers("build", "all", "--path", dir,
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			stderr, code := result1.Stderr, result1.Code

			assert.FatalIf(t, code != 1, "got exit %d, want 1:\n%s", code, stderr)
			assert.ErrorIf(
				t,
				!strings.Contains(stderr, tc.diagnostic),
				"missing diagnostic %q:\n%s",
				tc.diagnostic,
				stderr,
			)
			for _, unwanted := range []string{dir, "flux error: input error", "stat chart "} {
				assert.ErrorIf(t, strings.Contains(stderr, unwanted), "diagnostic contains %q:\n%s", unwanted, stderr)
			}
			assert.ErrorIf(
				t,
				tc.name == "chart" && strings.Count(stderr, tc.diagnostic) != 1,
				"duplicated chart diagnostic:\n%s",
				stderr,
			)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_Diff_SameDirectoryFailure(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/apps.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/apps.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
	result2 := harness.RunCLIBuffers("diff", "all", "--path", root, "--path-orig", root,
		"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "-o", "diff")
	stdout, stderr, code := result2.Stdout, result2.Stderr, result2.Code

	assert.ErrorIf(t, code != 1, "exit=%d, want 1\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	for _, want := range []string{"Kustomization  flux-system/apps", "orig snapshot:", "current snapshot:", "missing.yaml"} {
		assert.ErrorIf(t, !strings.Contains(stderr, want), "stderr missing %q:\n%s", want, stderr)
	}
	failures := 0
	for line := range strings.SplitSeq(stderr, "\n") {
		if strings.HasPrefix(line, "  ✗  orig snapshot: Kustomization  flux-system/apps  ") ||
			strings.HasPrefix(line, "  ✗  current snapshot: Kustomization  flux-system/apps  ") {
			failures++
			assert.ErrorIf(
				t,
				!strings.Contains(line, "missing.yaml"),
				"snapshot failure missing resource detail:\n%s",
				stderr,
			)
		}
	}
	assert.FatalIf(
		t,
		failures != 2 || strings.Contains(stderr, "flate error:"),
		"expected both snapshot failures without a duplicate fatal dump:\n%s",
		stderr,
	)
	assert.ErrorIf(t, stdout != "", "suppressed diff must have no product: %s", stdout)
	for _, want := range []string{"[DiffSuppressed] Kustomization flux-system/apps: 0 objects withheld; diff suppressed", "failed on both sides: orig:", "; current:"} {
		assert.ErrorIf(t, !strings.Contains(stderr, want), "stderr missing %q:\n%s", want, stderr)
	}
	for side := range strings.SplitSeq(stderr, "; current:") {
		assert.ErrorIf(
			t,
			!strings.Contains(side, "missing.yaml"),
			"suppressed failure missing resource detail:\n%s",
			stderr,
		)
	}
	for _, format := range []string{"html", "json"} {
		result4 := harness.RunCLIBuffers(
			"diff",
			"all",
			"--path",
			root,
			"--path-orig",
			root,
			"--concurrency=2",
			"-o",
			format,
		)
		product, diagnostic, exit := result4.Stdout, result4.Stderr, result4.Code

		assert.FatalIf(
			t,
			exit != 1 || strings.Count(diagnostic, "[DiffSuppressed]") != 1,
			"%s suppression: exit=%d stdout=%s stderr=%s",
			format,
			exit,
			product,
			diagnostic,
		)
		if format == "html" {
			assert.FatalIf(t, product != "", "suppressed HTML product: %s", product)
		} else {
			var result struct {
				Changes    []any `json:"Changes"`
				Suppressed []struct {
					Side   string `json:"Side"`
					Count  int    `json:"Count"`
					Reason string `json:"Reason"`
				} `json:"Suppressed"`
			}
			{
				err := json.Unmarshal(
					[]byte(product),
					&result,
				)
				assert.FatalIf(
					t,
					err != nil || len(result.Changes) != 0 || len(result.Suppressed) != 1 || result.Suppressed[0].Side != "both" || result.Suppressed[0].Count != 0 ||
						!strings.Contains(
							result.Suppressed[0].Reason,
							"missing.yaml",
						),
					"structured suppression lost: %s, %v",
					product,
					err,
				)
			}
		}
	}
	result3 := harness.RunCLIBuffers(
		"diff",
		"images",
		"--path",
		root,
		"--path-orig",
		root,
		"--concurrency=2",
		"-o",
		"json",
	)
	images, imageDiagnostic, imageExit := result3.Stdout, result3.Stderr, result3.Code

	var imageProduct []string
	{
		err := json.Unmarshal(
			[]byte(images),
			&imageProduct,
		)
		assert.FatalIf(t, err != nil || len(imageProduct) != 0 || imageExit != 1 ||
			strings.Count(
				imageDiagnostic,
				"[DiffSuppressed]",
			) != 1, "image suppression: exit=%d stdout=%s stderr=%s err=%v", imageExit, images, imageDiagnostic, err)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_Diff_OwnershipCycle(t *testing.T) {
	orig, current := t.TempDir(), t.TempDir()
	for _, side := range []struct {
		path  string
		value string
	}{
		{path: orig, value: "before"},
		{path: current, value: "after"},
	} {
		testutil.WriteFile(t, side.path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
		for _, name := range []string{"a", "b"} {
			testutil.WriteFile(
				t,
				side.path,
				"shared/"+name+".yaml",
				testutil.KustomizationYAML(
					name,
					"flux-system",
					"  interval: 10m\n  path: ./shared\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			)
		}
		testutil.WriteFile(t, side.path, "shared/kustomization.yaml", "resources: [a.yaml, b.yaml, cm.yaml]\n")
		testutil.WriteFile(t, side.path, "shared/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: `+side.value+`
`)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { panic("diff ownership walk did not complete") })
	defer stop()
	migratedResult1 := harness.RequireCLIOK(t, "diff", "all", "--path", current, "--path-orig", orig,
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress", "-o", "diff")
	out, stderr := migratedResult1.Stdout, migratedResult1.Stderr

	assert.ErrorIf(
		t,
		!strings.Contains(out, "-  value: before") || !strings.Contains(out, "+  value: after"),
		"expected ConfigMap value change:\n%s\nstderr:\n%s",
		out,
		stderr,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiffAutoBaseline_Base(t *testing.T) {
	adaptedResult1 := initGitFixture(t)
	clusterPath, repoRoot := adaptedResult1.path, adaptedResult1.
		// Edit cm.yaml in the working tree; HEAD still has the original.
		root

	harness.MutateFile(t, filepath.Join(repoRoot, "kubernetes", "apps", "cm.yaml"), "value: original", "value: changed")
	out := harness.RunCLI(t, "diff", "ks", "--path", clusterPath, "--base", "HEAD")
	assert.ErrorIf(t, !strings.Contains(out, "changed"), "expected 'changed' to surface in diff body:\n%s", out)
	assert.ErrorIf(
		t,
		!strings.Contains(out, "original"),
		"expected baseline 'original' to surface in diff body:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiffAutoBaseline_CommittedMembership(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rename bool
	}{
		{name: "delete"}, {name: "rename", rename: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adaptedResult2 := initGitFixture(t)
			clusterPath, root := adaptedResult2.path, adaptedResult2.root
			repo, err := gogit.PlainOpen(root)
			assert.NoError(t, err)
			base, err := repo.Head()
			assert.NoError(t, err)
			wt, err := repo.Worktree()
			assert.NoError(t, err)
			{
				_, err := wt.Remove("kubernetes/apps/cm.yaml")
				assert.FatalIf(t, err != nil, "%v", err)
			}
			resources := "resources: []\n"
			if tc.rename {
				resources = "resources: [renamed.yaml]\n"
				testutil.WriteFile(
					t,
					root,
					"kubernetes/apps/renamed.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: renamed, namespace: apps}\ndata: {value: committed}\n",
				)
			}
			testutil.WriteFile(t, root, "kubernetes/apps/kustomization.yaml", resources)
			testutil.WriteFile(
				t,
				root,
				"kubernetes/flux/peer.yaml",
				testutil.KustomizationYAML(
					"apps",
					"other",
					"  path: ./peer\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			)
			testutil.WriteFile(t, root, "peer/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"peer/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: peer-output}\n",
			)
			testutil.GitCommitAll(
				t,
				repo,
				"membership change",
				&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(1, 0)}},
			)
			testutil.WriteFile(t, root, "notes/dirty.txt", "dirty-unconsumed\n")
			testutil.WriteFile(
				t,
				root,
				"kubernetes/apps/stray.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty-unconsumed}\n",
			)
			migratedResult2 := harness.RequireCLIOK(
				t,
				"diff",
				"ks",
				"apps",
				"-n",
				"flux-system",
				"--path",
				clusterPath,
				"--base",
				base.Hash().String(),
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"-o",
				"diff",
			)
			stdout, stderr := migratedResult2.Stdout, migratedResult2.Stderr

			assert.FatalIf(t, !strings.Contains(stdout, "hello") || strings.Contains(stdout, "peer-output") ||
				strings.Contains(
					stdout,
					"dirty-unconsumed",
				), "committed old side or name/namespace scope lost:\n%s\n%s", stdout, stderr)
			{
				got := strings.Contains(stdout, "renamed")
				assert.FatalIf(t, got != tc.rename, "rename present=%t, want %t:\n%s", got, tc.rename, stdout)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiffSelfRefBaselineRenders(t *testing.T) {
	repoRoot := initSelfRefGitFixture(t)
	testutil.WriteFileAt(t, filepath.Join(repoRoot, "k8s", "apps", "cm.yaml"), `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: changed
`)
	// Isolated cache so the baseline is freshly materialized (the fixture's
	// deterministic commit SHA would otherwise reuse a cached baseline tree).
	out := harness.RunCLI(t, "diff", "ks", "--path", repoRoot, "--base", "HEAD",
		"--allow-missing-kinds=Secret,ConfigMap", "--cache-dir", t.TempDir())
	// The baseline must have rendered the old value, so the diff is a value
	// change. With the bug the baseline is empty and `original` never
	// appears — the whole ConfigMap shows as a wholesale addition.
	assert.ErrorIf(
		t,
		!strings.Contains(out, "original"),
		"baseline rendered empty (whole tree shown as additions, not the changeset):\n%s",
		out,
	)
	assert.ErrorIf(t, !strings.Contains(out, "changed"), "expected the changed value to surface:\n%s", out)
}

func initSelfRefGitFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	{
		_, err := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin", URLs: []string{"https://github.com/example/cluster.git"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("k8s/flux/entry.yaml", "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  url: ssh://git@github.com/example/cluster.git
  secretRef: {name: deploy-key}
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./k8s/apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`))
	writeFile("k8s/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("k8s/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	return dir
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiffImagesRequiresBaselineWhenNoGit(t *testing.T) {
	// Copy to a tempdir with no .git ancestor (t.TempDir's parent
	// chain stops at the OS temp root, which has no .git).
	dir := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
	migratedResult3 := harness.RunCLIExpectErr(t, "diff", "images", "--path", dir)
	out, code := migratedResult3.Output, migratedResult3.Code
	assert.FatalIf(t, code == 0, "expected non-zero exit with no baseline source, got 0:\n%s", out)
	assert.ErrorIf(t, !strings.Contains(out, "--path-orig"), "error should mention --path-orig as an option:\n%s", out)
	assert.ErrorIf(t, !strings.Contains(out, "--base"), "error should mention --base as an option:\n%s", out)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_BootstrapErrorSurfacesNotMasked(t *testing.T) {
	dir := t.TempDir()
	// Minimal repo: one Kustomization + one ResourceSet whose template
	// references an undefined function ("nope"). text/template's Parse
	// rejects it; the ResourceSet render returns an error; Bootstrap
	// returns it; Render nils the Result; the CLI must surface the
	// underlying error rather than running the testrunner on partial
	// state.
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "rs.yaml", `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: broken-rs, namespace: flux-system}
spec:
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: << nope >>, namespace: ns}
`)
	migratedResult4 := harness.RunCLIExpectErr(t, "test", "all", "--path", dir)
	out, code := migratedResult4.Output, migratedResult4.Code
	assert.FatalIf(t, code == 0, "expected non-zero exit; got 0:\n%s", out)
	// The template parse error mentions the undefined function.
	assert.ErrorIf(
		t,
		!strings.Contains(out, "nope"),
		"error must mention the underlying Bootstrap failure (template parse rejecting `nope`); got:\n%s",
		out,
	)
	// Guard against the regression: the testrunner used to run on
	// the partial Store and emit one "FAILED (no status reported)"
	// row per loaded object. Collect-N where N is the partial count
	// is the signature.
	assert.ErrorIf(
		t,
		strings.Contains(out, "no status reported"),
		"Bootstrap failure should NOT trigger testrunner output; got:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ExternalSourcedWidePathDoesNotBlockCluster(t *testing.T) {
	dir := t.TempDir()
	// Root entry: cluster-apps renders ./kubernetes/apps from the bootstrap
	// source. Its file lives under ./kubernetes/flux, so the wide sync path
	// ./kubernetes is its only structural ancestor — the trap.
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster.yaml",
		testutil.KustomizationYAML(
			"cluster-apps",
			"flux-system",
			"  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources: [./app-a.yaml, ./sync.yaml]\n")
	// A normal app sourced from the bootstrap tree — must pass.
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/apps/app-a.yaml",
		testutil.KustomizationYAML(
			"app-a",
			"default",
			"  path: ./kubernetes/apps/app-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "kubernetes/apps/app-a/kustomization.yaml", "resources: [./cm.yaml]\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/app-a/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: app-a, namespace: default}
data: {k: v}
`)
	// The trap: a sync KS with a WIDE path, sourced from an external private repo.
	testutil.WriteFile(t, dir, "kubernetes/apps/sync.yaml", "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: side-repo, namespace: flux-system}
spec:
  url: ssh://git@github.com/someone/private-side-repo.git
  ref: {branch: main}
  secretRef: {name: side-repo-deploy}
`+"---\n"+
		testutil.KustomizationYAML("side-sync", "flux-system", `  path: ./kubernetes
  sourceRef: {kind: GitRepository, name: side-repo, namespace: flux-system}
`))

	// --allow-missing-kinds Secret soft-skips the external source; a skip is not a
	// failure so the run is clean (harness.RunCLI requires exit 0).
	out := harness.RunCLI(t, "test", "all", "--path", dir, "--allow-missing-kinds=Secret,ConfigMap")
	assert.ErrorIf(
		t,
		!strings.Contains(out, "app-a") || !strings.Contains(out, "passed"),
		"a bootstrap-sourced app must render, not block, behind an external-sourced wide-path KS:\n%s",
		out,
	)
	assert.ErrorIf(
		t,
		strings.Contains(out, "blocked"),
		"an external-sourced wide-path KS must not parent (and block) the local tree:\n%s",
		out,
	)
	assert.ErrorIf(
		t,
		!strings.Contains(out, "skipped"),
		"the external-sourced sync KS (and its source) should skip:\n%s",
		out,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SecretMaterialPolicy(t *testing.T) {
	for _, field := range []string{"username", "password"} {
		for _, allow := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allow=%t", field, allow), func(t *testing.T) {
				root := t.TempDir()
				testutil.GitInit(t, root)
				testutil.WriteFile(
					t,
					root,
					"flux/root.yaml",
					testutil.KustomizationYAML(
						"local",
						"flux-system",
						"  path: ./apps\n",
					)+"---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec:\n  provider: github\n  url: https://fixture.invalid/external.git\n  secretRef: {name: auth}\n",
				)
				username, password := "user", "password"
				cipher := "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
				if field == "username" {
					username = cipher
				} else {
					password = cipher
				}
				testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [secret.yaml, cm.yaml]\n")
				testutil.WriteFile(
					t,
					root,
					"apps/secret.yaml",
					fmt.Sprintf(
						"apiVersion: v1\nkind: Secret\nmetadata: {name: auth, namespace: flux-system}\nstringData: {username: '%s', password: '%s'}\n",
						username,
						password,
					),
				)
				testutil.WriteFile(
					t,
					root,
					"apps/cm.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy-output}\ndata: {value: rendered}\n",
				)
				args := []string{
					"build",
					"all",
					"--path",
					filepath.Join(root, "flux"),
					"--cache-dir",
					t.TempDir(),
					"--concurrency",
					"2",
				}
				if allow {
					args = append(args, "--allow-missing-kinds", "Secret")
				}
				result5 := harness.RunCLIBuffers(args...)
				stdout, stderr, code := result5.Stdout, result5.Stderr, result5.Code
				assert.FatalIf(t, (code == 0) != allow || !strings.Contains(stdout, "healthy-output") ||
					strings.Contains(stdout+stderr, cipher) ||
					strings.Contains(stderr, "dial tcp") ||
					!strings.Contains(stderr, "[SOPSUnreadable]"),
					"exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			})
		}
	}
}
