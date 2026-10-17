// Package scope tests render selection, ownership and change scope.
package scope

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/config"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_GetKS_YAMLExposesProjectedFields(t *testing.T) {
	out := harness.RunCLIOutputOnly(t, "get", "ks", "--path", harness.TestdataPath(t, "simple"), "-o", "yaml")
	// Asserts the structured projection includes the new fields:
	// sourceRef block (kind/name/namespace), prune, wait.
	for _, want := range []string{"sourceRef:", "kind: GitRepository", "prune: true", "wait:"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in projected ks YAML:\n%s", want, out)
		}
	}
}

func TestE2E_MissingLocalInputDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, manifest, diagnostic string
	}{
		{
			name: "resource",
			manifest: `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: probe, namespace: flux-system}
spec:
  interval: 10m
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
`,
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
			diagnostic: "chart not found at charts/nochart",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://fixture.invalid/cluster.git}
`)
			testutil.WriteFile(t, dir, "probe.yaml", tc.manifest)
			if tc.name == "resource" {
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
			}
			_, stderr, code := harness.RunCLIBuffers("build", "all", "--path", dir,
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if code != 1 {
				t.Fatalf("got exit %d, want 1:\n%s", code, stderr)
			}
			if !strings.Contains(stderr, tc.diagnostic) {
				t.Errorf("missing diagnostic %q:\n%s", tc.diagnostic, stderr)
			}
			for _, unwanted := range []string{dir, "flux error: input error", "stat chart"} {
				if strings.Contains(stderr, unwanted) {
					t.Errorf("diagnostic contains %q:\n%s", unwanted, stderr)
				}
			}
			if tc.name == "chart" && strings.Count(stderr, tc.diagnostic) != 1 {
				t.Errorf("duplicated chart diagnostic:\n%s", stderr)
			}
		})
	}
}

func TestE2E_Diff_SameDirectoryFailure(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/apps.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
`)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
	stdout, stderr, code := harness.RunCLIBuffers("diff", "all", "--path", root, "--path-orig", root,
		"--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress", "-o", "diff")
	if code != 1 {
		t.Errorf("exit=%d, want 1\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apps", "orig snapshot:", "current snapshot:", "missing.yaml"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q:\n%s", want, stderr)
		}
	}
	for side := range strings.SplitSeq(stderr, "current snapshot:") {
		if !strings.Contains(side, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apps") || !strings.Contains(side, "missing.yaml") {
			t.Errorf("snapshot failure missing resource detail:\n%s", stderr)
		}
	}
	for _, want := range []string{"Kustomization.kustomize.toolkit.fluxcd.io flux-system/apps", "diff suppressed", "failed on both sides: orig:", "; current:"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout missing %q:\n%s", want, stdout)
		}
	}
	for side := range strings.SplitSeq(stdout, "; current:") {
		if !strings.Contains(side, "missing.yaml") {
			t.Errorf("suppressed failure missing resource detail:\n%s", stdout)
		}
	}
}

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
			testutil.WriteFile(t, side.path, "shared/"+name+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+name+`, namespace: flux-system}
spec:
  interval: 10m
  path: ./shared
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
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
	out, stderr := harness.RequireCLIOK(t, "diff", "all", "--path", current, "--path-orig", orig,
		"--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress", "-o", "diff")
	if !strings.Contains(out, "-  value: before") || !strings.Contains(out, "+  value: after") {
		t.Errorf("expected ConfigMap value change:\n%s\nstderr:\n%s", out, stderr)
	}
}

func TestE2E_DiffAutoBaseline_Base(t *testing.T) {
	clusterPath, repoRoot := initGitFixture(t)
	// Edit cm.yaml in the working tree; HEAD still has the original.
	harness.MutateFile(t, filepath.Join(repoRoot, "kubernetes", "apps", "cm.yaml"), "value: original", "value: changed")
	out := harness.RunCLI(t, "diff", "ks", "--path", clusterPath, "--base", "HEAD")
	if !strings.Contains(out, "changed") {
		t.Errorf("expected 'changed' to surface in diff body:\n%s", out)
	}
	if !strings.Contains(out, "original") {
		t.Errorf("expected baseline 'original' to surface in diff body:\n%s", out)
	}
}

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
		"--allow-missing-secrets", "--cache-dir", t.TempDir())
	// The baseline must have rendered the old value, so the diff is a value
	// change. With the bug the baseline is empty and `original` never
	// appears — the whole ConfigMap shows as a wholesale addition.
	if !strings.Contains(out, "original") {
		t.Errorf("baseline rendered empty (whole tree shown as additions, not the changeset):\n%s", out)
	}
	if !strings.Contains(out, "changed") {
		t.Errorf("expected the changed value to surface:\n%s", out)
	}
}

func initSelfRefGitFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	repo := harness.GitInit(t, dir)
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"https://github.com/example/cluster.git"},
	}); err != nil {
		t.Fatal(err)
	}
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("k8s/flux/entry.yaml", `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  url: ssh://git@github.com/example/cluster.git
  secretRef: {name: deploy-key}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./k8s/apps
  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}
`)
	writeFile("k8s/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("k8s/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	harness.GitCommitAll(t, repo)
	return dir
}

func TestE2E_DiffImagesRequiresBaselineWhenNoGit(t *testing.T) {
	// Copy to a tempdir with no .git ancestor (t.TempDir's parent
	// chain stops at the OS temp root, which has no .git).
	dir := harness.CopyTree(t, harness.TestdataPath(t, "simple"))
	out, code := harness.RunCLIExpectErr(t, "diff", "images", "--path", dir)
	if code == 0 {
		t.Fatalf("expected non-zero exit with no baseline source, got 0:\n%s", out)
	}
	if !strings.Contains(out, "--path-orig") {
		t.Errorf("error should mention --path-orig as an option:\n%s", out)
	}
	if !strings.Contains(out, "--base") {
		t.Errorf("error should mention --base as an option:\n%s", out)
	}
}

func TestE2E_BootstrapErrorSurfacesNotMasked(t *testing.T) {
	dir := t.TempDir()
	// Minimal repo: one Kustomization + one ResourceSet whose template
	// references an undefined function ("nope"). text/template's Parse
	// rejects it; the ResourceSet render returns an error; Bootstrap
	// returns it; Render nils the Result; the CLI must surface the
	// underlying error rather than running the testrunner on partial
	// state.
	testutil.WriteFile(t, dir, "ks.yaml", `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
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

	out, code := harness.RunCLIExpectErr(t, "test", "all", "--path", dir)
	if code == 0 {
		t.Fatalf("expected non-zero exit; got 0:\n%s", out)
	}
	// The template parse error mentions the undefined function.
	if !strings.Contains(out, "nope") {
		t.Errorf("error must mention the underlying Bootstrap failure (template parse rejecting `nope`); got:\n%s", out)
	}
	// Guard against the regression: the testrunner used to run on
	// the partial Store and emit one "FAILED (no status reported)"
	// row per loaded object. Collect-N where N is the partial count
	// is the signature.
	if strings.Contains(out, "no status reported") {
		t.Errorf("Bootstrap failure should NOT trigger testrunner output; got:\n%s", out)
	}
}

func TestE2E_ExternalSourcedWidePathDoesNotBlockCluster(t *testing.T) {
	dir := t.TempDir()
	// Root entry: cluster-apps renders ./kubernetes/apps from the bootstrap
	// source. Its file lives under ./kubernetes/flux, so the wide sync path
	// ./kubernetes is its only structural ancestor — the trap.
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster.yaml", `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: cluster-apps, namespace: flux-system}
spec:
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources: [./app-a.yaml, ./sync.yaml]\n")
	// A normal app sourced from the bootstrap tree — must pass.
	testutil.WriteFile(t, dir, "kubernetes/apps/app-a.yaml", `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app-a, namespace: default}
spec:
  path: ./kubernetes/apps/app-a
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	testutil.WriteFile(t, dir, "kubernetes/apps/app-a/kustomization.yaml", "resources: [./cm.yaml]\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/app-a/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: app-a, namespace: default}
data: {k: v}
`)
	// The trap: a sync KS with a WIDE path, sourced from an external private repo.
	testutil.WriteFile(t, dir, "kubernetes/apps/sync.yaml", `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: side-repo, namespace: flux-system}
spec:
  url: ssh://git@github.com/someone/private-side-repo.git
  ref: {branch: main}
  secretRef: {name: side-repo-deploy}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: side-sync, namespace: flux-system}
spec:
  path: ./kubernetes
  sourceRef: {kind: GitRepository, name: side-repo, namespace: flux-system}
`)

	// --allow-missing-secrets soft-skips the external source; a skip is not a
	// failure so the run is clean (harness.RunCLI requires exit 0).
	out := harness.RunCLI(t, "test", "all", "--path", dir, "--allow-missing-secrets")
	if !strings.Contains(out, "app-a") || !strings.Contains(out, "passed") {
		t.Errorf("a bootstrap-sourced app must render, not block, behind an external-sourced wide-path KS:\n%s", out)
	}
	if strings.Contains(out, "blocked") {
		t.Errorf("an external-sourced wide-path KS must not parent (and block) the local tree:\n%s", out)
	}
	if !strings.Contains(out, "skipped") {
		t.Errorf("the external-sourced sync KS (and its source) should skip:\n%s", out)
	}
}

func TestE2E_OrphanParentSource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		external bool
	}{
		{name: "local parent"},
		{name: "deeper external parent", external: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "kubernetes/flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: local-apps, namespace: flux-system}
spec:
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system}
`)
			resources := "resources: [configmap.yaml]\n"
			if tc.external {
				resources = "resources: [configmap.yaml, external.yaml]\n"
				testutil.WriteFile(t, dir, "kubernetes/apps/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec:
  url: https://fixture.invalid/external
  secretRef: {name: missing-auth}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: external-parent, namespace: flux-system}
spec:
  path: ./kubernetes/apps/orphan
  sourceRef: {kind: GitRepository, name: external}
`)
			}
			testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", resources)
			testutil.WriteFile(t, dir, "kubernetes/apps/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: healthy-output}
data: {value: rendered}
`)
			testutil.WriteFile(t, dir, "kubernetes/apps/orphan/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: orphan, namespace: flux-system}
spec:
  path: ./missing
  sourceRef: {kind: GitRepository, name: flux-system}
`)
			stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", dir,
				"--allow-missing-secrets", "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
			if code != 0 || !strings.Contains(stdout, "name: healthy-output") || strings.Contains(stdout, "name: orphan\n") {
				t.Fatalf("unreferenced child failed a healthy local build: exit=%d\n%s\n%s", code, stdout, stderr)
			}
		})
	}
}

func TestE2E_ComponentChangePropagatesToAllConsumers(t *testing.T) {
	src := harness.TestdataPath(t, "components")
	orig := harness.CopyTree(t, src)
	current := harness.CopyTree(t, src)

	// Mutate the shared component in only the "current" tree.
	harness.MutateFile(t, filepath.Join(current, "components", "shared", "shared-cm.yaml"), "value: original", "value: changed")

	// -o github explicitly: this test asserts github diff-syntax markers,
	// independent of the default style.
	out := harness.RunCLI(t, "diff", "ks", "--path", current, "--path-orig", orig, "-o", "github")

	// Coverage must propagate to BOTH consumers, so the
	// original→changed transition should surface at least twice (once
	// per consumer Kustomization). dyff prints value changes as bare
	// `- original` / `+ changed` lines under a `@@ data.value @@`
	// path header.
	if got := strings.Count(out, "+ changed"); got < 2 {
		t.Errorf("coverage did not propagate to both consumers (got %d hits, want >= 2):\n%s", got, out)
	}
	if !strings.Contains(out, "- original") {
		t.Errorf("removal of baseline value missing from diff:\n%s", out)
	}
}

func TestE2E_NonSharedChangeDoesNotPropagate(t *testing.T) {
	src := harness.TestdataPath(t, "components")
	orig := harness.CopyTree(t, src)
	current := harness.CopyTree(t, src)

	harness.MutateFile(t, filepath.Join(current, "apps", "app-a", "cm.yaml"), "app: app-a", "app: app-a-edited")

	out := harness.RunCLI(t, "diff", "ks", "--path", current, "--path-orig", orig)

	if !strings.Contains(out, "app-a-edited") {
		t.Errorf("expected app-a change to surface in diff:\n%s", out)
	}
	// app-b must be UNTOUCHED — no app-b-cm body should appear in
	// either diff direction.
	if strings.Contains(out, "name: app-b-cm") {
		t.Errorf("app-b leaked into diff for an app-a-only change (over-broad coverage):\n%s", out)
	}
}

func TestE2E_SkipKindsAppliesToKustomizationOutput(t *testing.T) {
	root := harness.CopyTree(t, harness.TestdataPath(t, "skip-kinds"))
	for _, tc := range []struct {
		name                        string
		args                        []string
		env                         map[string]string
		wantCM, wantSecret, wantCRD bool
	}{
		{name: "defaults", wantCM: true},
		{name: "empty skip", args: []string{"--skip-kinds", ""}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "replacement skip", args: []string{"--skip-kinds", "ConfigMap"}, wantSecret: true, wantCRD: true},
		{name: "only CRDs", args: []string{"--only-kinds", "CustomResourceDefinition"}, wantCRD: true},
		{name: "only Secret", args: []string{"--only-kinds", "Secret"}, wantSecret: true},
		{name: "only multiple", args: []string{"--only-kinds", "ConfigMap,Secret"}, wantCM: true, wantSecret: true},
		{name: "empty only", args: []string{"--only-kinds", ""}, wantCM: true},
		{name: "exact kind", args: []string{"--only-kinds", "secret"}},
		{name: "exact exclusion", args: []string{"--skip-kinds", "secret,customresourcedefinition"}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "env empty only", env: map[string]string{"FLATE_ONLY_KINDS": ""}, wantCM: true},
		{name: "argv overrides empty env skip", args: []string{"--skip-kinds", "Secret,CustomResourceDefinition"}, env: map[string]string{"FLATE_SKIP_KINDS": ""}, wantCM: true},
		{name: "explicit skip wins", args: []string{"--skip-kinds", "Secret,CustomResourceDefinition", "--only-kinds", "CustomResourceDefinition"}},
		{name: "env skip wins", args: []string{"--only-kinds", "CustomResourceDefinition"}, env: map[string]string{"FLATE_SKIP_KINDS": "CustomResourceDefinition"}},
		{name: "env only", env: map[string]string{"FLATE_ONLY_KINDS": "Secret"}, wantSecret: true},
		{name: "env empty skip", env: map[string]string{"FLATE_SKIP_KINDS": ""}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "argv overrides env skip", args: []string{"--skip-kinds", ""}, env: map[string]string{"FLATE_SKIP_KINDS": "ConfigMap"}, wantCM: true, wantSecret: true, wantCRD: true},
		{name: "argv empty only overrides env", args: []string{"--only-kinds", ""}, env: map[string]string{"FLATE_ONLY_KINDS": "Secret"}, wantCM: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			args := append([]string{"build", "ks", "apps", "--path", root, "--no-progress", "--cache-dir", t.TempDir(), "--concurrency", "2"}, tc.args...)
			out := harness.RunCLIStdout(t, args...)
			for kind, want := range map[string]bool{"ConfigMap": tc.wantCM, "Secret": tc.wantSecret, "CustomResourceDefinition": tc.wantCRD} {
				if got := strings.Contains(out, "kind: "+kind+"\n"); got != want {
					t.Errorf("kind %s present=%t, want %t: %s", kind, got, want, out)
				}
			}
		})
	}
}

func TestE2E_OrphanedKustomizationNotDiscovered(t *testing.T) {
	src := harness.TestdataPath(t, "orphans")
	root := harness.CopyTree(t, src)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	// "wired" is referenced and should pass.
	wiredLine := harness.MustExtractLine(t, out, "flux-system/wired")
	if !strings.Contains(wiredLine, "✓") {
		t.Errorf("wired should pass: %s", wiredLine)
	}
	// "orphan" must not appear anywhere in the test output — it was
	// never loaded because the kustomize graph doesn't reach it.
	if strings.Contains(out, "flux-system/orphan") {
		t.Errorf("orphan Kustomization should be invisible to graph-driven loader; output contained reference:\n%s", out)
	}
}

func initGitFixture(t *testing.T) (clusterPath, repoRoot string) {
	t.Helper()
	dir := t.TempDir()
	repo := harness.GitInit(t, dir)
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("kubernetes/flux/cluster.yaml", `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/x.git"}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  interval: 10m
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
	writeFile("kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("kubernetes/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	harness.GitCommitAll(t, repo)
	return filepath.Join(dir, "kubernetes", "flux"), dir
}

func TestE2E_MalformedManifestScope(t *testing.T) {
	for _, tc := range []struct {
		name      string
		graph     bool
		owned     bool
		shared    bool
		wide      bool
		suspended bool
		changed   bool
		generated bool
		external  bool
	}{
		{name: "owned graph", graph: true, owned: true},
		{name: "shared graph", graph: true, owned: true, shared: true},
		{name: "owned ad-hoc", owned: true},
		{name: "owned ad-hoc, wide scan", owned: true, wide: true},
		{name: "suspended owner", owned: true, wide: true, suspended: true},
		{name: "changed-only, unselected owner", owned: true, wide: true, changed: true},
		{name: "ResourceSet-generated owner", owned: true, wide: true, generated: true},
		{name: "suspended generated owner", owned: true, wide: true, generated: true, suspended: true},
		{name: "external-source owner", owned: true, wide: true, external: true},
		{name: "unowned graph", graph: true},
		{name: "unowned ad-hoc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			repo := harness.GitInit(t, dir)
			testutil.WriteFile(t, dir, "monitoring/blackbox-exporter.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: blackbox-exporter, namespace: flux-system}
spec:
  path: ./monitoring/blackbox-exporter/app
  sourceRef: {kind: GitRepository, name: flux-system}
brokenPointer: [ unclosed
`)
			if tc.graph {
				testutil.WriteFile(t, dir, "monitoring/kustomization.yaml", "resources:\n  - blackbox-exporter.yaml\n")
			}
			testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources:\n  - configmap.yaml\n")
			testutil.WriteFile(t, dir, "healthy/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: sibling-output}
data: {value: rendered}
`)
			scan := filepath.Join(dir, "monitoring")
			if tc.owned {
				scan = filepath.Join(dir, "flux")
				testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources:\n  - ks.yaml\n")
				owner := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec:
  path: ./monitoring
  sourceRef: {kind: GitRepository, name: flux-system}
`
				if tc.suspended {
					owner = strings.Replace(owner, "path: ./monitoring", "path: ./monitoring\n  suspend: true", 1)
				}
				if tc.generated {
					owner = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: owners, namespace: flux-system}
spec:
  resourcesTemplate: |
    ` + strings.ReplaceAll(strings.TrimSpace(owner), "\n", "\n    ") + "\n"
				}
				if tc.external {
					owner = strings.Replace(owner, "name: flux-system}", "name: external}", 1) + `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec: {url: 'https://fixture.invalid/other'}
`
				}
				testutil.WriteFile(t, dir, "flux/ks.yaml", owner+`---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: sibling, namespace: flux-system}
spec:
  path: ./healthy
  sourceRef: {kind: GitRepository, name: flux-system}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: dependent, namespace: flux-system}
spec:
  path: ./healthy
  dependsOn: [{name: owner}]
  sourceRef: {kind: GitRepository, name: flux-system}
`)
			}
			if tc.shared {
				testutil.WriteFile(t, dir, "flux/peer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: peer, namespace: flux-system}
spec:
  path: ./monitoring
  sourceRef: {kind: GitRepository, name: flux-system}
`)
				testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [ks.yaml, peer.yaml]\n")
			}
			if tc.wide {
				scan = dir
			}
			args := []string{"build", "all", "--path", scan, "--no-progress", "--concurrency", "2"}
			if tc.changed {
				harness.GitCommitAll(t, repo)
				original := harness.CopyTree(t, dir)
				harness.MutateFile(t, filepath.Join(dir, "healthy/configmap.yaml"), "value: rendered", "value: changed")
				args = append(args, "--path-orig", original)
			}
			if tc.external {
				args = []string{"diff", "all", "--path", scan, "--path-orig", harness.CopyTree(t, dir), "--no-progress", "--concurrency", "2"}
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			if tc.external {
				if code != 0 || stdout != "" || !strings.Contains(stderr, "outside the local tree; not rendered") || strings.Contains(stderr, "decode") {
					t.Fatalf("external-source owner failed the diff: exit=%d\n%s\n%s", code, stdout, stderr)
				}
				return
			}
			if tc.suspended || tc.changed {
				if code != 0 || !strings.Contains(stdout, "sibling-output") || strings.Contains(stderr, "decode") {
					t.Fatalf("unreconciled owner failed the run: exit=%d\n%s\n%s", code, stdout, stderr)
				}
				if tc.changed && (!strings.Contains(stdout, "value: changed") || strings.Contains(stdout, "name: blackbox-exporter")) {
					t.Fatalf("changed sibling did not render alone:\n%s", stdout)
				}
				return
			}
			if code != 1 || !strings.Contains(stderr, "blackbox-exporter.yaml") {
				t.Fatalf("expected exit 1 naming malformed file; got %d:\n%s", code, stderr)
			}
			if tc.owned {
				wantDecodes := 1
				if tc.shared {
					wantDecodes++
				}
				if got := strings.Count(stderr, "did not find expected"); got != wantDecodes {
					t.Fatalf("decode error reported %d times:\n%s", got, stderr)
				}
				if strings.Contains(stderr, "flate error: loader: decode") {
					t.Fatalf("owned decode error escaped resource attribution:\n%s", stderr)
				}
				if !strings.Contains(stdout, "sibling-output") || !strings.Contains(stderr, "flux-system/owner") || !strings.Contains(stderr, "flux-system/dependent") {
					t.Fatalf("expected sibling output and scoped owner/dependent failures:\n%s\n%s", stdout, stderr)
				}
				if tc.shared && !strings.Contains(stderr, "flux-system/peer") {
					t.Fatalf("second owner escaped the malformed file failure:\n%s", stderr)
				}
				stdout, stderr, code = harness.RunCLIBuffers("build", "ks", "sibling", "--path", scan, "--no-progress", "--concurrency", "2")
				if code != 0 || !strings.Contains(stdout, "sibling-output") {
					t.Fatalf("healthy named sibling failed: exit=%d\n%s\n%s", code, stdout, stderr)
				}
			} else if !strings.Contains(stderr, "decode") {
				t.Fatalf("unowned malformed file lost decode error:\n%s", stderr)
			}
		})
	}
}
