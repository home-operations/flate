// Package scope tests render selection, ownership and change scope.
package scope

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/go-git/go-git/v5/config"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
	testutil.WriteFile(t, root, "flux/apps.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
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
	_, failures, ok := strings.Cut(stderr, "flate error: ")
	if !ok {
		t.Fatalf("fatal report missing:\n%s", stderr)
	}
	for side := range strings.SplitSeq(failures, "current snapshot:") {
		if !strings.Contains(side, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apps") || !strings.Contains(side, "missing.yaml") {
			t.Errorf("snapshot failure missing resource detail:\n%s", stderr)
		}
	}
	if stdout != "" {
		t.Errorf("suppressed diff must have no product: %s", stdout)
	}
	for _, want := range []string{"Kustomization.kustomize.toolkit.fluxcd.io flux-system/apps", "diff suppressed", "failed on both sides: orig:", "; current:"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q:\n%s", want, stderr)
		}
	}
	for side := range strings.SplitSeq(stderr, "; current:") {
		if !strings.Contains(side, "missing.yaml") {
			t.Errorf("suppressed failure missing resource detail:\n%s", stderr)
		}
	}
	for _, format := range []string{"html", "json"} {
		product, diagnostic, exit := harness.RunCLIBuffers("diff", "all", "--path", root, "--path-orig", root, "--concurrency=2", "-o", format)
		if exit != 1 || strings.Count(diagnostic, "[DiffSuppressed]") != 1 {
			t.Fatalf("%s suppression: exit=%d stdout=%s stderr=%s", format, exit, product, diagnostic)
		}
		if format == "html" {
			if product != "" {
				t.Fatalf("suppressed HTML product: %s", product)
			}
		} else {
			var result struct {
				Changes    []any
				Suppressed []struct {
					Side   string
					Count  int
					Reason string
				}
			}
			if err := json.Unmarshal([]byte(product), &result); err != nil || len(result.Changes) != 0 || len(result.Suppressed) != 1 || result.Suppressed[0].Side != "both" || result.Suppressed[0].Count != 0 || !strings.Contains(result.Suppressed[0].Reason, "missing.yaml") {
				t.Fatalf("structured suppression lost: %s, %v", product, err)
			}
		}
	}
	images, imageDiagnostic, imageExit := harness.RunCLIBuffers("diff", "images", "--path", root, "--path-orig", root, "--concurrency=2", "-o", "json")
	var imageProduct []string
	if err := json.Unmarshal([]byte(images), &imageProduct); err != nil || len(imageProduct) != 0 || imageExit != 1 || strings.Count(imageDiagnostic, "[DiffSuppressed]") != 1 {
		t.Fatalf("image suppression: exit=%d stdout=%s stderr=%s err=%v", imageExit, images, imageDiagnostic, err)
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
			testutil.WriteFile(t, side.path, "shared/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  interval: 10m\n  path: ./shared\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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
	repo := testutil.GitInit(t, dir)
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: "origin", URLs: []string{"https://github.com/example/cluster.git"},
	}); err != nil {
		t.Fatal(err)
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
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
	return dir
}

func TestE2E_DiffImagesRequiresBaselineWhenNoGit(t *testing.T) {
	// Copy to a tempdir with no .git ancestor (t.TempDir's parent
	// chain stops at the OS temp root, which has no .git).
	dir := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "simple"), dir)
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
	testutil.WriteFile(t, dir, "ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources: [./app-a.yaml, ./sync.yaml]\n")
	// A normal app sourced from the bootstrap tree — must pass.
	testutil.WriteFile(t, dir, "kubernetes/apps/app-a.yaml", testutil.KustomizationYAML("app-a", "default", "  path: ./kubernetes/apps/app-a\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
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

func TestE2E_SecretMaterialPolicy(t *testing.T) {
	_, identity := testutil.SelfSignedClientCert(t)
	for _, field := range []string{"known_hosts", "bearerToken"} {
		type policyCase struct {
			name            string
			allowMissing    bool
			producer        bool
			malformed       bool
			invalidIdentity bool
			wantSkip        bool
		}
		cases := []policyCase{
			{name: "strict"},
			{name: "allowed", allowMissing: true, wantSkip: true},
			{name: "producer", producer: true, wantSkip: true},
			{name: "malformed allowed", malformed: true, allowMissing: true},
			{name: "malformed producer", malformed: true, producer: true},
		}
		if field == "known_hosts" {
			cases = append(cases,
				policyCase{name: "invalid identity allowed", allowMissing: true, invalidIdentity: true},
				policyCase{name: "invalid identity producer", producer: true, invalidIdentity: true},
			)
		}
		for _, tc := range cases {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				testutil.GitInit(t, dir)
				url := "https://fixture.invalid/external.git"
				if field == "known_hosts" {
					url = "ssh://git@fixture.invalid/external.git"
				}
				testutil.WriteFile(t, dir, "flux/root.yaml", testutil.KustomizationYAML("local", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n")+"---\n"+
					fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec:
  url: %s
  secretRef: {name: auth}
`, url)+"---\n"+testutil.KustomizationYAML("consumer", "flux-system", "  path: ./external\n  sourceRef: {kind: GitRepository, name: external}\n"))
				material := base64.StdEncoding.EncodeToString([]byte("..PLACEHOLDER_" + field + ".."))
				if tc.malformed {
					material = "%%%fixture%%%"
				}
				keyMaterial := identity
				if tc.invalidIdentity {
					keyMaterial = "invalid-private-fixture"
				}
				testutil.WriteFile(t, dir, "apps/secret.yaml", fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata: {name: auth, namespace: flux-system}
data:
  identity: %s
  username: dXNlcg==
  password: cGFzc3dvcmQ=
  %s: %q
`, base64.StdEncoding.EncodeToString([]byte(keyMaterial)), field, material))
				testutil.WriteFile(t, dir, "apps/healthy.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy-output}\ndata: {value: rendered}\n")
				resources := "resources: [secret.yaml, healthy.yaml]\n"
				if tc.producer {
					resources = "resources: [secret.yaml, healthy.yaml, producer.yaml]\n"
					testutil.WriteFile(t, dir, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: flux-system}
spec:
  target: {name: auth}
`)
				}
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", resources)
				args := []string{"build", "all", "--path", filepath.Join(dir, "flux"), "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir()}
				if tc.allowMissing {
					args = append(args, "--allow-missing-secrets")
				}
				stdout, stderr, code := harness.RunCLIBuffers(args...)
				wantCode := 1
				if tc.wantSkip {
					wantCode = 0
				}
				errorField := field
				if tc.invalidIdentity {
					errorField = "identity"
				}
				if !tc.wantSkip && (!strings.Contains(stderr, "flux-system/external") || !strings.Contains(stderr, "flux-system/auth") || !strings.Contains(stderr, errorField)) {
					t.Errorf("missing source/Secret/field failure context:\n%s", stderr)
				}
				if code != wantCode || !strings.Contains(stdout, "name: healthy-output") {
					t.Fatalf("exit=%d, want %d and healthy local output:\n%s\n%s", code, wantCode, stdout, stderr)
				}
				if tc.malformed && (strings.Contains(stderr, "skipped") || !strings.Contains(stderr, "base64")) {
					t.Errorf("malformed material must fail without a skip:\n%s", stderr)
				}
				if tc.invalidIdentity && (strings.Contains(stderr, "skipped") || strings.Contains(stderr, keyMaterial) || !strings.Contains(stderr, "parse SSH identity")) {
					t.Errorf("invalid identity must fail without a skip or payload disclosure:\n%s", stderr)
				}
				if strings.Contains(stderr, "dial tcp") || strings.Contains(stderr, material) {
					t.Errorf("material resolution must stop before fetch without exposing payloads:\n%s", stderr)
				}
				if tc.wantSkip {
					args[0] = "test"
					out, diagnostic, testCode := harness.RunCLIBuffers(args...)
					if testCode != 0 {
						t.Fatalf("skipped source failed test: exit=%d\n%s\n%s", testCode, out, diagnostic)
					}
					if !strings.Contains(out, "flux-system/consumer") || !strings.Contains(out, "flux-system/external") || !strings.Contains(out, "flux-system/local") || !strings.Contains(out, "1 passed · 2 skipped") {
						t.Errorf("expected dependent skip and local render:\n%s\n%s", out, diagnostic)
					}
				}
			})
		}
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
			testutil.WriteFile(t, dir, "kubernetes/flux/ks.yaml", testutil.KustomizationYAML("local-apps", "flux-system", "  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			resources := "resources: [configmap.yaml]\n"
			if tc.external {
				resources = "resources: [configmap.yaml, external.yaml]\n"
				testutil.WriteFile(t, dir, "kubernetes/apps/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec:
  url: https://fixture.invalid/external
  secretRef: {name: missing-auth}
`+"---\n"+
					testutil.KustomizationYAML("external-parent", "flux-system", `  path: ./kubernetes/apps/orphan
  sourceRef: {kind: GitRepository, name: external}
`))
			}
			testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", resources)
			testutil.WriteFile(t, dir, "kubernetes/apps/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: healthy-output}
data: {value: rendered}
`)
			testutil.WriteFile(t, dir, "kubernetes/apps/orphan/ks.yaml", testutil.KustomizationYAML("orphan", "flux-system", "  path: ./missing\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
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
	orig := t.TempDir()
	testutil.CopyTree(t, src, orig)
	current := t.TempDir()
	testutil.CopyTree(t, src, current)

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
	orig := t.TempDir()
	testutil.CopyTree(t, src, orig)
	current := t.TempDir()
	testutil.CopyTree(t, src, current)

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
	root := t.TempDir()
	testutil.CopyTree(t, harness.TestdataPath(t, "skip-kinds"), root)
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
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

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
	repo := testutil.GitInit(t, dir)
	writeFile := func(rel, body string) { testutil.WriteFile(t, dir, rel, body) }
	writeFile("kubernetes/flux/cluster.yaml", "---\n"+
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/x.git"}
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`))
	writeFile("kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	writeFile("kubernetes/apps/cm.yaml", `---
apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data:
  value: original
`)
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
			repo := testutil.GitInit(t, dir)
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
				owner := testutil.KustomizationYAML("owner", "flux-system", "  path: ./monitoring\n  sourceRef: {kind: GitRepository, name: flux-system}\n")
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
				testutil.WriteFile(t, dir, "flux/ks.yaml", owner+"---\n"+
					testutil.KustomizationYAML("sibling", "flux-system", `  path: ./healthy
  sourceRef: {kind: GitRepository, name: flux-system}
`)+"---\n"+
					testutil.KustomizationYAML("dependent", "flux-system", `  path: ./healthy
  dependsOn: [{name: owner}]
  sourceRef: {kind: GitRepository, name: flux-system}
`))
			}
			if tc.shared {
				testutil.WriteFile(t, dir, "flux/peer.yaml", testutil.KustomizationYAML("peer", "flux-system", "  path: ./monitoring\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
				testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [ks.yaml, peer.yaml]\n")
			}
			if tc.wide {
				scan = dir
			}
			args := []string{"build", "all", "--path", scan, "--no-progress", "--concurrency", "2"}
			if tc.changed {
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
				original := t.TempDir()
				testutil.CopyTree(t, dir, original)
				harness.MutateFile(t, filepath.Join(dir, "healthy/configmap.yaml"), "value: rendered", "value: changed")
				args = append(args, "--path-orig", original)
			}
			if tc.external {
				original := t.TempDir()
				testutil.CopyTree(t, dir, original)
				args = []string{"diff", "all", "--path", scan, "--path-orig", original, "--no-progress", "--concurrency", "2"}
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
			} else {
				want := "decode"
				if tc.graph {
					want = "kustomize build monitoring"
				}
				if !strings.Contains(stderr, want) {
					t.Fatalf("unowned malformed file lost its owning error:\n%s", stderr)
				}
			}
		})
	}
}

func TestE2E_BootstrapSiblingNamespaceProjection(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [sibling.yaml]\ntransformers: [transformers]\n")
	testutil.WriteFile(t, root, "app/transformers/kustomization.yaml", "namespace: apps\nresources: [../../transformers]\n")
	testutil.WriteFile(t, root, "transformers/kustomization.yaml", "resources: [transformer.yaml]\n")
	testutil.WriteFile(t, root, "transformers/transformer.yaml", `apiVersion: builtin
kind: NamespaceTransformer
metadata: {name: apply-ns, namespace: .invalid}
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`)
	testutil.WriteFile(t, root, "app/sibling.yaml", testutil.KustomizationYAML("sibling", "flux-system", "  path: ./leaf\n"))
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf}\n")

	args := []string{"build", "all", "--path", root, "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
	first, _ := harness.RequireCLIOK(t, args...)
	second, _ := harness.RequireCLIOK(t, args...)
	if first != second {
		t.Fatal("projected bootstrap output changed between runs")
	}
	docs, err := manifest.SplitDocs([]byte(first))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		if manifest.DocKind(doc) == "ConfigMap" && name == "leaf" {
			if namespace != "apps" {
				t.Fatalf("leaf namespace = %q, want apps:\n%s", namespace, first)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("projected bootstrap root did not render its leaf:\n%s", first)
	}
}

func TestE2E_SelectedKustomizeRoot(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "app/kustomization.yaml", `resources: [release.yaml, repository.yaml, child.yaml]
nameSuffix: -dev
patches:
  - target: {kind: HelmRelease, name: podinfo}
    patch: |-
      - op: replace
        path: /spec/chart/spec/version
        value: 6.5.0
`)
	testutil.WriteFile(t, root, "app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: podinfo, namespace: apps}
spec:
  suspend: true
  chart:
    spec:
      chart: podinfo
      version: 6.0.0
      sourceRef: {kind: HelmRepository, name: podinfo}
`)
	testutil.WriteFile(t, root, "app/repository.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: podinfo, namespace: apps}
spec: {suspend: true, url: https://fixture.invalid/charts}
`)
	testutil.WriteFile(t, root, "app/unlisted.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: unlisted, namespace: apps}
spec: {suspend: true}
`)
	testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./unavailable\n  suspend: true\n"))
	testutil.WriteFile(t, root, "app/excluded.yaml", testutil.KustomizationYAML("excluded-driver", "apps", "  path: ./must-not-run\n")+"---\n"+`apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: excluded-source, namespace: apps}
spec: {url: https://must-not-run.invalid/charts}
`)
	testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "flux-system", "  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n"))

	out, stderr := harness.RequireCLIOK(t, "get", "hr", "-o", "json", "--path", filepath.Join(root, "app"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	var releases []struct{ Name, Version string }
	if err := json.Unmarshal([]byte(out), &releases); err != nil {
		t.Fatal(err)
	}
	if len(releases) != 1 || releases[0].Name != "podinfo-dev" || releases[0].Version != "6.5.0" {
		t.Fatalf("selected root lost transformations or admitted a sibling:\n%s\n%s", out, stderr)
	}

	flags := []string{"--path", filepath.Join(root, "app"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
	build, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	second, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
	if build != second {
		t.Fatal("direct build output changed between runs")
	}
	docs, err := manifest.SplitDocs([]byte(build))
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range docs {
		name, _ := manifest.DocMetadata(doc)
		if name == "flate-bootstrap" || strings.HasPrefix(name, "excluded-") || name == "unlisted" || name == "child" {
			t.Fatalf("unexpected authored or bootstrap document: %v", doc)
		}
	}
	kss, _ := harness.RequireCLIOK(t, append([]string{"get", "ks", "-o", "json"}, flags...)...)
	var owners []struct{ Name string }
	if err := json.Unmarshal([]byte(kss), &owners); err != nil {
		t.Fatal(err)
	}
	if len(owners) != 1 || owners[0].Name != "child-dev" {
		t.Fatalf("ordinary root/child collection = %s", kss)
	}
	owned, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
	ownedDocs, err := manifest.SplitDocs([]byte(owned))
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 3 || len(ownedDocs) != 3 {
		t.Fatalf("direct/owned document counts: %d/%d", len(docs), len(ownedDocs))
	}
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		if namespace != "apps" || name != "podinfo-dev" && name != "child-dev" {
			t.Fatalf("selected entry lost a transformed identity: %v", doc)
		}
	}
	for _, doc := range ownedDocs {
		name, namespace := manifest.DocMetadata(doc)
		if namespace != "apps" || name != "podinfo-dev" && name != "child-dev" {
			t.Fatalf("actual Flux owner lost a transformed identity: %v", doc)
		}
		md := doc["metadata"].(map[string]any)
		labels := md["labels"].(map[string]any)
		if labels[manifest.FluxKustomizeDomain+"/name"] != "owner" || labels[manifest.FluxKustomizeDomain+"/namespace"] != "flux-system" {
			t.Fatalf("actual Flux owner labels missing: %v", labels)
		}
	}

	tested, _ := harness.RequireCLIOK(t, append([]string{"test", "hr"}, flags...)...)
	if !strings.Contains(tested, "podinfo-dev") || strings.Contains(tested, "flate-bootstrap") {
		t.Fatalf("test did not use real transformed identity: %s", tested)
	}

	t.Run("unsuspended child", func(t *testing.T) {
		testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"))
		testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf-output}\n")
		out, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
		if !strings.Contains(out, "name: leaf-output") || !strings.Contains(out, "kustomize.toolkit.fluxcd.io/name: child-dev") {
			t.Fatalf("built child did not render its leaf under its real ownership: %s", out)
		}
		named, _ := harness.RequireCLIOK(t, append([]string{"build", "ks", "child-dev"}, flags...)...)
		if !strings.Contains(named, "name: leaf-output") || strings.Contains(named, "name: podinfo-dev") {
			t.Fatalf("named child selection mixed root siblings with descendants: %s", named)
		}
	})

	t.Run("excluded entry dependency", func(t *testing.T) {
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [child.yaml, settings.yaml]\nnameSuffix: -dev\n")
		testutil.WriteFile(t, root, "app/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: resolved}\n")
		testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", `  path: ./leaf
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: settings-dev}]
`))
		testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [account.yaml]\n")
		testutil.WriteFile(t, root, "leaf/account.yaml", "apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: visible, annotations: {value: '${VALUE}'}}\n")
		args := append([]string{"build", "all", "--skip-kinds", "ConfigMap"}, flags...)
		out, _ := harness.RequireCLIOK(t, args...)
		warm, _ := harness.RequireCLIOK(t, args...)
		if out != warm {
			t.Fatal("excluded dependency output changed on a cache hit")
		}
		docs, err := manifest.SplitDocs([]byte(out))
		if err != nil {
			t.Fatal(err)
		}
		for _, doc := range docs {
			if manifest.DocKind(doc) == manifest.KindConfigMap {
				t.Fatalf("excluded entry dependency leaked into output: %v", doc)
			}
		}
		if !strings.Contains(out, "kind: ServiceAccount\n") || !strings.Contains(out, "value: resolved") || !strings.Contains(out, "kustomize.toolkit.fluxcd.io/name: child-dev") {
			t.Fatalf("excluded entry values lost admission or visible descendant stamping: %s", out)
		}
	})

}

func TestE2E_SelectedKustomizeRootNestedOwner(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "nameSuffix: -dev\nresources: [child.yaml, cm.yaml, nested]\n")
	testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./app/nested\n"))
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: outer, namespace: apps}\n")
	testutil.WriteFile(t, root, "app/nested/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/nested/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: inner, namespace: apps}\n")
	flags := []string{"build", "all", "--path", filepath.Join(root, "app"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
	out, _ := harness.RequireCLIOK(t, flags...)
	second, _ := harness.RequireCLIOK(t, flags...)
	if out != second {
		t.Fatal("selected entry output changed between runs")
	}
	t.Logf("full selected entry output:\n%s", out)
	docs, err := manifest.SplitDocs([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	identities := map[string]bool{}
	for _, doc := range docs {
		name, namespace := manifest.DocMetadata(doc)
		identities[manifest.DocKind(doc)+"/"+name] = true
		if name == "inner" {
			labels := doc["metadata"].(map[string]any)["labels"].(map[string]any)
			if namespace != "apps" || labels[manifest.FluxKustomizeDomain+"/name"] != "child-dev" || labels[manifest.FluxKustomizeDomain+"/namespace"] != "apps" {
				t.Fatalf("nested child reconciliation lost ownership: %v", doc)
			}
		}
	}
	want := map[string]bool{"ConfigMap/inner": true, "ConfigMap/inner-dev": true, "ConfigMap/outer-dev": true, "Kustomization/child-dev": true}
	if difference := cmp.Diff(want, identities); difference != "" {
		t.Fatalf("selected entry documents (-want +got):\n%s", difference)
	}
	if len(docs) != len(want) {
		t.Fatalf("selected entry document count = %d, want %d", len(docs), len(want))
	}
}

func TestE2E_SelectedKustomizeClassification(t *testing.T) {
	for _, tc := range []struct {
		name, file, body string
		fail             bool
	}{
		{name: "yaml", file: "kustomization.yaml", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "yml", file: "kustomization.yml", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "capitalized", file: "Kustomization", body: "resources: [cm.yaml]\nnameSuffix: -dev\n"},
		{name: "plain"},
		{name: "malformed", file: "kustomization.yaml", body: "resources: [\n", fail: true},
		{name: "multiple", file: "kustomization.yaml", body: "resources: [cm.yaml]\n", fail: true},
		{name: "component", file: "kustomization.yaml", body: "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [missing.yaml]\n", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: original}\n")
			if tc.file != "" {
				testutil.WriteFile(t, root, tc.file, tc.body)
			}
			if tc.name == "multiple" {
				testutil.WriteFile(t, root, "kustomization.yml", tc.body)
			}
			if tc.name == "plain" {
				testutil.WriteFile(t, root, "entry.yaml", testutil.KustomizationYAML("plain", "apps", "  path: ./unavailable\n  suspend: true\n"))
				out, _ := harness.RequireCLIOK(t, "get", "ks", "--path", root, "-o", "json", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
				var owners []struct{ Name string }
				if err := json.Unmarshal([]byte(out), &owners); err != nil || len(owners) != 1 || owners[0].Name != "plain" {
					t.Fatalf("plain folder roots = %s, %v", out, err)
				}
				return
			}
			out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root,
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if tc.fail {
				if code != 1 || strings.Contains(out, "value: original") || stderr == "" {
					t.Fatalf("upstream failure exposed raw fallback: code=%d stdout=%s stderr=%s", code, out, stderr)
				}
				return
			}
			want := "input-dev"
			if tc.name == "plain" {
				want = "input"
			}
			if code != 0 || !strings.Contains(out, "name: "+want+"\n") {
				t.Fatalf("entry classification lost its resource: code=%d stdout=%s stderr=%s", code, out, stderr)
			}
		})
	}
}

func TestE2E_SelectedKustomizeRootDiff(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		before, after                            string
		patch, sibling, plain, declaration, list bool
		want                                     map[string]string
	}{
		{name: "referenced patch", before: "resources: [deployment.yaml, cm.yaml]\npatches: [{path: patch.yaml}]\n", patch: true, want: map[string]string{"web": "changed"}},
		{name: "resource removed", before: "resources: [deployment.yaml, cm.yaml]\n", after: "resources: [deployment.yaml]\n", want: map[string]string{"config": "removed"}},
		{name: "resource added", before: "resources: [deployment.yaml]\n", after: "resources: [deployment.yaml, cm.yaml]\n", want: map[string]string{"config": "added"}},
		{name: "list member added", before: "resources: [deployment.yaml, cm.yaml]\n", list: true, want: map[string]string{"config": "added"}},
		{name: "list member removed", before: "resources: [deployment.yaml, cm.yaml]\n", list: true, want: map[string]string{"config": "removed"}},
		{name: "declaration removed", before: "resources: [deployment.yaml, child.yaml]\nnameSuffix: -dev\n", after: "resources: [deployment.yaml]\nnameSuffix: -dev\n", declaration: true, want: map[string]string{"child-dev": "removed"}},
		{name: "declaration added", before: "resources: [deployment.yaml]\nnameSuffix: -dev\n", after: "resources: [deployment.yaml, child.yaml]\nnameSuffix: -dev\n", declaration: true, want: map[string]string{"child-dev": "added"}},
		{name: "excluded sibling", before: "resources: [deployment.yaml, cm.yaml]\n", sibling: true},
		{name: "plain dependency", plain: true, sibling: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig, current := t.TempDir(), t.TempDir()
			for _, root := range []string{orig, current} {
				testutil.GitInit(t, root)
				if !tc.plain {
					testutil.WriteFile(t, root, "app/kustomization.yaml", tc.before)
				}
				testutil.WriteFile(t, root, "app/deployment.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: web, image: 'example.test/web:1'}]}}}\n")
				testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: config}\ndata: {value: selected}\n")
				testutil.WriteFile(t, root, "app/unlisted.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\ndata: {value: before}\n")
				if tc.declaration {
					testutil.WriteFile(t, root, "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"))
					testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")
				}
				if tc.patch {
					testutil.WriteFile(t, root, "app/patch.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: web, image: 'example.test/web:1'}]}}}\n")
				}
			}
			if tc.list {
				empty := "apiVersion: v1\nkind: ConfigMapList\nitems: []\n"
				member := "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: config}, data: {value: selected}}]\n"
				before, after := empty, member
				if tc.want["config"] == "removed" {
					before, after = member, empty
				}
				testutil.WriteFile(t, orig, "app/cm.yaml", before)
				testutil.WriteFile(t, current, "app/cm.yaml", after)
			}
			if tc.after != "" {
				testutil.WriteFile(t, current, "app/kustomization.yaml", tc.after)
			}
			if tc.patch {
				harness.MutateFile(t, filepath.Join(current, "app/patch.yaml"), "web:1", "web:2")
			}
			if tc.sibling {
				harness.MutateFile(t, filepath.Join(current, "app/unlisted.yaml"), "value: before", "value: after")
			}
			flags := []string{"--path", filepath.Join(current, "app"), "--path-orig", filepath.Join(orig, "app"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
			out, _ := harness.RequireCLIOK(t, append([]string{"diff", "all", "-o", "json"}, flags...)...)
			if tc.declaration {
				t.Logf("paired declaration diff: %s", out)
			}
			var result struct {
				Changes []struct {
					Name, Status string
					Parent       map[string]any
				}
				Suppressed []any
			}
			if err := json.Unmarshal([]byte(out), &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Changes) != len(tc.want) || len(result.Suppressed) != 0 {
				t.Fatalf("unexpected paired diff: %s", out)
			}
			for _, change := range result.Changes {
				if tc.want[change.Name] != change.Status {
					t.Fatalf("change did not retain resource identity: %s", out)
				}
				for _, value := range change.Parent {
					if value != "" {
						t.Fatalf("ownerless resource acquired parent attribution: %s", out)
					}
				}
			}
			if tc.list {
				reversed, _ := harness.RequireCLIOK(t, "diff", "all", "-o", "json", "--path", filepath.Join(orig, "app"), "--path-orig", filepath.Join(current, "app"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
				forward := result.Changes[0]
				if err := json.Unmarshal([]byte(reversed), &result); err != nil {
					t.Fatal(err)
				}
				if len(result.Changes) != 1 || len(result.Suppressed) != 0 {
					t.Fatalf("reversed member diff: %s", reversed)
				}
				back := result.Changes[0]
				want := "added"
				if forward.Status == "added" {
					want = "removed"
				}
				if back.Name != forward.Name || back.Status != want || cmp.Diff(back.Parent, forward.Parent) != "" {
					t.Fatalf("member pairing did not reverse: %s / %s", out, reversed)
				}
			}
			if tc.patch {
				images, _ := harness.RequireCLIOK(t, append([]string{"diff", "images", "--include-removed", "-o", "json"}, flags...)...)
				var got []string
				if err := json.Unmarshal([]byte(images), &got); err != nil {
					t.Fatal(err)
				}
				if difference := cmp.Diff([]string{"example.test/web:1", "example.test/web:2"}, got); difference != "" {
					t.Fatalf("ownerless image diff (-want +got):\n%s", difference)
				}
			}
		})
	}
}
