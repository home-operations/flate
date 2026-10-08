package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func writeCustomSourceDiffFixture(t *testing.T, sourceURL string) (current, orig string) {
	t.Helper()
	current = t.TempDir()
	if got := repoRootOf(current); got != current {
		t.Fatalf("fixture inherited a parent repository: %q", got)
	}
	testutil.WriteFile(t, current, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: home-kubernetes, namespace: gitops-system}
spec:
  url: `+sourceURL+`
  interval: 1h
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: gitops-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: home-kubernetes}
  interval: 1h
`)
	testutil.WriteFile(t, current, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, current, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hi}
`)
	testutil.WriteFile(t, current, "flux/second.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: second-apps, namespace: gitops-system}
spec:
  path: ./second-apps
  sourceRef: {kind: GitRepository, name: home-kubernetes}
  interval: 1h
`)
	testutil.WriteFile(t, current, "second-apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, current, "second-apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: second, namespace: apps}
data: {greeting: hi}
`)
	orig = t.TempDir()
	if got := repoRootOf(orig); got != orig {
		t.Fatalf("snapshot inherited a parent repository: %q", got)
	}
	copyTree(t, current, orig)
	testutil.WriteFile(t, current, ".git/config", `[core]
  repositoryformatversion = 0
[remote "origin"]
  url = git@github.com:example/home-ops.git
`)
	testutil.WriteFile(t, current, ".git/HEAD", "ref: refs/heads/main\n")
	return current, orig
}

func TestRun_DiffKS_PathOrigCustomSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		changed bool
	}{
		{name: "identical"},
		{name: "changed", changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/home-ops.git")
			if tc.changed {
				testutil.WriteFile(t, orig, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hola}
`)
			}
			stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			if stderr != "" {
				t.Errorf("local source should render without warnings: %s", stderr)
			}
			if tc.changed {
				for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
					if !strings.Contains(stdout, want) {
						t.Errorf("expected a real field change %q, got:\n%s", want, stdout)
					}
				}
			} else if stdout != "" {
				t.Errorf("identical snapshots should have no diff:\n%s", stdout)
			}
		})
	}
}

func TestRun_DiffKS_ExternalSourceDisclosure(t *testing.T) {
	current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/other-repo.git")
	testutil.WriteFile(t, orig, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hola}
`)
	for _, tc := range []struct {
		name string
		args []string
		warn bool
	}{
		{name: "default", warn: true},
		{name: "error log level", args: []string{"--log-level", "error"}, warn: true},
		{name: "other namespace", args: []string{"--namespace", "other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", filepath.Join(t.TempDir(), "cache"), "-o", "diff"}, tc.args...)
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			if stdout != "" {
				t.Errorf("external source must not render local manifests:\n%s", stdout)
			}
			if tc.warn {
				for _, want := range []string{"orig snapshot", "current snapshot", "Kustomization/gitops-system/apps", "external source", "GitRepository/gitops-system/home-kubernetes", "not rendered"} {
					if !strings.Contains(stderr, want) {
						t.Errorf("missing disclosure %q: %s", want, stderr)
					}
				}
			} else if stderr != "" {
				t.Errorf("out-of-scope source should not warn: %s", stderr)
			}
		})
	}
}

func TestRun_DiffKS_PathOrigRepositoryIdentity(t *testing.T) {
	const currentURL = "ssh://git@github.com/example/home-ops.git"
	const baselineURL = "https://example.invalid/upstream.git"
	for _, tc := range []struct {
		name     string
		linked   bool
		nested   bool
		matching bool
	}{
		{name: "different origins"},
		{name: "matching origins", matching: true},
		{name: "linked worktree", linked: true},
		{name: "nested scan", nested: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := writeCustomSourceDiffFixture(t, currentURL)
			writeBaselineGit(t, current, currentURL)
			url := baselineURL
			if tc.matching {
				url = currentURL
			}
			cluster, err := os.ReadFile(filepath.Join(orig, "flux", "cluster.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, orig, "flux/cluster.yaml", strings.ReplaceAll(string(cluster), currentURL, url))
			if tc.linked {
				common := t.TempDir()
				writeBaselineGit(t, common, url)
				writeBaselineWorktree(t, orig, common)
			} else {
				writeBaselineGit(t, orig, url)
			}
			for _, app := range []string{"apps", "second-apps"} {
				name := "hello"
				if app == "second-apps" {
					name = "second"
				}
				testutil.WriteFile(t, orig, app+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n")
			}
			for _, dir := range []string{current, orig} {
				testutil.WriteFile(t, dir, "flux/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: unrelated, namespace: beta}
spec: {url: "https://example.invalid/unrelated.git", interval: 1h}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: external-apps, namespace: beta}
spec:
  path: ./external
  sourceRef: {kind: GitRepository, name: unrelated}
  interval: 1h
`)
				testutil.WriteFile(t, dir, "external/kustomization.yaml", "resources:\n- cm.yaml\n")
				testutil.WriteFile(t, dir, "external/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: must-not-render}\n")
			}
			currentScan, origScan := current, orig
			if tc.nested {
				currentScan, origScan = filepath.Join(current, "flux"), filepath.Join(orig, "flux")
			}
			c := commonFlags{path: currentScan, pathOrig: origScan}
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer cleanup()
			if err != nil {
				t.Fatal(err)
			}
			if len(c.pathOrigSelfURLs) != 1 || c.pathOrigSelfURLs[0] != url {
				t.Fatalf("baseline identity = %v, want [%s]; stopping before render to prevent an external fetch", c.pathOrigSelfURLs, url)
			}
			args := []string{"diff", "ks", "--path", currentScan, "--path-orig", origScan,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff"}
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
				if count := strings.Count(stdout, want); count != 2 {
					t.Errorf("field diff %q occurred %d times, want 2:\n%s", want, count, stdout)
				}
			}
			for _, unwanted := range []string{"must-not-render", "+apiVersion:", "-apiVersion:", "suppressed"} {
				if strings.Contains(stdout, unwanted) {
					t.Errorf("unexpected %q in field diff:\n%s", unwanted, stdout)
				}
			}
			if strings.Count(stderr, "external source") != 2 || strings.Contains(stderr, "home-kubernetes") {
				t.Errorf("want only unrelated external-source disclosures: %s", stderr)
			}
			secondOut, secondErr, secondCode := runCLI(t, args...)
			if secondOut != stdout || secondErr != stderr || secondCode != code {
				t.Errorf("repeated diff changed output or exit code:\n%s\n%s", secondOut, secondErr)
			}
		})
	}
}

func TestRun_DiffKS_PathOrigCurrentOnlySource(t *testing.T) {
	const currentURL = "ssh://git@github.com/example/home-ops.git"
	for _, tc := range []struct {
		name string
		urls []string
	}{
		{name: "baseline without remotes"},
		{name: "current-only URL", urls: []string{"https://example.invalid/upstream.git"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := writeCustomSourceDiffFixture(t, currentURL)
			writeBaselineGit(t, current, currentURL)
			writeBaselineGit(t, orig, tc.urls...)
			testutil.WriteFile(t, orig, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n")
			c := commonFlags{path: current, pathOrig: orig}
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer cleanup()
			if err != nil {
				t.Fatal(err)
			}
			if len(c.pathOrigSelfURLs) != len(tc.urls) || (len(tc.urls) != 0 && c.pathOrigSelfURLs[0] != tc.urls[0]) {
				t.Fatalf("baseline borrowed current identity: %v", c.pathOrigSelfURLs)
			}
			stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			if strings.Contains(stdout, "hola") {
				t.Errorf("external baseline rendered local manifests:\n%s", stdout)
			}
			if !strings.Contains(stderr, "orig snapshot: Kustomization/gitops-system/apps skipped: external source") || strings.Contains(stderr, "current snapshot") {
				t.Errorf("want only baseline external-source disclosure: %s", stderr)
			}
		})
	}
}
