package cli

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func writeCustomSourceDiffFixture(t *testing.T, sourceURL string) (current, orig string) {
	t.Helper()
	current = t.TempDir()
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
	orig = t.TempDir()
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
				"--cache-dir", t.TempDir(), "-o", "diff")
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
