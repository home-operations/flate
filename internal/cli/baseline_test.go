package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestRun_BasePathDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, want string
	}{
		{"missing original path", "--path"},
		{"missing selected path", "--path"},
		{"non git", "is not inside a git working tree; pass --path-orig=<dir> or --base=<rev>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeFixture(t)
			args := []string{"diff", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"}
			switch tc.name {
			case "missing original path":
				args = append(args, "--path-orig", filepath.Join(root, "missing"))
			case "missing selected path":
				args[3] = filepath.Join(root, "missing")
				args = append(args, "--base", "HEAD")
			case "non git":
				args = append(args, "--base", "HEAD")
			}
			_, stderr, code := runCLI(t, args...)
			if code != 1 || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit=%d stderr=%s want %s", code, stderr, tc.want)
			}
			if strings.Contains(tc.name, "path") && !strings.Contains(stderr, filepath.Join(root, "missing")) {
				t.Fatalf("missing path context: %s", stderr)
			}
			if tc.name == "missing selected path" && !strings.Contains(stderr, "pass --path-orig=<dir> or --base=<rev>") {
				t.Fatalf("missing next-step guidance: %s", stderr)
			}
			if tc.name == "non git" && !strings.Contains(stderr, "--path \""+root+"\"") {
				t.Fatalf("missing flag context: %s", stderr)
			}
		})
	}
}

func TestRun_BaseLinkExclusion(t *testing.T) {
	for _, route := range []string{"base", "path orig", "referenced link"} {
		t.Run(route, func(t *testing.T) {
			root := writeFixture(t)
			assert.NoError(t, os.Symlink("target-A", filepath.Join(root, "apps", "link.yaml")))
			args := []string{"diff", "ks", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff"}
			switch route {
			case "base":
				repo := testutil.GitInit(t, root)
				pin := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
				args = append(args, "--base", pin.String())
			case "path orig":
				orig := writeFixture(t)
				assert.NoError(t, os.Symlink("target-A", filepath.Join(orig, "apps", "link.yaml")))
				args = append(args, "--path-orig", orig)
			case "referenced link":
				testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, link.yaml]\n")
				testutil.WriteFile(t, root, "apps/target-B", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: decoy}\n")
				args = []string{"build", "ks", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2"}
			}
			assert.NoError(t, os.Remove(filepath.Join(root, "apps", "link.yaml")))
			assert.NoError(t, os.Symlink("target-B", filepath.Join(root, "apps", "link.yaml")))
			stdout, stderr, code := runCLI(t, args...)
			if route == "referenced link" {
				if code != 1 || !strings.Contains(stderr, "apps/link.yaml") || !strings.Contains(stderr, "permission denied") || strings.Contains(stdout, "name: decoy") {
					t.Fatalf("omitted reference: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			} else if code != 0 || stdout != "" {
				t.Fatalf("link-only comparison: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestRun_BaseFailureIsolation(t *testing.T) {
	root := writeFixture(t)
	const url = "git://fixture.invalid/cluster"
	testutil.WriteFile(t, root, "flux/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: "+url+"}\n")
	for _, name := range []string{"apps", "same"} {
		testutil.WriteFile(t, root, "flux/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  {path: ./"+name+", sourceRef: {kind: GitRepository, name: cluster}}\n"))
	}
	assert.NoError(t, os.Remove(filepath.Join(root, "flux", "cluster.yaml")))
	testutil.WriteFile(t, root, "same/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "same/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {value: before}\n")
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}})
	assert.NoError(t, err)
	pin := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
	testutil.WriteFile(t, root, "same/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\ndata: {value: after}\n")
	assert.NoError(t, os.Rename(filepath.Join(root, "apps"), filepath.Join(root, "apps_real")))
	assert.NoError(t, os.Symlink("apps_real", filepath.Join(root, "apps")))
	stdout, stderr, code := runCLI(t, "diff", "ks", "--path", filepath.Join(root, "flux"), "--base", pin.String(), "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
	if code != 1 || !strings.Contains(stderr, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apps") || !strings.Contains(stderr, "diff suppressed") || strings.Contains(stdout, "diff suppressed") ||
		strings.Contains(stdout, "greeting:") || strings.Contains(stdout, "name: hello") ||
		!strings.Contains(stdout, "-  value: before") || !strings.Contains(stdout, "+  value: after") {
		t.Fatalf("failed producer lost isolation: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestRun_EntryIgnoreDenial(t *testing.T) {
	root := writeFixture(t)
	assert.NoError(t, os.Symlink("apps/cm.yaml", filepath.Join(root, ".krmignore")))
	stdout, stderr, code := runCLI(t, "build", "ks", "--path", root, "--concurrency", "2")
	if code != 1 || !strings.Contains(stderr, "permission denied") {
		t.Fatalf("entry denial: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestRun_PathOrigSameDirectory(t *testing.T) {
	root := writeFixture(t)
	alias := filepath.Join(t.TempDir(), "same")
	assert.NoError(t, os.Symlink(root, alias))
	stdout, stderr, code := runCLI(t, "build", "ks", "--path", root, "--path-orig", alias,
		"--cache-dir", t.TempDir(), "--concurrency", "2")
	if code != 0 || !strings.Contains(stdout, "name: hello") {
		t.Fatalf("same-directory full render changed: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}
