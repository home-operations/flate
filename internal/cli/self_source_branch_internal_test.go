package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
)

//nolint:paralleltest // CLI invocations call setLogLevel, which replaces slog.Default and log output.
func TestRun_DiffSelfSourceBranch(t *testing.T) {
	for _, checkout := range []string{"dirty", "branch", "detached", "nested"} {
		t.Run(checkout, func(t *testing.T) {
			root := writeFixture(t)
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [cluster.yaml, source.yaml]\n")
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: home-kubernetes, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {branch: main}
  ignore: |
    /ignored
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: supplier, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {name: refs/remotes/origin/main}
`)
			testutil.WriteFile(t, root, "flux/cluster.yaml", testutil.KustomizationYAML(
				"apps",
				"flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: home-kubernetes}\n",
			)+"---\n"+testutil.KustomizationYAML(
				"fixed-apps",
				"flux-system",
				"  path: ./fixed-apps\n  sourceRef: {kind: GitRepository, name: supplier}\n",
			))
			testutil.WriteFile(t, root, "fixed-apps/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"fixed-apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixed, namespace: apps}\ndata: {greeting: hi}\n",
			)

			scan := filepath.Join(root, "flux")
			if checkout == "nested" {
				scan = filepath.Join(scan, "config")
				assert.NoError(t, os.MkdirAll(scan, 0o755))
				assert.NoError(
					t,
					os.Rename(filepath.Join(root, "flux", "source.yaml"), filepath.Join(scan, "source.yaml")),
				)
				assert.NoError(
					t,
					os.Rename(filepath.Join(root, "flux", "cluster.yaml"), filepath.Join(root, "flux", "apps.yaml")),
				)
				testutil.WriteFile(
					t,
					scan,
					"cluster.yaml",
					testutil.KustomizationYAML(
						"cluster",
						"flux-system",
						"  path: ./flux\n  sourceRef: {kind: GitRepository, name: home-kubernetes}\n",
					),
				)
				testutil.WriteFile(t, scan, "kustomization.yaml", "resources: [cluster.yaml, source.yaml]\n")
				testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [apps.yaml, config]\n")
			}

			repo := testutil.GitInit(t, root)
			_, err := repo.CreateRemote(
				&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/home-ops.git"}},
			)
			assert.NoError(t, err)
			author := &object.Signature{Name: "t", Email: "t@e"}
			base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: author})
			assert.NoError(
				t,
				repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), base)),
			)
			assert.NoError(
				t,
				repo.Storer.SetReference(
					plumbing.NewHashReference(plumbing.NewRemoteReferenceName("origin", "main"), base),
				),
			)

			wt, err := repo.Worktree()
			assert.NoError(t, err)
			assert.NoError(
				t,
				wt.Checkout(
					&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName("pull-request"), Create: true},
				),
			)
			testutil.WriteFile(t, root, "branch.txt", "pull request\n")
			testutil.GitCommitAll(t, repo, "branch", &git.CommitOptions{Author: author})

			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"fixed-apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixed, namespace: apps}\ndata: {greeting: hola}\n",
			)

			if checkout != "dirty" {
				head := testutil.GitCommitAll(t, repo, "change", &git.CommitOptions{Author: author})
				if checkout == "detached" {
					assert.NoError(t, wt.Checkout(&git.CheckoutOptions{Hash: head}))
					assert.NoError(t, repo.Storer.RemoveReference(plumbing.NewBranchReferenceName("main")))
				}
			}

			result := runCLI(t, "diff", "all", "--path", scan, "--base", base.String(),
				"--concurrency", "2", "-o", "json")
			assert.FatalIf(t, result.code != 0, "diff exited %d: %s", result.code, result.stderr)

			var want bytes.Buffer
			assert.NoError(t, json.Indent(&want, []byte(`{"changes":[{
"parent":{"group":"kustomize.toolkit.fluxcd.io","kind":"Kustomization","namespace":"flux-system","name":"apps","path":"apps"},
"apiVersion":"v1","kind":"ConfigMap","namespace":"apps","name":"hello","status":"changed",
"old":{"apiVersion":"v1","data":{"greeting":"hi"},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"apps","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"hello","namespace":"apps"}},
"new":{"apiVersion":"v1","data":{"greeting":"hola"},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"apps","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"hello","namespace":"apps"}},
"oldChart":"","newChart":""}],"suppressed":[]}`), "", "  "))
			want.WriteByte('\n')
			assert.Diff(t, result.stdout, want.String())
		})
	}
}

//nolint:paralleltest // CLI invocations call setLogLevel, which replaces slog.Default and log output.
func TestRun_DiffSelfSourceGeneratedMembership(t *testing.T) {
	for _, status := range []string{"added", "removed"} {
		t.Run(status, func(t *testing.T) {
			root := writeFixture(t)
			assert.NoError(t, os.Remove(filepath.Join(root, "apps", "kustomization.yaml")))
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [cluster.yaml, source.yaml]\n")
			testutil.WriteFile(t, root, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {branch: main}
`)

			extra := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\ndata: {value: retained}\n"
			if status == "removed" {
				testutil.WriteFile(t, root, "apps/extra.yaml", extra)
			}

			repo := testutil.GitInit(t, root)
			_, err := repo.CreateRemote(
				&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/home-ops.git"}},
			)
			assert.NoError(t, err)
			author := &object.Signature{Name: "t", Email: "t@e"}
			base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: author})
			assert.NoError(
				t,
				repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), base)),
			)

			if status == "added" {
				testutil.WriteFile(t, root, "apps/extra.yaml", extra)
			} else {
				assert.NoError(t, os.Remove(filepath.Join(root, "apps", "extra.yaml")))
			}

			result := runCLI(
				t, "diff", "all", "--path", filepath.Join(root, "flux"), "--base", base.String(), "-o", "json",
			)
			assert.FatalIf(t, result.code != 0, "diff exited %d: %s", result.code, result.stderr)

			present := `{"apiVersion":"v1","data":{"value":"retained"},"kind":"ConfigMap","metadata":{"labels":{"kustomize.toolkit.fluxcd.io/name":"apps","kustomize.toolkit.fluxcd.io/namespace":"flux-system"},"name":"extra","namespace":"apps"}}`
			oldDocument, newDocument := "null", present
			if status == "removed" {
				oldDocument, newDocument = present, "null"
			}

			expected := fmt.Sprintf(`{"changes":[{
"parent":{"group":"kustomize.toolkit.fluxcd.io","kind":"Kustomization","namespace":"flux-system","name":"apps","path":"apps"},
"apiVersion":"v1","kind":"ConfigMap","namespace":"apps","name":"extra","status":%q,
"old":%s,"new":%s,"oldChart":"","newChart":""}],"suppressed":[]}`, status, oldDocument, newDocument)

			var want bytes.Buffer
			assert.NoError(t, json.Indent(&want, []byte(expected), "", "  "))
			want.WriteByte('\n')
			assert.Diff(t, result.stdout, want.String())
		})
	}
}

//nolint:paralleltest // CLI invocations call setLogLevel, which replaces slog.Default and log output.
func TestRun_Diff_BranchBootstrapReach(t *testing.T) {
	root := t.TempDir()
	const url = "https://example.invalid/home-ops.git"
	testutil.WriteFile(t, root, "kube/flux/kustomization.yaml",
		"resources: [sync.yaml, ../apps/selected, ../apps/idle]\n")
	testutil.WriteFile(t, root, "kube/flux/sync.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.invalid/home-ops.git
  ref: {branch: main}
  ignore: |
    /*
    !/kube
---
`+testutil.KustomizationYAML("cluster", "flux-system",
		`  path: ./kube/flux
  sourceRef: {kind: GitRepository, name: flux-system}
`))
	owner := func(name, namespace, substitution string) string {
		return testutil.KustomizationYAML(name+"-app", namespace, fmt.Sprintf(`  path: ./kube/apps/%[1]s/app
  targetNamespace: %[1]s
  components: [../../../shared]
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
%[2]s`, name, substitution))
	}
	const release = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s}
spec:
  chart:
    spec:
      chart: kube/chart
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  values: {greeting: "%s"}
`
	const postBuild = "  postBuild:\n    substitute: {GREETING: %s}\n"
	testutil.WriteFile(
		t,
		root,
		"kube/apps/selected/ks.yaml",
		owner("selected", "selected", fmt.Sprintf(postBuild, "before")),
	)
	testutil.WriteFile(t, root, "kube/apps/idle/ks.yaml", owner("idle", "flux-system", ""))
	testutil.WriteFile(t, root, "kube/apps/selected/app/release.yaml", fmt.Sprintf(release, "selected", "${GREETING}"))
	testutil.WriteFile(t, root, "kube/apps/idle/app/release.yaml", fmt.Sprintf(release, "idle", "before"))
	for _, name := range []string{"selected", "idle"} {
		testutil.WriteFile(t, root, "kube/apps/"+name+"/kustomization.yaml", "resources: [ks.yaml]\n")
	}
	testutil.WriteFile(t, root, "kube/shared/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component
resources: [alert.yaml]
`)
	testutil.WriteFile(t, root, "kube/shared/alert.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: shared-alert}
`)
	testutil.WriteFile(t, root, "kube/chart/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "kube/chart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}', namespace: '{{ .Release.Namespace }}'}
data: {greeting: '{{ .Values.greeting }}'}
`)
	testutil.WriteFile(t, root, "ignore.txt", "outside the source\n")
	repo := testutil.GitInit(t, root)
	_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}})
	assert.NoError(t, err)
	base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e"},
	})
	assert.NoError(
		t,
		repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewBranchReferenceName("main"), base)),
	)
	testutil.WriteFile(
		t,
		root,
		"kube/apps/selected/ks.yaml",
		owner("selected", "selected", fmt.Sprintf(postBuild, "after")),
	)

	result := runCLI(t, "diff", "all", "--path", filepath.Join(root, "kube", "flux"), "--base", base.String(),
		"--cache-dir", t.TempDir(), "--concurrency", "2", "--log-level", "debug", "--output", "diff")
	assert.FatalIf(t, result.code != 0, "diff exited %d: %s", result.code, result.stderr)
	const want = `--- ConfigMap selected/selected
+++ ConfigMap selected/selected
@@ -1,6 +1,6 @@
 apiVersion: v1
 data:
-  greeting: before
+  greeting: after
 kind: ConfigMap
 metadata:
   labels:

--- Kustomization selected/selected-app (kube/flux)
+++ Kustomization selected/selected-app (kube/flux)
@@ -12,7 +12,7 @@
   path: ./kube/apps/selected/app
   postBuild:
     substitute:
-      GREETING: before
+      GREETING: after
   sourceRef:
     kind: GitRepository
     name: flux-system

--- HelmRelease selected/selected (kube/apps/selected/app)
+++ HelmRelease selected/selected (kube/apps/selected/app)
@@ -15,5 +15,5 @@
         name: flux-system
         namespace: flux-system
   values:
-    greeting: before
+    greeting: after
` + " \n"
	assert.Diff(t, result.stdout, want)
	var discoveries, summaries, renders int
	for line := range strings.Lines(result.stderr) {
		switch {
		case strings.Contains(line, "discovery: loaded objects") && !strings.Contains(line, " count=0 "):
			discoveries++
			// Each side reads the four declarations once through its checkout mount.
			if !strings.Contains(line, " count=4 rounds=1 ") {
				t.Errorf("unexpected discovery work: %s", line)
			}
		case strings.Contains(line, "orchestrator: reconcile complete"):
			summaries++
			summary, _, _ := strings.Cut(line, " work=")
			if !strings.Contains(summary, "helm_releases=1 ") {
				t.Errorf("unexpected release reach: %s", summary)
			}
		case strings.Contains(line, "kustomize: build"):
			if strings.Contains(line, "path=./kube/apps/idle") {
				t.Errorf("unexpected owner build: %s", line)
			}
		case strings.Contains(line, "helm: render"):
			renders++
			assert.FatalIf(
				t,
				!strings.HasSuffix(
					strings.TrimSpace(line),
					"release=HelmRelease.helm.toolkit.fluxcd.io/selected/selected",
				),
				"unexpected Helm template: %s",
				line,
			)
		}
	}
	assert.Equal(t, discoveries, 2)
	assert.Equal(t, summaries, 2)
	assert.Equal(t, renders, 2)
}
