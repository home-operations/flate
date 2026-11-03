package helm

import (
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

func TestE2E_ChangedOnlyEmittedKustomizationPath(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		root       bool
	}{
		{name: "templated", path: "{{ .Values.root }}"},
		{name: "templated root Kustomization", path: "{{ .Values.root }}", root: true},
		{name: "literal", path: "./apps/child"},
		{name: "literal root Kustomization", path: "./apps/child", root: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			repo := testutil.GitInit(t, dir)
			_, err := repo.CreateRemote(
				&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}},
			)
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
			release := "flux/emitter.yaml"
			if tc.root {
				release = "apps/hr/emitter.yaml"
				testutil.WriteFile(
					t,
					dir,
					"flux/apps.yaml",
					testutil.KustomizationYAML(
						"apps",
						"flux-system",
						"  path: ./apps/hr\n  sourceRef: {kind: GitRepository, name: cluster}\n",
					),
				)
			}
			testutil.WriteFile(t, dir, release, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: emitter, namespace: flux-system}
spec:
  chart:
    spec:
      chart: charts/emitter
      sourceRef: {kind: GitRepository, name: cluster}
  values: {root: ./apps/child}
`)
			testutil.WriteFile(t, dir, "charts/emitter/Chart.yaml", "apiVersion: v2\nname: emitter\nversion: 1.0.0\n")
			testutil.WriteFile(
				t,
				dir,
				"charts/emitter/templates/child.yaml",
				testutil.KustomizationYAML(
					"child",
					"flux-system",
					"  path: \""+tc.path+"\"\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				),
			)
			testutil.WriteFile(t, dir, "apps/child/kustomization.yaml", "resources: [configmap.yaml]\n")
			body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: flux-system}\ndata: {value: before}\n"
			testutil.WriteFile(t, dir, "apps/child/configmap.yaml", body)
			base := testutil.GitCommitAll(t, repo, "fixture", &gogit.CommitOptions{
				Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
			})
			testutil.WriteFile(t, dir, "apps/child/configmap.yaml", strings.ReplaceAll(body, "before", "after"))
			args := []string{
				"diff", "all", "--path", filepath.Join(dir, "flux"), "--base", base.String(),
				"-o", "diff", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "--log-level", "debug",
			}
			var cold string
			for _, state := range []string{"cold", "warm"} {
				t.Run(state, func(t *testing.T) {
					out, stderr := harness.RequireCLIOK(t, args...)
					if !strings.Contains(out, "-  value: before") || !strings.Contains(out, "+  value: after") ||
						!strings.Contains(out, "child-output") {
						t.Fatalf("emitted child change missing:\n%s\n%s", out, stderr)
					}
					if cold == "" {
						cold = out
					} else {
						assert.Equal(t, out, cold)
					}
				})
			}
		})
	}
}
