package cli_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/cli"
	"github.com/home-operations/flate/internal/testutil"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

func TestE2E_DeclaredEntrySource(t *testing.T) {
	t.Setenv("FLATE_CACHE_DIR", "")
	var attempts atomic.Int64
	routes := sourcegit.Transports{
		"git": func(context.Context, sourcegit.TransportRequest) (sourcegit.Connection, error) {
			attempts.Add(1)
			return nil, transport.ErrRepositoryNotFound
		},
	}
	for _, declarationPath := range []string{"entry", "entry/config"} {
		t.Run(declarationPath, func(t *testing.T) {
			root := t.TempDir()
			if declarationPath == "entry" {
				testutil.WriteFile(
					t,
					root,
					"entry/kustomization.yaml",
					"resources: [source.yaml, cluster.yaml, apps.yaml]\n",
				)
			} else {
				testutil.WriteFile(
					t,
					root,
					"entry/config/kustomization.yaml",
					"resources: [source.yaml, cluster.yaml]\n",
				)
			}
			testutil.WriteFile(
				t,
				root,
				filepath.Join(declarationPath, "source.yaml"),
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: theshire, namespace: flux-system}\nspec: {url: git://declared.invalid/theshire.git, ref: {branch: main}}\n",
			)
			testutil.WriteFile(
				t,
				root,
				filepath.Join(declarationPath, "cluster.yaml"),
				testutil.KustomizationYAML("cluster", "flux-system",
					"  path: ./entry\n  sourceRef: {kind: GitRepository, name: theshire}\n"),
			)
			testutil.WriteFile(t, root, "entry/apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system",
				"  path: ./apps\n  sourceRef: {kind: GitRepository, name: theshire}\n"))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hello.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/hello.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: committed}\n",
			)
			repository := testutil.GitInit(t, root)
			remote := "https://mirror.invalid/theshire.git"
			if declarationPath == "entry" {
				remote = "git://declared.invalid/theshire.git"
			}
			_, err := repository.CreateRemote(
				&config.RemoteConfig{Name: "origin", URLs: []string{remote}},
			)
			assert.NoError(t, err)
			testutil.GitCommitAll(
				t,
				repository,
				"entry",
				&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
			)
			testutil.WriteFile(
				t,
				root,
				"apps/hello.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: dirty}\n",
			)
			want := "---\napiVersion: v1\ndata:\n  value: dirty\nkind: ConfigMap\nmetadata:\n  labels:\n    kustomize.toolkit.fluxcd.io/name: cluster-apps\n    kustomize.toolkit.fluxcd.io/namespace: flux-system\n  name: hello\n  namespace: apps\n"
			for range 2 {
				t.Setenv("XDG_CACHE_HOME", t.TempDir())
				var stdout, stderr bytes.Buffer
				code := cli.Run([]string{
					"build", "ks", "cluster-apps", "--path", filepath.Join(root, "entry"), "-n", "flux-system",
					"--concurrency", "2", "--no-progress",
				}, &stdout, &stderr, cli.RunConfig{GitTransports: routes})
				assert.Equal(t, code, 0)
				assert.Equal(t, stdout.String(), want)
				assert.Equal(t, stderr.String(), "")
				assert.Equal(t, attempts.Load(), int64(0))
			}
		})
	}
}

func TestE2E_ReadyEntrySource(t *testing.T) {
	t.Setenv("FLATE_CACHE_DIR", "")
	root := t.TempDir()
	testutil.WriteFile(t, root, "clusters/prod/config/kustomization.yaml", "resources: [flux.yaml, apps.yaml]\n")
	testutil.WriteFile(t, root, "clusters/prod/config/flux.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux, namespace: flux-system}
spec:
  sync:
    name: cluster-repository
    kind: GitRepository
    url: https://declared.invalid/cluster.git
    ref: refs/heads/main
    path: clusters/prod/config
`)
	testutil.WriteFile(
		t,
		root,
		"clusters/prod/config/apps.yaml",
		testutil.KustomizationYAML("cluster-apps", "flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: cluster-repository}\n"),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
	repository := testutil.GitInit(t, root)
	_, err := repository.CreateRemote(
		&config.RemoteConfig{Name: "origin", URLs: []string{"https://declared.invalid/cluster.git"}},
	)
	assert.NoError(t, err)
	testutil.GitCommitAll(
		t,
		repository,
		"entry",
		&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
	)
	want := "\n  ✓  Kustomization  flux-system/cluster-apps\n  ✓  GitRepository  flux-system/cluster-repository\n\n  ✓ 2 passed   <DUR>\n"
	for range 2 {
		t.Setenv("XDG_CACHE_HOME", t.TempDir())
		var stdout, stderr bytes.Buffer
		code := cli.Run([]string{
			"test", "all", "--path", filepath.Join(root, "clusters", "prod"),
			"--concurrency=2", "--no-progress", "--log-level=error",
		}, &stdout, &stderr, cli.RunConfig{})
		got := entrySourceTestOutput(stdout.String())
		assert.Equal(t, code, 0)
		assert.Equal(t, got, want)
		assert.Equal(t, stderr.String(), "")
		assert.Equal(t, strings.Count(stdout.String(), "✓  GitRepository  flux-system/cluster-repository\n"), 1)
	}
}

func TestE2E_ProducedEntrySource(t *testing.T) {
	t.Setenv("FLATE_CACHE_DIR", "")
	instance := `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux-system, namespace: flux-system}
spec:
  sync:
    kind: OCIRepository
    name: flux-system
    url: oci://registry.invalid/cluster
    ref: latest
    path: ./clusters/cluster-00
`
	resourceSet := `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: sync-producer, namespace: flux-system}
spec:
  dependsOn:
    - apiVersion: apiextensions.k8s.io/v1
      kind: CustomResourceDefinition
      name: helmreleases.helm.toolkit.fluxcd.io
  resourcesTemplate: |
    %s
`
	chart := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: flux-instance, namespace: flux-system}
spec:
  chartRef: {kind: OCIRepository, name: flux-instance}
`
	blocked := "  ⊘  ResourceSet    flux-system/sync-producer  blocked by helmreleases.helm.toolkit.fluxcd.io (not found)\n"
	blockedConsumers := `
  ⊘  Kustomization  flux-system/cluster  blocked by flux-system/flux-system (not found)
  ⊘  Kustomization  flux-system/flux-instance  blocked by flux-system/flux-system (not found)
  ⊘  Kustomization  flux-system/flux-operator  blocked by flux-system/flux-system (not found)
  ⊘  Kustomization  flux-system/kustomize-mutating-webhook  blocked by flux-system/flux-system (not found)
`
	chartConsumers := `
  ✗  Kustomization  flux-system/cluster  kustomization path is not a directory: ./apps/overlays/cluster-00: resolve ./apps/overlays/cluster-00: stat apps: file does not exist
  ✗  Kustomization  flux-system/flux-instance  kustomization path is not a directory: ./clusters/cluster-00/flux-system/flux-instance/app: resolve ./clusters/cluster-00/flux-system/flux-instance/app: stat clusters: file does not exist
  ✗  Kustomization  flux-system/flux-operator  kustomization path is not a directory: ./clusters/cluster-00/flux-system/flux-operator/app: resolve ./clusters/cluster-00/flux-system/flux-operator/app: stat clusters: file does not exist
  ✗  Kustomization  flux-system/kustomize-mutating-webhook  kustomization path is not a directory: ./clusters/cluster-00/flux-system/kustomize-mutating-webhook/app: resolve ./clusters/cluster-00/flux-system/kustomize-mutating-webhook/app: stat clusters: file does not exist
  ✓  OCIRepository  flux-system/flux-system
`
	for _, tc := range []struct{ name, document, want string }{
		{
			name: "direct", document: instance,
			want: blockedConsumers + "\n  ✗ 0 passed · 4 blocked   <DUR>\n",
		},
		{
			name:     "template",
			document: fmt.Sprintf(resourceSet, strings.ReplaceAll(strings.TrimSuffix(instance, "\n"), "\n", "\n    ")),
			want:     blockedConsumers + blocked + "\n  ✗ 0 passed · 5 blocked   <DUR>\n",
		},
		{
			name:     "chart",
			document: fmt.Sprintf(resourceSet, strings.ReplaceAll(strings.TrimSuffix(chart, "\n"), "\n", "\n    ")),
			want:     chartConsumers + blocked + "\n  ✗ 1 passed · 4 failed · 1 blocked   <DUR>\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			entry := "kubernetes/clusters/cluster-00"
			for _, consumer := range []struct{ name, path string }{
				{"cluster", "./apps/overlays/cluster-00"},
				{"flux-instance", "./clusters/cluster-00/flux-system/flux-instance/app"},
				{"flux-operator", "./clusters/cluster-00/flux-system/flux-operator/app"},
				{"kustomize-mutating-webhook", "./clusters/cluster-00/flux-system/kustomize-mutating-webhook/app"},
			} {
				testutil.WriteFile(
					t,
					root,
					entry+"/"+consumer.name+"/ks.yaml",
					testutil.KustomizationYAML(consumer.name, "flux-system",
						"  path: "+consumer.path+"\n  sourceRef: {kind: OCIRepository, name: flux-system}\n"),
				)
			}
			testutil.WriteFile(t, root, entry+"/flux-instance/app/producer.yaml", tc.document)
			repository := testutil.GitInit(t, root)
			testutil.GitCommitAll(
				t,
				repository,
				"entry",
				&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
			)
			for range 2 {
				t.Setenv("XDG_CACHE_HOME", t.TempDir())
				var stdout, stderr bytes.Buffer
				code := cli.Run([]string{
					"test", "all", "--path", filepath.Join(root, entry),
					"--concurrency=2", "--no-progress", "--log-level=error",
				}, &stdout, &stderr, cli.RunConfig{})
				got := entrySourceTestOutput(stdout.String())
				assert.Equal(t, code, 1)
				assert.Equal(t, got, tc.want)
				assert.Equal(t, stderr.String(), "")
			}
		})
	}
}

func entrySourceTestOutput(stream string) string {
	pattern := regexp.MustCompile(
		`^([[:space:]]*(✓|✗) [0-9]+ passed( · [0-9]+ (skipped|failed|blocked))*[[:space:]]+)([0-9]+h)?([0-9]+m)?[0-9]+(\.[0-9]+)?(ns|µs|us|ms|s|m|h)$`,
	)
	lines := strings.Split(stream, "\n")
	for i, line := range lines {
		lines[i] = pattern.ReplaceAllString(line, "${1}<DUR>")
	}
	return strings.Join(lines, "\n")
}
