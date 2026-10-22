package e2e_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/go-git/go-git/v5/config"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ArtifactGenerator_AllStrategies(t *testing.T) {
	root := t.TempDir()
	repository := testutil.GitInit(t, root)
	if _, err := repository.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.invalid/repo.git"}}); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "entry/sources.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: repo, namespace: apps}
spec: {url: https://example.invalid/repo.git, interval: 1m}
---
apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps, uid: authored}
spec:
  sources: [{alias: repo, kind: GitRepository, name: repo}]
  artifacts:
    - name: generated
      copy:
        - {from: '@repo/app/', to: '@artifact/app/'}
        - {from: '@repo/base.yaml', to: '@artifact/app/app/config.yaml'}
        - {from: '@repo/overlay.yaml', to: '@artifact/app/app/config.yaml', strategy: Merge}
        - {from: '@repo/archive.tgz', to: '@artifact/app/app/', strategy: Extract}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: apps}
spec:
  interval: 1m
  path: ./app/app
  sourceRef: {kind: ExternalArtifact, name: generated}
`)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [config.yaml, extracted.yaml]\n")
	testutil.WriteFile(t, root, "base.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: merged, namespace: apps}\ndata: {a: before, b: retained}\n")
	testutil.WriteFile(t, root, "overlay.yaml", "data: {a: after}\n")
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	tw := tar.NewWriter(gz)
	content := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extracted, namespace: apps}\ndata: {value: archive}\n"
	if err := tw.WriteHeader(&tar.Header{Name: "extracted.yaml", Mode: 0o644, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "archive.tgz"), compressed.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"build", "all", "--path", filepath.Join(root, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", t.TempDir(), "--skip-kinds="}
	stdout, _ := harness.RequireCLIOK(t, args...)
	for _, want := range []string{"name: merged", "a: after", "b: retained", "name: extracted", "value: archive", "kind: ExternalArtifact", "source.extensions.fluxcd.io/generator: authored"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q in output:\n%s", want, stdout)
		}
	}
	again, _ := harness.RequireCLIOK(t, args...)
	if stdout != again {
		t.Fatal("generated output changed between equal runs")
	}
}

func TestE2E_ArtifactGenerator_BootstrapInput(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/generator.yaml", `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps}
spec:
  sources: [{alias: repo, kind: GitRepository, name: missing}]
  artifacts:
    - name: generated
      copy: [{from: '@repo/app/', to: '@artifact/app/'}]
`)
	consumer := testutil.KustomizationYAML("consumer", "apps", "  path: ./app/app\n  sourceRef: {kind: ExternalArtifact, name: generated}\n")
	testutil.WriteFile(t, root, "entry/consumer.yaml", consumer)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: bootstrap, namespace: apps}\n")
	stdout, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", t.TempDir(), "--skip-kinds=")
	if !strings.Contains(stdout, "name: bootstrap") || !strings.Contains(stdout, "kind: ExternalArtifact") {
		t.Fatalf("missing bootstrap-generated output:\n%s", stdout)
	}
	original := t.TempDir()
	testutil.CopyTree(t, root, original)
	testutil.WriteFile(t, root, "entry/consumer.yaml", consumer+"  targetNamespace: apps\n")
	args := []string{"build", "all", "--path", filepath.Join(root, "entry"), "--path-orig", filepath.Join(original, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", t.TempDir(), "--skip-kinds="}
	stdout, stderr := harness.RequireCLIOK(t, args...)
	if strings.Count(stdout, "name: bootstrap") != 1 {
		t.Fatalf("consumer-only change skipped its static generator:\n%s\n%s", stdout, stderr)
	}
	again, _ := harness.RequireCLIOK(t, args...)
	if again != stdout {
		t.Fatal("static generator consumer output changed between equal runs")
	}
}

func TestE2E_ArtifactGenerator_PartialFailure(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/generator.yaml", `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps}
spec:
  sources: [{alias: repo, kind: GitRepository, name: missing}]
  artifacts:
    - {name: generated-a, copy: [{from: '@repo/a/', to: '@artifact/'}]}
    - {name: generated-b, copy: [{from: '@repo/b/', to: '@artifact/'}]}
    - {name: generated-c, copy: [{from: '@repo/c/', to: '@artifact/'}]}
`)
	for _, name := range []string{"a", "b", "c"} {
		testutil.WriteFile(t, root, "entry/"+name+".yaml", testutil.KustomizationYAML(name, "apps", fmt.Sprintf("  path: ./%s\n  sourceRef: {kind: ExternalArtifact, name: generated-%s}\n", name, name)))
		if name != "c" {
			testutil.WriteFile(t, root, name+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, name+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: accepted-"+name+", namespace: apps}\ndata: {value: "+name+"}\n")
		}
	}
	args := []string{"build", "all", "--path", filepath.Join(root, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", t.TempDir(), "--skip-kinds="}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	if code != 1 || !strings.Contains(stderr, "evaluate generated-c") || !strings.Contains(stderr, "required by apps/c") || !strings.Contains(stderr, "1 blocked") {
		t.Fatalf("partial generation lost failure diagnostics: code=%d\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, name := range []string{"a", "b"} {
		if strings.Count(stdout, "name: accepted-"+name) != 1 || !strings.Contains(stdout, "name: generated-"+name) {
			t.Fatalf("partial generation lost accepted %s output:\n%s\n%s", name, stdout, stderr)
		}
	}
	if strings.Contains(stdout, "name: generated-c") || strings.Contains(stdout, "name: accepted-c") {
		t.Fatalf("failed output acquired content:\n%s", stdout)
	}
	again, _, againCode := harness.RunCLIBuffers(args...)
	if againCode != code || again != stdout {
		t.Fatalf("partial generation changed between equal runs: exits=%d/%d\nfirst:\n%s\nsecond:\n%s", code, againCode, stdout, again)
	}
}

func TestE2E_ArtifactGenerator_EmittedChangedOnlyDiff(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./generators\n  components: [../shared]\n  targetNamespace: apps\n  sourceRef: {kind: GitRepository, name: missing}\n"))
	testutil.WriteFile(t, root, "entry/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n  sourceRef: {kind: ExternalArtifact, name: generated}\n"))
	testutil.WriteFile(t, root, "entry/unrelated.yaml", testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: missing}\n"))
	testutil.WriteFile(t, root, "generators/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [generator.yaml]\n")
	testutil.WriteFile(t, root, "shared/generator.yaml", `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, uid: authored}
spec:
  sources: [{alias: repo, kind: GitRepository, name: missing}]
  artifacts:
    - name: generated
      copy:
        - {from: '@repo/app/kustomization.yaml', to: '@artifact/app/kustomization.yaml'}
        - {from: '@repo/before.yaml', to: '@artifact/app/cm.yaml'}
`)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	for _, value := range []string{"before", "after"} {
		testutil.WriteFile(t, root, value+".yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-config, namespace: apps}\ndata: {value: "+value+"}\n")
	}
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-config, namespace: apps}\n")
	original := t.TempDir()
	testutil.CopyTree(t, root, original)
	testutil.WriteFile(t, original, ".git/shallow", strings.Repeat("f", 40)+"\n")
	harness.MutateFile(t, filepath.Join(root, "shared/generator.yaml"), "@repo/before.yaml", "@repo/after.yaml")
	cacheDir := t.TempDir()
	for _, tc := range []struct {
		name, command, current, original, before, after string
	}{
		{"build", "build", root, original, "before", "after"},
		{"forward", "diff", root, original, "before", "after"},
		{"reverse", "diff", original, root, "after", "before"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{tc.command, "all", "--path", filepath.Join(tc.current, "entry"), "--path-orig", filepath.Join(tc.original, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", cacheDir, "--skip-kinds="}
			if tc.command == "diff" {
				args = append(args, "-o", "diff")
			}
			stdout, stderr := harness.RequireCLIOK(t, args...)
			if !strings.Contains(stdout, "value: "+tc.after) || strings.Contains(stdout, "unrelated-config") {
				t.Fatalf("changed-only output lost generated consumer or selected unrelated sibling:\n%s\n%s", stdout, stderr)
			}
			if tc.command == "build" && (strings.Count(stdout, "name: generated-config") != 1 || !strings.Contains(stdout, "kind: ExternalArtifact")) {
				t.Fatalf("emitted generator output was missing or rendered twice:\n%s", stdout)
			}
			if tc.command == "diff" && (strings.Count(stdout, "-  value: "+tc.before) != 1 || strings.Count(stdout, "+  value: "+tc.after) != 1) {
				t.Fatalf("diff lost one generated side:\n%s", stdout)
			}
			again, _ := harness.RequireCLIOK(t, args...)
			if again != stdout {
				t.Fatalf("%s output changed between equal runs:\n%s\n%s", tc.command, stdout, again)
			}
		})
	}
}

func TestE2E_ArtifactGenerator_BuiltBootstrapInput(t *testing.T) {
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "entry/kustomization.yaml", `resources: [generator.yaml, consumer.yaml]
patches:
- target: {kind: ArtifactGenerator, name: generator}
  patch: |-
    - op: replace
      path: /spec/artifacts/0/copy/1/from
      value: '@repo/transformed.yaml'
    - op: replace
      path: /metadata/uid
      value: built-generator
`)
	testutil.WriteFile(t, root, "entry/generator.yaml", `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps, uid: raw-generator}
spec:
  sources: [{alias: repo, kind: GitRepository, name: missing}]
  artifacts:
    - name: generated
      copy:
        - {from: '@repo/app/kustomization.yaml', to: '@artifact/app/kustomization.yaml'}
        - {from: '@repo/raw.yaml', to: '@artifact/app/cm.yaml'}
`)
	testutil.WriteFile(t, root, "entry/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n  sourceRef: {kind: ExternalArtifact, name: generated}\n"))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	for _, value := range []string{"raw", "transformed"} {
		testutil.WriteFile(t, root, value+".yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-config, namespace: apps}\ndata: {value: "+value+"}\n")
	}
	args := []string{"build", "all", "--path", filepath.Join(root, "entry"), "--concurrency=2", "--no-progress", "--cache-dir", t.TempDir(), "--skip-kinds="}
	stdout, _ := harness.RequireCLIOK(t, args...)
	if strings.Count(stdout, "name: generated-config") != 1 || strings.Count(stdout, "\nkind: ExternalArtifact\n") != 1 || !strings.Contains(stdout, "value: transformed") || !strings.Contains(stdout, "source.extensions.fluxcd.io/generator: built-generator") || strings.Contains(stdout, "value: raw") || strings.Contains(stdout, "raw-generator") {
		t.Fatalf("built generator lost transformed input or duplicated output:\n%s", stdout)
	}
	again, _ := harness.RequireCLIOK(t, args...)
	if again != stdout {
		t.Fatal("built generator output changed between equal runs")
	}
}
