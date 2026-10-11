package e2e

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_SubstitutionProducerRendering(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteSubstitutionProducerCluster(t, root, kind)
			stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress")
			if code != 0 || !strings.Contains(stdout, "version: 4.6.2") || strings.Contains(stdout, "version: discovered") {
				t.Fatalf("rendered substitution: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestE2E_SubstitutionProducerCycle(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionCycleCluster(t, root)
	const want = "\n  ✗  Kustomization  flux-system/a  dependency cycle: blocked by Kustomization/flux-system/b\n  ✗  Kustomization  flux-system/b  dependency cycle: blocked by Kustomization/flux-system/a\n\n  ✗ 2 failed\n"
	for _, workers := range []string{"1", "2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 15 {
				stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || stdout != "" || stderr != want {
					t.Fatalf("substitution cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
			}
		})
	}
}

func TestE2E_SubstitutionProducerTransformedNamespace(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		version := "4.6.2"
		if kind == "Secret" {
			version = "NC42LjI="
		}
		for _, mode := range []string{"full", "changed"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				root, original := t.TempDir(), t.TempDir()
				for _, dir := range []string{root, original} {
					for _, ks := range []struct{ name, path, postBuild string }{
						{"producer", "data", ""},
						{"consumer", "app", "  postBuild: {substituteFrom: [{kind: " + kind + ", name: settings}]}\n"},
					} {
						testutil.WriteFile(t, dir, "flux/"+ks.name+".yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: "+ks.name+", namespace: flux-system}\nspec:\n  path: ./"+ks.path+"\n"+ks.postBuild)
					}
					testutil.WriteFile(t, dir, "data/kustomization.yaml", "namespace: flux-system\nresources: [settings.yaml]\n")
					testutil.WriteFile(t, dir, "data/settings.yaml", "apiVersion: v1\nkind: "+kind+"\nmetadata: {name: settings, namespace: original}\ndata: {VERSION: "+version+"}\n")
					testutil.WriteFile(t, dir, "app/kustomization.yaml", "namespace: flux-system\nresources: [cm.yaml]\n")
					testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {version: '${VERSION:=empty}'}\n")
				}
				testutil.WriteFile(t, original, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {version: old}\n")
				args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress"}
				if mode == "changed" {
					args = append(args, "--path-orig", original)
				}
				stdout, stderr, code := runCLIBuffers(args...)
				if code != 0 || !strings.Contains(stdout, "version: 4.6.2") {
					t.Fatalf("transformed substitution: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			})
		}
	}
}

func TestE2E_SubstitutionCycleDownstream(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionCycleCluster(t, root)
	testutil.WriteFile(t, root, "flux/consumer.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n  path: ./app\n  dependsOn: [{name: a}]\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n")
	var want string
	for _, workers := range []string{"2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 15 {
				stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || stdout != "" || strings.Contains(stderr, "flux-system/consumer  dependency cycle:") ||
					!strings.Contains(stderr, "blocks flux-system/consumer") || !strings.Contains(stderr, "2 failed · 1 blocked") {
					t.Fatalf("downstream cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
				if want != "" && stderr != want {
					t.Fatalf("unstable cycle report: want=%q got=%q", want, stderr)
				}
				want = stderr
			}
		})
	}
}
