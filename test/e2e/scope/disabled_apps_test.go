package scope

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_DisabledAppAdmission(t *testing.T) {
	for _, tc := range []struct{ name, body, message string }{
		{"api version", "apiVersion: source.toolkit.fluxcd.io/\nkind: GitRepository\nmetadata: {name: invalid, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n", "apiVersion"},
		{"yaml syntax", "apiVersion: [\n", "yaml"},
	} {
		t.Run("inactive invalid manifest/"+tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			disabledAppFixture(t, root, "single", false)
			testutil.WriteFile(t, root, "targets/game/cm.yaml", tc.body)
			out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--no-progress")
			if code != 0 || strings.Contains(out, "name: invalid") {
				t.Fatalf("disabled invalid manifest failed: exit=%d stderr=%s", code, stderr)
			}
			testutil.WriteFile(t, root, "apps/games/kustomization.yaml", "namespace: games\nresources: [disabled.yaml]\n")
			if _, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--no-progress"); code != 1 || !strings.Contains(stderr, tc.message) {
				t.Fatalf("enabled invalid manifest lost failure: exit=%d stderr=%s", code, stderr)
			}
		})
	}

	t.Run("shared active target and component", func(t *testing.T) {
		root := t.TempDir()
		testutil.GitInit(t, root)
		disabledAppFixture(t, root, "single", false)
		testutil.WriteFile(t, root, "active.yaml", testutil.KustomizationYAML("active", "games", "  path: ./targets/game\n  components: [../backup]\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
		out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--no-progress")
		if code != 0 {
			t.Fatalf("shared active target exit=%d stderr=%s", code, stderr)
		}
		docs, err := manifest.SplitDocs([]byte(out))
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, doc := range docs {
			name, _ := manifest.DocMetadata(doc)
			counts[name]++
		}
		if counts["disabled"] != 0 || counts["game-cm"] != 1 || counts["backup-cm"] != 1 {
			t.Fatalf("shared output lost or disabled owner rendered: %v", counts)
		}
	})
	t.Run("cross-package inclusion", func(t *testing.T) {
		root := t.TempDir()
		testutil.GitInit(t, root)
		disabledAppFixture(t, root, "single", false)
		testutil.WriteFile(t, root, "independent/kustomization.yaml", "resources: [../apps/games/disabled.yaml]\n")
		out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--no-progress")
		if code != 0 || !strings.Contains(out, "name: game-cm") || !strings.Contains(out, "name: backup-cm") {
			t.Fatalf("independent resource edge did not enable app: exit=%d stderr=%s output=%s", code, stderr, out)
		}
	})

	for _, tc := range []struct{ name, entry, layout string }{
		{"single-root", ".", "single"},
		{"single-scoped", "flux", "single"},
		{"multi-root", ".", "multi"},
		{"multi-scoped", "flux/main", "multi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			for _, enabled := range []bool{false, true} {
				disabledAppFixture(t, root, tc.layout, enabled)
				args := []string{"build", "all", "--path", filepath.Join(root, tc.entry), "--concurrency", "2", "--no-progress"}
				first, stderr, code := harness.RunCLIBuffers(args...)
				if code != 0 || strings.Contains(stderr, "[OrphanedResource]") {
					t.Fatalf("enabled=%t exit=%d stderr=%s", enabled, code, stderr)
				}
				second, _, code := harness.RunCLIBuffers(args...)
				if code != 0 || first != second {
					t.Fatal("identical inputs changed output bytes")
				}
				docs, err := manifest.SplitDocs([]byte(first))
				if err != nil {
					t.Fatal(err)
				}
				found := map[string]bool{}
				for _, doc := range docs {
					name, _ := manifest.DocMetadata(doc)
					found[name] = true
				}
				for _, name := range []string{"disabled", "game-cm", "backup-cm"} {
					if found[name] != enabled {
						t.Fatalf("%s present=%t want=%t; output=%s", name, found[name], enabled, first)
					}
				}
			}
			disabledAppFixture(t, root, tc.layout, false)
			testutil.WriteFile(t, root, "targets/game/kustomization.yaml", "resources: [cm.yaml]\nconfigMapGenerator:\n- name: broken\n  files: [missing.txt]\n")
			args := []string{"build", "all", "--path", filepath.Join(root, tc.entry), "--concurrency", "2", "--no-progress"}
			if _, stderr, code := harness.RunCLIBuffers(args...); code != 0 {
				t.Fatalf("disabled broken target failed: exit=%d stderr=%s", code, stderr)
			}
			games := "apps/games"
			if tc.layout == "multi" {
				games = "apps/main/games"
			}
			testutil.WriteFile(t, root, games+"/kustomization.yaml", "namespace: games\nresources: [disabled.yaml]\n")
			if _, stderr, code := harness.RunCLIBuffers(args...); code != 1 || !strings.Contains(stderr, "missing.txt") {
				t.Fatalf("enabled broken target lost failure: exit=%d stderr=%s", code, stderr)
			}
		})
	}
}

func TestE2E_DisabledAppDiffSides(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "remove"
		if enabled {
			name = "restore"
		}
		t.Run(name, func(t *testing.T) {
			current, original := t.TempDir(), t.TempDir()
			for _, root := range []string{current, original} {
				testutil.GitInit(t, root)
			}
			disabledAppFixture(t, current, "multi", enabled)
			disabledAppFixture(t, original, "multi", !enabled)
			for _, command := range []string{"build", "diff"} {
				args := []string{command, "all", "--path", current, "--path-orig", original, "--concurrency", "2", "--no-progress"}
				if command == "diff" {
					args = append(args, "-o", "diff")
				}
				out, stderr, code := harness.RunCLIBuffers(args...)
				if code != 0 || strings.Contains(stderr, "[OrphanedResource]") {
					t.Fatalf("%s exit=%d stderr=%s", command, code, stderr)
				}
				for _, resource := range []string{"game-cm", "backup-cm"} {
					want := enabled || command == "diff"
					if strings.Contains(out, resource) != want {
						t.Fatalf("%s %s enabled=%t output=%s", command, resource, enabled, out)
					}
				}
			}
		})
	}
}

func disabledAppFixture(t *testing.T, root, layout string, enabled bool) {
	t.Helper()
	entry, games := "flux", "apps/games"
	if layout == "multi" {
		entry, games = "flux/main", "apps/main/games"
	}
	ref := "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
	testutil.WriteFile(t, root, entry+"/parent.yaml", testutil.KustomizationYAML("parent", "flux-system", "  path: ./"+games+"\n"+ref))
	resources := "namespace: games\nresources: []\n"
	if enabled {
		resources = "namespace: games\nresources: [disabled.yaml]\n"
	}
	testutil.WriteFile(t, root, games+"/kustomization.yaml", resources)
	testutil.WriteFile(t, root, games+"/disabled.yaml", testutil.KustomizationYAML("disabled", "games", "  path: ./targets/game\n  components: [../backup]\n"+ref))
	testutil.WriteFile(t, root, "targets/game/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "targets/game/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: game-cm}\n")
	testutil.WriteFile(t, root, "targets/backup/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "targets/backup/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: backup-cm}\n")
	if layout == "multi" {
		testutil.WriteFile(t, root, "flux/test/parent.yaml", testutil.KustomizationYAML("test-parent", "flux-system", "  path: ./targets/test\n"+ref))
		testutil.WriteFile(t, root, "targets/test/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, root, "targets/test/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: test-cm}\n")
	}
}

func TestE2E_SelectedEntrySelfOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, entry      string
		builder, foreign bool
		want             bool
	}{
		{"selected package", "flux", true, false, true},
		{"plain entry with ancestor owner", "flux", false, false, true},
		{"disabled foreign owner", ".", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			pkg, target := "flux", "./"
			if !tc.builder {
				pkg = "flux/system"
			}
			if tc.foreign {
				pkg, target = "target", "./target"
				testutil.WriteFile(t, root, "controls/kustomization.yaml", "resources: []\n")
				testutil.WriteFile(t, root, "controls/disabled.yaml", testutil.KustomizationYAML("disabled", "flux-system", "  path: ./target\n"))
				testutil.WriteFile(t, root, pkg+"/kustomization.yaml", "resources: [sync.yaml, cm.yaml]\n")
				testutil.WriteFile(t, root, pkg+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: anchored}\n")
			} else {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: ["+pkg+", app]\n")
				testutil.WriteFile(t, root, pkg+"/kustomization.yaml", "resources: [sync.yaml]\n")
				testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: anchored}\n")
			}
			testutil.WriteFile(t, root, pkg+"/sync.yaml", testutil.KustomizationYAML("sync", "flux-system", "  path: "+target+"\n"))
			out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, tc.entry), "--concurrency", "2", "--no-progress")
			if code != 0 || strings.Contains(out, "name: anchored") != tc.want {
				t.Fatalf("exit=%d want output=%t, stdout=%s stderr=%s", code, tc.want, out, stderr)
			}
		})
	}
}
