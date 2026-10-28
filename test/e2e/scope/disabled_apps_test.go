package scope

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_DisabledAppAdmission(t *testing.T) {
	for _, declaration := range []string{"enabled", "absent", "commented"} {
		t.Run("nested cross source/"+declaration, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			enabled := declaration == "enabled"
			disabledAppFixture(t, root, "nested", enabled)
			if declaration == "commented" {
				testutil.WriteFile(t, root, "kube/deploy/app/kustomization.yaml", "resources: []\n# - ks.yaml\n")
			}
			for _, entry := range []string{".", "kube/clusters/main/flux"} {
				args := []string{"build", "all", "--path", filepath.Join(root, entry), "--allow-missing-secrets", "--log-level", "info", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
				out, stderr, code := harness.RunCLIBuffers(args...)
				if code != 0 || strings.Contains(stderr, "dial tcp") {
					t.Fatalf("nested build exit=%d output=%s stderr=%s", code, out, stderr)
				}
				docs, err := testutil.SplitDocs([]byte(out))
				if err != nil {
					t.Fatal(err)
				}
				found := map[string]bool{}
				for _, doc := range docs {
					name, _ := manifest.DocMetadata(doc)
					found[name] = true
				}
				if found["reachable-cm"] != enabled || found["excluded-cm"] || found["commented-cm"] || found["excluded-app"] || found["commented-app"] {
					t.Fatalf("nested admission enabled=%t output=%s stderr=%s", enabled, out, stderr)
				}
				again, diagnostic, code := harness.RunCLIBuffers(args...)
				if code != 0 || again != out {
					t.Fatalf("nested render changed bytes: exit=%d stderr=%s", code, diagnostic)
				}
				if enabled {
					args[0] = "test"
					status, diagnostic, code := harness.RunCLIBuffers(args...)
					if code != 0 || !strings.Contains(status, "flux-system/external") || !strings.Contains(status, "flux-system/external-sync") || !strings.Contains(status, "skipped") || !strings.Contains(diagnostic, "source: skipped (missing secret)") || !strings.Contains(diagnostic, "unavailable-auth") {
						t.Fatalf("whole-source skip missing: exit=%d output=%s stderr=%s", code, status, diagnostic)
					}
				}
			}
			if enabled {
				testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [cm.yaml]\nconfigMapGenerator: [{name: broken, files: [missing.txt]}]\n")
				out, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--allow-missing-secrets", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
				if code != 1 || !strings.Contains(stderr, "missing.txt") || strings.Contains(out, "reachable-cm") {
					t.Fatalf("reachable input failure hidden: exit=%d output=%s stderr=%s", code, out, stderr)
				}
			}
		})
	}
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
		docs, err := testutil.SplitDocs([]byte(out))
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
				docs, err := testutil.SplitDocs([]byte(first))
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
	t.Run("membership namespace authority", func(t *testing.T) {
		before, payload, shifted := t.TempDir(), t.TempDir(), t.TempDir()
		const entry = "kube/clusters/main/flux"
		for _, tc := range []struct{ root, namespace, value string }{
			{before, "media", "original"},
			{payload, "media", "changed"},
			{shifted, "shifted", "original"},
		} {
			testutil.GitInit(t, tc.root)
			disabledAppFixture(t, tc.root, "nested", true)
			testutil.WriteFile(t, tc.root, entry+"/kustomization.yaml", "resources: [sync.yaml, external.yaml]\n")
			testutil.WriteFile(t, tc.root, entry+"/sync.yaml", testutil.KustomizationYAML("sync", "flux-system", "  path: ./kube/deploy/app\n  targetNamespace: "+tc.namespace+"\n"))
			testutil.WriteFile(t, tc.root, entry+"/external.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec:\n  url: https://fixture.invalid/external\n  secretRef: {name: unavailable-auth}\n---\n"+testutil.KustomizationYAML("external-sync", "flux-system", "  path: ./kube/deploy/app\n  sourceRef: {kind: GitRepository, name: external}\n"))
			testutil.WriteFile(t, tc.root, "kube/deploy/app/kustomization.yaml", "namespace: inner\nresources: [ks.yaml]\n# - commented.yaml\n")
			testutil.WriteFile(t, tc.root, "payload/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: reachable-cm, namespace: apps}\ndata: {value: "+tc.value+"}\n")
		}
		checkDiagnostic := func(t *testing.T, stderr string, skipped bool) {
			t.Helper()
			credentials := 0
			if strings.Contains(stderr, "source: skipped (missing secret)") != skipped || skipped && !strings.Contains(stderr, "unavailable-auth") || strings.Contains(stderr, "dial tcp") || strings.Contains(stderr, "[OrphanedResource]") || strings.Contains(stderr, "[IncompleteInputCoverage]") {
				t.Fatalf("unexpected mixed-namespace diagnostics: %s", stderr)
			}
			for line := range strings.SplitSeq(stderr, "\n") {
				if warning := strings.TrimSpace(line); strings.HasPrefix(warning, "[") {
					category, _, _ := strings.Cut(warning[1:], "]")
					if category == manifest.WarnSourceCredentialsUnavailable && skipped && strings.Contains(warning, "flux-system/external") && strings.Contains(warning, "unavailable-auth") {
						credentials++
						continue
					}
					if category == manifest.WarnExternalSource && !skipped && strings.Contains(warning, "flux-system/external") {
						continue
					}
					assert.Equal(t, category, manifest.WarnRenderPersistenceDisabled)
				}
			}
			wantCredentials := 0
			if skipped {
				wantCredentials = 1
			}
			assert.Equal(t, credentials, wantCredentials)
		}
		checkBuild := func(t *testing.T, out, namespace, value string) {
			t.Helper()
			docs, err := testutil.SplitDocs([]byte(out))
			assert.NoError(t, err)
			var identities []manifest.NamedResource
			for _, doc := range docs {
				name, ns := manifest.DocMetadata(doc)
				if ns == "inner" || name == "excluded-app" || name == "commented-app" || name == "excluded-cm" || name == "commented-cm" {
					t.Fatalf("excluded identity rendered: %s/%s", ns, name)
				}
				if name != "app" && name != "reachable-cm" {
					continue
				}
				group, _, _ := strings.Cut(doc["apiVersion"].(string), "/")
				if group == "v1" {
					group = ""
				}
				identities = append(identities, manifest.NamedResource{Group: group, Kind: doc["kind"].(string), Namespace: ns, Name: name})
				if name == "reachable-cm" {
					assert.Equal(t, doc["data"].(map[string]any)["value"], any(value))
				}
			}
			slices.SortFunc(identities, manifest.NamedResource.Compare)
			assert.Diff(t, identities, []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "reachable-cm"}, testutil.KustomizationID(namespace, "app")})
		}
		flags := []string{"--allow-missing-secrets", "--log-level", "info", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
		fullArgs := append([]string{"build", "all", "--path", before}, flags...)
		full, stderr := harness.RequireCLIOK(t, fullArgs...)
		checkDiagnostic(t, stderr, true)
		checkBuild(t, full, "media", "original")
		again, stderr := harness.RequireCLIOK(t, fullArgs...)
		checkDiagnostic(t, stderr, true)
		assert.Equal(t, again, full)
		for _, tc := range []struct{ name, current, original, namespace, oldNamespace, value string }{
			{"payload", payload, before, "media", "media", "changed"},
			{"namespace forward", shifted, before, "shifted", "media", "original"},
			{"namespace reverse", before, shifted, "media", "shifted", "original"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				args := append([]string{"build", "all", "--path", tc.current, "--path-orig", tc.original}, flags...)
				out, stderr := harness.RequireCLIOK(t, args...)
				checkDiagnostic(t, stderr, false)
				checkBuild(t, out, tc.namespace, tc.value)
				args[0] = "diff"
				args = append(args, "-o", "json")
				out, stderr = harness.RequireCLIOK(t, args...)
				checkDiagnostic(t, stderr, false)
				var result struct {
					Changes []struct {
						Kind, Name, Namespace, Status string
						Parent                        struct{ Kind, Name, Namespace string }
					}
					Suppressed []any
				}
				assert.NoError(t, json.Unmarshal([]byte(out), &result))
				assert.Equal(t, len(result.Suppressed), 0)
				var changes []string
				for _, change := range result.Changes {
					if change.Namespace == "inner" || change.Parent.Namespace == "inner" || strings.Contains(change.Name, "excluded") || strings.Contains(change.Name, "commented") {
						t.Fatalf("excluded identity in diff: %s", out)
					}
					if change.Kind != "ConfigMap" || change.Name != "reachable-cm" {
						continue
					}
					assert.Equal(t, change.Parent.Kind, "Kustomization")
					assert.Equal(t, change.Parent.Name, "app")
					assert.Equal(t, change.Namespace, "apps")
					changes = append(changes, change.Parent.Namespace+"/"+change.Status)
				}
				want := []string{tc.namespace + "/changed"}
				if tc.namespace != tc.oldNamespace {
					want = []string{tc.namespace + "/added", tc.oldNamespace + "/removed"}
				}
				slices.Sort(changes)
				slices.Sort(want)
				assert.Diff(t, changes, want)
			})
		}
	})
	for _, scenario := range []string{"remove", "restore", "payload"} {
		t.Run("nested cross source/"+scenario, func(t *testing.T) {
			current, original := t.TempDir(), t.TempDir()
			for _, root := range []string{current, original} {
				testutil.GitInit(t, root)
			}
			currentEnabled, originalEnabled := scenario != "remove", scenario != "restore"
			disabledAppFixture(t, current, "nested", currentEnabled)
			disabledAppFixture(t, original, "nested", originalEnabled)
			if scenario == "payload" {
				testutil.WriteFile(t, current, "payload/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: reachable-cm, namespace: apps}\ndata: {value: changed}\n")
			}
			for _, command := range []string{"build", "diff"} {
				args := []string{command, "all", "--path", current, "--path-orig", original, "--allow-missing-secrets", "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
				if command == "diff" {
					args = append(args, "-o", "json")
				}
				out, stderr, code := harness.RunCLIBuffers(args...)
				if code != 0 || strings.Contains(stderr, "dial tcp") {
					t.Fatalf("nested %s exit=%d output=%s stderr=%s", command, code, out, stderr)
				}
				for _, excluded := range []string{"excluded-cm", "commented-cm", "excluded-app", "commented-app"} {
					if strings.Contains(out, excluded) {
						t.Fatalf("excluded %s appeared in %s: %s", excluded, command, out)
					}
				}
				if command == "build" {
					if strings.Contains(out, "name: reachable-cm") != currentEnabled {
						t.Fatalf("changed-only current tree selection: %s", out)
					}
					if scenario == "payload" && !strings.Contains(out, "value: changed") {
						t.Fatalf("changed payload not selected: %s", out)
					}
					continue
				}
				var result struct {
					Changes []struct {
						Kind, Name, Status string
						Parent             struct{ Kind, Name, Namespace string }
					}
					Suppressed []any
				}
				if err := json.Unmarshal([]byte(out), &result); err != nil {
					t.Fatal(err)
				}
				if len(result.Suppressed) != 0 {
					t.Fatalf("nested diff suppressed input: %s", out)
				}
				found := 0
				want := map[string]string{"remove": "removed", "restore": "added", "payload": "changed"}[scenario]
				for _, change := range result.Changes {
					if change.Kind != "ConfigMap" || change.Name != "reachable-cm" {
						continue
					}
					if change.Status != want || change.Parent.Kind != "Kustomization" || change.Parent.Name != "app" || change.Parent.Namespace != "flux-system" {
						t.Fatalf("nested diff lost identity/parent: %s", out)
					}
					found++
				}
				if found != 1 {
					t.Fatalf("expected one %s reachable payload: %s", want, out)
				}
			}
		})
	}
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
	if layout == "nested" {
		const entry = "kube/clusters/main/flux"
		ref := "  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
		testutil.WriteFile(t, root, entry+"/kustomization.yaml", "resources: [sync.yaml, external.yaml, ../../../deploy/app]\n")
		testutil.WriteFile(t, root, entry+"/sync.yaml", testutil.KustomizationYAML("sync", "flux-system", "  path: ./"+entry+"\n"))
		testutil.WriteFile(t, root, entry+"/external.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: external, namespace: flux-system}\nspec:\n  url: https://fixture.invalid/external\n  secretRef: {name: unavailable-auth}\n---\n"+testutil.KustomizationYAML("external-sync", "flux-system", "  path: ./"+entry+"\n  sourceRef: {kind: GitRepository, name: external}\n"))
		resources := "resources: []\n"
		if enabled {
			resources = "resources: [ks.yaml]\n# - commented.yaml\n"
		}
		testutil.WriteFile(t, root, "kube/deploy/app/kustomization.yaml", resources)
		for _, tc := range []struct{ file, name, target, payload string }{
			{"ks.yaml", "app", "payload", "reachable-cm"}, {"omitted.yaml", "excluded-app", "excluded", "excluded-cm"}, {"commented.yaml", "commented-app", "commented", "commented-cm"},
		} {
			testutil.WriteFile(t, root, "kube/deploy/app/"+tc.file, testutil.KustomizationYAML(tc.name, "flux-system", "  path: ./"+tc.target+"\n"+ref))
			testutil.WriteFile(t, root, tc.target+"/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, tc.target+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+tc.payload+", namespace: apps}\ndata: {value: original}\n")
		}
		return
	}
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
