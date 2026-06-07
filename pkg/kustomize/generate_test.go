package kustomize

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/source/sourceignore"
)

// testOverlayFS builds the same memory-over-disk overlay RenderFlux uses:
// source files are read from a secure on-disk FS rooted at root; writes go to
// memory.
func testOverlayFS(t *testing.T, root string) filesys.FileSystem {
	t.Helper()
	disk, err := fluxfilesys.MakeFsOnDiskSecure(root)
	if err != nil {
		t.Fatalf("secure fs: %v", err)
	}
	return newOverlayFS(disk)
}

// These tests pin flate's in-memory generateManifest to flux's real on-disk
// Generator.GenerateManifest. The acceptance gate for the whole in-memory
// redesign is byte-equivalence: for an identical source tree and Kustomization
// spec, the kustomization.yaml flate synthesizes in RAM must be byte-for-byte
// what flux writes to disk. If flux changes its merge, these tests fail and we
// re-port — they are the contract.

// cmYAML is a minimal manifest that kustomize's resource factory accepts, used
// wherever scanManifests must see a valid Kubernetes YAML.
func cmYAML(name string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\ndata:\n  k: v\n"
}

// existingKustomization is a user-authored kustomization.yaml referencing the
// given resource files.
func existingKustomization(resources ...string) string {
	lines := []string{"apiVersion: kustomize.config.k8s.io/v1beta1", "kind: Kustomization"}
	if len(resources) > 0 {
		lines = append(lines, "resources:")
		for _, r := range resources {
			lines = append(lines, "- "+r)
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

// writeTree materializes files (relative path -> contents) under a fresh temp
// dir, returning the symlink-resolved root (macOS /var -> /private/var) so the
// path flux's secure FS confirms matches what the test passes in.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("evalsymlinks tempdir: %v", err)
	}
	for rel, body := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	return root
}

// ksDoc wraps an inner spec in a full Flux Kustomization document.
func ksDoc(spec map[string]any) map[string]any {
	if spec == nil {
		spec = map[string]any{}
	}
	return map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "app", "namespace": "flux-system"},
		"spec":       spec,
	}
}

func deepCopyDoc(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&unstructured.Unstructured{Object: doc})
	if err != nil {
		t.Fatalf("deep copy doc: %v", err)
	}
	return runtime.DeepCopyJSON(out)
}

func TestGenerateManifest_ByteEquivalentToFlux(t *testing.T) {
	patch := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: app\nspec:\n  replicas: 3\n"

	cases := []struct {
		name    string
		files   map[string]string
		subPath string
		spec    map[string]any
	}{
		{
			name:    "existing kustomization with resources",
			files:   map[string]string{"kustomization.yaml": existingKustomization("deploy.yaml"), "deploy.yaml": cmYAML("deploy")},
			subPath: ".",
		},
		{
			name:    "existing kustomization no resources gets originAnnotations",
			files:   map[string]string{"kustomization.yaml": existingKustomization()},
			subPath: ".",
			spec:    map[string]any{"patches": []any{map[string]any{"patch": patch}}},
		},
		{
			name:    "generated from loose manifests",
			files:   map[string]string{"deploy.yaml": cmYAML("deploy"), "service.yaml": cmYAML("service")},
			subPath: ".",
		},
		{
			name:    "generated empty dir gets placeholder",
			files:   map[string]string{"README.md": "# not yaml\n"},
			subPath: ".",
		},
		{
			name: "generated with nested kustomization dir as resource",
			files: map[string]string{
				"base.yaml":               cmYAML("base"),
				"apps/kustomization.yaml": existingKustomization("inner.yaml"),
				"apps/inner.yaml":         cmYAML("inner"),
			},
			subPath: ".",
		},
		{
			name:    "targetNamespace + namePrefix + nameSuffix",
			files:   map[string]string{"kustomization.yaml": existingKustomization("deploy.yaml"), "deploy.yaml": cmYAML("deploy")},
			subPath: ".",
			spec:    map[string]any{"targetNamespace": "team-a", "namePrefix": "pre-", "nameSuffix": "-suf"},
		},
		{
			name:    "patches with target selector",
			files:   map[string]string{"kustomization.yaml": existingKustomization("deploy.yaml"), "deploy.yaml": cmYAML("deploy")},
			subPath: ".",
			spec: map[string]any{"patches": []any{
				map[string]any{"patch": patch, "target": map[string]any{"kind": "Deployment", "labelSelector": "app=x"}},
			}},
		},
		{
			name:    "patchesStrategicMerge",
			files:   map[string]string{"kustomization.yaml": existingKustomization("deploy.yaml"), "deploy.yaml": cmYAML("deploy")},
			subPath: ".",
			spec: map[string]any{"patchesStrategicMerge": []any{
				map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "app"}, "spec": map[string]any{"replicas": int64(2)}},
			}},
		},
		{
			name:    "patchesJson6902",
			files:   map[string]string{"kustomization.yaml": existingKustomization("deploy.yaml"), "deploy.yaml": cmYAML("deploy")},
			subPath: ".",
			spec: map[string]any{"patchesJson6902": []any{
				map[string]any{
					"patch":  []any{map[string]any{"op": "replace", "path": "/spec/replicas", "value": int64(5)}},
					"target": map[string]any{"group": "apps", "version": "v1", "kind": "Deployment", "name": "app"},
				},
			}},
		},
		{
			name: "images with in-place dedup and append",
			files: map[string]string{
				"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n- deploy.yaml\nimages:\n- name: nginx\n  newTag: \"1.0\"\n",
				"deploy.yaml":        cmYAML("deploy"),
			},
			subPath: ".",
			spec: map[string]any{"images": []any{
				map[string]any{"name": "nginx", "newTag": "2.0"},
				map[string]any{"name": "redis", "newName": "ghcr.io/redis"},
			}},
		},
		{
			name: "components with ignoreMissing",
			files: map[string]string{
				"kustomization.yaml":      existingKustomization("deploy.yaml"),
				"deploy.yaml":             cmYAML("deploy"),
				"comp/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n",
			},
			subPath: ".",
			spec:    map[string]any{"components": []any{"./comp", "./missing"}, "ignoreMissingComponents": true},
		},
		{
			name:    "buildMetadata overrides default",
			files:   map[string]string{"kustomization.yaml": existingKustomization()},
			subPath: ".",
			spec:    map[string]any{"buildMetadata": []any{"managedByLabel", "originAnnotations"}},
		},
		{
			name: "nested subPath existing kustomization",
			files: map[string]string{
				"apps/app1/kustomization.yaml": existingKustomization("deploy.yaml"),
				"apps/app1/deploy.yaml":        cmYAML("deploy"),
			},
			subPath: "apps/app1",
			spec:    map[string]any{"namePrefix": "x-"},
		},
		{
			name: "nested subPath generated",
			files: map[string]string{
				"apps/app1/deploy.yaml": cmYAML("deploy"),
				"apps/app1/svc.yaml":    cmYAML("svc"),
			},
			subPath: "apps/app1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writeTree(t, tc.files)
			doc := ksDoc(tc.spec)

			gen := fluxkustomize.NewGenerator(root, unstructured.Unstructured{Object: deepCopyDoc(t, doc)})
			want, _, _, err := gen.GenerateManifest(filepath.Join(root, tc.subPath))
			if err != nil {
				t.Fatalf("flux GenerateManifest: %v", err)
			}

			got, _, err := generateManifest(testOverlayFS(t, root), filepath.Join(root, tc.subPath), deepCopyDoc(t, doc), nil)
			if err != nil {
				t.Fatalf("generateManifest: %v", err)
			}

			if string(got) != string(want) {
				t.Fatalf("kustomization.yaml mismatch\n--- flux (want) ---\n%s\n--- in-memory (got) ---\n%s", want, got)
			}
		})
	}
}

// renderTree runs flate's full in-memory render of subPath against an in-memory
// fs built from root: generate the merged kustomization.yaml, write it into the
// fs, build. It returns the rendered resources as YAML.
func renderInMemory(t *testing.T, root, subPath string, doc map[string]any) []byte {
	t.Helper()
	fsys := testOverlayFS(t, root)
	absSub := filepath.Join(root, subPath)
	data, kfile, err := generateManifest(fsys, absSub, doc, nil)
	if err != nil {
		t.Fatalf("generateManifest: %v", err)
	}
	if err := fsys.WriteFile(kfile, data); err != nil {
		t.Fatalf("write merged kustomization: %v", err)
	}
	rm, err := fluxkustomize.Build(fsys, absSub)
	if err != nil {
		t.Fatalf("in-memory build: %v", err)
	}
	out, err := rm.AsYaml()
	if err != nil {
		t.Fatalf("AsYaml: %v", err)
	}
	return out
}

// TestBuild_InMemoryMatchesSecureBuild proves the end-to-end render — generate
// then build — is byte-identical between flate's in-memory fs and flux's
// on-disk SecureBuild, the path real Flux uses.
func TestBuild_InMemoryMatchesSecureBuild(t *testing.T) {
	cases := []struct {
		name    string
		files   map[string]string
		subPath string
		spec    map[string]any
	}{
		{
			name:    "existing kustomization with prefix and namespace",
			files:   map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("config")},
			subPath: ".",
			spec:    map[string]any{"namePrefix": "pre-", "targetNamespace": "team-a"},
		},
		{
			name:    "generated from loose manifests",
			files:   map[string]string{"a.yaml": cmYAML("alpha"), "b.yaml": cmYAML("beta")},
			subPath: ".",
		},
		{
			name: "relative parent base",
			files: map[string]string{
				"app/kustomization.yaml":  existingKustomization("../base"),
				"base/kustomization.yaml": existingKustomization("cm.yaml"),
				"base/cm.yaml":            cmYAML("shared"),
			},
			subPath: "app",
			spec:    map[string]any{"namePrefix": "x-"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writeTree(t, tc.files)
			doc := ksDoc(tc.spec)

			// flux on-disk pipeline: write the generated kustomization, SecureBuild.
			absSub := filepath.Join(root, tc.subPath)
			gen := fluxkustomize.NewGenerator(root, unstructured.Unstructured{Object: deepCopyDoc(t, doc)})
			if _, err := gen.WriteFile(absSub); err != nil {
				t.Fatalf("flux WriteFile: %v", err)
			}
			rm, err := fluxkustomize.SecureBuild(root, absSub, false)
			if err != nil {
				t.Fatalf("SecureBuild: %v", err)
			}
			want, err := rm.AsYaml()
			if err != nil {
				t.Fatalf("flux AsYaml: %v", err)
			}

			// flate in-memory pipeline (capture happens inside, before any disk
			// mutation matters — the snapshot is independent bytes).
			got := renderInMemory(t, root, tc.subPath, deepCopyDoc(t, doc))

			if string(got) != string(want) {
				t.Fatalf("rendered output mismatch\n--- SecureBuild (want) ---\n%s\n--- in-memory (got) ---\n%s", want, got)
			}
		})
	}
}

// TestBuild_InMemorySandbox confirms the in-memory fs reproduces SecureBuild's
// security posture by construction: a resource escaping the source root cannot
// be loaded (the node does not exist in the fs), while a legal in-tree parent
// reference still resolves.
func TestBuild_InMemorySandbox(t *testing.T) {
	t.Run("escaping resource fails", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"evil/kustomization.yaml": existingKustomization("../../../../../../../../etc/passwd"),
		})
		if _, err := fluxkustomize.Build(testOverlayFS(t, root), filepath.Join(root, "evil")); err == nil {
			t.Fatal("expected build to fail for an out-of-tree resource, got nil error")
		}
	})

	t.Run("legal in-tree parent renders", func(t *testing.T) {
		root := writeTree(t, map[string]string{
			"app/kustomization.yaml":  existingKustomization("../base"),
			"base/kustomization.yaml": existingKustomization("cm.yaml"),
			"base/cm.yaml":            cmYAML("shared"),
		})
		out := renderInMemory(t, root, "app", ksDoc(nil))
		if !strings.Contains(string(out), "name: shared") {
			t.Fatalf("expected rendered ConfigMap 'shared', got:\n%s", out)
		}
	})
}

// TestGenerateManifest_SourceIgnoreFiltersScan is the bo0tzz fix: a working tree
// carrying a root .sops.yaml renders cleanly through a `path: ./` Kustomization
// because the sourceignore matcher keeps the SOPS config out of the
// auto-generated kustomization's resource list — the same artifact a real
// GitRepository fetch would produce. Without the matcher the SOPS config is
// scanned as a manifest and the build fails.
func TestGenerateManifest_SourceIgnoreFiltersScan(t *testing.T) {
	root := writeTree(t, map[string]string{
		".sops.yaml": "creation_rules:\n  - path_regex: .*\n    age: age1example\n",
		"cm.yaml":    cmYAML("config"),
	})
	matcher, err := sourceignore.New(root, nil, true)
	if err != nil {
		t.Fatalf("matcher: %v", err)
	}

	// With the matcher, scanManifests skips .sops.yaml so a path:./ build over
	// the working tree renders the ConfigMap.
	fsys := testOverlayFS(t, root)
	data, kfile, err := generateManifest(fsys, root, ksDoc(nil), matcher)
	if err != nil {
		t.Fatalf("generateManifest(ignore): %v", err)
	}
	if err := fsys.WriteFile(kfile, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	rm, err := fluxkustomize.Build(fsys, root)
	if err != nil {
		t.Fatalf("build over ignored tree: %v", err)
	}
	out, err := rm.AsYaml()
	if err != nil {
		t.Fatalf("AsYaml: %v", err)
	}
	if !strings.Contains(string(out), "name: config") {
		t.Fatalf("expected rendered ConfigMap, got:\n%s", out)
	}

	// Without the matcher (a fetched artifact, already filtered, would not hit
	// this), scanManifests includes .sops.yaml and fails to decode it — the bug
	// the filter fixes.
	if _, _, err := generateManifest(testOverlayFS(t, root), root, ksDoc(nil), nil); err == nil {
		t.Fatal("expected generate/scan to fail on the unfiltered .sops.yaml, got nil")
	}
}
