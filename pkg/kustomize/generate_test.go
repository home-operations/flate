package kustomize

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"github.com/go-git/go-git/v5/plumbing"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func localSource(tb testing.TB, root string) *store.SourceArtifact {
	tb.Helper()
	location, err := gitrepo.Locate(tb.Context(), root, root)
	assert.NoError(tb, err)
	admitted, err := gittree.AdmitDirectory(tb.Context(), location)
	assert.NoError(tb, err)
	return &store.SourceArtifact{LocalPath: root, FS: admitted.FS}
}

func testTreeFS(t *testing.T, root string) filesys.FileSystem {
	t.Helper()
	input, _, err := source.OperationView(t.Context(), localSource(t, root).FS)
	if err != nil {
		t.Fatal(err)
	}
	return newTreeFS(input, root, nil)
}

func testIgnoreTreeFS(t *testing.T, root string) filesys.FileSystem {
	t.Helper()
	input, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: localSource(t, root).FS})
	if err != nil {
		t.Fatal(err)
	}
	return newTreeFS(input, root, nil)
}

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

func parsedKS(tb testing.TB, doc map[string]any) *manifest.Kustomization {
	tb.Helper()
	obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	return obj.(*manifest.Kustomization)
}

func renderDoc(tb testing.TB, ks *manifest.Kustomization) map[string]any {
	tb.Helper()
	doc, err := ks.RenderDocument()
	if err != nil {
		tb.Fatal(err)
	}
	return doc
}

func typedFluxDoc(tb testing.TB, doc map[string]any) map[string]any {
	tb.Helper()
	raw, err := json.Marshal(doc)
	assert.NoError(tb, err)
	var cr kustomizev1.Kustomization
	assert.NoError(tb, json.Unmarshal(raw, &cr))
	typed, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cr)
	assert.NoError(tb, err)
	return typed
}

func TestGenerateManifest_ByteEquivalentToFlux(t *testing.T) {
	t.Parallel()
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
					"patch":  []any{map[string]any{"op": "replace", "path": "/spec/replicas", "value": int64(9007199254740993)}},
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
			name: "image tag override preserves registry",
			files: map[string]string{
				"kustomization.yaml": existingKustomization("deploy.yaml") + "images:\n- name: nginx\n  newName: mirror/nginx\n  newTag: \"1.0\"\n",
				"deploy.yaml":        cmYAML("deploy"),
			},
			subPath: ".",
			spec:    map[string]any{"images": []any{map[string]any{"name": "nginx", "newTag": "2.0"}}},
		},
		{
			name: "image registry override preserves version",
			files: map[string]string{
				"kustomization.yaml": existingKustomization("deploy.yaml") + "images:\n- name: nginx\n  newName: mirror/nginx\n  digest: sha256:old\n",
				"deploy.yaml":        cmYAML("deploy"),
			},
			subPath: ".",
			spec:    map[string]any{"images": []any{map[string]any{"name": "nginx", "newName": "other/nginx"}}},
		},
		{
			name: "image digest override clears tag",
			files: map[string]string{
				"kustomization.yaml": existingKustomization("deploy.yaml") + "images:\n- name: nginx\n  newTag: \"1.0\"\n",
				"deploy.yaml":        cmYAML("deploy"),
			},
			subPath: ".",
			spec:    map[string]any{"images": []any{map[string]any{"name": "nginx", "digest": "sha256:new"}}},
		},
		{
			name: "image tag override clears digest",
			files: map[string]string{
				"kustomization.yaml": existingKustomization("deploy.yaml") + "images:\n- name: nginx\n  digest: sha256:old\n",
				"deploy.yaml":        cmYAML("deploy"),
			},
			subPath: ".",
			spec:    map[string]any{"images": []any{map[string]any{"name": "nginx", "newTag": "2.0"}}},
		},
		{
			name: "repeated spec image merges fields",
			files: map[string]string{
				"kustomization.yaml": existingKustomization("deploy.yaml") + "images:\n- name: nginx\n  newTag: \"1.0\"\n",
				"deploy.yaml":        cmYAML("deploy"),
			},
			subPath: ".",
			spec:    map[string]any{"images": []any{map[string]any{"name": "nginx", "newName": "mirror/nginx"}, map[string]any{"name": "nginx", "newTag": "2.0"}}},
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

	for _, field := range []string{"targetNamespace", "namePrefix", "nameSuffix"} {
		for _, shape := range []string{"absent", "empty", "null"} {
			spec := map[string]any{}
			switch shape {
			case "empty":
				spec[field] = ""
			case "null":
				spec[field] = nil
			}
			cases = append(cases, struct {
				name    string
				files   map[string]string
				subPath string
				spec    map[string]any
			}{field + "/" + shape, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml") + "namespace: original\nnamePrefix: before-\nnameSuffix: -after\n",
				"cm.yaml":            cmYAML("config"),
			}, ".", spec})
		}
	}
	for _, field := range []string{"patches", "images", "components", "buildMetadata", "patchesStrategicMerge", "patchesJson6902"} {
		for _, shape := range []string{"null", "null elements", "mixed null elements", "invalid field"} {
			if shape == "invalid field" && field != "patchesStrategicMerge" && field != "patchesJson6902" {
				continue
			}
			var value any
			if shape == "invalid field" {
				value = "not-a-list"
			}
			if shape == "null elements" {
				value = []any{nil, nil}
			}
			if shape == "mixed null elements" {
				var valid any = map[string]any{}
				if field == "components" || field == "buildMetadata" {
					valid = "keep"
				}
				value = []any{valid, nil, valid, nil}
			}
			cases = append(cases, struct {
				name    string
				files   map[string]string
				subPath string
				spec    map[string]any
			}{field + "/" + shape, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("config"),
			}, ".", map[string]any{field: value}})
		}
	}

	for _, namespace := range []string{"", "apps"} {
		cases = append(cases, struct {
			name    string
			files   map[string]string
			subPath string
			spec    map[string]any
		}{
			name: "namespace identity/" + namespace,
			files: map[string]string{
				"kustomization.yaml": existingKustomization("deployment.yaml", "role.yaml", "gateway.yaml"),
				"deployment.yaml":    "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: deployment}\n",
				"role.yaml":          "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: role}\n",
				"gateway.yaml":       "apiVersion: gateway.networking.k8s.io/v1\nkind: GatewayClass\nmetadata: {name: gateway}\n",
			},
			subPath: ".", spec: map[string]any{"targetNamespace": namespace},
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, tc.files)
			doc := ksDoc(tc.spec)

			gen := fluxkustomize.NewGenerator(root, unstructured.Unstructured{Object: typedFluxDoc(t, doc)})
			want, _, _, wantErr := gen.GenerateManifest(filepath.Join(root, tc.subPath))

			borrowed := renderDoc(t, parsedKS(t, manifest.DeepCopyMap(doc)))
			snapshot := manifest.DeepCopyMap(borrowed)
			got, _, err := generateManifest(testTreeFS(t, root), filepath.Join(root, tc.subPath), borrowed)
			assert.Diff(t, borrowed, snapshot)
			if wantErr != nil || err != nil {
				if fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatalf("generator error = %v, want %v", err, wantErr)
				}
				return
			}

			if string(got) != string(want) {
				t.Fatalf("kustomization.yaml mismatch\n--- flux (want) ---\n%s\n--- in-memory (got) ---\n%s", want, got)
			}
		})
	}
}

func TestGenerateManifest_TypedOverrides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		authored, effective map[string]any
	}{
		{name: "scalar nulls", authored: map[string]any{"targetNamespace": nil, "namePrefix": nil, "nameSuffix": nil}, effective: map[string]any{"targetNamespace": "apps", "namePrefix": "pre-", "nameSuffix": "-post"}},
		{name: "scalar empties", authored: map[string]any{"targetNamespace": "", "namePrefix": "", "nameSuffix": ""}, effective: map[string]any{"targetNamespace": "apps", "namePrefix": "pre-", "nameSuffix": "-post"}},
		{name: "patches null", authored: map[string]any{"patches": nil}, effective: map[string]any{"patches": []any{map[string]any{"patch": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: config}\ndata: {value: typed}\n"}}}},
		{name: "patch element null", authored: map[string]any{"patches": []any{nil}}, effective: map[string]any{"patches": []any{map[string]any{"patch": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: config}\ndata: {value: typed}\n"}}}},
		{name: "images null", authored: map[string]any{"images": nil}, effective: map[string]any{"images": []any{map[string]any{"name": "nginx", "newTag": "typed"}}}},
		{name: "image element null", authored: map[string]any{"images": []any{nil}}, effective: map[string]any{"images": []any{map[string]any{"name": "nginx", "newTag": "typed"}}}},
		{name: "components null", authored: map[string]any{"components": nil}, effective: map[string]any{"components": []any{"./component"}}},
		{name: "component element null", authored: map[string]any{"components": []any{nil}}, effective: map[string]any{"components": []any{"./component"}}},
		{name: "buildMetadata null", authored: map[string]any{"buildMetadata": nil}, effective: map[string]any{"buildMetadata": []any{"originAnnotations"}}},
		{name: "buildMetadata element null", authored: map[string]any{"buildMetadata": []any{nil}}, effective: map[string]any{"buildMetadata": []any{"originAnnotations"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("config")})
			ks := parsedKS(t, ksDoc(tc.authored)).Clone()
			plain := parsedKS(t, ksDoc(tc.effective))
			ks.KustomizationSpec = plain.KustomizationSpec
			got, _, err := generateManifest(testTreeFS(t, root), root, renderDoc(t, ks))
			assert.NoError(t, err)
			oracle := fluxkustomize.NewGenerator(root, unstructured.Unstructured{Object: typedFluxDoc(t, ksDoc(tc.effective))})
			want, _, _, err := oracle.GenerateManifest(root)
			assert.NoError(t, err)
			assert.Equal(t, string(got), string(want))
			assert.Equal(t, manifest.Fingerprint(ks.Content(nil)), manifest.Fingerprint(plain.Content(nil)))
		})
	}
}

func TestRender_NamespaceIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace string
		authored        any
	}{
		{name: "empty", authored: ""},
		{name: "target", namespace: "apps", authored: "apps"},
		{name: "typed over empty", namespace: "apps", authored: ""},
		{name: "typed over null", namespace: "apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("deployment.yaml", "role.yaml", "gateway.yaml"),
				"deployment.yaml":    "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: deployment}\n",
				"role.yaml":          "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: role}\n",
				"gateway.yaml":       "apiVersion: gateway.networking.k8s.io/v1\nkind: GatewayClass\nmetadata: {name: gateway}\n",
			})
			ks := parsedKS(t, ksDoc(map[string]any{"targetNamespace": tc.authored})).Clone()
			ks.TargetNamespace = tc.namespace
			out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{
				Source: localSource(t, root), SubPath: ".", Spec: ks,
			}, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			docs := out
			assert.Equal(t, len(docs), 3)
			namespaces := make(map[string]string, len(docs))
			for _, doc := range docs {
				_, ns := manifest.DocMetadata(doc)
				namespaces[manifest.DocKind(doc)] = ns
			}
			assert.Diff(t, namespaces, map[string]string{
				"Deployment": tc.namespace, "ClusterRole": "", "GatewayClass": tc.namespace,
			})
		})
	}
}

// TestGenerateManifest_SourceIgnoreFiltersScan is the bo0tzz fix: a working tree
// carrying a root .sops.yaml renders cleanly through a `path: ./` Kustomization
// because the ignore filesystem keeps the SOPS config out of the
// auto-generated kustomization's resource list — the same artifact a real
// GitRepository fetch would produce. Without the filter the SOPS config is
// scanned as a manifest and the build fails.
func TestGenerateManifest_SourceIgnoreFiltersScan(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		".sops.yaml": "creation_rules:\n  - path_regex: .*\n    age: age1example\n",
		"cm.yaml":    cmYAML("config"),
	})

	// Behind the ignore filesystem, scanManifests never sees .sops.yaml so a
	// path:./ build over the working tree renders the ConfigMap.
	fsys := testIgnoreTreeFS(t, root)
	data, kfile, err := generateManifest(fsys, root, renderDoc(t, parsedKS(t, ksDoc(nil))))
	if err != nil {
		t.Fatalf("generateManifest(ignore): %v", err)
	}
	if err := fsys.WriteFile(kfile, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	kustomizestate.BuildMutex.Lock()
	rm, err := fluxkustomize.Build(fsys, root)
	kustomizestate.BuildMutex.Unlock()
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

	// Without the filter (a fetched artifact, already filtered, would not hit
	// this), scanManifests includes .sops.yaml and fails to decode it — the bug
	// the filter fixes.
	if _, _, err := generateManifest(testTreeFS(t, root), root, renderDoc(t, parsedKS(t, ksDoc(nil)))); err == nil {
		t.Fatal("expected generate/scan to fail on the unfiltered .sops.yaml, got nil")
	}
}

// SkipDir must prune a subpackage from both layers after preflight writes into
// it. The parent lists the subpackage once, never its kustomization.yaml.
func TestFindOrGenerate_MemoryPopulatedSubdirNotDescended(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		// Parent dir intentionally has NO kustomization.yaml -> auto-generate.
		"cnpg/kustomization.yaml":  existingKustomization("./app/cm.yaml"),
		"cnpg/app/cm.yaml":         cmYAML("cnpg-cm"),
		"other/kustomization.yaml": existingKustomization("./cm.yaml"),
		"other/cm.yaml":            cmYAML("other-cm"),
	})
	fsys := testTreeFS(t, root)
	// Simulate preflightRemoteResources rewriting a remote resource under cnpg/
	// into memory, so cnpg/ exists in BOTH the memory and disk layers.
	seed := filepath.Join(root, "cnpg", "app", remoteResourcePrefix+"deadbeef.yaml")
	if err := fsys.WriteFile(seed, []byte(cmYAML("remote"))); err != nil {
		t.Fatalf("seed memory: %v", err)
	}

	data, _, foundExisting, err := findOrGenerateKustomization(fsys, root)
	if err != nil {
		t.Fatalf("findOrGenerateKustomization: %v", err)
	}
	if foundExisting {
		t.Fatal("expected auto-generate (root has no kustomization.yaml)")
	}
	got := string(data)
	if strings.Contains(got, "kustomization.yaml") {
		t.Errorf("auto-generated kustomization lists a kustomization.yaml file as a resource:\n%s", got)
	}
	for _, want := range []string{"./cnpg", "./other"} {
		if !strings.Contains(got, want) {
			t.Errorf("auto-generated kustomization missing %q resource:\n%s", want, got)
		}
	}
}
