package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// BenchmarkLoad_LargeRepo measures Loader.Load against a synthesized
// repo shape representative of the mid-size home-ops target — 200
// Flux Kustomizations, 100 HelmReleases, 50 source CRs, plus the
// kustomization.yaml glue that wires them together.
func BenchmarkLoad_LargeRepo(b *testing.B) {
	dir := b.TempDir()
	synthLargeRepo(b, dir)

	b.ReportAllocs()
	for b.Loop() {
		s := store.New()
		if _, err := New(s).Load(context.Background(), dir); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}

// BenchmarkLoad_DeepComponents measures the kustomize Components walk
// against a 5-level-deep nested Component graph. Each level's
// kustomization.yaml references the next via `components:`.
func BenchmarkLoad_DeepComponents(b *testing.B) {
	dir := b.TempDir()
	synthDeepComponents(b, dir, 5)

	b.ReportAllocs()
	for b.Loop() {
		s := store.New()
		if _, err := New(s).Load(context.Background(), dir); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}

func BenchmarkParseFile_Kustomization(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "ks.yaml")
	body := testutil.KustomizationYAML("apps", "flux-system", `  interval: 10m
  path: ./apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
  postBuild:
    substitute:
      CLUSTER_NAME: home
    substituteFrom:
    - kind: ConfigMap
      name: cluster-settings
  dependsOn:
  - name: cluster-config
    namespace: flux-system
`)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		b.Fatal(err)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""}
	reader := readerForTest(dir)

	b.ReportAllocs()
	for b.Loop() {
		objs, err := reader.parseFile(path, opts)
		if err != nil {
			b.Fatalf("parseFile: %v", err)
		}
		if len(objs) == 0 {
			b.Fatalf("expected at least one object")
		}
	}
}

func BenchmarkParseFile_HelmRelease(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "hr.yaml")
	body := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app-template
  namespace: default
spec:
  interval: 15m
  chart:
    spec:
      chart: app-template
      version: "3.5.1"
      sourceRef:
        kind: HelmRepository
        name: bjw-s
        namespace: flux-system
      valuesFiles:
      - values.yaml
      - values-prod.yaml
  install:
    crds: CreateReplace
    remediation:
      retries: 3
  upgrade:
    cleanupOnFail: true
    remediation:
      strategy: rollback
      retries: 3
  values:
    controllers:
      main:
        replicas: 2
        containers:
          main:
            image:
              repository: nginx
              tag: latest
            env:
              TZ: UTC
            resources:
              requests:
                cpu: 10m
                memory: 64Mi
              limits:
                memory: 256Mi
    service:
      main:
        ports:
          http:
            port: 80
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		b.Fatal(err)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""}
	reader := readerForTest(dir)

	b.ReportAllocs()
	for b.Loop() {
		objs, err := reader.parseFile(path, opts)
		if err != nil {
			b.Fatalf("parseFile: %v", err)
		}
		if len(objs) == 0 {
			b.Fatalf("expected at least one object")
		}
	}
}

func BenchmarkRecordObject_HelmRelease(b *testing.B) {
	benchmarkRecordObject(b, manifest.KindHelmRelease)
}

func BenchmarkRecordObject_Kustomization(b *testing.B) {
	benchmarkRecordObject(b, manifest.KindKustomization)
}

func benchmarkRecordObject(b *testing.B, kind string) {
	b.Helper()
	for _, count := range []int{0, 1, 3, 16} {
		b.Run(fmt.Sprintf("refs=%d", count), func(b *testing.B) {
			var obj manifest.BaseManifest = &manifest.Kustomization{Name: "app", Namespace: "ns"}
			if kind == manifest.KindHelmRelease {
				obj = &manifest.HelmRelease{Name: "app", Namespace: "ns"}
			}
			if count > 0 {
				refs := make([]any, count-1)
				for i := range refs {
					refs[i] = map[string]any{"kind": "ConfigMap", "name": fmt.Sprintf("vars-%d", i)}
				}
				api := "kustomize.toolkit.fluxcd.io/v1"
				spec := map[string]any{"sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"},
					"postBuild": map[string]any{"substituteFrom": refs}}
				if kind == manifest.KindHelmRelease {
					api = "helm.toolkit.fluxcd.io/v2"
					spec = map[string]any{"chart": map[string]any{"spec": map[string]any{"chart": "app", "sourceRef": map[string]any{"kind": "HelmRepository", "name": "repo"}}}, "valuesFrom": refs}
				}
				var err error
				obj, _, _, err = manifest.ParseDoc(map[string]any{"apiVersion": api, "kind": kind,
					"metadata": map[string]any{"name": "app", "namespace": "ns"}, "spec": spec}, manifest.ParseDocOptions{DefaultNamespace: ""})
				if err != nil {
					b.Fatal(err)
				}
			}
			l := New(store.New())
			b.ReportAllocs()
			for b.Loop() {
				l.Index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{}
				l.recordObject(obj, "/repo/object.yaml")
			}
		})
	}
}

// BenchmarkIgnoreMatches probes ignoreSet.matches against a 100-pattern
// .krmignore over 1000 path probes per b.N iteration — the per-walk
// cost the loader pays in a large monorepo.
func BenchmarkIgnoreMatches(b *testing.B) {
	root := b.TempDir()
	var patterns strings.Builder
	for i := range 100 {
		fmt.Fprintf(&patterns, "ignored-%d/**\n", i)
	}
	testutil.WriteFile(b, root, ".krmignore", patterns.String())
	ig, err := readerForTest(root).loadIgnore(root)
	if err != nil {
		b.Fatalf("loadIgnore: %v", err)
	}

	probes := make([]string, 0, 1000)
	for i := range 1000 {
		probes = append(probes, filepath.Join(root, fmt.Sprintf("apps/group-%d/app-%d/manifest.yaml", i%20, i)))
	}

	b.ReportAllocs()
	for b.Loop() {
		for _, p := range probes {
			_ = ig.matches(p, root, false)
		}
	}
}

func synthLargeRepo(b *testing.B, root string) {
	b.Helper()
	const (
		nKS  = 200
		nHR  = 100
		nSrc = 50
	)
	var topResources strings.Builder
	for i := range nKS {
		fmt.Fprintf(&topResources, "  - ./apps/app-%d\n", i)
	}
	testutil.WriteFile(b, root, "kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n"+topResources.String())

	for i := range nKS {
		base := fmt.Sprintf("apps/app-%d", i)
		var resources strings.Builder
		resources.WriteString("  - ./ks.yaml\n")
		if i < nHR {
			resources.WriteString("  - ./hr.yaml\n")
		}
		if i < nSrc {
			resources.WriteString("  - ./source.yaml\n")
		}
		testutil.WriteFile(b, root, base+"/kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n"+resources.String())
		testutil.WriteFile(b, root, base+"/ks.yaml", testutil.KustomizationYAML("app-"+fmt.Sprintf("%d", i), "flux-system", "  interval: 10m\n  path: ./apps/app-"+fmt.Sprintf("%d", i)+"\n  prune: true\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n"))
		if i < nHR {
			testutil.WriteFile(b, root, base+"/hr.yaml", fmt.Sprintf(
				`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app-%d
  namespace: default
spec:
  interval: 15m
  chart:
    spec:
      chart: app-template
      version: "3.5.1"
      sourceRef:
        kind: HelmRepository
        name: bjw-s
        namespace: flux-system
  values:
    image:
      repository: nginx
      tag: latest
`, i))
		}
		if i < nSrc {
			testutil.WriteFile(b, root, base+"/source.yaml", fmt.Sprintf(
				`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: src-%d
  namespace: flux-system
spec:
  interval: 10m
  url: https://example.com/repo-%d
  ref:
    branch: main
`, i, i))
		}
	}
}

// synthDeepComponents writes a 5-level deep nested kustomize Components
// chain. Each level's kustomization.yaml references the next via the
// `components:` field so the loader's component-walk recurses through
// all levels.
func synthDeepComponents(b *testing.B, root string, depth int) {
	b.Helper()
	// Top-level kustomization references level 0 as a component.
	testutil.WriteFile(b, root, "kustomization.yaml", fmt.Sprintf(
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ./cm.yaml\ncomponents:\n  - ./components/level-%d\n", 0))
	testutil.WriteFile(b, root, "cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: root\n  namespace: flux-system\ndata:\n  k: v\n")

	for i := range depth {
		base := fmt.Sprintf("components/level-%d", i)
		// Each component refers to its child unless it's the leaf.
		body := "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources:\n  - ./cm.yaml\n"
		if i < depth-1 {
			body += fmt.Sprintf("components:\n  - ../level-%d\n", i+1)
		}
		testutil.WriteFile(b, root, base+"/kustomization.yaml", body)
		testutil.WriteFile(b, root, base+"/cm.yaml", fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm-%d\n  namespace: flux-system\ndata:\n  k: v\n", i))
	}
}
