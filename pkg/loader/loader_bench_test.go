package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/store"
)

// writeBenchFile mirrors writeBenchFile but accepts testing.TB so it
// works in *testing.B benchmarks (testutil's helper is *testing.T
// only). Tiny duplication; not worth widening testutil for one
// caller.
func writeBenchFile(tb testing.TB, root, rel, body string) {
	tb.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		tb.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		tb.Fatalf("write %s: %v", p, err)
	}
}

// largeRepoFixture builds a tree shaped like a mid-size home-ops
// monorepo: one root Kustomization, many leaf KSes via a shared
// kustomization.yaml graph plus per-leaf ConfigMaps. Tuned so the
// total file count lands around 1k+ for the BenchmarkLoad_LargeRepo
// run.
func largeRepoFixture(tb testing.TB, ksCount int) string {
	tb.Helper()
	root := tb.TempDir()

	// Root kustomization.yaml lists every leaf via `resources:`.
	var rootKust strings.Builder
	rootKust.WriteString("apiVersion: kustomize.config.k8s.io/v1\nkind: Kustomization\nresources:\n")
	for i := 0; i < ksCount; i++ {
		fmt.Fprintf(&rootKust, "  - apps/app-%03d\n", i)
	}
	writeBenchFile(tb, root, "kustomization.yaml", rootKust.String())

	for i := 0; i < ksCount; i++ {
		dir := filepath.Join("apps", fmt.Sprintf("app-%03d", i))
		// Leaf kustomization references its KS + a ConfigMap.
		writeBenchFile(tb, root, filepath.Join(dir, "kustomization.yaml"),
			"apiVersion: kustomize.config.k8s.io/v1\nkind: Kustomization\nresources:\n  - ks.yaml\n  - cm.yaml\n")
		writeBenchFile(tb, root, filepath.Join(dir, "ks.yaml"), fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app-%03d
  namespace: flux-system
spec:
  path: ./apps/app-%03d
  sourceRef:
    kind: GitRepository
    name: flux-system
`, i, i))
		writeBenchFile(tb, root, filepath.Join(dir, "cm.yaml"), fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: cm-%03d
  namespace: ns
data:
  k: v
`, i))
	}
	return root
}

// deepComponentsFixture builds a tree with deeply chained
// kustomize Components — exercises the on-disk component lookup
// path that the shared ComponentCache memoizes.
func deepComponentsFixture(tb testing.TB, leafCount, depth int) string {
	tb.Helper()
	root := tb.TempDir()
	// Component chain: components/c0 → components/c1 → ... → components/c(depth-1)
	for d := 0; d < depth; d++ {
		var k strings.Builder
		k.WriteString("apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n")
		if d+1 < depth {
			fmt.Fprintf(&k, "components:\n  - ../c%d\n", d+1)
		}
		writeBenchFile(tb, root, filepath.Join("components", fmt.Sprintf("c%d", d), "kustomization.yaml"), k.String())
	}
	// Leaves each include the component root.
	for i := 0; i < leafCount; i++ {
		dir := filepath.Join("apps", fmt.Sprintf("leaf-%03d", i))
		writeBenchFile(tb, root, filepath.Join(dir, "kustomization.yaml"),
			"apiVersion: kustomize.config.k8s.io/v1\nkind: Kustomization\ncomponents:\n  - ../../components/c0\nresources:\n  - ks.yaml\n")
		writeBenchFile(tb, root, filepath.Join(dir, "ks.yaml"), fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: leaf-%03d
  namespace: flux-system
spec:
  path: ./apps/leaf-%03d
`, i, i))
	}
	// Root references every leaf.
	var rootKust strings.Builder
	rootKust.WriteString("apiVersion: kustomize.config.k8s.io/v1\nkind: Kustomization\nresources:\n")
	for i := 0; i < leafCount; i++ {
		fmt.Fprintf(&rootKust, "  - apps/leaf-%03d\n", i)
	}
	writeBenchFile(tb, root, "kustomization.yaml", rootKust.String())
	return root
}

func BenchmarkLoad_LargeRepo(b *testing.B) {
	root := largeRepoFixture(b, 250)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		s := store.New()
		if _, err := New(s).Load(ctx, root); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}

func BenchmarkLoad_DeepComponents(b *testing.B) {
	root := deepComponentsFixture(b, 50, 5)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		s := store.New()
		if _, err := New(s).Load(ctx, root); err != nil {
			b.Fatalf("Load: %v", err)
		}
	}
}

// BenchmarkIgnoreMatches stresses the per-file ignoreSet.matches
// path that the walker memoizes. Builds a non-trivial pattern set
// + a representative repo-relative path slice and rotates through
// them so the cache hit/miss distribution mirrors a real walk.
func BenchmarkIgnoreMatches(b *testing.B) {
	root := b.TempDir()
	writeBenchFile(b, root, ".krmignore",
		"apps/junk/**\ntmp/\n*.bak\nvendor/**\nnode_modules/\n.cache/\n")
	ig, err := loadIgnore(root)
	if err != nil {
		b.Fatalf("loadIgnore: %v", err)
	}
	paths := []string{
		"apps/app-001/ks.yaml",
		"apps/app-001/cm.yaml",
		"apps/junk/old.yaml",
		"apps/junk/sub/old.yaml",
		"vendor/lib/a.yaml",
		"infra/cluster.yaml",
		"tmp/scratch.yaml",
		"apps/app-002/values.yaml",
	}
	for i := range paths {
		paths[i] = filepath.Join(root, filepath.FromSlash(paths[i]))
	}
	b.ResetTimer()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = ig.matches(paths[i%len(paths)], root, false)
		i++
	}
}
