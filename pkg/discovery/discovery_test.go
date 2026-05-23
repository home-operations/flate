package discovery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestRun_SmallTree exercises the discovery phase end-to-end on a
// minimal three-file repo: a parent KS that points at apps/, a child
// KS under apps/, and an unrelated GR. After Run we expect the store
// populated with both KSes + the GR + the synthetic bootstrap GR, with
// SourceFiles tracking each one and ParentOf wiring the child to the
// parent.
func TestRun_SmallTree(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	mustWrite(t, filepath.Join(dir, "flux", "parent.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: parent
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
  interval: 10m
`)
	mustWrite(t, filepath.Join(dir, "apps", "child.yaml"), `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: child
  namespace: flux-system
spec:
  path: ./apps/leaf
  sourceRef:
    kind: GitRepository
    name: flux-system
  interval: 10m
`)
	mustWrite(t, filepath.Join(dir, "apps", "leaf", "kustomization.yaml"), `resources: []
`)

	st := store.New()
	res, err := discovery.Run(context.Background(), discovery.Config{
		Path: dir, Store: st, WipeSecrets: true,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	wantRoot, _ := filepath.EvalSymlinks(dir)
	if res.RepoRoot != wantRoot {
		t.Errorf("RepoRoot = %q, want %q", res.RepoRoot, wantRoot)
	}

	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "parent"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
	for _, id := range []manifest.NamedResource{parent, child} {
		if _, ok := res.SourceFiles[id]; !ok {
			t.Errorf("SourceFiles missing %s", id)
		}
		if st.GetObject(id) == nil {
			t.Errorf("Store missing %s", id)
		}
	}

	if got := res.ParentOf[child]; got != parent {
		t.Errorf("ParentOf[child] = %v, want %v", got, parent)
	}

	// Synthetic bootstrap GR should be Ready so KSes resolve their
	// sourceRef without an explicit GitRepository file in the tree.
	bootstrap := manifest.BootstrapSourceID
	if st.GetObject(bootstrap) == nil {
		t.Errorf("bootstrap GitRepository not seeded")
	}
}

func TestRun_RequiresStoreAndLoader(t *testing.T) {
	t.Parallel()
	if _, err := discovery.Run(context.Background(), discovery.Config{Path: t.TempDir()}); err == nil {
		t.Error("Run with nil Store/Loader: want error, got nil")
	}
}

func TestResolveScanPath_SymlinkResolved(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	got, err := discovery.ResolveScanPath(link)
	if err != nil {
		t.Fatalf("ResolveScanPath: %v", err)
	}
	want, _ := filepath.EvalSymlinks(target)
	if got != want {
		t.Errorf("ResolveScanPath(link) = %q, want %q", got, want)
	}
}

func TestFindRepoRoot_NoGitFallsBack(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if got := discovery.FindRepoRoot(dir); got != dir {
		t.Errorf("FindRepoRoot(%q) = %q; expected unchanged when no .git ancestor", dir, got)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
