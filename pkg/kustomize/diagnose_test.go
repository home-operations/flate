package kustomize

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// fluxKS is a minimal Flux Kustomization spec for driving RenderFlux against a
// subPath; the auto-generate path keys off the directory, not these fields.
func fluxKS() *manifest.Kustomization {
	return &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
}

func cmChild(t *testing.T, root, name string, id manifest.NamedResource) {
	t.Helper()
	dir := "apps/" + name
	testutil.WriteFile(t, root, dir+"/kustomization.yaml", "namespace: "+id.Namespace+"\nresources:\n- cm.yaml\n")
	testutil.WriteFile(t, root, dir+"/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+id.Name+"}\ndata: {k: v}\n")
}

// TestRenderFlux_DuplicateID_NamesProducers is the repro: two children emit the
// same id, the auto-generated parent accumulates both and fails, and the error
// now names BOTH producing paths (sorted) instead of just the one kustomize hit.
func TestRenderFlux_DuplicateID_NamesProducers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cmChild(t, root, "a", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	// No apps/kustomization.yaml → RenderFlux synthesizes one over a + b.

	_, err := RenderFlux(context.Background(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()}, sourcecache.RetryConfig{})
	if err == nil {
		t.Fatal("expected a duplicate-id build failure")
	}
	s := err.Error()
	for _, want := range []string{
		"already registered id",
		"duplicate resource(s) produced by multiple accumulated paths",
		"./apps/a",
		"./apps/b",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("error missing %q:\n%s", want, s)
		}
	}
	if strings.Index(s, "./apps/a") > strings.Index(s, "./apps/b") {
		t.Errorf("producer paths must be sorted (a before b):\n%s", s)
	}
}

// TestRenderFlux_DistinctIDs_NoDiagnostic confirms a healthy aggregate renders
// without ever running the diagnostic.
func TestRenderFlux_DistinctIDs_NoDiagnostic(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cmChild(t, root, "a", manifest.NamedResource{Namespace: "shared", Name: "a-cm"})
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "b-cm"})

	out, err := RenderFlux(context.Background(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("empty render")
	}
}

// TestRenderFlux_NonDuplicateError_Unannotated confirms a build failure that
// isn't a duplicate id is surfaced verbatim — the diagnostic stays out of it.
func TestRenderFlux_NonDuplicateError_Unannotated(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")

	_, err := RenderFlux(context.Background(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()}, sourcecache.RetryConfig{})
	if err == nil {
		t.Fatal("expected a build failure")
	}
	if strings.Contains(err.Error(), "duplicate resource(s) produced") {
		t.Errorf("a non-duplicate error must not carry the diagnostic:\n%s", err)
	}
}

func TestRenderFlux_NonDuplicateError_DuplicatesBehindIt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	cmChild(t, root, "c", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	_, err := RenderFlux(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()}, sourcecache.RetryConfig{})
	if err == nil {
		t.Fatal("expected a build failure")
	}
	if strings.Contains(err.Error(), "duplicate resource(s) produced") {
		t.Errorf("non-duplicate error carries the diagnostic:\n%s", err)
	}
}

// TestRenderFlux_DuplicateID_Unattributable_FallsBack confirms graceful
// fallback: when the collision is between two leaf files of one kustomization
// (no directory producer to attribute), the raw error stands unchanged.
func TestRenderFlux_DuplicateID_Unattributable_FallsBack(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml",
		"namespace: shared\nresources:\n- one.yaml\n- two.yaml\n")
	testutil.WriteFile(t, root, "apps/one.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")
	testutil.WriteFile(t, root, "apps/two.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")

	_, err := RenderFlux(context.Background(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()}, sourcecache.RetryConfig{})
	if err == nil {
		t.Fatal("expected a duplicate-id build failure")
	}
	s := err.Error()
	if !strings.Contains(s, "already registered id") {
		t.Errorf("expected the raw duplicate-id error:\n%s", s)
	}
	if strings.Contains(s, "duplicate resource(s) produced") {
		t.Errorf("a file-level dup is unattributable; diagnostic must stay silent:\n%s", s)
	}
}

func TestRenderFlux_DuplicateID_ManyIDsNamesProducers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var body strings.Builder
	for i := range dupDiagBuildLimit + 1 {
		body.WriteString(cmYAML(fmt.Sprintf("cm-%03d", i)) + "---\n")
	}
	testutil.WriteFile(t, root, "apps/k00/kustomization.yaml", existingKustomization("cm.yaml"))
	testutil.WriteFile(t, root, "apps/k00/cm.yaml", body.String())
	testutil.WriteFile(t, root, "apps/k01/kustomization.yaml", existingKustomization("cm.yaml"))
	testutil.WriteFile(t, root, "apps/k01/cm.yaml", cmYAML("cm-000"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", existingKustomization("k00", "k01"))
	_, err := RenderFlux(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()}, sourcecache.RetryConfig{})
	if err == nil {
		t.Fatal("expected duplicate-id failure")
	}
	_, diagnostic, ok := strings.Cut(err.Error(), "duplicate resource(s) produced by multiple accumulated paths:")
	if !ok {
		t.Fatalf("missing diagnostic attribution: %v", err)
	}
	for _, producer := range []string{"./apps/k00", "./apps/k01"} {
		if !strings.Contains(diagnostic+"\n", "\n      - "+producer+"\n") {
			t.Errorf("diagnostic missing producer %s: %s", producer, diagnostic)
		}
	}
}

type diagnosticFS struct {
	filesys.FileSystem
	children map[string]bool
}

func (f diagnosticFS) ReadFile(path string) ([]byte, error) {
	if filepath.Base(path) == "kustomization.yaml" {
		f.children[filepath.Dir(path)] = true
	}
	return f.FileSystem.ReadFile(path)
}

func TestDuplicateID_BoundedRebuilds(t *testing.T) {
	t.Parallel()
	fsys := filesys.MakeFsInMemory()
	var data strings.Builder
	data.WriteString("resources:\n")
	for i := range 300 {
		dir := fmt.Sprintf("/apps/k%03d", i)
		if err := fsys.WriteFile(dir+"/kustomization.yaml", []byte("resources: [missing.yaml]\n")); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&data, "- k%03d\n", i)
	}
	counting := diagnosticFS{FileSystem: fsys, children: make(map[string]bool)}
	duplicateProducers(counting, "/apps", "apps", []byte(data.String()))
	if got := len(counting.children); got != dupDiagBuildLimit {
		t.Fatalf("rebuilt %d children, want %d", got, dupDiagBuildLimit)
	}
}
