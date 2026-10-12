package kustomize

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/testutil"
)

// fluxKS is a minimal Flux Kustomization spec for driving RenderFlux against a
// subPath; the auto-generate path keys off the directory, not these fields.
func fluxKS() map[string]any {
	return map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "./apps"},
	}
}

// cmChild writes apps/<name>/{kustomization.yaml,cm.yaml} producing a ConfigMap
// named cmName in namespace ns — so two children sharing (cmName, ns) clash only
// once a parent accumulates them, mirroring the real cluster-apps case.
func cmChild(t *testing.T, root, name, ns, cmName string) {
	t.Helper()
	dir := "apps/" + name
	testutil.WriteFile(t, root, dir+"/kustomization.yaml", "namespace: "+ns+"\nresources:\n- cm.yaml\n")
	testutil.WriteFile(t, root, dir+"/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+cmName+"}\ndata: {k: v}\n")
}

// TestRenderFlux_DuplicateID_NamesProducers is the repro: two children emit the
// same id, the auto-generated parent accumulates both and fails, and the error
// now names BOTH producing paths (sorted) instead of just the one kustomize hit.
func TestRenderFlux_DuplicateID_NamesProducers(t *testing.T) {
	root := t.TempDir()
	cmChild(t, root, "a", "shared", "dup")
	cmChild(t, root, "b", "shared", "dup")
	// No apps/kustomization.yaml → RenderFlux synthesizes one over a + b.

	_, err := RenderFlux(context.Background(), NewTreeCache(), RenderRequest{Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()})
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
	root := t.TempDir()
	cmChild(t, root, "a", "shared", "a-cm")
	cmChild(t, root, "b", "shared", "b-cm")

	out, err := RenderFlux(context.Background(), NewTreeCache(), RenderRequest{Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()})
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
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")

	_, err := RenderFlux(context.Background(), NewTreeCache(), RenderRequest{Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()})
	if err == nil {
		t.Fatal("expected a build failure")
	}
	if strings.Contains(err.Error(), "duplicate resource(s) produced") {
		t.Errorf("a non-duplicate error must not carry the diagnostic:\n%s", err)
	}
}

func TestRenderFlux_NonDuplicateError_DuplicatesBehindIt(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")
	cmChild(t, root, "b", "shared", "dup")
	cmChild(t, root, "c", "shared", "dup")
	_, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Label: root, SubPath: "apps", Spec: fluxKS()})
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
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml",
		"namespace: shared\nresources:\n- one.yaml\n- two.yaml\n")
	testutil.WriteFile(t, root, "apps/one.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")
	testutil.WriteFile(t, root, "apps/two.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")

	_, err := RenderFlux(context.Background(), NewTreeCache(), RenderRequest{Label: root, ApplyIgnore: false, SubPath: "apps", Spec: fluxKS()})
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
	root := t.TempDir()
	for i := range 80 {
		var body strings.Builder
		for j := range 4 {
			name := fmt.Sprintf("cm-%02d-%d", i, j)
			if i == 70 && j == 0 {
				name = "cm-03-0"
			}
			body.WriteString(cmYAML(name) + "---\n")
		}
		dir := fmt.Sprintf("apps/k%02d/", i)
		testutil.WriteFile(t, root, dir+"kustomization.yaml", existingKustomization("cm.yaml"))
		testutil.WriteFile(t, root, dir+"cm.yaml", body.String())
	}
	_, err := RenderFlux(t.Context(), NewTreeCache(), RenderRequest{Label: root, SubPath: "apps", Spec: fluxKS()})
	if err == nil {
		t.Fatal("expected duplicate-id failure")
	}
	for _, producer := range []string{"./apps/k03", "./apps/k70"} {
		if !strings.Contains(err.Error(), producer) {
			t.Errorf("missing producer %s: %v", producer, err)
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
