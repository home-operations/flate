package kustomize

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

// fluxKS is a minimal Flux Kustomization spec for driving Render against a
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

func TestRender_DuplicateID_NamesProducers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cmChild(t, root, "a", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "dup"})

	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "apps",
				Spec:        fluxKS(),
			},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err == nil, "%s", "expected a duplicate-id build failure")
	}
	{
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
			"duplicate failure lost renderer attribution: %v",
			err,
		)
	}
	s := err.Error()
	for _, want := range []string{
		"already registered id",
		"duplicate resource(s) produced by multiple accumulated paths",
		"./apps/a",
		"./apps/b",
	} {
		{
			assert.ErrorIf(t, !strings.Contains(s, want), "error missing %q:\n%s", want, s)
		}
	}
	{
		assert.ErrorIf(
			t,
			strings.Index(s, "./apps/a") > strings.Index(s, "./apps/b"),
			"producer paths must be sorted (a before b):\n%s",
			s,
		)
	}
}

// TestRender_DistinctIDs_NoDiagnostic confirms a healthy aggregate renders
// without ever running the diagnostic.
func TestRender_DistinctIDs_NoDiagnostic(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cmChild(t, root, "a", manifest.NamedResource{Namespace: "shared", Name: "a-cm"})
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "b-cm"})

	out, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "apps",
				Spec:        fluxKS(),
			},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err != nil, "Render: %v", err)
	}
	{
		assert.FatalIf(t, len(out) == 0, "%s", "empty render")
	}
}

func TestRender_NonDuplicateError_Unannotated(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")

	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "apps",
				Spec:        fluxKS(),
			},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err == nil, "%s", "expected a build failure")
	}
	{
		assert.ErrorIf(
			t,
			!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
			"build failure lost renderer attribution: %v",
			err,
		)
	}
	{
		assert.ErrorIf(
			t,
			strings.Contains(err.Error(), "duplicate resource(s) produced"),
			"a non-duplicate error must not carry the diagnostic:\n%s",
			err,
		)
	}
}

func TestRender_NonDuplicateError_DuplicatesBehindIt(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/a/kustomization.yaml", "resources:\n- missing.yaml\n")
	cmChild(t, root, "b", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	cmChild(t, root, "c", manifest.NamedResource{Namespace: "shared", Name: "dup"})
	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err == nil, "%s", "expected a build failure")
	}
	{
		assert.ErrorIf(
			t,
			strings.Contains(err.Error(), "duplicate resource(s) produced"),
			"non-duplicate error carries the diagnostic:\n%s",
			err,
		)
	}
}

func TestRender_DuplicateID_Unattributable_FallsBack(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/kustomization.yaml",
		"namespace: shared\nresources:\n- one.yaml\n- two.yaml\n")
	testutil.WriteFile(t, root, "apps/one.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")
	testutil.WriteFile(t, root, "apps/two.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dup}\ndata: {k: v}\n")

	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, root),
				Label:       root,
				ApplyIgnore: false,
				SubPath:     "apps",
				Spec:        fluxKS(),
			},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err == nil, "%s", "expected a duplicate-id build failure")
	}
	{
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
			"duplicate failure lost renderer attribution: %v",
			err,
		)
	}
	s := err.Error()
	{
		assert.ErrorIf(t, !strings.Contains(s, "already registered id"), "expected the raw duplicate-id error:\n%s", s)
	}
	{
		assert.ErrorIf(
			t,
			strings.Contains(s, "duplicate resource(s) produced"),
			"a file-level dup is unattributable; diagnostic must stay silent:\n%s",
			s,
		)
	}
}

func TestRender_DuplicateID_ManyIDsNamesProducers(t *testing.T) {
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
	_, _, err := testutil.RenderMaps(
		Render(
			t.Context(),
			newTestTreeCache(t, ""),
			RenderRequest{Source: localSource(t, root), Label: root, SubPath: "apps", Spec: fluxKS()},
			sourcecache.RetryConfig{},
		),
	)
	{
		assert.FatalIf(t, err == nil, "%s", "expected duplicate-id failure")
	}
	{
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrFlux) || !strings.HasPrefix(err.Error(), "kustomize build apps: "),
			"duplicate failure lost renderer attribution: %v",
			err,
		)
	}
	_, diagnostic, ok := strings.Cut(err.Error(), "duplicate resource(s) produced by multiple accumulated paths:")
	{
		assert.FatalIf(t, !ok, "missing diagnostic attribution: %v", err)
	}
	for _, producer := range []string{"./apps/k00", "./apps/k01"} {
		{
			assert.ErrorIf(
				t,
				!strings.Contains(diagnostic+"\n", "\n      - "+producer+"\n"),
				"diagnostic missing producer %s: %s",
				producer,
				diagnostic,
			)
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
		{
			err := fsys.WriteFile(dir+"/kustomization.yaml", []byte("resources: [missing.yaml]\n"))
			assert.NoError(t, err)
		}
		fmt.Fprintf(&data, "- k%03d\n", i)
	}
	counting := diagnosticFS{FileSystem: fsys, children: make(map[string]bool)}
	duplicateProducers(counting, "/apps", "apps", []byte(data.String()))
	{
		got := len(counting.children)
		assert.FatalIf(t, got != dupDiagBuildLimit, "rebuilt %d children, want %d", got, dupDiagBuildLimit)
	}
}
