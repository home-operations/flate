package testfixture

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/kustomize"
)

func TestScenario_Catalog(t *testing.T) {
	t.Parallel()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate scenario catalog")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "testdata")
	catalog, err := os.ReadFile(filepath.Join(root, "TESTDATA.md"))
	must(t, err)
	paths, err := filepath.Glob(filepath.Join(root, "*", "scenario.json"))
	must(t, err)
	for _, path := range paths {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			t.Parallel()
			dir := filepath.Dir(path)
			s, err := load(dir)
			must(t, err)
			for _, c := range s.Cases {
				if !strings.Contains(string(catalog), "`"+filepath.Base(dir)+"/"+c.Name+"`") {
					t.Errorf("case %s requires a TESTDATA.md catalog entry", c.Name)
				}
			}
			assertScenarioFiles(t, dir, s.Files)
		})
	}
}

func assertScenarioFiles(t *testing.T, dir string, files []fileUse) {
	t.Helper()
	used := make(map[string]bool, len(files))
	for _, f := range files {
		if used[f.Path] {
			t.Errorf("duplicate file use %s", f.Path)
		}
		used[f.Path] = true
		info, err := os.Stat(filepath.Join(dir, f.Path))
		must(t, err)
		if !info.Mode().IsRegular() {
			t.Errorf("file use %s is not a regular file", f.Path)
		}
	}
	must(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel != "scenario.json" && !used[filepath.ToSlash(rel)] {
			t.Errorf("fixture file %s requires a declared case and purpose", rel)
		}
		if entry.Name() == "kustomization.yaml" {
			assertScenarioPackage(t, path)
		}
		return nil
	}))
}

func assertScenarioPackage(t *testing.T, path string) {
	t.Helper()
	resources, err := kustomize.Build(filesys.MakeFsOnDisk(), filepath.Dir(path))
	must(t, err)
	if resources.Size() == 0 {
		t.Errorf("successful package %s emits no objects", path)
	}
}
