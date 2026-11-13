package testfixture

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"golang.org/x/mod/module"
	"sigs.k8s.io/kustomize/api/konfig"
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
			missing := assertScenarioFiles(t, dir, s)
			assertScenarioPackages(t, dir, s, missing)
		})
	}
}

func assertScenarioFiles(t *testing.T, dir string, s *scenario) map[string]bool {
	t.Helper()
	files := s.Files
	missing := make(map[string]bool)
	used := make(map[string]bool, len(files))
	for _, f := range files {
		if used[f.Path] {
			t.Errorf("duplicate file use %s", f.Path)
		}
		used[f.Path] = true
		info, err := os.Stat(filepath.Join(dir, f.Path))
		if os.IsNotExist(err) {
			missing[f.Path] = true
			continue
		}
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
		zipPath := filepath.ToSlash(filepath.Join("testdata", filepath.Base(dir), rel))
		if err := module.CheckFilePath(zipPath); err != nil {
			t.Errorf("fixture file breaks the module zip: %v", err)
		}
		return nil
	}))
	return missing
}

type packageObservation struct {
	files, packages map[string]bool
	complete        map[string]bool
}

func assertScenarioPackages(t *testing.T, dir string, s *scenario, missing map[string]bool) {
	t.Helper()
	complete := assertRawPackages(t, dir, s.Files, missing)
	observations := make([]packageObservation, len(s.Cases))
	t.Cleanup(func() { s.assertPackageCoverage(t, missing, complete, observations) })
	for caseIndex := range s.Cases {
		t.Run(s.Cases[caseIndex].Name, func(t *testing.T) {
			t.Parallel()
			observation := &observations[caseIndex]
			observation.files = make(map[string]bool)
			observation.packages = make(map[string]bool)
			observation.complete = complete
			s.assertCasePackages(t, dir, &s.Cases[caseIndex], observation)
		})
	}
}

func assertRawPackages(t *testing.T, dir string, files []fileUse, missing map[string]bool) map[string]bool {
	t.Helper()
	complete := make(map[string]bool)
	for _, file := range files {
		if missing[file.Path] || file.Package == "component" ||
			!slices.Contains(konfig.RecognizedKustomizationFileNames(), filepath.Base(file.Path)) {
			continue
		}
		kustomize.BuildMutex.Lock()
		resources, err := kustomize.Build(filesys.MakeFsOnDisk(), filepath.Join(dir, filepath.Dir(file.Path)))
		kustomize.BuildMutex.Unlock()
		if err == nil {
			complete[file.Path] = true
			if resources.Size() == 0 {
				t.Errorf("successful package %s emits no objects", file.Path)
			}
		}
	}
	return complete
}

func (s *scenario) assertPackageCoverage(
	t *testing.T,
	missing, complete map[string]bool,
	observations []packageObservation,
) {
	t.Helper()
	for _, file := range s.Files {
		if file.Package != "" &&
			!slices.Contains(konfig.RecognizedKustomizationFileNames(), filepath.Base(file.Path)) {
			t.Errorf("package role on non-package file %s", file.Path)
		}
		if complete[file.Path] ||
			!missing[file.Path] &&
				!slices.Contains(konfig.RecognizedKustomizationFileNames(), filepath.Base(file.Path)) {
			continue
		}
		if !packageObserved(&file, s.Cases, missing[file.Path], observations) {
			t.Errorf("fixture file %s requires a materialized case state", file.Path)
		}
	}
}

func packageObserved(file *fileUse, cases []Case, missing bool, observations []packageObservation) bool {
	for i := range cases {
		if !slices.Contains(file.Cases, cases[i].Name) {
			continue
		}
		observed := observations[i].packages
		if missing {
			observed = observations[i].files
		}
		if observed[file.Path] {
			return true
		}
	}
	return false
}

func (s *scenario) assertCasePackages(t *testing.T, dir string, c *Case, observation *packageObservation) {
	t.Helper()
	f := materialize(t, dir, c.Name)
	files := make(map[string]fileUse, len(s.Files))
	for _, file := range s.Files {
		files[filepath.Join(dir, file.Path)] = file
	}
	var codes map[int]bool
	sdkOnly := !slices.ContainsFunc(c.Steps, func(step Step) bool { return step.Invoke != nil })
	for i, step := range c.Steps {
		for _, source := range f.origins {
			rel, err := filepath.Rel(dir, source)
			must(t, err)
			observation.files[filepath.ToSlash(rel)] = true
		}
		if step.Edit != nil {
			f.Edit(t, step)
			if sdkOnly {
				f.assertMaterializedPackages(t, files, observation, nil)
			}
			continue
		}
		if i == 0 || c.Steps[i-1].Edit != nil {
			codes = stateExitCodes(c.Steps[i:])
		}
		f.observeCommittedPackages(t, dir, step.Invoke, observation)
		f.assertMaterializedPackages(t, files, observation, codes)
	}
}

func (f *Fixture) observeCommittedPackages(
	t *testing.T,
	dir string,
	invocation *Invocation,
	observation *packageObservation,
) {
	t.Helper()
	// Committed inputs MUST contribute coverage even when the working tree selects another revision.
	for name, origins := range f.commitOrigins {
		token := "@@commit:" + name + "@@"
		if !slices.ContainsFunc(invocation.Argv, func(arg string) bool { return strings.Contains(arg, token) }) {
			continue
		}
		for _, source := range origins {
			rel, err := filepath.Rel(dir, source)
			must(t, err)
			name := filepath.ToSlash(rel)
			observation.files[name] = true
			if slices.Contains(konfig.RecognizedKustomizationFileNames(), filepath.Base(name)) {
				observation.packages[name] = true
			}
		}
	}
}

func stateExitCodes(steps []Step) map[int]bool {
	codes := make(map[int]bool)
	for _, step := range steps {
		if step.Edit != nil {
			break
		}
		codes[*step.Invoke.Expect.Code] = true
	}
	return codes
}

func (f *Fixture) assertMaterializedPackages(
	t *testing.T,
	files map[string]fileUse,
	observation *packageObservation,
	codes map[int]bool,
) {
	t.Helper()
	for _, repo := range f.schema.Repositories {
		root := filepath.Join(f.root, repo.Name)
		must(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && entry.Name() == ".git" {
				return filepath.SkipDir
			}
			if entry.IsDir() || !slices.Contains(konfig.RecognizedKustomizationFileNames(), entry.Name()) {
				return nil
			}
			role, ok := files[f.origins[path]]
			if !ok {
				return fmt.Errorf("package %s requires an active file use for case %s", path, f.Plan.Name)
			}
			if codes == nil && observation.complete[role.Path] {
				return nil
			}
			observation.packages[role.Path] = true
			return f.assertMaterializedPackage(t, path, &role, codes)
		}))
	}
}

func (f *Fixture) assertMaterializedPackage(t *testing.T, path string, role *fileUse, codes map[int]bool) error {
	t.Helper()
	target := filepath.Dir(path)
	observed := &componentRead{FileSystem: filesys.MakeFsOnDisk()}
	if role.Package == "component" {
		relative, err := filepath.Rel(f.root, path)
		must(t, err)
		repository, _, _ := strings.Cut(filepath.ToSlash(relative), "/")
		_, consumer, _ := strings.Cut(role.Consumer, "/")
		target = f.path(t, repository+"/"+consumer)
		observed.path, err = filepath.EvalSymlinks(path)
		if err != nil {
			return fmt.Errorf("resolve component %s: %w", role.Path, err)
		}
	}
	kustomize.BuildMutex.Lock()
	resources, err := kustomize.Build(observed, target)
	kustomize.BuildMutex.Unlock()
	if err != nil {
		if role.Package == "failure" && codes[1] || role.Package == "excluded" && codes[0] {
			return nil
		}
		return err
	}
	if resources.Size() == 0 {
		t.Errorf("successful package %s emits no objects", role.Path)
	}
	if role.Package == "component" && !observed.read {
		t.Errorf("consumer %s did not read component %s", role.Consumer, role.Path)
	}
	return nil
}

type componentRead struct {
	filesys.FileSystem

	path string
	read bool
}

func (f *componentRead) ReadFile(path string) ([]byte, error) {
	body, err := f.FileSystem.ReadFile(path)
	if err == nil && filepath.Clean(path) == f.path {
		f.read = true
	}
	return body, err
}
