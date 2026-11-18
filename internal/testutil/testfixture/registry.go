package testfixture

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func (f *Fixture) bindRegistry(tb testing.TB, declaration *registry) {
	tb.Helper()
	url := f.registryURL(tb, declaration.Chart)
	for _, file := range declaration.Files {
		path := f.path(tb, file)
		body, err := os.ReadFile(path)
		must(tb, err)
		if !strings.Contains(string(body), declaration.Sentinel) {
			tb.Fatalf("missing registry sentinel in %s", file)
		}
		must(tb, os.WriteFile(path, []byte(strings.ReplaceAll(string(body), declaration.Sentinel, url)), 0o600))
	}
}

func (f *Fixture) registryURL(tb testing.TB, chartPath string) string {
	tb.Helper()
	if url := f.registries[chartPath]; url != "" {
		return url
	}
	t, ok := tb.(*testing.T)
	if !ok {
		tb.Fatal("registry fixtures require a test")
	}
	chart := make(map[string]string)
	root := filepath.Join(f.dir, chartPath)
	must(tb, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("read chart: %w", readErr)
		}
		relative, relativeErr := filepath.Rel(filepath.Dir(root), path)
		if relativeErr != nil {
			return fmt.Errorf("chart path: %w", relativeErr)
		}
		chart[filepath.ToSlash(relative)] = string(body)
		return nil
	}))
	url := testutil.NewChartRegistry(t, []map[string]string{chart}, false).URL
	if f.registries == nil {
		f.registries = make(map[string]string)
	}
	f.registries[chartPath] = url
	return url
}
