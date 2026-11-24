package helm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCaptureChart_SiblingIdentity(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"parent/Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  alias: renamed\n  version: ^1.0.0\n  repository: file://../child\n",
			),
		},
		"child/Chart.yaml":        {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
		"child/templates/cm.yaml": {Data: []byte("before")},
	}
	captured, missing, err := captureChart(t.Context(), input, "parent", false)
	assert.NoError(t, err)
	assert.Equal(t, missing, false)
	identity := captured.fingerprint()
	input["child/templates/cm.yaml"].Data = []byte("after")
	loaded, err := captured.load()
	assert.NoError(t, err)
	assert.Equal(t, loaded.Dependencies()[0].Name(), "renamed")
	assert.Equal(t, string(loaded.Dependencies()[0].Templates[0].Data), "before")
	next, _, err := captureChart(t.Context(), input, "parent", false)
	assert.NoError(t, err)
	assert.Equal(t, next.fingerprint() == identity, false)
	assert.Equal(t, captured.fingerprint(), identity)

	client := NewClient(nil, nil)
	selector := chartSelector{Path: "parent"}
	first, err := client.describeInput(t.Context(), input, selector, "")
	assert.NoError(t, err)
	firstChart, err := first.captured.load()
	assert.NoError(t, err)
	unchanged, err := client.describeInput(t.Context(), input, selector, "")
	assert.NoError(t, err)
	assert.Equal(t, unchanged.key, first.key)
	assert.Equal(t, unchanged.captured.parent == first.captured.parent, true)
	input["child/templates/cm.yaml"].Data = []byte("latest")
	changed, err := client.describeInput(t.Context(), input, selector, "")
	assert.NoError(t, err)
	assert.Equal(t, changed.key == first.key, false)
	assert.Equal(t, changed.captured.parent == first.captured.parent, true)
	changedChart, err := changed.captured.load()
	assert.NoError(t, err)
	assert.Equal(t, string(firstChart.Dependencies()[0].Templates[0].Data), "after")
	assert.Equal(t, string(changedChart.Dependencies()[0].Templates[0].Data), "latest")
}

func TestCaptureChart_DependencyFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, reference, version string }{
		{"missing", "file://../missing", "1.0.0"},
		{"escape", "file://../../outside", "1.0.0"},
		{"constraint", "file://../child", "2.0.0"},
		{"symlink", "file://../linked", "1.0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"parent/Chart.yaml",
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: "+tc.version+"\n  repository: "+tc.reference+"\n",
			)
			testutil.WriteFile(t, root, "child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
			if tc.name == "symlink" {
				outside := t.TempDir()
				testutil.WriteFile(t, outside, "Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
				assert.NoError(t, os.Symlink(outside, filepath.Join(root, "linked")))
			}
			_, _, err := captureChart(t.Context(), os.DirFS(root), "parent", false)
			assert.FatalIf(t, err == nil, "dependency %s succeeded", tc.name)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, strings.Contains(err.Error(), "child"), true)
			if tc.name == "missing" {
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
		})
	}
}

func TestCapturedChart_DependencyErrorOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, message string
		archive       []byte
		cause         error
	}{
		{name: "present_empty_archive", archive: []byte{}, cause: io.EOF, message: "dependency first: EOF"},
		{name: "unresolved", cause: manifest.ErrInput, message: "unresolved chart dependency first (https://example.invalid)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			captured := capturedChart{
				parent: &chart.Chart{Metadata: &chart.Metadata{Name: "parent"}},
				dependencies: &chartinput.Description{Dependencies: []chartinput.Dependency{
					{
						Name:        "first",
						Declaration: chart.Dependency{Repository: "https://example.invalid"},
						Capture:     chartinput.Capture{Archive: tc.archive},
					},
					{Name: "later"},
				}},
			}
			_, err := captured.load()
			assert.FatalIf(t, err == nil, "load succeeded")
			assert.Equal(t, errors.Is(err, tc.cause), true)
			assert.Equal(t, err.Error(), tc.message)
		})
	}
}

func TestCaptureChart_ArchiveCloseFailure(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"chart.tgz": {
			Data: chartArchive(
				t,
				fstest.MapFS{"Chart.yaml": {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")}},
			),
		},
	}
	failure := errors.New("archive close failed")
	observed := chartOpenFS{FS: input, open: func(name string) (fs.File, error) {
		file, err := input.Open(name)
		if err != nil {
			return nil, err
		}
		return &chartErrorFile{File: file, err: failure, closeOnly: true}, nil
	}}
	_, _, err := captureChart(t.Context(), observed, "chart.tgz", false)
	assert.Equal(t, errors.Is(err, failure), true)
}

func TestCaptureChart_SymlinkDependency(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, repository, link, target, chart, wantPath string
	}{
		{"file URL child", "file://../linked", "linked", "child", "child", "linked"},
		{"file URL ancestor", "file://../linked/child", "linked", "siblings", "siblings/child", "linked/child"},
		{"local child", "", "parent/charts/child", "../../sibling", "sibling", "parent/charts/child"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"parent/Chart.yaml",
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  repository: "+tc.repository+"\n",
			)
			testutil.WriteFile(t, root, "parent/.helmignore", "charts/child\n")
			testutil.WriteFile(t, root, tc.chart+"/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n")
			link := filepath.Join(root, tc.link)
			assert.NoError(t, os.MkdirAll(filepath.Dir(link), 0o700))
			assert.NoError(t, os.Symlink(tc.target, link))
			captured, missing, err := captureChart(t.Context(), os.DirFS(root), "parent", false)
			pathErr, ok := errors.AsType[*fs.PathError](err)
			assert.FatalIf(t, !ok || !errors.Is(err, fs.ErrNotExist), "want missing dependency, got %v", err)
			assert.Equal(t, missing, false)
			assert.Equal(t, pathErr.Path, tc.wantPath)
			dependencies := captured.dependencyInputs()
			assert.Equal(t, len(dependencies), 1)
			assert.Equal(t, dependencies[0].Path, tc.wantPath)
		})
	}
}

func TestCaptureChart_DependencyAssembly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, repository, lock, ignore string
		vendored                       bool
		want                           []string
	}{
		{name: "preserve_existing", repository: "file://../child", vendored: true, want: []string{"existing", "renamed"}},
		{name: "lock_precedence", repository: "file://../wrong", lock: "dependencies:\n- name: child\n  alias: locked\n  version: ^1.0.0\n  repository: file://../child\n", want: []string{"locked"}},
		{name: "empty_lock", repository: "file://../wrong", lock: "dependencies: []\n"},
		{name: "empty_repository", ignore: "charts/\n", want: []string{"renamed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := "apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  alias: renamed\n  version: ^1.0.0\n  repository: " + tc.repository + "\n"
			input := fstest.MapFS{
				"parent/Chart.yaml": {Data: []byte(parent)},
				"child/Chart.yaml":  {Data: []byte("apiVersion: v2\nname: child\nversion: 1.2.0\n")},
			}
			if tc.vendored {
				input["parent/Chart.yaml"].Data = []byte(
					parent + "- name: existing\n  version: 1.0.0\n  repository: file://../absent\n",
				)
				input["parent/charts/existing/Chart.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v2\nname: existing\nversion: 1.0.0\n"),
				}
				input["parent/charts/existing/templates/cm.yaml"] = &fstest.MapFile{Data: []byte("existing bytes")}
			}
			if tc.lock != "" {
				input["parent/Chart.lock"] = &fstest.MapFile{Data: []byte(tc.lock)}
			}
			if tc.ignore != "" {
				input["parent/.helmignore"] = &fstest.MapFile{Data: []byte(tc.ignore)}
				input["parent/charts/child/Chart.yaml"] = input["child/Chart.yaml"]
			}
			captured, _, err := captureChart(t.Context(), input, "parent", false)
			assert.NoError(t, err)
			loaded, err := captured.load()
			assert.NoError(t, err)
			names := slices.Grow([]string(nil), len(loaded.Dependencies()))
			for _, child := range loaded.Dependencies() {
				names = append(names, child.Name())
				if child.Name() == "existing" {
					assert.Equal(t, string(child.Templates[0].Data), "existing bytes")
				}
			}
			assert.Diff(t, names, tc.want)
		})
	}
}

func captureChart(ctx context.Context, input fs.FS, name string, archiveShape bool) (capturedChart, bool, error) {
	d, missing, err := chartinput.Describe(
		ctx,
		input,
		chartinput.Options{Name: name, Archive: archiveShape},
		nil,
	)
	captured := capturedChart{files: d.Parent.Files, archive: d.Parent.Archive}
	if len(d.Dependencies) != 0 {
		captured.dependencies = &d
	}
	if err != nil {
		if missing {
			err = fmt.Errorf("chart not found at %s: %w", name, err)
		}
		err = manifest.NewError(manifest.ErrInput, err, "")
	}
	return captured, missing, err
}
