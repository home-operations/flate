package chartinput_test

import (
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/helm/chartinput"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestDescribe_MissingDependencies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, lock, vendored, key, repository, childPath string
		count                                            int
	}{
		{name: "local", count: 1},
		{name: "absolute_url_path", repository: "file:///sub/child", childPath: "parent/sub/child", count: 1},
		{name: "quoted_key", key: `"dependencies"`, count: 1},
		{name: "encoded_key", key: `"\x64ependencies"`, count: 1},
		{name: "case_insensitive_key", key: "Dependencies", count: 1},
		{name: "lock", lock: "dependencies:\n- name: child\n  version: 1.0.0\n  repository: file://../child\n", count: 1},
		{name: "equal_count", vendored: "other", count: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testDescribeMissingDependenciesCase(t, &tc)
		})
	}
}

func testDescribeMissingDependenciesCase(t *testing.T, tc *struct {
	name, lock, vendored, key, repository, childPath string
	count                                            int
},
) {
	t.Helper()
	repository, childPath := "file://../child", "child"
	if tc.repository != "" {
		repository, childPath = tc.repository, tc.childPath
	}
	input := fstest.MapFS{
		"parent/Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  alias: renamed\n  version: 1.0.0\n  repository: " + repository + "\n",
			),
		},
		childPath + "/Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: child\nversion: 1.0.0\ndependencies:\n- name: grandchild\n  version: 1.0.0\n  repository: https://unrequested.invalid\n",
			),
		},
	}
	if tc.key != "" {
		input["parent/Chart.yaml"].Data = []byte(
			strings.ReplaceAll(string(input["parent/Chart.yaml"].Data), "dependencies:", tc.key+":"),
		)
	}
	if tc.lock != "" {
		input["parent/Chart.lock"] = &fstest.MapFile{Data: []byte(tc.lock)}
	}
	if tc.vendored != "" {
		input["parent/charts/other/Chart.yaml"] = &fstest.MapFile{
			Data: []byte("apiVersion: v2\nname: other\nversion: 1.0.0\n"),
		}
	}
	description, missing, err := chartinput.Describe(
		t.Context(),
		input,
		chartinput.Options{Name: "parent"},
		nil,
	)
	assert.FatalIf(t, err != nil || missing, "description: missing=%v err=%v", missing, err)
	assert.FatalIf(
		t,
		len(description.Dependencies) != tc.count,
		"dependencies=%d, want %d",
		len(description.Dependencies),
		tc.count,
	)
	if tc.count == 0 {
		return
	}
	dep := description.Dependencies[0]
	assert.FatalIf(t, dep.Path != childPath || dep.Remote(), "dependency: %+v", dep)
	child, err := dep.Capture.Load()
	assert.NoError(t, err)
	assert.Equal(t, child.Name(), "child")
	want := "renamed"
	if tc.lock != "" {
		want = "child"
	}
	assert.FatalIf(t, dep.Name != want, "name=%q, want %q", dep.Name, want)
}

func TestDescription_RecaptureClearsStaleCoordinates(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"parent/Chart.yaml": {
			Data: []byte(`apiVersion: v2
name: parent
version: 1.0.0
dependencies:
- name: child
  version: 1.0.0
  repository: file://../child
`),
		},
		"child/Chart.yaml": {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
	}
	initial, _, err := chartinput.Describe(t.Context(), input, chartinput.Options{Name: "parent"}, nil)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name, parent, repository, path string
		err                            error
	}{
		{name: "outside", parent: ".", err: fs.ErrInvalid},
		{name: "malformed_url", parent: "parent", repository: "file://%zz/child"},
		{name: "missing", parent: "moved/parent", path: "moved/child", err: fs.ErrNotExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original := initial
			original.Dependencies = slices.Clone(initial.Dependencies)
			if tc.repository != "" {
				original.Dependencies[0].Declaration.Repository = tc.repository
			}
			recaptured, err := original.Complete(t.Context(), input, tc.parent)
			assert.FatalIf(t, err == nil, "recapture succeeded for %s", tc.name)
			if tc.err != nil {
				assert.Equal(t, errors.Is(err, tc.err), true)
			}
			assert.Equal(t, recaptured.Dependencies[0].Path, tc.path)
			assert.Equal(t, recaptured.Dependencies[0].Capture.Empty(), true)
			assert.Equal(t, original.Dependencies[0].Path, "child")
			assert.Equal(t, original.Dependencies[0].Capture.Empty(), false)
		})
	}
}

func TestDescribe_EmptyRepositoryMissing(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"parent/Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n",
			),
		},
	}
	description, missing, err := chartinput.Describe(
		t.Context(),
		input,
		chartinput.Options{Name: "parent"},
		nil,
	)
	if !errors.Is(err, fs.ErrNotExist) || missing {
		t.Fatalf("missing=%v error=%v", missing, err)
	}
	if len(description.Dependencies) != 1 || description.Dependencies[0].Path != "parent/charts/child" {
		t.Fatalf("dependencies=%+v", description.Dependencies)
	}
}

func TestDescribe_PartialInputs(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"parent/Chart.yaml": {
			Data: []byte(
				"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: absent\n  version: 1.0.0\n  repository: file://../absent\n- name: remote\n  version: 1.0.0\n  repository: https://repo.example\n",
			),
		},
	}
	description, _, err := chartinput.Describe(
		t.Context(),
		input,
		chartinput.Options{Name: "parent"},
		nil,
	)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("error=%v", err)
	}
	if len(description.Dependencies) != 2 ||
		description.Dependencies[1].Declaration.Repository != "https://repo.example" {
		t.Fatalf("dependencies=%+v", description.Dependencies)
	}
}
