package cache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/tree"
)

func compressedForTest(t *testing.T, files fstest.MapFS) []byte {
	t.Helper()
	var data bytes.Buffer
	gz := gzip.NewWriter(&data)
	w := tar.NewWriter(gz)
	{
		err := w.AddFS(files)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := w.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := gz.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	return data.Bytes()
}

func TestEvaluate(t *testing.T) {
	t.Parallel()
	archive := compressedForTest(
		t,
		fstest.MapFS{"member.yaml": {Data: []byte("inside"), Mode: 0o755}, "empty": {Mode: fs.ModeDir | 0o755}},
	)
	for _, tc := range []struct {
		name       string
		operations []Copy
		want       map[string]string
		modes      map[string]fs.FileMode
		invalid    bool
	}{
		{"file_rename", []Copy{{From: "app/file", To: "renamed"}}, map[string]string{"renamed": "file"}, nil, false},
		{"file_slash_target", []Copy{{From: "app/file", To: "out/"}}, map[string]string{"out/file": "file"}, nil, false},
		{"file_directory_target", []Copy{{From: "empty", To: "out"}, {From: "app/file", To: "out/empty"}}, map[string]string{"out/empty/file": "file"}, nil, false},
		{"directory_nesting", []Copy{{From: "app/", To: "app/"}}, map[string]string{"app/app/file": "file", "app/app/deep/nested": "nested"}, nil, false},
		{"ordinary_double_star", []Copy{{From: "app/**", To: "out"}}, map[string]string{"out/file": "file", "out/deep/nested": "nested"}, nil, false},
		{"ordinary_glob_coordinates", []Copy{{From: "app/*/nested", To: "out"}}, map[string]string{"out/app/deep/nested": "nested"}, nil, false},
		{"ordinary_double_star_not_recursive", []Copy{{From: "**/nested", To: "out"}}, nil, nil, true},
		{"exclude_root_coordinates", []Copy{{From: "app", To: ".", Exclude: []string{"app/deep"}}}, map[string]string{"app/file": "file"}, nil, false},
		{"exclude_prefix_coordinates", []Copy{{From: "app/**", To: "out", Exclude: []string{"deep/nested"}}}, map[string]string{"out/file": "file"}, nil, false},
		{"exclude_basename", []Copy{{From: "app", To: ".", Exclude: []string{"nested"}}}, map[string]string{"app/file": "file"}, nil, false},
		{"direct_excluded_succeeds_empty", []Copy{{From: "app/file", To: "file", Exclude: []string{"file"}}}, map[string]string{}, nil, false},
		{"all_glob_matches_excluded", []Copy{{From: "app/*", To: ".", Exclude: []string{"*"}}}, nil, nil, true},
		{"no_glob_matches", []Copy{{From: "absent*", To: "."}}, nil, nil, true},
		{"source_absolute", []Copy{{From: "/file", To: "file"}}, nil, nil, true},
		{"source_parent", []Copy{{From: "app/../file", To: "file"}}, nil, nil, true},
		{"source_backslash", []Copy{{From: `app\file`, To: "file"}}, nil, nil, true},
		{"destination_absolute", []Copy{{From: "app/file", To: "/file"}}, nil, nil, true},
		{"destination_parent", []Copy{{From: "app/file", To: "app/../file"}}, nil, nil, true},
		{"copy_rejects_link", []Copy{{From: "link", To: "link"}}, nil, nil, true},
		{"merge_new_destination_plain_copy", []Copy{{From: "invalid.yaml", To: "values.yaml", Mode: MergeYAML}}, map[string]string{"values.yaml": "[invalid"}, nil, false},
		{"merge_directory_plain_copy", []Copy{{From: "base.yaml", To: "merge/invalid.yaml"}, {From: "merge", To: ".", Mode: MergeYAML}}, map[string]string{"merge/invalid.yaml": "[invalid"}, nil, false},
		{"merge_selected_file", []Copy{{From: "base.yaml", To: "values.yaml"}, {From: "overlay.yaml", To: "values.yaml", Mode: MergeYAML}}, map[string]string{"values.yaml": "array:\n- new\nfolded: kept\nmap: scalar\nnullable: null\nretained: true\n"}, map[string]fs.FileMode{"values.yaml": 0o755}, false},
		{"merge_invalid_existing", []Copy{{From: "invalid.yaml", To: "values.yaml"}, {From: "overlay.yaml", To: "values.yaml", Mode: MergeYAML}}, nil, nil, true},
		{"extract_recursive_and_case", []Copy{{From: "**/*.TGZ", To: "out", Mode: ExtractArchive}}, map[string]string{"out/member.yaml": "inside"}, map[string]fs.FileMode{"out/member.yaml": 0o755, "out/empty": fs.ModeDir | 0o755}, false},
		{"extract_tar_gz", []Copy{{From: "archive.tar.gz", To: "out", Mode: ExtractArchive}}, map[string]string{"out/member.yaml": "inside"}, nil, false},
		{"extract_preserves_existing_mode", []Copy{{From: "plain", To: "out/member.yaml"}, {From: "archive.TGZ", To: "out", Mode: ExtractArchive}}, map[string]string{"out/member.yaml": "inside"}, map[string]fs.FileMode{"out/member.yaml": 0o600}, false},
		{"extract_skips_nontars", []Copy{{From: "*", To: "out", Mode: ExtractArchive}}, map[string]string{"out/member.yaml": "inside"}, nil, false},
		{"extract_excludes_archives_only", []Copy{{From: "*.TGZ", To: "out", Mode: ExtractArchive, Exclude: []string{"member.yaml"}}}, map[string]string{"out/member.yaml": "inside"}, nil, false},
		{"extract_direct_nontar", []Copy{{From: "archive.txt", To: ".", Mode: ExtractArchive}}, nil, nil, true},
		{"extract_direct_directory", []Copy{{From: "app", To: ".", Mode: ExtractArchive}}, nil, nil, true},
		{"extract_only_nontar_glob", []Copy{{From: "app/*", To: ".", Mode: ExtractArchive}}, map[string]string{}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEvaluateCase(t, testEvaluateCaseInputs{archive: &archive, tc: &tc})
		},
		)
	}
}

func TestEvaluate_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	e := composition{
		request: Composition{Inputs: []CompositionInput{{FS: fstest.MapFS{"file": {Data: []byte("file")}}}}},
	}
	{
		err := e.apply(
			ctx,
			tree.NewBuilder(nil),
			&Copy{From: "file", To: "file"},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "evaluate() = %v", err)
	}
}

func TestCopyFile_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &compositionObservedFS{FS: fstest.MapFS{"file": {Data: []byte("a: 1\n---\na: 2\n")}}}
	builder := tree.NewBuilder(nil)
	{
		err := builder.AddFile("existing", []byte("a: 0\n"), 0o600)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := copyFile(
			ctx,
			input,
			builder,
			&Copy{From: "file", To: "existing", Mode: MergeYAML},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "copyFile() = %v", err)
	}
	assert.FatalIf(t, input.opens.Load() != 0, "%v", "canceled copy opened input")
	{
		data, err := fs.ReadFile(builder, "existing")
		assert.FatalIf(
			t,
			err != nil || string(data) != "a: 0\n",
			"canceled merge changed destination: %q, %v",
			data,
			err,
		)
	}
	{
		_, err := loadYAML(ctx, input, "file")
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "loadYAML() = %v", err)
	}
}

type testEvaluateCaseInputs struct {
	archive *[]byte
	tc      *struct {
		name string

		operations []Copy
		want       map[string]string
		modes      map[string]fs.FileMode
		invalid    bool
	}
}

func testEvaluateCase(t *testing.T, testCaseInput testEvaluateCaseInputs) {
	t.Helper()

	owner := cachetest.New(t, "", New)
	e := composition{
		owner:  owner,
		maxAge: Forever,
		request: Composition{Inputs: []CompositionInput{{FS: fstest.MapFS{
			"app/file": {
				Data: []byte("file"),
			},
			"app/deep/nested": {Data: []byte("nested")},
			"empty":           {Mode: fs.ModeDir | 0o755},
			"plain":           {Data: []byte("plain"), Mode: 0o600},
			"base.yaml": {
				Data: []byte("array: [old]\nmap: {key: value}\nnullable: present\nretained: yes\n"),
				Mode: 0o755,
			},
			"overlay.yaml": {
				Data: []byte(
					"array: [discarded]\nfolded: kept\n---\narray: [new]\nmap: scalar\nnullable: null\n",
				),
			},
			"invalid.yaml":       {Data: []byte("[invalid")},
			"merge/invalid.yaml": {Data: []byte("[invalid")},
			"archive.TGZ": {
				Data: (*testCaseInput.archive),
			},
			"archive.tar.gz":   {Data: (*testCaseInput.archive)},
			"archive.txt":      {Data: (*testCaseInput.archive)},
			"deep/archive.TGZ": {Data: (*testCaseInput.archive)},
		}}}},
	}
	if testCaseInput.tc.name == "copy_rejects_link" {
		e.request.Inputs[0].FS.(fstest.MapFS)["link"] = &fstest.MapFile{Mode: fs.ModeSymlink}
	}
	builder := tree.NewBuilder(nil)
	var err error
	for _, op := range testCaseInput.tc.operations {
		if op.Mode == ExtractArchive {
			op.ExtractContent = func(name, destination string) any {
				return struct{ Path, Destination string }{name, destination}
			}
		}
		if err = e.apply(t.Context(), builder, &op); err != nil {
			break
		}
	}
	assert.FatalIf(
		t,
		(err != nil) != testCaseInput.tc.invalid,
		"evaluate() = %v; invalid=%v",
		err,
		testCaseInput.tc.invalid,
	)
	if testCaseInput.tc.invalid {
		return
	}
	got := map[string]string{}
	{
		err := fs.WalkDir(builder, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			data, err := fs.ReadFile(builder, name)
			got[name] = string(data)
			return err
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		diff := cmp.Diff(testCaseInput.tc.want, got)
		assert.FatalIf(t, diff != "", "%v", diff)
	}
	for name, mode := range testCaseInput.tc.modes {
		info, err := fs.Stat(builder, name)
		assert.FatalIf(
			t,
			err != nil || info.Mode() != mode,
			"%s mode = %v, %v; want %v",
			name,
			info,
			err,
			mode,
		)
	}
}

type compositionObservedFS struct {
	fs.FS

	opens atomic.Int64
}

func (f *compositionObservedFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}
