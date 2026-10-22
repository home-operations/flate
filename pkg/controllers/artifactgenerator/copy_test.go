package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func compressedForTest(t *testing.T, files fstest.MapFS) []byte {
	t.Helper()
	var data bytes.Buffer
	gz := gzip.NewWriter(&data)
	w := tar.NewWriter(gz)
	if err := w.AddFS(files); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func TestEvaluate(t *testing.T) {
	t.Parallel()
	archive := compressedForTest(t, fstest.MapFS{"member.yaml": {Data: []byte("inside"), Mode: 0o755}, "empty": {Mode: fs.ModeDir | 0o755}})
	for _, tc := range []struct {
		name       string
		operations []operation
		want       map[string]string
		modes      map[string]fs.FileMode
		invalid    bool
	}{
		{"file_rename", []operation{{From: "@repo/app/file", To: "@artifact/renamed"}}, map[string]string{"renamed": "file"}, nil, false},
		{"file_slash_target", []operation{{From: "@repo/app/file", To: "@artifact/out/"}}, map[string]string{"out/file": "file"}, nil, false},
		{"file_directory_target", []operation{{From: "@repo/empty", To: "@artifact/out"}, {From: "@repo/app/file", To: "@artifact/out/empty"}}, map[string]string{"out/empty/file": "file"}, nil, false},
		{"directory_nesting", []operation{{From: "@repo/app/", To: "@artifact/app/"}}, map[string]string{"app/app/file": "file", "app/app/deep/nested": "nested"}, nil, false},
		{"ordinary_double_star", []operation{{From: "@repo/app/**", To: "@artifact/out"}}, map[string]string{"out/file": "file", "out/deep/nested": "nested"}, nil, false},
		{"ordinary_glob_coordinates", []operation{{From: "@repo/app/*/nested", To: "@artifact/out"}}, map[string]string{"out/app/deep/nested": "nested"}, nil, false},
		{"ordinary_double_star_not_recursive", []operation{{From: "@repo/**/nested", To: "@artifact/out"}}, nil, nil, true},
		{"exclude_root_coordinates", []operation{{From: "@repo/app", To: "@artifact/", Exclude: []string{"app/deep"}}}, map[string]string{"app/file": "file"}, nil, false},
		{"exclude_prefix_coordinates", []operation{{From: "@repo/app/**", To: "@artifact/out", Exclude: []string{"deep/nested"}}}, map[string]string{"out/file": "file"}, nil, false},
		{"exclude_basename", []operation{{From: "@repo/app", To: "@artifact/", Exclude: []string{"nested"}}}, map[string]string{"app/file": "file"}, nil, false},
		{"direct_excluded_succeeds_empty", []operation{{From: "@repo/app/file", To: "@artifact/file", Exclude: []string{"file"}}}, map[string]string{}, nil, false},
		{"all_glob_matches_excluded", []operation{{From: "@repo/app/*", To: "@artifact/", Exclude: []string{"*"}}}, nil, nil, true},
		{"no_glob_matches", []operation{{From: "@repo/absent*", To: "@artifact/"}}, nil, nil, true},
		{"unknown_alias", []operation{{From: "@absent/file", To: "@artifact/file"}}, nil, nil, true},
		{"source_absolute", []operation{{From: "@repo//file", To: "@artifact/file"}}, nil, nil, true},
		{"source_parent", []operation{{From: "@repo/app/../file", To: "@artifact/file"}}, nil, nil, true},
		{"source_backslash", []operation{{From: `@repo/app\file`, To: "@artifact/file"}}, nil, nil, true},
		{"destination_absolute", []operation{{From: "@repo/app/file", To: "@artifact//file"}}, nil, nil, true},
		{"destination_parent", []operation{{From: "@repo/app/file", To: "@artifact/app/../file"}}, nil, nil, true},
		{"destination_prefix", []operation{{From: "@repo/app/file", To: "file"}}, nil, nil, true},
		{"copy_rejects_link", []operation{{From: "@repo/link", To: "@artifact/link"}}, nil, nil, true},
		{"merge_new_destination_plain_copy", []operation{{From: "@repo/invalid.yaml", To: "@artifact/values.yaml", Strategy: "Merge"}}, map[string]string{"values.yaml": "[invalid"}, nil, false},
		{"merge_directory_plain_copy", []operation{{From: "@repo/base.yaml", To: "@artifact/merge/invalid.yaml"}, {From: "@repo/merge", To: "@artifact/", Strategy: "Merge"}}, map[string]string{"merge/invalid.yaml": "[invalid"}, nil, false},
		{"merge_selected_file", []operation{{From: "@repo/base.yaml", To: "@artifact/values.yaml"}, {From: "@repo/overlay.yaml", To: "@artifact/values.yaml", Strategy: "Merge"}}, map[string]string{"values.yaml": "array:\n- new\nfolded: kept\nmap: scalar\nnullable: null\nretained: true\n"}, map[string]fs.FileMode{"values.yaml": 0o755}, false},
		{"merge_invalid_existing", []operation{{From: "@repo/invalid.yaml", To: "@artifact/values.yaml"}, {From: "@repo/overlay.yaml", To: "@artifact/values.yaml", Strategy: "Merge"}}, nil, nil, true},
		{"extract_recursive_and_case", []operation{{From: "@repo/**/*.TGZ", To: "@artifact/out", Strategy: "Extract"}}, map[string]string{"out/member.yaml": "inside"}, map[string]fs.FileMode{"out/member.yaml": 0o755, "out/empty": fs.ModeDir | 0o755}, false},
		{"extract_tar_gz", []operation{{From: "@repo/archive.tar.gz", To: "@artifact/out", Strategy: "Extract"}}, map[string]string{"out/member.yaml": "inside"}, nil, false},
		{"extract_preserves_existing_mode", []operation{{From: "@repo/plain", To: "@artifact/out/member.yaml"}, {From: "@repo/archive.TGZ", To: "@artifact/out", Strategy: "Extract"}}, map[string]string{"out/member.yaml": "inside"}, map[string]fs.FileMode{"out/member.yaml": 0o600}, false},
		{"extract_skips_nontars", []operation{{From: "@repo/*", To: "@artifact/out", Strategy: "Extract"}}, map[string]string{"out/member.yaml": "inside"}, nil, false},
		{"extract_excludes_archives_only", []operation{{From: "@repo/*.TGZ", To: "@artifact/out", Strategy: "Extract", Exclude: []string{"member.yaml"}}}, map[string]string{"out/member.yaml": "inside"}, nil, false},
		{"extract_direct_nontar", []operation{{From: "@repo/archive.txt", To: "@artifact/", Strategy: "Extract"}}, nil, nil, true},
		{"extract_direct_directory", []operation{{From: "@repo/app", To: "@artifact/", Strategy: "Extract"}}, nil, nil, true},
		{"extract_only_nontar_glob", []operation{{From: "@repo/app/*", To: "@artifact/", Strategy: "Extract"}}, map[string]string{}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", cache.New)
			e := evaluation{owner: owner, maxAge: cache.Forever, inputs: map[string]*store.SourceArtifact{"repo": {FS: fstest.MapFS{
				"app/file": {Data: []byte("file")}, "app/deep/nested": {Data: []byte("nested")}, "empty": {Mode: fs.ModeDir | 0o755}, "plain": {Data: []byte("plain"), Mode: 0o600},
				"base.yaml":    {Data: []byte("array: [old]\nmap: {key: value}\nnullable: present\nretained: yes\n"), Mode: 0o755},
				"overlay.yaml": {Data: []byte("array: [discarded]\nfolded: kept\n---\narray: [new]\nmap: scalar\nnullable: null\n")},
				"invalid.yaml": {Data: []byte("[invalid")}, "merge/invalid.yaml": {Data: []byte("[invalid")},
				"archive.TGZ": {Data: archive}, "archive.tar.gz": {Data: archive}, "archive.txt": {Data: archive}, "deep/archive.TGZ": {Data: archive},
			}}}}
			if tc.name == "copy_rejects_link" {
				e.inputs["repo"].FS.(fstest.MapFS)["link"] = &fstest.MapFile{Mode: fs.ModeSymlink}
			}
			builder := tree.NewBuilder(nil)
			var err error
			for _, op := range tc.operations {
				if err = e.apply(t.Context(), builder, op); err != nil {
					break
				}
			}
			if (err != nil) != tc.invalid {
				t.Fatalf("evaluate() = %v; invalid=%v", err, tc.invalid)
			}
			if tc.invalid {
				return
			}
			got := map[string]string{}
			if err := fs.WalkDir(builder, ".", func(name string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				data, err := fs.ReadFile(builder, name)
				got[name] = string(data)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatal(diff)
			}
			for name, mode := range tc.modes {
				info, err := fs.Stat(builder, name)
				if err != nil || info.Mode() != mode {
					t.Fatalf("%s mode = %v, %v; want %v", name, info, err, mode)
				}
			}
		})
	}
}

func TestEvaluate_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	e := evaluation{inputs: map[string]*store.SourceArtifact{"repo": {FS: fstest.MapFS{"file": {Data: []byte("file")}}}}}
	if err := e.apply(ctx, tree.NewBuilder(nil), operation{From: "@repo/file", To: "@artifact/file"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("evaluate() = %v", err)
	}
}

func TestCopyFile_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &observedFS{FS: fstest.MapFS{"file": {Data: []byte("a: 1\n---\na: 2\n")}}}
	builder := tree.NewBuilder(nil)
	if err := builder.AddFile("existing", []byte("a: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(ctx, input, builder, operation{From: "file", To: "existing", Strategy: "Merge"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("copyFile() = %v", err)
	}
	if input.opens.Load() != 0 {
		t.Fatal("canceled copy opened input")
	}
	if data, err := fs.ReadFile(builder, "existing"); err != nil || string(data) != "a: 0\n" {
		t.Fatalf("canceled merge changed destination: %q, %v", data, err)
	}
	if _, err := loadYAML(ctx, input, "file"); !errors.Is(err, context.Canceled) {
		t.Fatalf("loadYAML() = %v", err)
	}
}

func TestEvaluate_ExtractIdentity(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", cache.New)
	identities := []inputIdentity{{Alias: "repo", Key: "family", Digest: "generation"}}
	e := evaluation{owner: owner, maxAge: cache.Forever, identity: identities, inputs: map[string]*store.SourceArtifact{"repo": {FS: fstest.MapFS{"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte("bytes")}})}}}}}
	selection := archiveSelection{Alias: "repo", Path: "archive.tgz", Destination: "out"}
	if err := e.extract(t.Context(), tree.NewBuilder(nil), selection); err != nil {
		t.Fatal(err)
	}
	key := cache.ArchiveKey{Policy: "extract", Content: struct {
		Namespace string
		Inputs    []inputIdentity
		Selection archiveSelection
	}{"artifactgenerator/extract/v1", identities, selection}}
	called := false
	input, _, id, err := owner.Acquire(t.Context(), key, cache.Policy{MaxAge: cache.Forever}, func(context.Context, cache.ProducerInput) (cache.ProducerResult, error) {
		called = true
		return cache.ProducerResult{Descriptor: ocispec.Descriptor{}}, errors.New("unexpected extraction")
	})
	if err != nil || input == nil || id == "" || called {
		t.Fatalf("nested extraction identity = %v, %q, %v; produced=%v", input, id, err, called)
	}
}
