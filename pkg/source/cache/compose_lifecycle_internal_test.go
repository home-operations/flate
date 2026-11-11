package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompose_PublishedOutputOwnsBacking(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"memory", "disk"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backend == "disk" {
				root = t.TempDir()
			}
			owner := cachetest.New(t, root, New)
			backing := t.TempDir()
			testutil.WriteFile(t, backing, "nested/file", "borrowed")
			input := os.DirFS(backing)
			before := compositionFiles(t, input)
			output, _, _, err := owner.Compose(t.Context(), Composition{
				Content: "owned output", Inputs: []CompositionInput{{FS: input, Content: "generation"}},
				Copies: []Copy{{From: "nested", To: "."}}, Policy: "fixed",
			}, &Policy{MaxAge: Forever})
			assert.NoError(t, err)
			assert.Diff(t, compositionFiles(t, input), before)
			assert.NoError(t, os.RemoveAll(backing))
			assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{
				"nested": {Mode: fs.ModeDir | 0o750}, "nested/file": {Data: "borrowed", Mode: 0o600},
			})
		})
	}
}

func TestCompose_CancellationDoesNotPublish(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"before planning", "between mixed operations"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", New)
			files := fstest.MapFS{
				"base":        {Data: []byte("base: true\n"), Mode: 0o640},
				"overlay":     {Data: []byte("overlay: true\n"), Mode: 0o600},
				"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"member": {Data: []byte("archive")}})},
			}
			before := compositionFiles(t, files)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			observed := &compositionObservedFS{FS: files}
			input := compositionReadFS{FS: observed, read: func(name string) ([]byte, error) {
				reads++
				return fs.ReadFile(files, name)
			}}
			request := Composition{
				Content: stage, Inputs: []CompositionInput{{FS: input, Content: "generation"}}, Policy: "fixed",
				Copies: []Copy{
					{From: "base", To: "values"},
					{From: "overlay", To: "values", Mode: MergeYAML},
					{
						From: "archive.tgz", To: "out", Mode: ExtractArchive,
						ExtractContent: func(string, string) any {
							cancel()
							return "extract"
						},
					},
				},
			}
			if stage == "before planning" {
				cancel()
			}
			output, descriptor, key, err := owner.Compose(ctx, request, &Policy{MaxAge: Forever})
			assert.Equal(t, output == nil, true)
			assertCompositionFailure(t, key, err, context.Canceled)
			assert.Diff(t, descriptor, ocispec.Descriptor{})
			wantReads := 0
			if stage == "between mixed operations" {
				wantReads = 2
			}
			assert.Equal(t, reads, wantReads)
			if stage == "before planning" {
				assert.Equal(t, observed.opens.Load(), int64(0))
			}
			assert.Diff(t, compositionFiles(t, files), before)
			_, _, _, err = owner.Acquire(t.Context(), ArchiveKey{Content: stage, Policy: "fixed"},
				&Policy{MaxAge: Forever}, unexpectedComposition)
			assert.Equal(t, err != nil, true)
		})
	}
}

func TestCompose_FailurePreservesOtherGeneration(t *testing.T) {
	t.Parallel()
	for _, backend := range []string{"memory", "disk"} {
		t.Run(backend, func(t *testing.T) {
			t.Parallel()
			root := ""
			if backend == "disk" {
				root = t.TempDir()
			}
			owner := cachetest.New(t, root, New)
			files := fstest.MapFS{"file": {Data: []byte("complete"), Mode: 0o640}}
			before := compositionFiles(t, files)
			request := Composition{
				Content: "stable family", Inputs: []CompositionInput{{FS: files, Content: ""}},
				Copies: []Copy{{From: "file", To: "out"}}, Policy: "fixed",
			}
			previous, descriptor, previousKey, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
			assert.NoError(t, err)
			request.Content = "failed generation"
			request.Copies = append(request.Copies, Copy{From: "missing", To: "partial"})
			output, failedDescriptor, key, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
			assert.Equal(t, output == nil, true)
			assertCompositionFailure(t, key, err, fs.ErrNotExist)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			domain, ok := errors.AsType[*manifest.Error](err)
			assert.FatalIf(t, !ok, "unclassified composition failure: %v", err)
			assert.Equal(t, domain.Error(), "stat: open missing: file does not exist")
			assert.Equal(
				t,
				domain.Cause().Error(),
				"copy 1 \"missing\" to \"partial\": stat: open missing: file does not exist",
			)
			assert.Diff(t, failedDescriptor, ocispec.Descriptor{})
			assert.Diff(t, compositionFiles(t, files), before)
			assert.Diff(
				t,
				compositionFiles(t, previous),
				map[string]compositionFile{"out": {Data: "complete", Mode: 0o600}},
			)
			reused, reusedDescriptor, reusedKey, err := owner.Acquire(t.Context(),
				ArchiveKey{Content: "stable family", Policy: "fixed"}, &Policy{MaxAge: Forever}, unexpectedComposition)
			assert.NoError(t, err)
			assert.Equal(t, reusedKey, previousKey)
			assert.Diff(t, reusedDescriptor, descriptor)
			assert.Diff(t, compositionFiles(t, reused), compositionFiles(t, previous))
		})
	}
}

func TestCompose_MemoryAndDiskAgree(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"base":    {Data: []byte("base: true\n"), Mode: 0o640},
		"overlay": {Data: []byte("overlay: true\n"), Mode: 0o600},
		"skip":    {Data: []byte("excluded"), Mode: 0o600},
		"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{
			"member": {Data: []byte("archive"), Mode: 0o750}, "empty": {Mode: fs.ModeDir | 0o755},
		})},
	}
	before := compositionFiles(t, files)
	request := Composition{
		Content: "backend agreement", Inputs: []CompositionInput{{FS: files, Content: "generation"}},
		Copies: []Copy{
			{From: "base", To: "values"},
			{From: "overlay", To: "values", Mode: MergeYAML},
			{From: "skip", To: "skip", Exclude: []string{"skip"}},
			{
				From: "archive.tgz", To: "out", Mode: ExtractArchive,
				ExtractContent: func(name, destination string) any { return []string{"nested", name, destination} },
			},
		}, Policy: "fixed",
	}
	want := map[string]compositionFile{
		"values": {Data: "base: true\noverlay: true\n", Mode: 0o600},
		"out":    {Mode: fs.ModeDir | 0o750}, "out/empty": {Mode: fs.ModeDir | 0o750},
		"out/member": {Data: "archive", Mode: 0o700},
	}
	expected := fstest.MapFS{
		"values": {Data: []byte("base: true\noverlay: true\n"), Mode: 0o640},
		"out":    {Mode: fs.ModeDir | 0o755}, "out/empty": {Mode: fs.ModeDir | 0o755},
		"out/member": {Data: []byte("archive"), Mode: 0o750},
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	assert.NoError(t, writer.AddFS(expected))
	assert.NoError(t, writer.Close())
	expectedIdentity := manifest.Fingerprint(struct {
		Digest         digest.Digest
		Ignore, Policy string
		Version        int
	}{digest.FromBytes(archive.Bytes()), "", "fixed", 1})
	expectedTree := "sha256:" + manifest.Fingerprint([]struct {
		Path    string
		Mode    fs.FileMode
		Size    int64
		Content string
	}{
		{"out", fs.ModeDir | 0o750, 0, ""},
		{"out/empty", fs.ModeDir | 0o750, 0, ""},
		{"out/member", 0o700, 7, manifest.Fingerprint([]byte("archive"))},
		{
			"values",
			0o600,
			int64(len("base: true\noverlay: true\n")),
			manifest.Fingerprint([]byte("base: true\noverlay: true\n")),
		},
	})
	var previousDescriptor ocispec.Descriptor
	var previousKey, previousIdentity string
	for _, backend := range []string{"memory", "disk"} {
		root := ""
		if backend == "disk" {
			root = t.TempDir()
		}
		for run := range 2 {
			owner, err := New(t.Context(), root)
			assert.NoError(t, err)
			policy := Policy{MaxAge: Forever}
			if backend == "disk" && run == 1 {
				policy.Admit = func(context.Context) (func(), error) { return nil, errors.New("reopen must reuse") }
			}
			output, descriptor, key, err := owner.Compose(t.Context(), request, &policy)
			assert.NoError(t, err)
			assert.Diff(t, compositionFiles(t, output), want)
			assert.Diff(t, compositionFiles(t, files), before)
			assert.Diff(
				t,
				descriptor,
				ocispec.Descriptor{Annotations: map[string]string{"flate.tree-digest": expectedTree}},
			)
			identity := AcquisitionIdentity(output)
			assert.Equal(t, identity, expectedIdentity)
			assert.Equal(t, identity != "", true)
			if previousKey != "" {
				assert.Diff(t, descriptor, previousDescriptor)
				assert.Equal(t, key, previousKey)
				assert.Equal(t, identity, previousIdentity)
			}
			previousDescriptor, previousKey, previousIdentity = descriptor, key, identity
			assert.NoError(t, owner.Close())
		}
	}
}
