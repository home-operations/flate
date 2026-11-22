package cache

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompose_InvalidOperation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		op        Copy
		missingFS bool
	}{
		{name: "extract requires a tarball", op: Copy{From: "file", To: "output", Mode: ExtractArchive}},
		{name: "negative index", op: Copy{Input: -1}},
		{name: "index past inputs", op: Copy{Input: 1}},
		{name: "missing filesystem", missingFS: true},
		{name: "unsupported mode", op: Copy{Mode: CopyMode(255)}},
		{name: "absolute source", op: Copy{From: "/file"}},
		{name: "source traversal", op: Copy{From: "dir/../file"}},
		{name: "source backslash", op: Copy{From: `dir\file`}},
		{name: "absolute destination", op: Copy{From: "file", To: "/file"}},
		{name: "destination traversal", op: Copy{From: "file", To: "dir/../file"}},
		{name: "destination backslash", op: Copy{From: "file", To: `dir\file`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", New)
			input := CompositionInput{FS: fstest.MapFS{"file": {Data: []byte("input")}}, Content: "generation"}
			if tc.missingFS {
				input.FS = nil
			}
			output, descriptor, key, err := owner.Compose(t.Context(), Composition{
				Inputs: []CompositionInput{input}, Copies: []Copy{tc.op}, Policy: "fixed",
			}, &Policy{MaxAge: Forever})
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, output == nil, true)
			assert.Equal(t, key, "")
			assert.Equal(t, descriptor.Digest.String(), "")
		})
	}
}

func TestCompose_IdentityIncludesAllInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		input int
	}{
		{"used generation", 0}, {"unused declared generation", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", New)
			var previousKey string
			for run := range 2 {
				generations := []string{"first", "unused"}
				if run == 1 {
					generations[tc.input] = "changed"
				}
				request := Composition{
					Inputs: []CompositionInput{
						{FS: fstest.MapFS{"file": {Data: []byte("bytes"), Mode: 0o640}}, Content: generations[0]},
						{FS: fstest.MapFS{"unused": {}}, Content: generations[1]},
					}, Copies: []Copy{{From: "file", To: "out"}}, Policy: "fixed",
				}
				output, descriptor, key, err := owner.Compose(t.Context(), request, &Policy{MaxAge: Forever})
				assert.NoError(t, err)
				assert.Diff(
					t,
					compositionFiles(t, output),
					map[string]compositionFile{"out": {Data: "bytes", Mode: 0o600}},
				)
				assert.Equal(t, key == previousKey, false)
				previousKey = key
				reused, reusedDescriptor, reusedKey, err := owner.Acquire(t.Context(),
					request.key(),
					&Policy{MaxAge: Forever}, unexpectedComposition)
				assert.NoError(t, err)
				assert.Equal(t, reusedKey, key)
				assert.Diff(t, reusedDescriptor, descriptor)
				assert.Diff(t, compositionFiles(t, reused), compositionFiles(t, output))
			}
		})
	}
}

type compositionFile struct {
	Data string
	Mode fs.FileMode
}

func compositionFiles(t *testing.T, input fs.FS) map[string]compositionFile {
	t.Helper()
	result := map[string]compositionFile{}
	err := fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := compositionFile{Mode: info.Mode()}
		if !entry.IsDir() {
			data, err := fs.ReadFile(input, name)
			if err != nil {
				return err
			}
			value.Data = string(data)
		}
		result[name] = value
		return nil
	})
	assert.NoError(t, err)
	return result
}

type compositionReadFS struct {
	fs.FS

	read func(string) ([]byte, error)
}

func (f compositionReadFS) ReadFile(name string) ([]byte, error) { return f.read(name) }

func assertCompositionFailure(t *testing.T, key string, err, cause error) {
	t.Helper()
	assert.Equal(t, errors.Is(err, cause), true)
	assert.Equal(t, key, "")
}

func unexpectedComposition(context.Context, ProducerInput) (ProducerResult, error) {
	return ProducerResult{}, errors.New("unexpected production")
}
