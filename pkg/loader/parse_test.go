package loader

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

	"github.com/go-git/go-git/v5/plumbing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestParseFile_ResultOwnership(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		cached  bool
		header  bool
		invalid bool
	}{
		{name: "Uncached"},
		{name: "CachedMissAndHit", cached: true},
		{name: "CachedDocumentFields", cached: true, header: true},
		{name: "UncachedPartialError", invalid: true},
		{name: "CachedPartialError", cached: true, invalid: true},
		{name: "CachedDocumentFieldsOnError", cached: true, header: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			path := filepath.Join(root, "objects.yaml")
			body := `apiVersion: v1
kind: ConfigMap
metadata: {name: first, namespace: apps}
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: apps}
spec: {target: {name: generated}}
---
`
			if tc.invalid {
				body += `apiVersion: source.toolkit.fluxcd.io/
kind: GitRepository
metadata: {name: invalid, namespace: apps}
---
`
			} else {
				body += `apiVersion: v1
kind: ConfigMap
metadata: {name: "${APP}", namespace: apps}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: template-namespace, namespace: "${NAMESPACE}"}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {namespace: apps}
---
`
			}
			body += "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: last, namespace: apps}\n"
			testutil.WriteFile(t, root, "objects.yaml", body)
			r := readerForTest(root)
			if !tc.cached {
				r.cache = nil
			}
			var header document
			if tc.header {
				header = document{body: &types.Kustomization{Namespace: "retained"}, file: "header.yaml", loaded: true}
				r.cache[path] = header
			}
			want := []string{"first", "last"}
			assertNames := func(objects []store.Child, names []string) {
				t.Helper()
				got := make([]string, 0, len(objects))
				for _, child := range objects {
					obj := child.Obj
					if obj == nil {
						t.Fatal("nil object in parse result")
					}
					got = append(got, obj.Named().Name)
				}
				assert.Diff(t, got, names)
			}
			first, err := r.parseFile(t.Context(), path, manifest.ParseDocOptions{})
			assertNames(first, want)
			assert.NoError(t, err)
			slices.Reverse(first)
			first[0] = store.Child{}
			second, err := r.parseFile(t.Context(), path, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assertNames(second, want)
			slices.Reverse(second)
			second[0] = store.Child{}
			third, err := r.parseFile(t.Context(), path, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assertNames(third, want)
			if tc.cached {
				cached := r.cache[path]
				var children []store.Child
				for _, child := range cached.objects {
					if child.outcome.Err == nil {
						children = append(children, child.Child)
					}
				}
				assertNames(children, []string{"first", "producer", "last"})
				var failure error
				for _, child := range cached.objects {
					if child.outcome.Err != nil {
						failure = child.outcome.Err
						if child.Obj != nil {
							t.Fatal("failed document retained an object")
						}
					}
				}
				if !errors.Is(failure, manifest.ErrInput) || !errors.Is(failure, manifest.ErrFlux) {
					t.Fatalf("retained failure = %v", failure)
				}
				if tc.invalid {
					invalid, ok := errors.AsType[*field.Error](failure)
					if !ok || invalid.Field != "apiVersion" {
						t.Fatalf("retained API error = %v", failure)
					}
				}

				if _, ok := cached.objects[1].Obj.(*manifest.RawObject); !ok {
					t.Errorf("cached producer = %T, want raw object", cached.objects[1])
				}
				if cached.objects[0].Doc == nil || cached.objects[1].Doc != nil || cached.objects[len(cached.objects)-1].Doc == nil {
					t.Fatal("only tracked objects should retain authoritative documents")
				}
				targets := manifest.ProducerTargets(cached.objects[1].Obj)
				if len(targets) != 1 || targets[0].Name != "generated" {
					t.Fatalf("released raw document corrupted producer spec: %v", targets)
				}
				assert.Equal(t, cached.body, header.body)
				assert.Equal(t, cached.file, header.file)
				assert.Equal(t, cached.loaded, header.loaded)
			}
		})
	}
}

func TestParseFile_NoTrackedObjects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		body    string
		wantNil bool
	}{
		{name: "Empty", wantNil: true},
		{name: "RawOnly", body: "apiVersion: example.io/v1\nkind: Example\nmetadata: {name: raw}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			testutil.WriteFile(t, root, "objects.yaml", tc.body)
			reader := readerForTest(root)
			path := filepath.Join(root, "objects.yaml")
			for range 2 {
				objects, err := reader.parseFile(t.Context(), path, manifest.ParseDocOptions{})
				assert.NoError(t, err)
				assert.Equal(t, len(objects), 0)
				assert.Equal(t, objects == nil, tc.wantNil)
			}
		})
	}
}

func TestParseFile_CompletedEmptyAndFailure(t *testing.T) {
	t.Parallel()
	for _, body := range []string{"", "key: first\nkey: duplicate\n"} {
		name := "empty"
		if body != "" {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			file := filepath.Join(root, "input.yaml")
			testutil.WriteFile(t, root, "input.yaml", body)
			reader := readerForTest(root)
			objects, err := reader.parseFile(t.Context(), file, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assert.Equal(t, len(objects), 0)
			before := reader.cache[file].objects
			if before == nil {
				t.Fatal("completed read was not retained")
			}
			assert.NoError(t, os.Remove(file))
			objects, err = reader.parseFile(t.Context(), file, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assert.Equal(t, len(objects), 0)
			after := reader.cache[file].objects
			assert.Equal(t, len(after), len(before))
			if name == "failed" && (len(after) != 1 || after[0].outcome.Ordinal != 0 || !errors.Is(after[0].outcome.Err, before[0].outcome.Err)) {
				t.Fatalf("failure replaced: %+v", after)
			}
		})
	}
}

func TestParseFile_ReadFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		stream bool
		cause  error
	}{
		{name: "missing", cause: fs.ErrNotExist},
		{name: "permission", cause: fs.ErrPermission},
		{name: "stream", stream: true, cause: io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			file := filepath.Join(root, "input.yaml")
			input := &failingInputFS{FS: fstest.MapFS{"input.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: prefix}\n---\npartial: [" + strings.Repeat(" ", 1024))}}, cause: tc.cause, stream: tc.stream}
			reader := readerForTest(root)
			reader.views[root] = input
			objects, err := reader.fileObjects(file, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			want := 1
			if tc.stream {
				want++
				assert.Equal(t, objects[0].Obj.Named().Name, "prefix")
			}
			assert.Equal(t, len(objects), want)
			failure := objects[len(objects)-1]
			if failure.Obj != nil || failure.Doc != nil || failure.outcome.Document != nil || failure.outcome.Ordinal != want-1 || !errors.Is(failure.outcome.Err, tc.cause) || !errors.Is(failure.outcome.Err, manifest.ErrInput) || !errors.Is(failure.outcome.Err, manifest.ErrFlux) {
				t.Fatalf("invalid retained failure: %+v", failure)
			}
			again, err := reader.fileObjects(file, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assert.Equal(t, len(again), want)
			assert.Equal(t, again[len(again)-1].outcome.Err, failure.outcome.Err)
			assert.Equal(t, input.opens, 1)
		})
	}
}

func TestParseFile_RunFailures(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{fmt.Errorf("wrapped: %w", context.Canceled), fmt.Errorf("wrapped: %w", context.DeadlineExceeded), errSourceUnavailable} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			file := filepath.Join(root, "input.yaml")
			input := &failingInputFS{cause: cause}
			reader := readerForTest(root)
			reader.views[root] = input
			for range 2 {
				objects, err := reader.fileObjects(file, manifest.ParseDocOptions{})
				if objects != nil || !errors.Is(err, cause) || reader.cache[file].objects != nil {
					t.Fatalf("run failure cached: objects=%v err=%v", objects, err)
				}
			}
			assert.Equal(t, input.opens, 2)
		})
	}
}

func TestParseFile_OpenRegularRejection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: fstest.MapFS{
		"pipe.yaml": {Mode: fs.ModeNamedPipe | 0o600},
	}})
	assert.NoError(t, err)
	reader := readerForTest(root)
	reader.views[root], reader.regular = input, true
	objects, err := reader.fileObjects(filepath.Join(root, "pipe.yaml"), manifest.ParseDocOptions{})
	assert.NoError(t, err)
	if len(objects) != 1 || objects[0].Obj != nil || objects[0].Doc != nil || !errors.Is(objects[0].outcome.Err, fs.ErrNotExist) {
		t.Fatalf("OpenRegular rejection not retained: %+v", objects)
	}
}

type failingInputFS struct {
	fs.FS
	cause  error
	stream bool
	opens  int
}

func (f *failingInputFS) Open(name string) (fs.File, error) {
	f.opens++
	if !f.stream {
		return nil, &fs.PathError{Op: "open", Path: name, Err: f.cause}
	}
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return failingInputFile{File: file, cause: f.cause}, nil
}

type failingInputFile struct {
	fs.File
	cause error
}

func (f failingInputFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if errors.Is(err, io.EOF) {
		return n, f.cause
	}
	return n, err
}
