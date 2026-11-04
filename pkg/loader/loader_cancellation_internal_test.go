package loader

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestLoader_RespectsCanceledContext(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "a.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := New(store.New()).Load(ctx, dir)
	assert.ErrorIf(t, !errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
	for _, site := range []string{"graph", "siblings"} {
		t.Run(site, func(t *testing.T) {
			t.Parallel()
			testLoaderRespectsCanceledContextCase(t, &site)
		},
		)
	}
}

type cancelIterationFS struct {
	fs.FS

	cancel   context.CancelFunc
	canceled bool
	after    int
}

func (f *cancelIterationFS) Open(name string) (fs.File, error) {
	if f.canceled {
		f.after++
		return nil, context.Canceled
	}
	file, err := f.FS.Open(name)
	if err == nil && filepath.Base(name) == "first.yaml" {
		return cancelIterationFile{File: file, cancel: func() { f.canceled = true; f.cancel() }}, nil
	}
	return file, err
}

func (f *cancelIterationFS) Stat(name string) (fs.FileInfo, error) {
	if f.canceled {
		f.after++
		return nil, context.Canceled
	}
	return fs.Stat(f.FS, name)
}

func (f *cancelIterationFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.canceled {
		f.after++
		return nil, context.Canceled
	}
	return fs.ReadDir(f.FS, name)
}

type cancelIterationFile struct {
	fs.File

	cancel context.CancelFunc
}

func (f cancelIterationFile) Close() error {
	err := f.File.Close()
	f.cancel()
	return err
}

func testLoaderRespectsCanceledContextCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	base := ""
	if (*testCaseInput) == "siblings" {
		base = "package/"
		testutil.WriteFile(t, root, base+"kustomization.yaml", "resources: []\n")
	} else {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [first.yaml, second.yaml]\n")
	}
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(
			t,
			root,
			base+name+".yaml",
			testutil.KustomizationYAML(name, "apps", "  path: ./workload\n"),
		)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &cancelIterationFS{FS: os.DirFS(root), cancel: cancel}
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input},
	)
	l := New(st)
	l.SourceRoot = root
	count, err := l.Load(ctx, root)
	if !errors.Is(err, context.Canceled) || count != 1 || input.after != 0 || !input.canceled {
		t.Fatalf("Load = (%d, %v), canceled=%t, subsequent I/O=%d", count, err, input.canceled, input.after)
	}
	{
		_, complete := l.scopes.scanned[root]
		assert.FatalIf(t, complete, "%s", "canceled root marked scanned")
	}
	{
		_, ok := l.Index.Lookup(testutil.KustomizationID("apps", "second"))
		assert.FatalIf(t, ok, "%s", "sibling admitted after cancellation")
	}
}
