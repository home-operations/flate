package cache

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/tree"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestAcquire_ArchiveNames(t *testing.T) {
	t.Parallel()
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, backing.root, New)
			for _, tc := range []struct {
				name, entry string
				kind        byte
				wantErr     error
			}{
				{name: "root_directory", entry: ".", kind: tar.TypeDir},
				{name: "root_file", entry: ".", kind: tar.TypeReg, wantErr: tree.ErrConflict},
				{name: "invalid_file", entry: "a/../x", kind: tar.TypeReg, wantErr: fs.ErrInvalid},
				{name: "invalid_directory", entry: "a/../", kind: tar.TypeDir, wantErr: fs.ErrInvalid},
				{name: "normalized_file", entry: "././dir/file", kind: tar.TypeReg},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					var data bytes.Buffer
					w := tar.NewWriter(&data)
					h := &tar.Header{Name: tc.entry, Typeflag: tc.kind, Mode: 0o600}
					if tc.kind == tar.TypeReg {
						h.Size = 1
					}
					if err := w.WriteHeader(h); err != nil {
						t.Fatal(err)
					}
					if tc.kind == tar.TypeReg {
						if _, err := io.WriteString(w, "x"); err != nil {
							t.Fatal(err)
						}
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
					input, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: tc.name}, Policy{MaxAge: 0}, producer(data.Bytes(), ocispec.Descriptor{}))
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("archive name %q: %v, want %v", tc.entry, err, tc.wantErr)
					}
					if tc.wantErr != nil {
						return
					}
					if tc.kind == tar.TypeDir {
						if root, err := fs.Stat(input, "."); err != nil || !root.IsDir() {
							t.Fatalf("archive root=%v, err=%v", root, err)
						}
					} else if got, err := fs.ReadFile(input, "dir/file"); err != nil || string(got) != "x" {
						t.Fatalf("normalized file: %q, %v", got, err)
					}
				})
			}
		})
	}
}

func TestAcquire_TrailingRecordPadding(t *testing.T) {
	t.Parallel()
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x", "other": "y"}, 0o644)
	data := append(payloadTar.Bytes(), make([]byte, 8192)...)
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			t.Parallel()
			root := backing.root
			c := cachetest.New(t, root, New)
			if _, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-padding"}, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{})); err != nil {
				t.Fatalf("root=%q: %v", root, err)
			}
		})
	}
}

func TestAcquire_ArchiveEntryConflicts(t *testing.T) {
	t.Parallel()
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, backing.root, New)
			for _, tc := range []struct {
				name          string
				first, second tar.Header
				want          error
			}{
				{"duplicate_regular", tar.Header{Name: "file", Typeflag: tar.TypeReg}, tar.Header{Name: "file", Typeflag: tar.TypeReg}, nil},
				{"duplicate_link", tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, nil},
				{"duplicate_hardlink", tar.Header{Name: "file", Typeflag: tar.TypeLink}, tar.Header{Name: "file", Typeflag: tar.TypeLink}, nil},
				{"file_then_link", tar.Header{Name: "file", Typeflag: tar.TypeReg}, tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, tree.ErrConflict},
				{"hardlink_then_file", tar.Header{Name: "file", Typeflag: tar.TypeLink}, tar.Header{Name: "file", Typeflag: tar.TypeReg}, tree.ErrConflict},
				{"directory_then_link", tar.Header{Name: "file", Typeflag: tar.TypeDir}, tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, tree.ErrConflict},
				{"link_then_directory", tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, tar.Header{Name: "file", Typeflag: tar.TypeDir}, tree.ErrConflict},
				{"link_then_child", tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, tar.Header{Name: "file/child", Typeflag: tar.TypeReg}, tree.ErrConflict},
				{"child_then_link", tar.Header{Name: "file/child", Typeflag: tar.TypeReg}, tar.Header{Name: "file", Typeflag: tar.TypeSymlink}, tree.ErrConflict},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					var data bytes.Buffer
					w := tar.NewWriter(&data)
					for i, h := range []tar.Header{tc.first, tc.second} {
						if h.Typeflag == tar.TypeReg {
							h.Size = 1
						} else if h.Typeflag != tar.TypeDir {
							h.Linkname = fmt.Sprintf("../absent-%d", i)
						}
						assert.NoError(t, w.WriteHeader(&h))
						if h.Typeflag == tar.TypeReg {
							_, err := fmt.Fprint(w, i)
							assert.NoError(t, err)
						}
					}
					assert.NoError(t, w.Close())
					input, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: tc.name}, Policy{}, producer(data.Bytes(), ocispec.Descriptor{}))
					if !errors.Is(err, tc.want) {
						t.Fatalf("acquire = %v; want %v", err, tc.want)
					}
					if tc.want != nil {
						return
					}
					got, err := fs.ReadFile(input, "file")
					if tc.second.Typeflag == tar.TypeReg {
						assert.NoError(t, err)
						assert.Equal(t, string(got), "1")
					} else {
						assert.Equal(t, len(got), 0)
						assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
					}
				})
			}
		})
	}
}
