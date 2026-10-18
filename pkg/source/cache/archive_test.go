package cache

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/tree"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestAcquire_ArchiveNames(t *testing.T) {
	t.Parallel()
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			t.Parallel()
			c := newCache(t, backing.root)
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

func gnuSparseEntry(logicalSize uint64) []byte {
	blk := make([]byte, 512)
	copy(blk[0:], "big")
	copy(blk[100:], "0000644\x00")
	copy(blk[108:], "0000000\x00")
	copy(blk[116:], "0000000\x00")
	copy(blk[124:], "00000000000\x00")
	copy(blk[136:], "00000000000\x00")
	blk[156] = 'S'
	copy(blk[257:], "ustar  \x00")
	blk[483] = 0x80 // GNU sparse logical size is base-256 encoded.
	binary.BigEndian.PutUint64(blk[483+4:], logicalSize)
	for i := 148; i < 156; i++ {
		blk[i] = ' '
	}
	sum := 0
	for _, b := range blk {
		sum += int(b)
	}
	copy(blk[148:], fmt.Sprintf("%06o\x00 ", sum))
	return append(bytes.Clone(blk), make([]byte, 1024)...)
}

func TestAcquire_SkippedSparseEntryCountsAgainstCap(t *testing.T) {
	old := maxExtractedBytes
	maxExtractedBytes = 1 << 20
	t.Cleanup(func() { maxExtractedBytes = old })
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			root := backing.root
			c := newCache(t, root)
			_, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-sparse-cap"}, Policy{MaxAge: 0}, producer(gnuSparseEntry(1<<36), ocispec.Descriptor{}))
			if err == nil {
				t.Fatalf("root=%q: skipped sparse entry declaring 64 GiB bypassed the 1 MiB expanded-byte cap", root)
			}
		})
	}
}

func TestAcquire_EmptyEntryHeadersCountAgainstCap(t *testing.T) {
	old := maxExtractedBytes
	maxExtractedBytes = 1024
	t.Cleanup(func() { maxExtractedBytes = old })
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			c := newCache(t, backing.root)
			key := ArchiveKey{Content: "source-empty-headers-cap"}
			if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, producer(archive(t, "a", "", "b", "", "c", ""), ocispec.Descriptor{})); err == nil {
				t.Fatal("empty entry headers bypassed extract cap")
			}
			if _, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: time.Hour}, producer(archive(t, "a", "", "b", ""), ocispec.Descriptor{})); err != nil {
				t.Fatalf("retry at extract cap: %v", err)
			}
		})
	}
}

func TestAcquire_TrailingRecordPadding(t *testing.T) {
	old := maxExtractedBytes
	maxExtractedBytes = 2*512 + 2
	t.Cleanup(func() { maxExtractedBytes = old })
	data := append(archive(t, "file", "x", "other", "y"), make([]byte, 8192)...)
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			root := backing.root
			c := newCache(t, root)
			if _, _, _, err := c.Acquire(t.Context(), ArchiveKey{Content: "source-padding"}, Policy{MaxAge: time.Hour}, producer(data, ocispec.Descriptor{})); err != nil {
				t.Fatalf("root=%q: %v", root, err)
			}
		})
	}
}
