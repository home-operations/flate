package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

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
			_, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-sparse-cap"}, 0, producer(gnuSparseEntry(1<<36), ocispec.Descriptor{}))
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
			key := ArchiveKey{ID: "source-empty-headers-cap"}
			if _, _, err := c.Acquire(t.Context(), key, time.Hour, producer(archive(t, "a", "", "b", "", "c", ""), ocispec.Descriptor{})); err == nil {
				t.Fatal("empty entry headers bypassed extract cap")
			}
			if _, _, err := c.Acquire(t.Context(), key, time.Hour, producer(archive(t, "a", "", "b", ""), ocispec.Descriptor{})); err != nil {
				t.Fatalf("retry at extract cap: %v", err)
			}
		})
	}
}

func TestAcquire_TrailingRecordPadding(t *testing.T) {
	data := append(archive(t, "file", "x"), make([]byte, 8192)...)
	for _, backing := range []struct{ name, root string }{{"memory", ""}, {"disk", t.TempDir()}} {
		t.Run(backing.name, func(t *testing.T) {
			root := backing.root
			c := newCache(t, root)
			if _, _, err := c.Acquire(t.Context(), ArchiveKey{ID: "source-padding"}, time.Hour, producer(data, ocispec.Descriptor{})); err != nil {
				t.Fatalf("root=%q: %v", root, err)
			}
		})
	}
}
