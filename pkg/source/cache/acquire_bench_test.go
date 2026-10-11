package cache

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/source/cacheroot"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func BenchmarkAcquire_Warm(b *testing.B) {
	for _, backing := range []string{"memory", "disk"} {
		b.Run(backing, func(b *testing.B) {
			root := ""
			if backing == "disk" {
				root = b.TempDir()
			}
			c, err := New(b.Context(), cacheroot.New(root))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = c.Close() })
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			if err := tw.WriteHeader(&tar.Header{Name: "file", Size: 4096, Mode: 0o600}); err != nil {
				b.Fatal(err)
			}
			if _, err := tw.Write(make([]byte, 4096)); err != nil {
				b.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				b.Fatal(err)
			}
			key := ArchiveKey{ID: "source-benchmark"}
			input, _, err := c.Acquire(b.Context(), key, time.Hour, producer(buf.Bytes(), ocispec.Descriptor{}))
			if err != nil {
				b.Fatal(err)
			}
			b.Run("acquire", func(b *testing.B) {
				for b.Loop() {
					if _, _, err := c.Acquire(b.Context(), key, time.Hour, func(io.Writer) (ocispec.Descriptor, error) {
						b.Fatal("warm acquisition called producer")
						return ocispec.Descriptor{}, nil
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("open", func(b *testing.B) {
				for b.Loop() {
					file, err := input.Open("file")
					if err != nil {
						b.Fatal(err)
					}
					if err := file.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
