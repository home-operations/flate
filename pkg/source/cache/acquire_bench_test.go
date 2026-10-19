package cache

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func BenchmarkAcquire_Warm(b *testing.B) {
	for _, backing := range []string{"memory", "disk"} {
		b.Run(backing, func(b *testing.B) {
			root := ""
			if backing == "disk" {
				root = b.TempDir()
			}
			c, err := New(b.Context(), root)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = c.Close() })
			var buf bytes.Buffer
			testutil.WriteTar(b, &buf, map[string]string{"file": string(make([]byte, 4096))}, 0o600)
			key := ArchiveKey{Content: "source-benchmark"}
			input, _, _, err := c.Acquire(b.Context(), key, Policy{MaxAge: time.Hour}, producer(buf.Bytes(), ocispec.Descriptor{}))
			if err != nil {
				b.Fatal(err)
			}
			b.Run("acquire", func(b *testing.B) {
				for b.Loop() {
					if _, _, _, err := c.Acquire(b.Context(), key, Policy{MaxAge: time.Hour}, func(context.Context, ProducerInput) (ProducerResult, error) {
						b.Fatal("warm acquisition called producer")
						return ProducerResult{}, nil
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
