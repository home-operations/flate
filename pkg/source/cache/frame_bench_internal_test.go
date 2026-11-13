package cache

import (
	"bytes"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkOpenFrame_Input(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("metadata=%d", size), func(b *testing.B) {
			metadata := frameBenchmarkMetadata(size)
			frame, err := NewFrame(inputFrameDomain, metadata, []byte("payload"))
			assert.NoError(b, err)
			content := struct{ Origin, URL, Content string }{"source", "url", frame.raw}
			publication, err := frame.PreparePublication(b.Context(), FramePublicationInput{
				Owner: manifest.BootstrapSourceID, Content: content, Complete: true,
				Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
			})
			assert.NoError(b, err)
			var wire bytes.Buffer
			_, err = publication.WriteTo(&wire)
			assert.NoError(b, err)
			generation := frameGeneration(wire.Bytes())
			for _, history := range []bool{false, true} {
				b.Run(fmt.Sprintf("history=%t", history), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var decoded []string
						opened, openErr := OpenFrame(b.Context(), inputFrameDomain, generation, &decoded)
						assert.NoError(b, openErr)
						if history {
							_, identity := InputFrameIdentity("source", "url", opened)
							content.Content = identity
							_, complete := opened.History(manifest.BootstrapSourceID, content)
							assert.Equal(b, complete, true)
						}
					}
				})
			}
		})
	}
}

func BenchmarkFrameIdentity_Input(b *testing.B) {
	for _, size := range []int{1 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("metadata=%d", size), func(b *testing.B) {
			frame, err := NewFrame(inputFrameDomain, frameBenchmarkMetadata(size), nil)
			assert.NoError(b, err)
			b.ReportAllocs()
			for b.Loop() {
				frameIdentity(frame.header[framePrefixSize:])
			}
		})
	}
}

func frameBenchmarkMetadata(size int) []string {
	metadata := make([]string, size/64)
	value := strings.Repeat("x", 61)
	for i := range metadata {
		metadata[i] = value
	}
	metadata[0] = value[:len(value)-1]
	return metadata
}
