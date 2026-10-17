package manifest

import (
	"bytes"
	"testing"
)

func BenchmarkFingerprint_Content(b *testing.B) {
	files := make([][]byte, 128)
	for i := range files {
		files[i] = bytes.Repeat([]byte{byte(i)}, 64<<10)
	}
	for _, tt := range []struct {
		name    string
		payload any
	}{
		{"identity", struct{ Root, Path string }{"source", "apps"}},
		{"values", map[string]any{"replicas": 2, "enabled": true, "image": map[string]any{"repository": "app", "tag": "v1"}}},
		{"chart", struct {
			Name  string
			Files [][]byte
		}{"chart<&>\u2028", files}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				Fingerprint(tt.payload)
			}
		})
	}
}
