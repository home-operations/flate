package sourceignore

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkNew_Defaults(b *testing.B) {
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := New(root, nil, true); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNew_Patterns(b *testing.B) {
	root := b.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".sourceignore"), []byte("*.tmp\n"), 0o600); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := New(root, nil, true); err != nil {
			b.Fatal(err)
		}
	}
}
