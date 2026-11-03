package sourceignore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func BenchmarkNew_Defaults(b *testing.B) {
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		{
			_, err := NewFS(os.DirFS(root), nil, "", true)
			assert.NoError(b, err)
		}
	}
}

func BenchmarkNew_Patterns(b *testing.B) {
	root := b.TempDir()
	{
		err := os.WriteFile(filepath.Join(root, ".sourceignore"), []byte("*.tmp\n"), 0o600)
		assert.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		{
			_, err := NewFS(os.DirFS(root), nil, "", true)
			assert.NoError(b, err)
		}
	}
}
