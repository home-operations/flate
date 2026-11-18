package kustomize

import (
	"fmt"
	"testing"
)

func BenchmarkTreeFS_ReadFile(b *testing.B) {
	lower := memoryTree(b, map[string]string{"file": "content"})
	for _, recorded := range []bool{false, true} {
		b.Run(fmt.Sprintf("recorder=%v", recorded), func(b *testing.B) {
			var rec *readSet
			if recorded {
				rec = newReadSet("/virtual")
			}
			adapter := newTreeFS(lower, "/virtual", rec)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := adapter.ReadFile("/virtual/file"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTreeFS_ReadDir(b *testing.B) {
	lower := memoryTree(b, map[string]string{"dir/a": "a", "dir/b": "b"})
	for _, recorded := range []bool{false, true} {
		b.Run(fmt.Sprintf("recorder=%v", recorded), func(b *testing.B) {
			var rec *readSet
			if recorded {
				rec = newReadSet("/virtual")
			}
			adapter := newTreeFS(lower, "/virtual", rec)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := adapter.ReadDir("/virtual/dir"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
