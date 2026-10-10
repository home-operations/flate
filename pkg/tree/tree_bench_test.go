package tree_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/tree"
)

func benchmarkTrees(b *testing.B, count int) (*tree.Tree, fstest.MapFS) {
	b.Helper()
	builder := tree.NewBuilder(nil)
	reference := make(fstest.MapFS, count)
	data := bytes.Repeat([]byte("x"), 894)
	for i := range count {
		name := fmt.Sprintf("dir-%04d/file-%04d.yaml", i/8, i)
		must(b, builder.AddFile(name, data, 0o644))
		reference[name] = &fstest.MapFile{Data: data, Mode: 0o644}
	}
	return build(b, builder), reference
}

func benchmarkReaders(b *testing.B, run func(*testing.B, fs.FS)) {
	b.Helper()
	for _, count := range []int{476, 12000} {
		b.Run(fmt.Sprintf("files=%d", count), func(b *testing.B) {
			tr, reference := benchmarkTrees(b, count)
			for _, tc := range []struct {
				name string
				fs   fs.FS
			}{
				{name: "Tree", fs: tr},
				{name: "MapFS", fs: reference},
			} {
				b.Run("fs="+tc.name, func(b *testing.B) {
					b.ReportAllocs()
					run(b, tc.fs)
				})
			}
		})
	}
}

func BenchmarkTree_ReadFile(b *testing.B) {
	benchmarkReaders(b, func(b *testing.B, fsys fs.FS) {
		b.SetBytes(894)
		for b.Loop() {
			_, err := fs.ReadFile(fsys, "dir-0000/file-0000.yaml")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTree_ReadDir(b *testing.B) {
	benchmarkReaders(b, func(b *testing.B, fsys fs.FS) {
		for b.Loop() {
			_, err := fs.ReadDir(fsys, "dir-0000")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTree_WalkDir(b *testing.B) {
	benchmarkReaders(b, func(b *testing.B, fsys fs.FS) {
		fn := func(_ string, _ fs.DirEntry, err error) error { return err }
		for b.Loop() {
			if err := fs.WalkDir(fsys, ".", fn); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTree_Stat(b *testing.B) {
	benchmarkReaders(b, func(b *testing.B, fsys fs.FS) {
		for b.Loop() {
			_, err := fs.Stat(fsys, "dir-0000/file-0000.yaml")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkOverlay_GeneratedFile(b *testing.B) {
	for _, count := range []int{476, 12000} {
		b.Run(fmt.Sprintf("files=%d", count), func(b *testing.B) {
			lower, _ := benchmarkTrees(b, count)
			data := []byte("resources:\n- dir-0000/file-0000.yaml\n")
			b.ReportAllocs()
			for b.Loop() {
				upper := tree.NewBuilder(lower)
				if err := upper.AddFile("kustomization.yaml", data, 0o644); err != nil {
					b.Fatal(err)
				}
				tr, err := upper.Build()
				if err != nil {
					b.Fatal(err)
				}
				_, err = tr.ReadFile("kustomization.yaml")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTree_SharedReaders(b *testing.B) {
	benchmarkReaders(b, func(b *testing.B, fsys fs.FS) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := fs.ReadFile(fsys, "dir-0000/file-0000.yaml")
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
