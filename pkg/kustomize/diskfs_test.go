package kustomize

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestDiskInput_ArtifactEntries(t *testing.T) {
	root := writeTree(t, map[string]string{"regular.txt": "content"})
	for name, target := range map[string]string{"file-link": "regular.txt", "dir-link": "."} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	disk := testTreeFS(t, root).(*treeFS).lower.(diskInput)
	entries, err := fs.ReadDir(disk, ".")
	if err != nil || !slices.Equal(entryNames(entries), []string{"regular.txt"}) {
		t.Fatalf("ReadDir = %v, %v", entryNames(entries), err)
	}
	dir, err := disk.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	entries, err = dir.(fs.ReadDirFile).ReadDir(-1)
	if err != nil || !slices.Equal(entryNames(entries), []string{"regular.txt"}) {
		t.Fatalf("opened directory = %v, %v", entryNames(entries), err)
	}
	for _, name := range []string{"file-link", "dir-link", "dir-link/regular.txt", "regular.txt"} {
		t.Run(name, func(t *testing.T) {
			var want error
			if name != "regular.txt" {
				want = fs.ErrNotExist
			}
			info, err := disk.Stat(name)
			if !errors.Is(err, want) || err == nil && !info.Mode().IsRegular() {
				t.Fatalf("Stat = %v, %v", info, err)
			}
			file, err := disk.Open(name)
			if !errors.Is(err, want) {
				t.Fatalf("Open = %v", err)
			}
			if err == nil {
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			data, err := fs.ReadFile(disk, name)
			if !errors.Is(err, want) || err == nil && string(data) != "content" {
				t.Fatalf("ReadFile = %q, %v", data, err)
			}
		})
	}
}
