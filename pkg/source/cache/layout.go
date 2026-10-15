package cache

import (
	"path/filepath"
	"strings"

	"github.com/opencontainers/go-digest"
)

type layout struct{ Root string }

func join(base string, segs ...string) string {
	n := len(base)
	for _, s := range segs {
		n += 1 + len(s)
	}
	var b strings.Builder
	b.Grow(n)
	b.WriteString(base)
	for _, s := range segs {
		b.WriteByte(filepath.Separator)
		b.WriteString(s)
	}
	return b.String()
}

func (l layout) OCI() string     { return join(l.Root, "oci") }
func (l layout) OCILock() string { return join(l.Root, "oci.lock") }
func blobPath(d digest.Digest) string {
	return join("oci", "blobs", d.Algorithm().String(), d.Encoded())
}

func generationPath(id string) string {
	if id == "" {
		return join("oci", "git")
	}
	return join("oci", "git", id)
}
func gitObjectsPath(id string) string         { return join(generationPath(id), "objects") }
func gitWorktreePath(id string) string        { return join(generationPath(id), "worktree") }
func (l layout) GitObjects(id string) string  { return join(l.Root, gitObjectsPath(id)) }
func (l layout) GitWorktree(id string) string { return join(l.Root, gitWorktreePath(id)) }
