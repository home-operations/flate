package cache

import (
	"path/filepath"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	ociDir  = "oci"
	ociLock = "oci.lock"
)

func indexPath() string { return filepath.Join(ociDir, ocispec.ImageIndexFile) }

func blobPath(d digest.Digest) string {
	return filepath.Join(ociDir, "blobs", d.Algorithm().String(), d.Encoded())
}

func acquirePath(id string) string       { return filepath.Join(ociDir, "acquire-"+id) }
func generationPath(id string) string    { return filepath.Join(ociDir, "git", id) }
func gitObjectsPath(id string) string    { return filepath.Join(generationPath(id), "objects") }
func gitObjectDataPath(id string) string { return filepath.Join(gitObjectsPath(id), "objects") }
func gitWorktreePath(id string) string   { return filepath.Join(generationPath(id), "worktree") }
