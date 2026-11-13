package testutil

import (
	"bytes"
	"io"
	"slices"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	formatcfg "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/object"
)

// GitInit initializes a Git repository at dir.
func GitInit(tb testing.TB, dir string) *git.Repository {
	tb.Helper()
	repo, err := git.PlainInit(dir, false)
	if err != nil {
		tb.Fatal(err)
	}
	cfg, err := repo.Config()
	if err != nil {
		tb.Fatal(err)
	}
	cfg.Commit.GpgSign = config.OptBoolFalse
	if err := repo.SetConfig(cfg); err != nil {
		tb.Fatal(err)
	}
	return repo
}

// GitCommitAll stages the entire worktree and commits it with the supplied options.
func GitCommitAll(tb testing.TB, repo *git.Repository, message string, options *git.CommitOptions) plumbing.Hash {
	tb.Helper()
	wt, err := repo.Worktree()
	if err != nil {
		tb.Fatal(err)
	}
	if _, addErr := wt.Add("."); addErr != nil {
		tb.Fatal(addErr)
	}
	hash, err := wt.Commit(message, options)
	if err != nil {
		tb.Fatal(err)
	}
	return hash
}

// GitBlobHash MUST retain SHA1 byte framing for fingerprint assertions.
func GitBlobHash(tb testing.TB, data []byte) [formatcfg.SHA1Size]byte {
	tb.Helper()
	hash, err := plumbing.FromObjectFormat(formatcfg.SHA1).Compute(plumbing.BlobObject, data)
	if err != nil {
		tb.Fatal(err)
	}
	return [formatcfg.SHA1Size]byte(hash.Bytes())
}

// GitMalformedTree MUST inject invalid names after encoding because upstream rejects them.
func GitMalformedTree(tb testing.TB, tree *object.Tree, position int) *plumbing.MemoryObject {
	tb.Helper()
	name := tree.Entries[position].Name
	clone := *tree
	clone.Entries = slices.Clone(tree.Entries)
	clone.Entries[position].Name = "flate-fixture-entry"
	encoded := &plumbing.MemoryObject{}
	if err := clone.Encode(encoded); err != nil {
		tb.Fatal(err)
	}
	reader, err := encoded.Reader()
	if err != nil {
		tb.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		tb.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		tb.Fatal(err)
	}
	data = bytes.Replace(data, []byte(" flate-fixture-entry\x00"), []byte(" "+name+"\x00"), 1)
	malformed := &plumbing.MemoryObject{}
	malformed.SetType(plumbing.TreeObject)
	if _, err := malformed.Write(data); err != nil {
		tb.Fatal(err)
	}
	return malformed
}
