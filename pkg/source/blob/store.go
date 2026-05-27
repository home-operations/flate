// Package blob is a small content-addressed storage layer for flate's
// fetched artifacts. Blobs are indexed by sha256 digest; two artifacts
// with identical content share the same on-disk slot regardless of
// which CR resolved them. The store is the substrate the cache rework
// builds on — see pkg/source for ref-keyed slots that compose with
// this CAS via separate ref tables.
//
// Layout under root:
//
//	<root>/blobs/sha256/<hex>/
//
// Each blob is a directory so individual files (chart.tgz, README,
// etc.) can sit inside it without escaping the digest namespace.
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// Store manages a content-addressed blob directory on disk. Safe for
// concurrent use; the keylock.KeyMap serializes Put for the same digest
// so two callers writing the same content don't race on rename
// finalize.
type Store struct {
	layout cacheroot.Layout
	locks  *keylock.KeyMap[string]
}

// NewStore constructs a Store backed by the supplied Layout. The
// blob subtree is created lazily on first write; Layout's blob
// path methods are the single source of truth for on-disk
// positioning.
func NewStore(layout cacheroot.Layout) *Store {
	return &Store{layout: layout, locks: keylock.New[string]()}
}

// Path returns the on-disk path for digest. Does not stat — callers
// use Exists to check populated-ness.
func (s *Store) Path(digest string) string {
	return s.layout.Blob(digest)
}

// Exists reports whether a blob has been finalized for digest.
func (s *Store) Exists(digest string) bool {
	info, err := os.Stat(s.Path(digest))
	return err == nil && info.IsDir()
}

// PutBytes installs content as a single file named filename inside the
// blob directory keyed by content's sha256. The digest is recomputed
// from the bytes (never trusted from caller input). Concurrent callers
// targeting the same digest serialize on a per-digest lock; the first
// finalizes via atomic rename and the rest observe ErrExists internally
// and return without rewriting. ctx cancellation aborts the lock
// acquire (no write performed).
//
// Takes the package-level GC shared lock so a concurrent gc.Sweep
// can't delete a blob between the Exists check and the caller's
// subsequent Refs.Put. The early-return path also refreshes the
// blob's mtime — without that bump, a reused-but-old blob whose
// "fresh" ref lands after Sweep's mark walk would be age-pruned even
// though a live caller just touched it.
//
// Returns the populated blob directory path and the computed digest.
func (s *Store) PutBytes(ctx context.Context, content []byte, filename string) (string, string, error) {
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	dir := s.Path(digest)
	parent := filepath.Dir(dir)

	unlockGC := rLockGC()
	defer unlockGC()

	release, err := s.locks.Acquire(ctx, digest)
	if err != nil {
		return "", "", err
	}
	defer release()

	if s.Exists(digest) {
		now := time.Now()
		_ = os.Chtimes(dir, now, now)
		return dir, digest, nil
	}
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", "", fmt.Errorf("blob parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, filepath.Base(dir)+".tmp.*")
	if err != nil {
		return "", "", fmt.Errorf("blob staging: %w", err)
	}
	target := filepath.Join(staging, filename)
	if err := os.WriteFile(target, content, 0o600); err != nil {
		_ = os.RemoveAll(staging)
		return "", "", fmt.Errorf("blob write: %w", err)
	}
	if err := os.Rename(staging, dir); err != nil {
		_ = os.RemoveAll(staging)
		// A racing writer may have finalized while we were composing
		// staging. If the blob now exists with the same digest, adopt
		// it — content-addressing guarantees identical bytes.
		if errors.Is(err, os.ErrExist) || s.Exists(digest) {
			return dir, digest, nil
		}
		return "", "", fmt.Errorf("blob finalize: %w", err)
	}
	return dir, digest, nil
}
