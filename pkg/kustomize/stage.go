package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// StagingCache materializes one-or-more source roots into a temp
// directory so Flux's kustomize Generator can safely write into the
// staged copy without touching the user's working tree.
//
// Staging is done at most once per source root via sync.OnceValues.
// The first reconciliation against a root pays the copy cost; every
// subsequent reconciliation (including for other Kustomizations rooted
// at the same source artifact) reuses the same stage.
//
// Lifecycle is tied to the surrounding orchestrator run — call Close
// to remove every staged copy.
type StagingCache struct {
	mu     sync.Mutex
	stages map[string]*stage
	root   string
	// remoteFetches dedupes URL fetches across every preflight pass in
	// one orchestrator run. A single kustomization.yaml URL may be
	// reached via multiple Flux Kustomizations (parent emits a child
	// that shares the same path; multiple KSes whose subPath crosses
	// the same nested kustomization). Without this, every reconcile
	// re-fetches the same URL and re-emits the same WARN line.
	remoteFetches sync.Map // url string -> *remoteFetch
}

type remoteFetch struct {
	once func() ([]byte, error)
}

type stage struct {
	once func() (string, error)
}

// NewStagingCache constructs a cache that places staged copies under
// the given parent directory. If parent is empty, the OS tempdir is
// used.
func NewStagingCache(parent string) (*StagingCache, error) {
	if parent == "" {
		parent = os.TempDir()
	}
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return nil, err
	}
	return &StagingCache{
		stages: make(map[string]*stage),
		root:   parent,
	}, nil
}

// FetchRemote returns the body of urlStr, fetched at most once per
// StagingCache lifetime. Both successful bodies and errors are
// cached — concurrent callers block on a single sync.OnceValues and
// every caller sees the same result. The error (if any) is returned
// to the caller; logging is left to whichever reconcile path
// surfaces the failure, so the user sees one structured error in
// `flate test`'s report rather than a separate log line per call
// site.
func (c *StagingCache) FetchRemote(ctx context.Context, urlStr string) ([]byte, error) {
	actual, _ := c.remoteFetches.LoadOrStore(urlStr, &remoteFetch{
		once: sync.OnceValues(func() ([]byte, error) {
			return httpGetURL(ctx, urlStr)
		}),
	})
	return actual.(*remoteFetch).once()
}

// Stage returns the on-disk staged copy of source. The copy is created
// on first call; concurrent callers block on a single sync.OnceValues.
func (c *StagingCache) Stage(source string) (string, error) {
	resolved, err := filepath.EvalSymlinks(source)
	if err == nil {
		source = resolved
	}
	c.mu.Lock()
	s, ok := c.stages[source]
	if !ok {
		copyOnce := sync.OnceValues(func() (string, error) {
			return c.copyTree(source)
		})
		s = &stage{once: copyOnce}
		c.stages[source] = s
	}
	c.mu.Unlock()
	return s.once()
}

// Close removes every staged copy.
func (c *StagingCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, s := range c.stages {
		path, err := s.once()
		if err != nil {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			errs = append(errs, err)
		}
	}
	c.stages = make(map[string]*stage)
	return errors.Join(errs...)
}

// copyTree makes a fresh directory and copies every regular file from
// src into it. Symlinks are dereferenced (we want the file content),
// dotfiles are skipped to keep stages clean.
func (c *StagingCache) copyTree(src string) (string, error) {
	dst, err := os.MkdirTemp(c.root, "flate-stage-*")
	if err != nil {
		return "", err
	}
	walkErr := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		base := d.Name()
		if d.IsDir() {
			// Skip anything that isn't user content: .git / node_modules
			// and every dot-prefixed dir (which captures .flate-cache).
			if base == "node_modules" || (len(base) > 0 && base[0] == '.') {
				return fs.SkipDir
			}
			return os.MkdirAll(filepath.Join(dst, rel), 0o750)
		}
		// Read regular files (or follow symlinks to regular files).
		info, err := os.Stat(path)
		if err != nil {
			// A dangling symlink in the user's working tree (a common
			// local-only state for editor lockfiles, gitignored
			// .pre-commit-config.yaml, IDE caches) used to abort the
			// entire stage. flate doesn't need the link target — Flux's
			// reconcile wouldn't either — so skip silently when the
			// target is missing. Other Stat errors (permissions, I/O)
			// still surface.
			if d.Type()&fs.ModeSymlink != 0 && errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyFile(path, filepath.Join(dst, rel), info.Mode())
	})
	if walkErr != nil {
		_ = os.RemoveAll(dst)
		return "", fmt.Errorf("stage %s: %w", src, walkErr)
	}
	return dst, nil
}

func copyFile(srcPath, dstPath string, mode os.FileMode) error {
	src, err := os.Open(srcPath) //nolint:gosec // srcPath is a tree-walk result under our source root
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode.Perm()) //nolint:gosec // dstPath is inside our staging tempdir
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}
