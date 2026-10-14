// Package gitrepo selects and opens local Git working trees through go-git.
package gitrepo

import (
	"errors"
	"os"
	"path/filepath"
	"slices"

	"github.com/go-git/go-git/v5"
)

// Locate opens the repository containing path. Incomplete .git directories are
// skipped. Metadata failures stop the search: ancestors return no repository;
// path's own lookup errors return no root and open errors retain the root.
// Gitfiles and common directories are decoded exclusively by go-git.
func Locate(path string) (root string, repo *git.Repository, err error) {
	if path == "" {
		return "", nil, nil
	}
	root, err = filepath.Abs(path)
	if err != nil {
		return "", nil, err
	}
	start := root
	for {
		dot := filepath.Join(root, ".git")
		info, err := os.Stat(dot)
		selected := err == nil && info.Mode().IsRegular()
		if err == nil && info.IsDir() {
			for _, name := range []string{"HEAD", "objects"} {
				_, err = os.Stat(filepath.Join(dot, name))
				if err != nil {
					break
				}
			}
			selected = err == nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			if root != start {
				return "", nil, nil
			}
			return "", nil, err
		}
		if selected {
			repo, err := git.PlainOpenWithOptions(root, &git.PlainOpenOptions{
				EnableDotGitCommonDir: true,
			})
			if err != nil && root != start {
				return "", nil, nil
			}
			return root, repo, err
		}
		parent := filepath.Dir(root)
		if parent == root {
			return "", nil, nil
		}
		root = parent
	}
}

// RemoteURLs returns sorted raw remote URLs. The caller owns normalization and
// the policy for optional configuration errors.
func RemoteURLs(repo *git.Repository) ([]string, error) {
	cfg, err := repo.Config()
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, remote := range cfg.Remotes {
		urls = append(urls, remote.URLs...)
	}
	slices.Sort(urls)
	return urls, nil
}
