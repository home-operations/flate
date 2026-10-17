// Package gitrepo selects and opens local Git working trees through go-git.
package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
)

// Location retains canonical input coordinates and repository metadata for one admission.
// Callers MUST treat its configuration, URLs and repository storage as read-only.
type Location struct {
	ScanPath       string
	ContentRoot    string
	RepositoryRoot string
	Repository     *git.Repository
	Config         *config.Config
	URLs           []string
	LookupErr      error
	ConfigErr      error
}

// Locate resolves each distinct input coordinate once and finds its repository.
// Mandatory path errors are returned separately from optional repository metadata errors.
func Locate(ctx context.Context, path, contentRoot string) (*Location, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	canonical := func(name string) (string, error) {
		abs, err := filepath.Abs(name)
		if err != nil {
			return "", err
		}
		resolved, err := filepath.EvalSymlinks(abs)
		if errors.Is(err, fs.ErrNotExist) {
			return abs, nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve input %q: %w", name, err)
		}
		return resolved, nil
	}
	scan, err := canonical(path)
	if err != nil {
		return nil, err
	}
	location := &Location{ScanPath: scan}
	switch {
	case contentRoot != "" && (contentRoot == path || filepath.Clean(contentRoot) == scan):
		location.ContentRoot = scan
	case contentRoot != "":
		location.ContentRoot, err = canonical(contentRoot)
		if err != nil {
			return nil, err
		}
	}
	search := location.ContentRoot
	if search == "" {
		search = scan
	}
	root := search
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
			if root == search {
				location.LookupErr = err
			}
			break
		}
		if selected {
			repo, err := git.PlainOpenWithOptions(root, &git.PlainOpenOptions{EnableDotGitCommonDir: true})
			if err == nil || root == search {
				location.RepositoryRoot, location.Repository, location.LookupErr = root, repo, err
			}
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			break
		}
		root = parent
	}
	if location.ContentRoot == "" {
		location.ContentRoot = location.RepositoryRoot
		if location.ContentRoot == "" {
			location.ContentRoot = scan
		}
	}
	if location.Repository != nil {
		location.Config, location.ConfigErr = location.Repository.Config()
		if location.ConfigErr == nil {
			for _, remote := range location.Config.Remotes {
				location.URLs = append(location.URLs, remote.URLs...)
			}
			slices.Sort(location.URLs)
		}
	}
	return location, nil
}
