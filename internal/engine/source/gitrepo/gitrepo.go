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

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
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
		return nil, fmt.Errorf("locate input: %w", err)
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
	location.selectRepository(search)
	if location.ContentRoot == "" {
		location.ContentRoot = location.RepositoryRoot
		if location.ContentRoot == "" {
			location.ContentRoot = scan
		}
	}
	location.loadConfig()
	return location, nil
}

func canonical(name string) (string, error) {
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", fmt.Errorf("absolute input %q: %w", name, err)
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

func (l *Location) selectRepository(search string) {
	root := search
	for {
		selected, err := repositoryMarker(root)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			if root == search {
				l.LookupErr = err
			}
			break
		}
		if selected {
			repo, err := git.PlainOpenWithOptions(root, &git.PlainOpenOptions{})
			l.RepositoryRoot, l.Repository, l.LookupErr = root, repo, err
			if err != nil {
				l.LookupErr = fmt.Errorf("open repository %q: %w", root, err)
			}
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			break
		}
		root = parent
	}
}

func (l *Location) loadConfig() {
	if l.Repository != nil {
		l.Config, l.ConfigErr = l.Repository.Config()
		if l.ConfigErr == nil {
			for _, remote := range l.Config.Remotes {
				l.URLs = append(l.URLs, remote.URLs...)
			}
			slices.Sort(l.URLs)
		}
	}
}

func repositoryMarker(root string) (bool, error) {
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
	return selected, err //nolint:wrapcheck // Filesystem adapters retain fs.PathError operation, path, and concrete error identity.
}
