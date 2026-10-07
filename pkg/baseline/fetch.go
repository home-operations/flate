package baseline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
)

// fetchedRef is where a fetched baseline lands in the throwaway repo.
const fetchedRef plumbing.ReferenceName = "refs/flate/baseline"

// fetched is a baseline commit held in a throwaway bare repo rather
// than the user's checkout, which stays untouched.
type fetched struct {
	repo    *git.Repository
	hash    plumbing.Hash
	cleanup func()
}

// fetchBase fetches rev from url into a fresh bare repo and returns the
// commit it names. rev is matched against the remote's refs the way
// `git fetch origin <rev>` would: a full ref name as is, a branch before
// a tag of the same name, origin/<name> as the branch, and a full SHA
// through whichever ref points at it.
func fetchBase(ctx context.Context, url, rev string) (*fetched, error) {
	dir, err := os.MkdirTemp("", "flate-baseline-fetch-*")
	if err != nil {
		return nil, fmt.Errorf("baseline fetch tempdir: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	f, err := fetchInto(ctx, dir, url, rev)
	if err != nil {
		cleanup()
		return nil, err
	}
	f.cleanup = cleanup
	return f, nil
}

func fetchInto(ctx context.Context, dir, url, rev string) (*fetched, error) {
	repo, err := git.PlainInit(dir, true)
	if err != nil {
		return nil, fmt.Errorf("baseline fetch init: %w", err)
	}
	remote, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}})
	if err != nil {
		return nil, fmt.Errorf("baseline fetch remote: %w", err)
	}
	src, err := remoteRef(ctx, remote, rev)
	if err != nil {
		return nil, err
	}
	slog.Info("baseline: fetching from origin", "rev", src, "url", url)
	err = remote.FetchContext(ctx, &git.FetchOptions{
		RefSpecs: []config.RefSpec{config.RefSpec(src + ":" + fetchedRef.String())},
		Depth:    fetchDepth(url),
		Tags:     git.NoTags,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return nil, fmt.Errorf("fetch %s: %w", src, err)
	}
	ref, err := repo.Reference(fetchedRef, true)
	if err != nil {
		return nil, fmt.Errorf("fetched %s: %w", src, err)
	}
	hash := ref.Hash()
	// An annotated tag points at a tag object; the baseline is the
	// commit behind it.
	if tag, err := repo.TagObject(hash); err == nil {
		commit, err := tag.Commit()
		if err != nil {
			return nil, fmt.Errorf("peel tag %s: %w", src, err)
		}
		hash = commit.Hash
	}
	return &fetched{repo: repo, hash: hash}, nil
}

// remoteRef picks the ref on the remote that rev names. A full SHA
// fetches as the ref pointing at it when one exists, which every server
// allows, and as the bare SHA otherwise, which needs the server's
// consent (GitHub gives it for reachable commits).
func remoteRef(ctx context.Context, remote *git.Remote, rev string) (string, error) {
	refs, err := remote.ListContext(ctx, &git.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list origin refs: %w", err)
	}
	if plumbing.IsHash(rev) {
		for _, ref := range refs {
			if ref.Hash().String() == rev {
				return ref.Name().String(), nil
			}
		}
		return rev, nil
	}
	present := make(map[plumbing.ReferenceName]bool, len(refs))
	for _, ref := range refs {
		present[ref.Name()] = true
	}
	var candidates []plumbing.ReferenceName
	if strings.HasPrefix(rev, "refs/") {
		candidates = []plumbing.ReferenceName{plumbing.ReferenceName(rev)}
	} else {
		name := strings.TrimPrefix(rev, "origin/")
		candidates = []plumbing.ReferenceName{
			plumbing.NewBranchReferenceName(name),
			plumbing.NewTagReferenceName(name),
		}
	}
	for _, name := range candidates {
		if present[name] {
			return name.String(), nil
		}
	}
	return "", fmt.Errorf("origin has no ref named %s", rev)
}

// fetchDepth is 1 for network remotes. A local-path remote is served by
// go-git's in-process server, which refuses shallow fetches, and has
// nothing to save by one.
func fetchDepth(url string) int {
	if ep, err := transport.NewEndpoint(url); err == nil && ep.Protocol == "file" {
		return 0
	}
	return 1
}

// originURL is the checkout's origin remote URL, when it has one.
func originURL(repo *git.Repository) (string, bool) {
	remote, err := repo.Remote("origin")
	if err != nil || len(remote.Config().URLs) == 0 {
		return "", false
	}
	return remote.Config().URLs[0], true
}
