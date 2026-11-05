package testfixture

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

const (
	gitIndexVersion      = 2
	cacheFailureInterval = 15 * time.Minute
)

func (f *Fixture) createCommit(tb testing.TB, c *commit) {
	tb.Helper()
	repo := f.repositories[c.Repository]
	wt, err := repo.Worktree()
	must(tb, err)
	parents := make([]plumbing.Hash, 0, len(c.Parents))
	for _, name := range c.Parents {
		parents = append(parents, f.commits[name])
	}
	if len(parents) > 0 {
		must(tb, wt.Checkout(&git.CheckoutOptions{Hash: parents[0], Force: true}))
		f.useCommitOrigins(c.Repository, c.Parents[0])
	} else {
		root := filepath.Join(f.root, c.Repository)
		entries, readErr := os.ReadDir(root)
		must(tb, readErr)
		for _, entry := range entries {
			if entry.Name() != ".git" {
				must(tb, os.RemoveAll(filepath.Join(root, entry.Name())))
			}
		}
		for _, r := range f.schema.Repositories {
			if r.Name == c.Repository {
				f.copyRepository(tb, r, root)
			}
		}
		must(tb, repo.Storer.SetIndex(&index.Index{Version: gitIndexVersion}))
		// go-git defaults an empty parent list to HEAD, so root commits need an unborn ref.
		must(tb, repo.Storer.SetReference(plumbing.NewSymbolicReference(
			plumbing.HEAD, plumbing.ReferenceName("refs/fixture/"+c.Name),
		)))
	}
	if c.Overlay != "" {
		f.apply(tb, &operation{Overlay: c.Overlay})
	}
	f.resolveCommitTokens(tb, c.Repository, wt)
	must(tb, wt.AddWithOptions(&git.AddOptions{All: true}))
	when, err := time.Parse(time.RFC3339, c.Time)
	must(tb, err)
	signature := &object.Signature{Name: c.Author, Email: c.Email, When: when}
	hash, err := wt.Commit(
		c.Name,
		&git.CommitOptions{Author: signature, Committer: signature, Parents: parents, AllowEmptyCommits: true},
	)
	must(tb, err)
	f.commits[c.Name] = hash
	f.commitOrigins[c.Name] = f.repositoryOrigins(c.Repository)
	for _, ref := range c.Refs {
		must(tb, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(ref), hash)))
	}
}

func (f *Fixture) resolveCommitTokens(tb testing.TB, repository string, wt *git.Worktree) {
	tb.Helper()
	if len(f.schema.Tokens) == 0 {
		return
	}
	status, err := wt.Status()
	must(tb, err)
	for _, token := range f.schema.Tokens {
		relative, belongs := strings.CutPrefix(filepath.ToSlash(token.File), repository+"/")
		if !belongs {
			continue
		}
		changed, ok := status[relative]
		if !ok || changed.Worktree == git.Deleted || changed.Worktree == git.Unmodified {
			continue
		}
		path := f.path(tb, token.File)
		data, readErr := os.ReadFile(path)
		must(tb, readErr)
		if !strings.Contains(string(data), token.Sentinel) {
			tb.Fatalf("missing fixture sentinel %q", token.Sentinel)
		}
		must(
			tb,
			os.WriteFile(
				path,
				[]byte(strings.ReplaceAll(string(data), token.Sentinel, f.Resolve(tb, token.Value, ""))),
				0o600,
			),
		)
	}
}

func (f *Fixture) path(tb testing.TB, path string) string {
	tb.Helper()
	if !local(path) {
		tb.Fatalf("path escapes fixture: %q", path)
	}
	root, _, _ := strings.Cut(filepath.ToSlash(path), "/")
	if _, ok := f.repositories[root]; !ok && root != inputsDirectory {
		tb.Fatalf("path is outside declared trees: %q", path)
	}
	return filepath.Join(f.root, path)
}

func (f *Fixture) apply(tb testing.TB, op *operation) {
	tb.Helper()
	switch {
	case op.Overlay != "":
		if op.Overlay == "base" {
			return
		}
		f.applyOverlay(tb, op.Overlay)
	case op.Commit != "":
		f.checkout(tb, op.Commit)
	case op.NoGit != "":
		must(tb, os.RemoveAll(filepath.Join(f.root, op.NoGit, ".git")))
	case op.Delete != "":
		path := f.path(tb, op.Delete)
		must(tb, os.RemoveAll(path))
		f.forgetOrigins(path)
	case op.Rename != nil:
		f.renamePath(tb, op.Rename)
	case op.CacheFailure != nil:
		f.seedCacheFailure(tb, op.CacheFailure)
	case op.Symlink != nil:
		path := f.path(tb, op.Symlink.Path)
		must(tb, os.MkdirAll(filepath.Dir(path), 0o700))
		must(tb, os.Symlink(op.Symlink.Target, path))
		f.origins[path] = filepath.Join(f.dir, op.Symlink.Path)
	case op.Chmod != nil:
		must(tb, os.Chmod(f.path(tb, op.Chmod.Path), fs.FileMode(op.Chmod.Mode)))
	case op.Shallow != "":
		c := f.namedCommit(op.Shallow)
		must(tb, f.repositories[c.Repository].Storer.SetShallow([]plumbing.Hash{f.commits[c.Name]}))
	case op.MissingBlob != nil:
		c := f.namedCommit(op.MissingBlob.Commit)
		repo := f.repositories[c.Repository]
		obj, err := repo.CommitObject(f.commits[c.Name])
		must(tb, err)
		file, err := obj.File(op.MissingBlob.Path)
		must(tb, err)
		hash := file.Hash.String()
		must(tb, os.Remove(filepath.Join(f.root, c.Repository, ".git", "objects", hash[:2], hash[2:])))
	case op.Submodule != nil:
		f.addSubmodule(tb, op.Submodule)
	case op.Ref != nil:
		repo, ok := f.repositories[op.Ref.Repository]
		if !ok {
			tb.Fatal("unknown ref repository")
		}
		hash, ok := f.commits[op.Ref.Commit]
		if !ok {
			tb.Fatal("unknown ref commit")
		}
		must(tb, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(op.Ref.Name), hash)))
	}
}

func (f *Fixture) renamePath(tb testing.TB, move *rename) {
	tb.Helper()
	dest := f.path(tb, move.To)
	must(tb, os.MkdirAll(filepath.Dir(dest), 0o700))
	from := f.path(tb, move.From)
	must(tb, os.Rename(from, dest))
	moved := make(map[string]string)
	for path, source := range f.origins {
		if path == from || strings.HasPrefix(path, from+string(filepath.Separator)) {
			moved[dest+strings.TrimPrefix(path, from)] = source
		}
	}
	f.forgetOrigins(from)
	f.forgetOrigins(dest)
	maps.Copy(f.origins, moved)
}

func (f *Fixture) checkout(tb testing.TB, name string) {
	tb.Helper()
	c := f.namedCommit(name)
	wt, err := f.repositories[c.Repository].Worktree()
	must(tb, err)
	must(tb, wt.Checkout(&git.CheckoutOptions{Hash: f.commits[name], Force: true}))
	must(tb, wt.Clean(&git.CleanOptions{Dir: true}))
	f.useCommitOrigins(c.Repository, name)
}

func (f *Fixture) namedCommit(name string) *commit {
	for i := range f.schema.Commits {
		c := &f.schema.Commits[i]
		if c.Name == name {
			return c
		}
	}
	panic("validated commit missing")
}

func (f *Fixture) applyOverlay(tb testing.TB, name string) {
	tb.Helper()
	for _, overlay := range f.schema.Overlays {
		if overlay.Name != name {
			continue
		}
		if overlay.Directory != "" {
			for _, r := range f.schema.Repositories {
				sourcePath := filepath.Join(f.dir, overlay.Directory, r.Name)
				if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
					continue
				}
				must(tb, copyTree(sourcePath, filepath.Join(f.root, r.Name), f.origins))
			}
		}
		for i := range overlay.Operations {
			f.apply(tb, &overlay.Operations[i])
		}
		return
	}
	tb.Fatalf("unknown overlay %q", name)
}

// Load permits Git transport only to a declared repository's origin URL.
//
//nolint:ireturn // go-git's server.Loader contract requires storer.Storer.
func (f *Fixture) Load(endpoint *transport.Endpoint) (storer.Storer, error) {
	for _, r := range f.schema.Repositories {
		for _, remote := range r.Remotes {
			if remote.URL == endpoint.String() {
				return f.repositories[r.Name].Storer, nil
			}
		}
	}
	return nil, transport.ErrRepositoryNotFound
}

func (f *Fixture) seedCacheFailure(tb testing.TB, failure *cacheFailure) {
	tb.Helper()
	t, ok := tb.(*testing.T)
	if !ok {
		tb.Fatal("cache failures require a test")
	}
	// Final synthetic completion MUST precede real replay to avoid rollback invalidation.
	completed := time.Now()
	key := cacheFailureKey(tb, failure)
	synctest.Test(t, func(t *testing.T) {
		start := completed.Add(-time.Duration(failure.Attempts-1) * cacheFailureInterval)
		<-time.NewTimer(time.Until(start)).C
		c, err := sourcecache.New(t.Context(), f.CachePath(failure.Cache))
		must(t, err)
		defer func() { must(t, c.Close()) }()
		for attempt := range failure.Attempts {
			_, _, _, err = c.Blob(t.Context(), key, &sourcecache.Policy{},
				func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					return sourcecache.ProducerResult{}, &sourcecache.TerminalError{
						Err: syscall.ECONNRESET, Attempted: true,
					}
				})
			terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !terminal.Attempted || !errors.Is(err, syscall.ECONNRESET) {
				t.Fatalf("declared failure lost its attempted classification: %v", err)
			}
			if attempt+1 < failure.Attempts {
				<-time.NewTimer(cacheFailureInterval).C
			}
		}
	})
}

func cacheFailureKey(tb testing.TB, failure *cacheFailure) sourcecache.BlobKey {
	tb.Helper()
	repository := &manifest.HelmRepository{}
	repository.URL = failure.Repository
	chart := &manifest.HelmChartSource{}
	chart.Chart, chart.Version = failure.Chart, failure.Version
	material, err := source.ResolveTransport(nil, manifest.DiagnosticFor(repository.Named(), repository), nil)
	must(tb, err)
	return sourcecache.BlobKey{
		Content: struct{ Repository, Chart, Transport any }{
			repository.Content(), chart.Content(), material.Identity(nil),
		},
		Required: []string{"chart-version", "chart-url"},
	}
}

func (f *Fixture) addSubmodule(tb testing.TB, submodule *blob) {
	tb.Helper()
	c := f.namedCommit(submodule.Commit)
	repo := f.repositories[c.Repository]
	hash := f.commits[c.Name]
	commit, err := repo.CommitObject(hash)
	must(tb, err)
	tree, err := commit.Tree()
	must(tb, err)
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: submodule.Path, Mode: filemode.Submodule, Hash: hash})
	sort.Sort(object.TreeEntrySorter(tree.Entries))
	encoded := repo.Storer.NewEncodedObject()
	must(tb, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	must(tb, err)
	encoded = repo.Storer.NewEncodedObject()
	must(tb, commit.Encode(encoded))
	f.commits[c.Name], err = repo.Storer.SetEncodedObject(encoded)
	must(tb, err)
}
