package baseline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	objectcache "github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/transactional"

	"github.com/home-operations/flate/pkg/change"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

type selection struct {
	repo     *git.Repository
	base     *resolution
	current  *gittree.FS
	worktree billy.Filesystem
	owner    *sourcecache.Cache
	content  any
}

func selectChanges(ctx context.Context, s selection) (*gittree.FS, *change.Set, string, error) {
	baseTree, entries, err := s.baseEntries(ctx)
	if err != nil {
		return nil, nil, "", err
	}
	candidates, dirty, err := s.candidates(ctx, baseTree, entries)
	if err != nil {
		return nil, nil, "", err
	}
	if err := s.acquireBodies(ctx, entries, candidates); err != nil {
		return nil, nil, "", err
	}
	routing, err := s.compareCandidates(ctx, candidates, dirty, entries)
	if err != nil {
		return nil, nil, "", err
	}
	slices.Sort(routing)
	backing := s.repo.Storer.(*filesystem.Storage).Filesystem()
	if s.base.backing != nil {
		backing = s.base.backing
	}
	baseline, identity, err := gittree.New(ctx, backing, s.base.Hash, gittree.ViewOptions{Content: s.content, Mode: gittree.Direct, Current: s.current, Changes: routing})
	if err != nil {
		return nil, nil, "", err
	}
	changes, err := s.filterChanges(ctx, baseline, routing, entries)
	if err != nil {
		return nil, nil, "", err
	}
	return baseline, changes, identity, nil
}

func (s selection) baseEntries(ctx context.Context) (*object.Tree, map[string]object.TreeEntry, error) {
	var baseTree *object.Tree
	var entries map[string]object.TreeEntry
	for {
		before, err := s.base.Repo.CommitObject(s.base.Hash)
		if err == nil {
			baseTree, err = before.Tree()
		}
		if err == nil {
			entries, err = treeEntries(ctx, baseTree)
		}
		if err == nil {
			break
		}
		if !errors.Is(err, plumbing.ErrObjectNotFound) || s.base.remote != nil {
			return nil, nil, err
		}
		if err := s.acquireBase(ctx); err != nil {
			return nil, nil, err
		}
	}
	return baseTree, entries, nil
}

func (s selection) candidates(ctx context.Context, baseTree *object.Tree, entries map[string]object.TreeEntry) (map[string]bool, map[string]bool, error) {
	candidates := make(map[string]bool)
	dirty := make(map[string]bool)
	headEntries := map[string]object.TreeEntry{}
	head, err := s.repo.Head()
	if err == nil {
		commit, err := s.repo.CommitObject(head.Hash())
		if err != nil {
			return nil, nil, err
		}
		headTree, err := commit.Tree()
		if err != nil {
			return nil, nil, err
		}
		headEntries, err = treeEntries(ctx, headTree)
		if err != nil {
			return nil, nil, err
		}
		diff, err := object.DiffTree(baseTree, headTree)
		if err != nil {
			return nil, nil, err
		}
		for _, c := range diff {
			if c.From.Name != "" {
				candidates[c.From.Name] = true
			}
			if c.To.Name != "" {
				candidates[c.To.Name] = true
			}
		}
	} else if errors.Is(err, plumbing.ErrReferenceNotFound) {
		for name := range entries {
			candidates[name], dirty[name] = true, true
		}
	} else {
		return nil, nil, err
	}
	worktree, err := s.repo.Worktree()
	if err != nil {
		return nil, nil, err
	}
	status, statusErr := worktree.Status()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if statusErr != nil {
		slog.Warn("baseline: Git status unavailable; comparing checkout conservatively", "error", statusErr)
	}
	for name, st := range status {
		if st.Staging != git.Unmodified || st.Worktree != git.Unmodified {
			candidates[name] = true
			dirty[name] = true
		}
	}
	err = walkCurrent(ctx, s.worktree, ".", func(name string) {
		if _, ok := headEntries[name]; !ok {
			candidates[name] = true
			dirty[name] = true
		}
	})
	if err != nil {
		return nil, nil, err
	}
	if statusErr != nil {
		for name := range entries {
			candidates[name] = true
			dirty[name] = true
		}
	}
	return candidates, dirty, nil
}

func (s selection) acquireBodies(ctx context.Context, entries map[string]object.TreeEntry, candidates map[string]bool) error {
	routed := make(map[plumbing.Hash]bool)
	missing := false
	for name, entry := range entries {
		if !candidates[name] {
			continue
		}
		routed[entry.Hash] = true
		if err := s.base.Repo.Storer.HasEncodedObject(entry.Hash); errors.Is(err, plumbing.ErrObjectNotFound) {
			missing = true
		} else if err != nil {
			return err
		}
	}
	if missing && s.base.remote == nil {
		if err := s.acquireBase(ctx); err != nil {
			return err
		}
	}
	if s.base.remote == nil {
		return nil
	}
	hashes := slices.SortedFunc(maps.Keys(routed), func(a, b plumbing.Hash) int { return bytes.Compare(a[:], b[:]) })
	content := struct {
		URL    string
		Commit string
		Wants  []plumbing.Hash
	}{s.base.remote.url, s.base.Hash.String(), hashes}
	generation, _, err := s.owner.Git(ctx, sourcecache.GitKey{Content: content, Ref: s.base.Hash.String()}, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(ctx context.Context, upper storage.Storer, _ billy.Filesystem) (plumbing.Hash, error) {
		if err := copyMetadata(ctx, s.base.Repo.Storer, upper, s.base.Hash); err != nil {
			return plumbing.ZeroHash, err
		}
		var wants []plumbing.Hash
		for _, hash := range hashes {
			if err := ctx.Err(); err != nil {
				return plumbing.ZeroHash, err
			}
			obj, err := s.base.Repo.Storer.EncodedObject(plumbing.BlobObject, hash)
			if errors.Is(err, plumbing.ErrObjectNotFound) {
				wants = append(wants, hash)
			} else if err != nil {
				return plumbing.ZeroHash, err
			} else if _, err := upper.SetEncodedObject(obj); err != nil {
				return plumbing.ZeroHash, err
			}
		}
		if len(wants) > 0 {
			if err := s.base.remote.fetch(ctx, upper, wants); err != nil {
				return plumbing.ZeroHash, err
			}
		}
		for _, hash := range wants {
			err := upper.HasEncodedObject(hash)
			if errors.Is(err, plumbing.ErrObjectNotFound) && !s.base.remote.full {
				s.base.remote.full = true
				if err := s.base.remote.fetch(ctx, upper, nil); err != nil {
					return plumbing.ZeroHash, err
				}
			} else if err != nil {
				return plumbing.ZeroHash, err
			}
		}
		for _, hash := range hashes {
			if err := upper.HasEncodedObject(hash); err != nil {
				return plumbing.ZeroHash, err
			}
		}
		return s.base.Hash, nil
	})
	if err != nil {
		return err
	}
	s.base.backing = generation.Backing
	upper := filesystem.NewStorageWithOptions(generation.Backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	s.base.Repo = &git.Repository{Storer: transactional.NewStorage(s.repo.Storer, upper)}
	if _, err := upper.Reference(fullBaseRef); err == nil {
		s.base.remote.full = true
	} else if !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return err
	}
	return nil
}

func (s selection) compareCandidates(ctx context.Context, candidates, dirty map[string]bool, entries map[string]object.TreeEntry) ([]string, error) {
	routing := make([]string, 0, len(candidates))
	for name := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, exists := entries[name]
		if !exists {
			routing = append(routing, name)
			continue
		}
		if !dirty[name] {
			routing = append(routing, name)
			continue
		}
		info, err := s.current.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			routing = append(routing, name)
			continue
		}
		if err != nil {
			return nil, err
		}
		mode, err := entry.Mode.ToOSFileMode()
		if err != nil {
			return nil, err
		}
		if info.Mode().Type() != mode.Type() || info.Mode().Perm()&0o111 != mode.Perm()&0o111 {
			routing = append(routing, name)
			continue
		}
		blob, err := object.GetBlob(s.base.Repo.Storer, entry.Hash)
		if err != nil {
			return nil, err
		}
		reader, err := blob.Reader()
		if err != nil {
			return nil, err
		}
		current, err := s.current.Open(name)
		if err != nil {
			_ = reader.Close()
			return nil, err
		}
		equal, compareErr := equalReaders(ctx, reader, current)
		if err := errors.Join(compareErr, reader.Close(), current.Close()); err != nil {
			return nil, err
		}
		if !equal {
			routing = append(routing, name)
		}
	}
	return routing, nil
}

func (s selection) filterChanges(ctx context.Context, baseline *gittree.FS, routing []string, entries map[string]object.TreeEntry) (*change.Set, error) {
	beforeIgnore, err := sourceignore.NewFS(baseline, nil, "", true)
	if err != nil {
		return nil, err
	}
	afterIgnore, err := sourceignore.NewFS(s.current, nil, "", true)
	if err != nil {
		return nil, err
	}
	selected := make([]string, 0, len(routing))
	for _, name := range routing {
		_, existed := entries[name]
		_, currentErr := s.current.Lstat(name)
		if (existed && !beforeIgnore.Match(filepath.FromSlash(name), false)) || (currentErr == nil && !afterIgnore.Match(filepath.FromSlash(name), false)) {
			selected = append(selected, name)
		}
	}
	if slices.ContainsFunc(routing, func(name string) bool { return path.Base(name) == ".sourceignore" }) {
		if err := walkCurrent(ctx, s.worktree, ".", func(name string) {
			if _, existed := entries[name]; existed &&
				beforeIgnore.Match(filepath.FromSlash(name), false) != afterIgnore.Match(filepath.FromSlash(name), false) {
				selected = append(selected, name)
			}
		}); err != nil {
			return nil, err
		}
	}
	return change.NewSet(selected), nil
}

func (s selection) acquireBase(ctx context.Context) error {
	url, ok := originURL(s.repo)
	if !ok {
		return fmt.Errorf("base %s is missing objects and no origin is configured", s.base.Hash)
	}
	fetched, err := fetchBase(ctx, url, s.base.Hash.String(), s.owner)
	if err != nil {
		return err
	}
	s.base.remote, s.base.backing = fetched.remote, fetched.backing
	upper := filesystem.NewStorageWithOptions(fetched.backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	s.base.Repo = &git.Repository{Storer: transactional.NewStorage(s.repo.Storer, upper)}
	return nil
}

func treeEntries(ctx context.Context, root *object.Tree) (map[string]object.TreeEntry, error) {
	out := make(map[string]object.TreeEntry)
	walker := object.NewTreeWalker(root, true, nil)
	defer walker.Close()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, entry, err := walker.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if entry.Mode.IsFile() && entry.Mode != filemode.Symlink {
			out[name] = entry
		}
	}
}

func walkCurrent(ctx context.Context, input billy.Filesystem, dir string, visit func(string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := input.ReadDir(filepath.FromSlash(dir))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == ".git" {
			continue
		}
		name := path.Join(dir, entry.Name())
		if entry.IsDir() {
			if err := walkCurrent(ctx, input, name, visit); err != nil {
				return err
			}
		} else if entry.Mode().IsRegular() {
			visit(name)
		}
	}
	return nil
}

func equalReaders(ctx context.Context, a, b io.Reader) (bool, error) {
	var aa, bb [32 * 1024]byte
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		na, ea := io.ReadFull(a, aa[:])
		nb, eb := io.ReadFull(b, bb[:])
		if ea != nil && !errors.Is(ea, io.EOF) && !errors.Is(ea, io.ErrUnexpectedEOF) {
			return false, ea
		}
		if eb != nil && !errors.Is(eb, io.EOF) && !errors.Is(eb, io.ErrUnexpectedEOF) {
			return false, eb
		}
		if na != nb || !bytes.Equal(aa[:na], bb[:nb]) {
			return false, nil
		}
		if na < len(aa) {
			return true, nil
		}
	}
}
