package baseline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

type selection struct {
	repo    *git.Repository
	base    *resolution
	current *gittree.FS
	owner   *sourcecache.Cache
	content any
}

func selectChanges(ctx context.Context, s selection) (*gittree.FS, *change.Set, string, []manifest.Warning, error) {
	baseTree, entries, err := s.baseEntries(ctx)
	if err != nil {
		return nil, nil, "", nil, err
	}
	candidates, warnings, err := s.candidates(ctx, baseTree, entries)
	if err != nil {
		return nil, nil, "", warnings, err
	}
	if err := s.acquireBodies(ctx, entries, candidates); err != nil {
		return nil, nil, "", warnings, err
	}
	routing, err := s.compareCandidates(ctx, candidates, entries)
	if err != nil {
		return nil, nil, "", warnings, err
	}
	slices.Sort(routing)
	backing := s.repo.Storer.(*filesystem.Storage).Filesystem()
	if s.base.backing != nil {
		backing = s.base.backing
	}
	baseline, identity, err := gittree.New(ctx, backing, s.base.Hash, gittree.ViewOptions{Location: s.current.Location(), Content: s.content, Mode: gittree.Direct, Current: s.current, Changes: routing})
	if err != nil {
		return baseline, nil, "", warnings, err
	}
	changes, err := s.filterChanges(ctx, baseline, routing, entries)
	if err != nil {
		return &gittree.FS{Warnings: baseline.Warnings}, nil, "", warnings, err
	}
	return baseline, changes, identity, warnings, nil
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

func (s selection) candidates(ctx context.Context, baseTree *object.Tree, entries map[string]object.TreeEntry) (map[string]bool, []manifest.Warning, error) {
	candidates := make(map[string]bool)
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
				candidates[c.From.Name] = false
			}
			if c.To.Name != "" {
				candidates[c.To.Name] = false
			}
		}
	} else if errors.Is(err, plumbing.ErrReferenceNotFound) {
		for name := range entries {
			candidates[name] = true
		}
	} else {
		return nil, nil, err
	}
	worktree, err := s.repo.Worktree()
	if err != nil {
		return nil, nil, err
	}
	status, statusErr := worktree.Status()
	var warnings []manifest.Warning
	if statusErr != nil {
		warnings = []manifest.Warning{{Category: manifest.WarnBaselineStatusUnavailable,
			Message: "Git status unavailable; comparing checkout conservatively", Count: 1, Detail: []string{statusErr.Error()}}}
	}
	if err := ctx.Err(); err != nil {
		return nil, warnings, err
	}
	for name, st := range status {
		if st.Staging != git.Unmodified || st.Worktree != git.Unmodified {
			candidates[name] = true
		}
	}
	err = walkCurrent(ctx, s.current, ".", func(name string) {
		if _, ok := headEntries[name]; !ok {
			candidates[name] = true
		}
	})
	if err != nil {
		return nil, warnings, err
	}
	if statusErr != nil {
		for name := range entries {
			candidates[name] = true
		}
	}
	return candidates, warnings, nil
}

func (s selection) acquireBodies(ctx context.Context, entries map[string]object.TreeEntry, candidates map[string]bool) error {
	routed := make(map[plumbing.Hash]bool)
	missing := false
	for name, entry := range entries {
		if _, ok := candidates[name]; !ok {
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
	return s.base.adopt(s.repo.Storer)
}

func (s selection) compareCandidates(ctx context.Context, candidates map[string]bool, entries map[string]object.TreeEntry) ([]string, error) {
	routing := make([]string, 0, len(candidates))
	for name, dirty := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, exists := entries[name]
		if !exists {
			routing = append(routing, name)
			continue
		}
		if !dirty {
			routing = append(routing, name)
			continue
		}
		info, err := s.current.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
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
	beforeIgnore, err := baseline.SourcePolicy(ctx)
	if err != nil {
		return nil, err
	}
	afterIgnore, err := s.current.SourcePolicy(ctx)
	if err != nil {
		return nil, err
	}
	before, after := make(map[string]bool, len(routing)), make(map[string]bool, len(routing))
	for _, name := range routing {
		if _, existed := entries[name]; existed {
			before[name] = true
		}
		if _, err := s.current.Lstat(name); err == nil {
			after[name] = true
		}
	}
	var membership []string
	if slices.ContainsFunc(routing, func(name string) bool { return path.Base(name) == ".sourceignore" }) {
		if err := walkCurrent(ctx, s.current, ".", func(name string) {
			if _, existed := entries[name]; existed {
				membership = append(membership, name)
				before[name], after[name] = true, true
			}
		}); err != nil {
			return nil, err
		}
	}
	sourceignore.Select(beforeIgnore, before, nil)
	sourceignore.Select(afterIgnore, after, nil)
	selected := make([]string, 0, len(routing))
	for _, name := range routing {
		if before[name] || after[name] {
			selected = append(selected, name)
		}
	}
	for _, name := range membership {
		if before[name] != after[name] {
			selected = append(selected, name)
		}
	}
	slices.Sort(selected)
	selected = slices.Compact(selected)
	changes := change.NewSet(selected)
	for _, name := range selected {
		if before[name] == after[name] {
			continue
		}
		var input fs.FS = s.current
		if before[name] {
			input = baseline
		}
		if err := changes.CollectInputs(ctx, input, name, before[name]); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return changes, nil
}

func (s selection) acquireBase(ctx context.Context) error {
	url, ok := originURL(s.current.Location().Config)
	if !ok {
		return fmt.Errorf("%w: base %s is missing objects and no origin is configured", manifest.ErrInput, s.base.Hash)
	}
	r, err := fetchBase(ctx, url, s.base.Hash.String(), s.owner)
	if err != nil {
		return err
	}
	if err := r.adopt(s.repo.Storer); err != nil {
		return err
	}
	r.Source = s.base.Source
	*s.base = *r
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

func walkCurrent(ctx context.Context, input fs.ReadDirFS, dir string, visit func(string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := input.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := path.Join(dir, entry.Name())
		if entry.IsDir() {
			if err := walkCurrent(ctx, input, name, visit); err != nil {
				return err
			}
		} else if entry.Type().IsRegular() {
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
