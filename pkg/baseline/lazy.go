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
	gitindex "github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

type selection struct {
	repo    *git.Repository
	base    *resolution
	current *gittree.FS
	input   *store.SourceArtifact
	owner   *sourcecache.Cache
	content any
}

func selectChanges(ctx context.Context, s selection) (*gittree.FS, *change.Set, string, []manifest.Warning, error) {
	entries, comparison, err := s.baseEntries(ctx)
	if err != nil {
		return nil, nil, "", nil, err
	}
	candidates, warnings, err := s.candidates(ctx, entries, comparison)
	if err != nil {
		return nil, nil, "", warnings, err
	}
	if localErr := s.acquireBodies(ctx, entries, candidates); localErr != nil {
		return nil, nil, "", warnings, localErr
	}
	routing := slices.Sorted(maps.Keys(candidates))
	backing := s.repo.Storer.(*filesystem.Storage).Filesystem() //nolint:errcheck // The repository uses filesystem storage.
	if s.base.backing != nil {
		backing = s.base.backing
	}
	// go-git initializes its pack index without synchronization, before readers share it.
	if localErr := s.current.Location().Repository.Storer.HasEncodedObject(
		plumbing.ZeroHash,
	); localErr != nil &&
		!errors.Is(localErr, plumbing.ErrObjectNotFound) {
		return nil, nil, "", warnings, fmt.Errorf("hasencodedobject: %w", localErr)
	}
	opts := gittree.ViewOptions{
		Location: s.current.Location(),
		Content:  s.content,
		Mode:     gittree.Direct,
		Current:  s.current,
		Changes:  routing,
	}
	baseline, identity, err := gittree.New(ctx, backing, s.base.Hash, opts)
	if err != nil {
		return baseline, nil, "", warnings, err
	}
	current := s.input
	if current == nil {
		current = &store.SourceArtifact{FS: s.current}
	}
	original := &store.SourceArtifact{
		FS:       baseline,
		Key:      identity,
		Content:  identity,
		Revision: s.base.Hash.String(),
		Kind:     manifest.KindGitRepository,
	}
	if location := s.current.Location(); location != nil {
		original.LocalPath, _ = gittree.Label(
			gittree.LabelOptions{Root: location.ContentRoot, Mode: gittree.Direct, Key: s.base.Hash.String()},
		)
		original.LocalRoot = location.ContentRoot
	}
	changes, err := change.Detect(ctx,
		original,
		current,
		change.DetectConfig{Candidates: candidates},
	)
	if err != nil {
		return &gittree.FS{Warnings: baseline.Warnings}, nil, "", warnings, err
	}
	selected := changes.Paths()
	if !slices.Equal(routing, selected) {
		opts.Changes = selected
		baseline, _, err = gittree.New(ctx, backing, s.base.Hash, opts)
		if err != nil {
			return baseline, nil, "", warnings, err
		}
		if _, err := baseline.SourcePolicy(ctx); err != nil {
			return &gittree.FS{Warnings: baseline.Warnings}, nil, "", warnings, err
		}
		original.FS = baseline
	}
	return baseline, changes, identity, warnings, nil
}

func (s selection) baseEntries(ctx context.Context) (map[string]object.TreeEntry, *gitindex.Index, error) {
	var baseTree *object.Tree
	var entries map[string]object.TreeEntry
	var comparison *gitindex.Index
	for {
		before, err := s.base.Repo.CommitObject(s.base.Hash)
		if err == nil {
			baseTree, err = before.Tree()
		}
		if err == nil {
			entries, comparison, err = treeEntries(ctx, baseTree)
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
	return entries, comparison, nil
}

func (s selection) candidates(
	ctx context.Context,
	entries map[string]object.TreeEntry,
	comparison *gitindex.Index,
) (map[string]bool, []manifest.Warning, error) {
	candidates := make(map[string]bool)
	head, err := s.repo.Head()
	if err != nil && !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return nil, nil, fmt.Errorf("head: %w", err)
	}
	if err == nil {
		commit, commitErr := s.repo.CommitObject(head.Hash())
		if commitErr != nil {
			return nil, nil, fmt.Errorf("commitobject: %w", commitErr)
		}
		if _, treeErr := commit.Tree(); treeErr != nil {
			return nil, nil, fmt.Errorf("tree: %w", treeErr)
		}
	}
	worktree, err := s.repo.Worktree()
	if err != nil {
		return nil, nil, fmt.Errorf("worktree: %w", err)
	}
	status, statusErr := s.worktreeChanges(ctx, worktree, comparison)
	var warnings []manifest.Warning
	if statusErr != nil {
		warnings = []manifest.Warning{
			{
				Category: manifest.WarnBaselineStatusUnavailable,
				Message:  "Git status unavailable; comparing checkout conservatively",
				Count:    1,
				Detail:   []string{statusErr.Error()},
			},
		}
	}
	if localErr := ctx.Err(); localErr != nil {
		return nil, warnings, fmt.Errorf("context: %w", localErr)
	}
	for _, change := range status {
		if name := change.From.String(); name != "" {
			candidates[name] = false
		}
		if name := change.To.String(); name != "" {
			// Upstream uses a zero content hash when checkout reads fail.
			// Such observations MUST retain the ordinary comparison's I/O error.
			uncertain := bytes.Equal(change.To.Hash()[:len(plumbing.ZeroHash)], plumbing.ZeroHash[:])
			candidates[name] = uncertain
		}
	}
	if statusErr != nil {
		for name := range entries {
			candidates[name] = true
		}
		if err := walkCurrent(ctx, s.current, ".", func(name string) { candidates[name] = true }); err != nil {
			return nil, warnings, err
		}
	}
	return candidates, warnings, nil
}

func (s selection) acquireBodies(
	ctx context.Context,
	entries map[string]object.TreeEntry,
	candidates map[string]bool,
) error {
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
			return fmt.Errorf("hasencodedobject: %w", err)
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
	generation, _, err := s.owner.Git(
		ctx,
		sourcecache.GitKey{Content: content, Ref: s.base.Hash.String()},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		func(ctx context.Context, upper storage.Storer, _ billy.Filesystem) (plumbing.Hash, error) {
			if err := copyMetadata(ctx, s.base.Repo.Storer, upper, s.base.Hash); err != nil {
				return plumbing.ZeroHash, err
			}
			if err := s.transferBodies(ctx, upper, hashes); err != nil {
				return plumbing.ZeroHash, err
			}
			return s.base.Hash, nil
		},
	)
	if err != nil {
		return err
	}
	s.base.backing = generation.Backing
	return s.base.adopt(s.repo.Storer)
}

func (s selection) transferBodies(ctx context.Context, upper storage.Storer, hashes []plumbing.Hash) error {
	var wants []plumbing.Hash
	for _, hash := range hashes {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		obj, err := s.base.Repo.Storer.EncodedObject(plumbing.BlobObject, hash)
		if errors.Is(err, plumbing.ErrObjectNotFound) {
			wants = append(wants, hash)
		} else if err != nil {
			return fmt.Errorf("encodedobject: %w", err)
		} else if _, err := upper.SetEncodedObject(obj); err != nil {
			return fmt.Errorf("setencodedobject: %w", err)
		}
	}
	if len(wants) > 0 {
		if err := s.base.remote.fetch(ctx, upper, wants); err != nil {
			return err
		}
	}
	if err := s.ensureBodies(ctx, upper, wants); err != nil {
		return err
	}
	for _, hash := range hashes {
		if err := upper.HasEncodedObject(hash); err != nil {
			return fmt.Errorf("hasencodedobject: %w", err)
		}
	}
	return nil
}

func (s selection) ensureBodies(ctx context.Context, upper storage.Storer, wants []plumbing.Hash) error {
	for _, hash := range wants {
		err := upper.HasEncodedObject(hash)
		if errors.Is(err, plumbing.ErrObjectNotFound) && !s.base.remote.full {
			s.base.remote.full = true
			if localErr := s.base.remote.fetch(ctx, upper, nil); localErr != nil {
				return localErr
			}
		} else if err != nil {
			return fmt.Errorf("hasencodedobject: %w", err)
		}
	}
	return nil
}

func (s selection) acquireBase(ctx context.Context) error {
	url, ok := originURL(s.current.Location().Config)
	if !ok {
		return manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf("base %s is missing objects and no origin is configured", s.base.Hash),
		)
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

func treeEntries(ctx context.Context, root *object.Tree) (map[string]object.TreeEntry, *gitindex.Index, error) {
	comparison := &gitindex.Index{}
	out := make(map[string]object.TreeEntry)
	walker := object.NewTreeWalker(root, true, nil)
	defer walker.Close()
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, fmt.Errorf("context: %w", err)
		}
		name, entry, err := walker.Next()
		if errors.Is(err, io.EOF) {
			return out, comparison, nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("next: %w", err)
		}
		if entry.Mode.IsFile() {
			comparison.Entries = append(
				comparison.Entries,
				&gitindex.Entry{Name: name, Hash: entry.Hash, Mode: entry.Mode},
			)
		}
		if entry.Mode.IsFile() && entry.Mode != filemode.Symlink {
			out[name] = entry
		}
	}
}

func walkCurrent(ctx context.Context, input fs.ReadDirFS, dir string, visit func(string)) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	entries, err := input.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
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
