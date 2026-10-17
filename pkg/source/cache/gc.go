package cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/oci"
)

// SweepOpts tunes Sweep's behavior.
type SweepOpts struct {
	// MaxAge retires named tags whose payload mtime predates now-MaxAge.
	// Coalesced access timestamps allow one second of retention grace.
	// Zero disables age retirement; unreachable Git generations are always collected.
	MaxAge time.Duration
	// DryRun reports the selection without locking or changing the cache.
	DryRun bool
}

// SweepResult summarizes Sweep's selection and any failures applying it.
type SweepResult struct {
	// Removed lists selected tags and absolute Git-generation paths, sorted together.
	Removed []string
	// Bytes counts selected Git-generation bytes only. Upstream graph GC decides
	// blob reclamation; dry-run does not estimate reclaimed blob bytes.
	Bytes int64
	// Errors aggregates failures applying the selection.
	Errors []error
}

// Sweep retires unused named tags and collects unreachable Git generations.
// Destructive sweeps require exclusive store ownership; dry-run never writes.
func Sweep(ctx context.Context, root string, opts SweepOpts) (SweepResult, error) {
	var res SweepResult
	if root == "" {
		return res, &CacheError{errors.New("sweep: empty cache root")}
	}
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return res, nil
		}
		return res, &CacheError{fmt.Errorf("stat sweep root: %w", err)}
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return res, &CacheError{fmt.Errorf("resolve sweep root: %w", err)}
	}
	r, err := os.OpenRoot(root)
	if err != nil {
		return res, &CacheError{fmt.Errorf("open sweep root: %w", err)}
	}
	defer func() { _ = r.Close() }()
	if _, err := r.Stat(ociDir); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return res, nil
		}
		return res, &CacheError{fmt.Errorf("stat store: %w", err)}
	}
	if !opts.DryRun {
		lock := flock.New(filepath.Join(r.Name(), ociLock), flock.SetPermissions(0o600))
		defer func() { _ = lock.Close() }()
		locked, err := lock.TryLock()
		if err != nil {
			return res, &CacheError{fmt.Errorf("lock sweep store: %w", err)}
		}
		if !locked {
			return res, &CacheError{errors.New("sweep: store is busy")}
		}
	}
	selection, err := selectSweep(ctx, r, opts.MaxAge)
	if err != nil {
		return res, err
	}
	res = selection.result
	if opts.DryRun {
		return res, nil
	}
	writable, err := oci.NewWithContext(ctx, filepath.Join(r.Name(), ociDir))
	if err != nil {
		return res, &CacheError{fmt.Errorf("open sweep store: %w", err)}
	}
	for _, tag := range selection.tags {
		if err := writable.Untag(ctx, tag); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("untag %q: %w", tag, err))
		}
	}
	// Failed retirement may leave a selected generation reachable.
	if len(res.Errors) > 0 {
		return res, nil
	}
	if err := writable.GC(ctx); err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("collect store blobs: %w", err))
		return res, nil
	}
	if err := writable.SaveIndex(); err != nil {
		res.Errors = append(res.Errors, fmt.Errorf("save swept index: %w", err))
		return res, nil
	}
	for _, path := range selection.generations {
		if err := r.RemoveAll(path); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("remove git generation: %w", err))
		}
	}
	return res, nil
}

// Selection MUST finish every read before maintenance opens a writable store.
type sweepSelection struct {
	root        *os.Root
	store       *oci.ReadOnlyStore
	retained    map[string]struct{}
	seen        map[digest.Digest]bool
	result      SweepResult
	tags        []string
	generations []string
}

func selectSweep(ctx context.Context, r *os.Root, maxAge time.Duration) (sweepSelection, error) {
	fsys, err := fs.Sub(r.FS(), ociDir)
	if err != nil {
		return sweepSelection{}, &CacheError{fmt.Errorf("open store: %w", err)}
	}
	store, err := oci.NewFromFS(ctx, fsys)
	if err != nil {
		return sweepSelection{}, &CacheError{fmt.Errorf("read sweep store: %w", err)}
	}
	selection := sweepSelection{root: r, store: store, retained: make(map[string]struct{}), seen: make(map[digest.Digest]bool)}
	var tags []string
	if err := store.Tags(ctx, "", func(names []string) error {
		tags = append(tags, names...)
		return nil
	}); err != nil {
		return sweepSelection{}, &CacheError{fmt.Errorf("list sweep tags: %w", err)}
	}
	cutoff := time.Time{}
	if maxAge > 0 {
		cutoff = time.Now().Add(-maxAge).Add(-accessInterval)
	}
	for _, tag := range tags {
		desc, err := store.Resolve(ctx, tag)
		if err != nil {
			return sweepSelection{}, &CacheError{fmt.Errorf("resolve sweep tag %q: %w", tag, err)}
		}
		if err := desc.Digest.Validate(); err != nil {
			return sweepSelection{}, &CacheError{fmt.Errorf("validate sweep tag %q: %w", tag, err)}
		}
		info, err := r.Stat(blobPath(desc.Digest))
		if err != nil {
			return sweepSelection{}, &CacheError{fmt.Errorf("stat sweep tag %q: %w", tag, err)}
		}
		keep := cutoff.IsZero() || info.ModTime().After(cutoff)
		if err := selection.visit(ctx, desc, keep); err != nil {
			return sweepSelection{}, &CacheError{fmt.Errorf("walk sweep tag %q: %w", tag, err)}
		}
		if !keep {
			selection.tags = append(selection.tags, tag)
		}
	}
	entries, err := fs.ReadDir(r.FS(), generationPath(""))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return sweepSelection{}, &CacheError{fmt.Errorf("read git generations: %w", err)}
	}
	for _, entry := range entries {
		id := entry.Name()
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.String() != id || !entry.IsDir() {
			continue
		}
		if _, ok := selection.retained[id]; ok {
			continue
		}
		path := generationPath(id)
		if err := fs.WalkDir(r.FS(), path, func(_ string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if !d.IsDir() {
				info, err := d.Info()
				if err != nil {
					return err
				}
				selection.result.Bytes += info.Size()
			}
			return nil
		}); err != nil {
			return sweepSelection{}, &CacheError{fmt.Errorf("size git generation: %w", err)}
		}
		selection.generations = append(selection.generations, path)
	}
	if err := ctx.Err(); err != nil {
		return sweepSelection{}, &CacheError{err}
	}
	selection.result.Removed = append(selection.result.Removed, selection.tags...)
	for _, path := range selection.generations {
		selection.result.Removed = append(selection.result.Removed, filepath.Join(r.Name(), path))
	}
	slices.Sort(selection.result.Removed)
	return selection, nil
}

func (s *sweepSelection) visit(ctx context.Context, desc ocispec.Descriptor, keep bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if id := desc.Annotations[prefix+"git-generation"]; id != "" {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed.String() != id {
			return fmt.Errorf("invalid git generation %q", id)
		}
		if keep {
			s.retained[id] = struct{}{}
		}
	}
	// Retained paths MUST revisit nodes first reached from a retiring tag.
	if kept, ok := s.seen[desc.Digest]; ok && (kept || !keep) {
		return nil
	}
	s.seen[desc.Digest] = keep
	if err := desc.Digest.Validate(); err != nil {
		return err
	}
	if _, err := s.root.Stat(blobPath(desc.Digest)); err != nil {
		return err
	}
	children, err := content.Successors(ctx, s.store, desc)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := s.visit(ctx, child, keep); err != nil {
			return err
		}
	}
	return nil
}
