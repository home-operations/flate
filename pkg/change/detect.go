package change

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Set is the immutable result of Detect, containing changed relative paths
// and exact one-sided data identities for selection.
type Set struct {
	paths  map[string]struct{}
	inputs map[manifest.NamedResource]struct{}
}

// NewSet constructs a path-only Set without Detect's one-sided input identities.
func NewSet(paths []string) *Set {
	out := &Set{paths: make(map[string]struct{}, len(paths))}
	for _, p := range paths {
		out.paths[filepath.ToSlash(p)] = struct{}{}
	}
	return out
}

// Len reports how many files differ.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.paths)
}

// Paths returns the changed files as a sorted slice.
func (s *Set) Paths() []string {
	if s == nil {
		return nil
	}
	// Stable order makes logs and CI output deterministic.
	return slices.Sorted(maps.Keys(s.paths))
}

// Contains reports whether rel is in the change set. rel is expected
// to be filepath.ToSlash-normalized.
func (s *Set) Contains(rel string) bool {
	if s == nil {
		return false
	}
	_, ok := s.paths[filepath.ToSlash(rel)]
	return ok
}

// Detect compares admitted filesystems under each side's Flux ignore policy.
func Detect(ctx context.Context, before, after *store.SourceArtifact) (*Set, error) {
	if before == nil || after == nil || before.FS == nil || after.FS == nil {
		return nil, fmt.Errorf("%w: both admitted inputs required", manifest.ErrInput)
	}
	var bef, aft map[string]fileMeta
	eg, scanCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		bef, err = scanTree(scanCtx, before, after)
		return err
	})
	eg.Go(func() error {
		var err error
		aft, err = scanTree(scanCtx, after, before)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	paths := make(map[string]struct{}, len(aft)/8)
	type hashJob struct {
		name    string
		changed bool
	}
	var jobs []hashJob
	var oneSided []string
	for name, right := range aft {
		left, ok := bef[name]
		if !ok || left.size != right.size {
			paths[name] = struct{}{}
			if !ok {
				oneSided = append(oneSided, name)
			}
			continue
		}
		jobs = append(jobs, hashJob{name: name})
	}
	for name := range bef {
		if _, ok := aft[name]; !ok {
			paths[name] = struct{}{}
			oneSided = append(oneSided, name)
		}
	}
	hg, hashCtx := errgroup.WithContext(ctx)
	const hashWorkers = 8
	for worker := range min(hashWorkers, len(jobs)) {
		hg.Go(func() error {
			for i := worker; i < len(jobs); i += hashWorkers {
				if err := hashCtx.Err(); err != nil {
					return err
				}
				job := &jobs[i]
				b, err := hashFile(hashCtx, before.FS, job.name)
				if err != nil {
					return err
				}
				a, err := hashFile(hashCtx, after.FS, job.name)
				if err != nil {
					return err
				}
				job.changed = a != b
			}
			return nil
		})
	}
	if err := hg.Wait(); err != nil {
		return nil, err
	}
	for _, job := range jobs {
		if job.changed {
			paths[job.name] = struct{}{}
		}
	}
	out := &Set{paths: paths}
	slices.Sort(oneSided)
	for _, name := range oneSided {
		input := before.FS
		if _, ok := aft[name]; ok {
			input = after.FS
		}
		if err := out.collectInputs(ctx, input, name); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type inputReader func([]byte) (int, error)

func (r inputReader) Read(p []byte) (int, error) { return r(p) }

func (s *Set) collectInputs(ctx context.Context, input fs.FS, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yaml", ".yml", ".json":
	default:
		return nil
	}
	f, err := input.Open(name)
	if err != nil {
		return fmt.Errorf("%w: open input %s: %w", manifest.ErrInput, name, err)
	}
	defer func() { _ = f.Close() }()
	var terminal, readErr error
	reader := inputReader(func(p []byte) (int, error) {
		if err := ctx.Err(); err != nil {
			terminal = err
			return 0, err
		}
		n, err := f.Read(p)
		terminal = err
		if err != nil && err != io.EOF { //nolint:errorlint // io.Reader requires an unwrapped EOF; preserve other read errors.
			readErr = err
		}
		if err := ctx.Err(); err != nil {
			terminal = err
			return 0, err
		}
		return n, err
	})
	docs, decodeErr := manifest.DecodeDocs(reader)
	if decodeErr != nil && terminal == nil {
		// A parser error must not hide later file errors or cancellation.
		_, _ = io.Copy(io.Discard, reader)
	}
	err = ctx.Err()
	if err == nil && readErr != nil {
		err = fmt.Errorf("%w: read input %s: %w", manifest.ErrInput, name, readErr)
	}
	if err != nil {
		for _, doc := range docs {
			manifest.ReleaseDoc(doc)
		}
		return err
	}
	if decodeErr != nil {
		return nil
	}
	for _, doc := range docs {
		id := manifest.NamedResource{Kind: manifest.DocKind(doc)}
		id.Name, id.Namespace = manifest.DocMetadata(doc)
		eligible := manifest.DocAPIVersion(doc) == manifest.CoreAPIVersion && id.KindInfo().Data &&
			id.Name != "" && !manifest.HasEnvsubstReference(id.Name) && !manifest.HasEnvsubstReference(id.Namespace)
		manifest.ReleaseDoc(doc)
		if eligible {
			if s.inputs == nil {
				s.inputs = make(map[manifest.NamedResource]struct{})
			}
			s.inputs[id] = struct{}{}
		}
	}
	return ctx.Err()
}

type fileMeta struct {
	size int64
}

// Excluded directories MUST still be traversed so deeper .sourceignore re-includes
// survive, matching github.com/fluxcd/pkg/sourceignore v0.19.0 LoadIgnorePatterns.
func scanTree(ctx context.Context, input, opposite *store.SourceArtifact) (map[string]fileMeta, error) {
	out := make(map[string]fileMeta)
	scoped := input.FS
	oppositeName, err := filepath.Rel(input.LocalPath, opposite.LocalPath)
	if err == nil {
		oppositeName = filepath.ToSlash(oppositeName)
	}

	files := []string{}
	err = fs.WalkDir(scoped, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name != "." && (d.Name() == ".git" || name == oppositeName) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.Name() == ".sourceignore" {
			files = append(files, name)
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		out[name] = fileMeta{size: info.Size()}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var matcher *sourceignore.Matcher
	if admitted, ok := scoped.(*gittree.FS); ok {
		matcher, err = admitted.SourcePolicy(ctx)
	} else {
		matcher, err = sourceignore.NewFS(scoped, files, "", true)
	}
	if err != nil {
		return nil, err
	}
	sourceignore.Select(matcher, out, files)
	return out, nil
}

type hashBuffer struct {
	hash  hash.Hash
	bytes [32 * 1024]byte
	sum   [sha256.Size]byte
}

var hashBuffers = sync.Pool{New: func() any {
	return &hashBuffer{hash: sha256.New()}
}}

func hashFile(ctx context.Context, input fs.FS, name string) ([sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, err
	}
	f, err := input.Open(name)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer func() { _ = f.Close() }()
	buf := hashBuffers.Get().(*hashBuffer)
	defer hashBuffers.Put(buf)
	h := buf.hash
	h.Reset()
	for {
		if err := ctx.Err(); err != nil {
			return [sha256.Size]byte{}, err
		}
		n, err := f.Read(buf.bytes[:])
		if n > 0 {
			_, _ = h.Write(buf.bytes[:n])
		}
		if err == io.EOF { //nolint:errorlint // io.Reader requires an unwrapped EOF; preserve other read errors.
			break
		}
		if err != nil {
			return [sha256.Size]byte{}, err
		}
	}
	h.Sum(buf.sum[:0])
	return buf.sum, nil
}
