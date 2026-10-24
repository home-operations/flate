package change

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Set carries immutable bootstrap-path changes and qualified source evidence.
// Completed sets are immutable and safe to share; CollectInputs is construction-only.
type Set struct {
	oneSided [2]map[string]struct{}
	origins  [2]*store.SourceArtifact
	inputs   map[manifest.NamedResource]string
	paths    map[string]struct{}
	sources  map[manifest.NamedResource]sourceDelta
	groups   []sourceGroup
}

type sourceDelta struct {
	changes      *Set
	artifacts    [2]*store.SourceArtifact
	declarations [2]manifest.BaseManifest
}

type sourceGroup struct {
	changes *Set
	refs    []manifest.NamedResource
}

// Qualify compares available admitted source pairs before reconciliation.
// Supplied changes retain their bootstrap coordinates and are never mutated.
func Qualify(ctx context.Context, changes *Set, before, after *store.Store) (*Set, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stores := [2]*store.Store{before, after}
	ids := map[manifest.NamedResource]struct{}{manifest.BootstrapSourceID: {}}
	for _, st := range stores {
		for _, obj := range st.Objects() {
			if _, ok := st.GetArtifact(obj.Named()).(*store.SourceArtifact); ok {
				ids[obj.Named()] = struct{}{}
			}
		}
	}
	out := &Set{sources: make(map[manifest.NamedResource]sourceDelta, len(ids))}
	type readerPair struct {
		before, after any
		exclusions    [2]string
	}
	compared := map[readerPair]*Set{}
	for _, id := range slices.SortedFunc(maps.Keys(ids), manifest.NamedResource.Compare) {
		evidence := sourceDelta{}
		for i, st := range stores {
			evidence.artifacts[i], _ = st.GetArtifact(id).(*store.SourceArtifact)
			evidence.declarations[i] = st.GetObject(id)
		}
		a, b := evidence.artifacts[0], evidence.artifacts[1]
		if a == nil || b == nil || a.FS == nil || b.FS == nil {
			if id == manifest.BootstrapSourceID && changes == nil {
				return nil, fmt.Errorf("%w: both admitted inputs required", manifest.ErrInput)
			}
			continue
		}
		delta := changes
		if id != manifest.BootstrapSourceID || delta == nil {
			pair := readerPair{before: a, after: b}
			for i, input := range evidence.artifacts {
				exclusion, err := filepath.Rel(input.LocalPath, evidence.artifacts[1-i].LocalPath)
				exclusion = filepath.ToSlash(exclusion)
				if err == nil && fs.ValidPath(exclusion) && exclusion != "." {
					pair.exclusions[i] = exclusion
				}
			}
			if reflect.ValueOf(a.FS).Comparable() && reflect.ValueOf(b.FS).Comparable() {
				pair.before, pair.after = a.FS, b.FS
			}
			delta = compared[pair]
			if delta == nil {
				selected := id != manifest.BootstrapSourceID
				equal := selected && a.Key != "" && a.Key == b.Key && a.Revision == b.Revision && a.Digest == b.Digest
				if selected && !equal && a.ContentFingerprint != nil && b.ContentFingerprint != nil {
					var fingerprints [2]string
					for i, input := range evidence.artifacts {
						var err error
						fingerprints[i], err = input.ContentFingerprint(ctx, input.FS)
						if err != nil {
							if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								return nil, err
							}
							return nil, fmt.Errorf("%w: fingerprint source %s: %w", manifest.ErrInput, id, err)
						}
					}
					equal = fingerprints[0] != "" && fingerprints[0] == fingerprints[1]
				}
				if equal {
					delta = &Set{}
				} else {
					var err error
					delta, err = Detect(ctx, a, b)
					if err != nil {
						return nil, fmt.Errorf("compare source %s: %w", id, err)
					}
				}
				compared[pair] = delta
			}
		}
		evidence.changes = delta
		out.sources[id] = evidence
		if id == manifest.BootstrapSourceID {
			out.paths, out.inputs, out.origins, out.oneSided = delta.paths, delta.inputs, evidence.artifacts, delta.oneSided
		}
	}
	if out.paths == nil && changes != nil {
		out.paths = changes.paths
	}
	out.groupSources()
	return out, nil
}

func (s *Set) groupSources() {
	s.groups = nil
	groups := make(map[*Set]int)
	add := func(ref manifest.NamedResource, changes *Set) {
		i, known := groups[changes]
		if !known {
			i = len(s.groups)
			groups[changes] = i
			s.groups = append(s.groups, sourceGroup{changes: changes})
		}
		s.groups[i].refs = append(s.groups[i].refs, ref)
	}
	if _, known := s.sources[manifest.BootstrapSourceID]; !known {
		add(manifest.BootstrapSourceID, s)
	}
	for _, ref := range slices.SortedFunc(maps.Keys(s.sources), manifest.NamedResource.Compare) {
		add(ref, s.sources[ref].changes)
	}
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
	var digests [2]map[string]string
	for i, input := range [2]*store.SourceArtifact{before, after} {
		if source, ok := input.FS.(*gittree.FS); ok {
			digests[i] = source.FileDigests()
		}
	}
	if digests[0] == nil || digests[1] == nil {
		digests = [2]map[string]string{}
	}
	var bef, aft map[string]fileMeta
	eg, scanCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		bef, err = scanTree(scanCtx, before, after, digests[0])
		return err
	})
	eg.Go(func() error {
		var err error
		aft, err = scanTree(scanCtx, after, before, digests[1])
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
		if !ok || left.size != right.size || digests[0] != nil && digests[0][name] != digests[1][name] {
			paths[name] = struct{}{}
			if !ok {
				oneSided = append(oneSided, name)
			}
			continue
		}
		if digests[0] == nil {
			jobs = append(jobs, hashJob{name: name})
		}
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
	out := &Set{paths: paths, origins: [2]*store.SourceArtifact{before, after}}
	slices.Sort(oneSided)
	for _, name := range oneSided {
		_, onAfter := aft[name]
		input := before.FS
		if onAfter {
			input = after.FS
		}
		if err := out.CollectInputs(ctx, input, name, !onAfter); err != nil {
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

// CollectInputs records a path admitted on exactly one side and the data it defines.
// Before identifies the baseline side; WithReaders resolves its exact read dependents.
// Call only while constructing an unpublished Set, never concurrently, and discard
// the set on error. Completed sets returned by Detect or baseline resolution MUST
// remain immutable.
func (s *Set) CollectInputs(ctx context.Context, input fs.FS, name string, before bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	side := 1
	if before {
		side = 0
	}
	if s.oneSided[side] == nil {
		s.oneSided[side] = make(map[string]struct{})
	}
	s.oneSided[side][name] = struct{}{}
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
				s.inputs = make(map[manifest.NamedResource]string)
			}
			s.inputs[id] = name
		}
	}
	return ctx.Err()
}

// WithReaders returns a set including consumers of one-sided files in their admitting index.
// The receiver and both discovery indexes remain immutable.
func (s *Set) WithReaders(before, after *manifest.DiscoveryIndex) *Set {
	if s == nil {
		return nil
	}
	indexes := [2]*manifest.DiscoveryIndex{before, after}
	roots := [2]string{}
	origins := s.origins
	if bootstrap, ok := s.sources[manifest.BootstrapSourceID]; ok {
		origins = bootstrap.artifacts
	}
	for side, input := range origins {
		if input != nil {
			roots[side] = input.LocalPath
		}
	}
	if len(s.sources) == 0 {
		return s.withReaders(indexes, manifest.BootstrapSourceID, origins, roots)
	}
	out := *s
	out.sources = maps.Clone(s.sources)
	for ref, evidence := range s.sources {
		evidence.changes = evidence.changes.withReaders(indexes, ref, evidence.artifacts, roots)
		out.sources[ref] = evidence
		if ref == manifest.BootstrapSourceID {
			out.inputs = evidence.changes.inputs
		}
	}
	out.groupSources()
	return &out
}

func (s *Set) withReaders(indexes [2]*manifest.DiscoveryIndex, source manifest.NamedResource, artifacts [2]*store.SourceArtifact, roots [2]string) *Set {
	if len(s.oneSided[0])+len(s.oneSided[1]) == 0 {
		return s
	}
	out := *s
	out.inputs = maps.Clone(s.inputs)
	for side, index := range indexes {
		if index == nil {
			continue
		}
		for name := range s.oneSided[side] {
			file := name
			if input := artifacts[side]; input != nil && roots[side] != "" {
				relative, err := filepath.Rel(roots[side], loader.OriginPath(input, filepath.Join(input.LocalPath, filepath.FromSlash(name))))
				if err != nil {
					continue
				}
				file = filepath.ToSlash(relative)
			}
			for reader := range index.ReadersOfFile(file) {
				if reader.Source != source {
					continue
				}
				if out.inputs == nil {
					out.inputs = make(map[manifest.NamedResource]string)
				}
				out.inputs[reader.Reader] = name
			}
		}
	}
	return &out
}

type fileMeta struct {
	size int64
}

// Excluded directories MUST still be traversed so deeper .sourceignore re-includes
// survive, matching github.com/fluxcd/pkg/sourceignore v0.19.0 LoadIgnorePatterns.
func scanTree(ctx context.Context, input, opposite *store.SourceArtifact, digests map[string]string) (map[string]fileMeta, error) {
	out := make(map[string]fileMeta)
	scoped := input.FS
	oppositeName, err := filepath.Rel(input.LocalPath, opposite.LocalPath)
	if err == nil {
		oppositeName = filepath.ToSlash(oppositeName)
	}

	if digests != nil {
		oppositePrefix := oppositeName + "/"
		for name := range digests {
			if name != oppositeName && !strings.HasPrefix(name, oppositePrefix) {
				out[name] = fileMeta{}
			}
		}
		return out, nil
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
