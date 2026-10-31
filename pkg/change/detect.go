package change

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"iter"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Set carries immutable bootstrap-path changes and qualified source evidence.
// Completed sets are immutable and safe to share; CollectInputs is construction-only.
type Set struct {
	oneSided     [2]map[string]struct{}
	origins      [2]*store.SourceArtifact
	inputs       map[manifest.NamedResource]InputChange
	paths        map[string]struct{}
	sources      map[manifest.NamedResource]sourceDelta
	groups       []sourceGroup
	declarations map[comparisonKey]declarationComparison
}

// InputChange records a changed input and its static discovery authority.
type InputChange struct {
	File         string
	Declarations []string
	Productions  []manifest.Production
	Unbound      bool // Neither side supplies an admitted read of File.
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
			if _, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.SourceArtifact); ok {
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
			evidence.artifacts[i], _ = st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			evidence.declarations[i] = st.GetObject(id)
		}
		a, b := evidence.artifacts[0], evidence.artifacts[1]
		if a == nil || b == nil || a.FS == nil || b.FS == nil {
			if id == manifest.BootstrapSourceID && changes == nil {
				return nil, manifest.NewError(manifest.ErrInput, nil, "both admitted inputs required")
			}
			continue
		}
		delta := changes
		var previous sourceDelta
		if changes != nil {
			previous = changes.sources[id]
		}
		if previous.changes != nil && previous.artifacts == evidence.artifacts && previous.declarations == evidence.declarations {
			delta = previous.changes
		} else if id != manifest.BootstrapSourceID || delta == nil {
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
				equal := selected && a.Content != "" && a.Content == b.Content && a.Key != "" && a.Key == b.Key && a.Revision == b.Revision && a.Digest == b.Digest
				if selected && !equal && a.ContentFingerprint != nil && b.ContentFingerprint != nil {
					var fingerprints [2]string
					for i, input := range evidence.artifacts {
						var err error
						fingerprints[i], err = input.ContentFingerprint(ctx, input.FS)
						if err != nil {
							if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								return nil, err
							}
							return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("fingerprint source %s: %w", id, err), "")
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
		if len(delta.sources) != 0 || len(delta.groups) != 0 || len(delta.declarations) != 0 {
			local := *delta
			local.sources, local.groups, local.declarations = nil, nil, nil
			evidence.changes = &local
		}
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
		return nil, manifest.NewError(manifest.ErrInput, nil, "both admitted inputs required")
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
		return manifest.NewError(manifest.ErrInput, fmt.Errorf("open input %s: %w", name, err), "")
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
	docs, decodeErr := manifest.DecodeDocs(reader, manifest.IncidentalDocuments)
	decodeErr = errors.Join(decodeErr, manifest.DocumentFailure(docs))
	if decodeErr != nil && terminal == nil {
		// A parser error must not hide later file errors or cancellation.
		_, _ = io.Copy(io.Discard, reader)
	}
	err = ctx.Err()
	if err == nil && readErr != nil {
		err = manifest.NewError(manifest.ErrInput, fmt.Errorf("read input %s: %w", name, readErr), "")
	}
	if err != nil {
		for _, doc := range docs {
			manifest.ReleaseDoc(doc.Document)
		}
		return err
	}
	if decodeErr != nil {
		for _, doc := range docs {
			manifest.ReleaseDoc(doc.Document)
		}
		return nil
	}
	for _, occurrence := range docs {
		doc := occurrence.Document
		id := manifest.NamedResource{Kind: manifest.DocKind(doc)}
		id.Name, id.Namespace = manifest.DocMetadata(doc)
		eligible := manifest.DocAPIVersion(doc) == manifest.CoreAPIVersion && id.KindInfo().Data &&
			id.Name != "" && !manifest.HasEnvsubstReference(id.Name) && !manifest.HasEnvsubstReference(id.Namespace)
		manifest.ReleaseDoc(doc)
		if eligible {
			if s.inputs == nil {
				s.inputs = make(map[manifest.NamedResource]InputChange)
			}
			s.inputs[id] = InputChange{File: name, Unbound: true}
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
	origins := s.origins
	if bootstrap, ok := s.sources[manifest.BootstrapSourceID]; ok {
		origins = bootstrap.artifacts
	}
	if len(s.sources) == 0 {
		return s.withReaders(indexes, manifest.BootstrapSourceID, origins, s)
	}
	out := *s
	out.sources = maps.Clone(s.sources)
	for ref, evidence := range s.sources {
		evidence.changes = evidence.changes.withReaders(indexes, ref, evidence.artifacts, s)
		out.sources[ref] = evidence
		if ref == manifest.BootstrapSourceID {
			out.inputs = evidence.changes.inputs
		}
	}
	out.groupSources()
	return &out
}

func (s *Set) withReaders(indexes [2]*manifest.DiscoveryIndex, source manifest.NamedResource, artifacts [2]*store.SourceArtifact, bindings *Set) *Set {
	if s.Len() == 0 {
		return s
	}
	roots := [2]string{}
	_, bootstrap, _ := bindings.ForSource(manifest.BootstrapSourceID)
	for side, input := range bootstrap {
		if input != nil {
			roots[side] = input.LocalPath
		}
	}
	qualifiedSource := func(side int, actual manifest.NamedResource) bool {
		if actual == (manifest.NamedResource{}) {
			actual = manifest.BootstrapSourceID
		}
		if actual == source {
			return true
		}
		_, admitted, _ := bindings.ForSource(actual)
		left, right := artifacts[side], admitted[side]
		return left != nil && right != nil && (left.InputAlias || right.InputAlias || left.OriginPath(left.LocalPath) == roots[side]) && left.FS != nil && right.FS != nil && left.Reads.Valid() && right.Reads.Valid() && left.OriginPath(left.LocalPath) == right.OriginPath(right.LocalPath)
	}
	out := *s
	out.inputs = maps.Clone(s.inputs)
	for id, input := range out.inputs {
		if len(input.Declarations) != 0 {
			delete(out.inputs, id)
		}
	}
	coordinate := func(side int, name string) string {
		if input := artifacts[side]; input != nil && roots[side] != "" {
			relative, err := filepath.Rel(roots[side], input.OriginPath(filepath.Join(input.LocalPath, filepath.FromSlash(name))))
			if err != nil {
				return ""
			}
			return filepath.ToSlash(relative)
		}
		return name
	}
	for id, input := range out.inputs {
		if !input.Unbound {
			continue
		}
		for side, index := range indexes {
			if _, present := s.oneSided[side][input.File]; !present {
				continue
			}
			for reader := range index.ReadersOfFile(coordinate(side, input.File)) {
				if qualifiedSource(side, reader.Source) {
					input.Unbound = false
					out.inputs[id] = input
					break
				}
			}
		}
	}
	for side, index := range indexes {
		if index == nil {
			continue
		}
		files := make(map[string]string, len(s.paths))
		for name := range s.paths {
			files[coordinate(side, name)] = name
		}
		qualified := func(record manifest.DiscoveryRecord) bool {
			if len(record.Productions) == 0 {
				return qualifiedSource(side, manifest.BootstrapSourceID)
			}
			return slices.ContainsFunc(record.Productions, func(p manifest.Production) bool { return qualifiedSource(side, p.Source) })
		}
		for id, record := range index.Files() {
			if !qualified(record) {
				continue
			}
			input := out.inputs[id]
			var declarations []string
			var productions []manifest.Production
			name := ""
			pending := []manifest.NamedResource{id}
			seen := map[manifest.NamedResource]bool{}
			for len(pending) != 0 {
				owner := pending[0]
				pending = pending[1:]
				if seen[owner] {
					continue
				}
				seen[owner] = true
				r, found := index.Lookup(owner)
				if !found || !qualified(r) {
					continue
				}
				if owner != id && owner.KindInfo().Render && !owner.ReportHidden() {
					productions = append(productions, manifest.Production{Owner: owner, Declaration: owner, Authored: record.Authored, Source: source, File: record.File})
				}
				comparison, known := bindings.declarations[comparisonKey{source: source, owner: owner, input: owner}]
				if !known {
					for alias := range bindings.Sources() {
						if alias == source || !qualifiedSource(0, alias) || !qualifiedSource(1, alias) {
							continue
						}
						if paired, present := bindings.declarations[comparisonKey{source: alias, owner: owner, input: owner}]; present {
							comparison, known = paired, true
							break
						}
					}
				}
				if r.File != "" {
					file := r.File
					if artifact := artifacts[side]; artifact != nil && roots[side] != "" {
						relative, err := filepath.Rel(artifact.OriginPath(artifact.LocalPath), filepath.Join(roots[side], filepath.FromSlash(file)))
						if err != nil || !filepath.IsLocal(relative) {
							continue
						}
						file = filepath.ToSlash(relative)
					}
					declarations = append(declarations, file)
					if changed, found := files[r.File]; found {

						if !known || comparison.changed {
							if name == "" || changed < name {
								name = changed
							}
						}
					}
				}
				for _, file := range r.Inputs {
					if changed, found := files[file]; found && (!known || comparison.changed) && (name == "" || changed < name) {
						name = changed
					}
				}
				pending = append(pending, r.Parent, r.BuilderOwner)
				for _, production := range r.Productions {
					pending = append(pending, production.Owner, production.Declaration)
				}
			}
			if name == "" {
				continue
			}
			if out.inputs == nil {
				out.inputs = map[manifest.NamedResource]InputChange{}
			}
			input.File, input.Unbound = name, false
			input.Declarations = append(slices.Clone(input.Declarations), declarations...)
			slices.Sort(input.Declarations)
			input.Declarations = slices.Compact(input.Declarations)
			input.Productions = append(slices.Clone(input.Productions), productions...)
			slices.SortFunc(input.Productions, func(a, b manifest.Production) int {
				return cmp.Or(a.Owner.Compare(b.Owner), a.Authored.Compare(b.Authored), a.Source.Compare(b.Source), strings.Compare(a.File, b.File))
			})
			input.Productions = slices.Compact(input.Productions)
			out.inputs[id] = input
		}
		for name := range s.oneSided[side] {
			for reader := range index.ReadersOfFile(coordinate(side, name)) {
				if reader.Materialize || !qualifiedSource(side, reader.Source) {
					continue
				}
				if out.inputs == nil {
					out.inputs = make(map[manifest.NamedResource]InputChange)
				}
				input := out.inputs[reader.Reader]
				if input.File == "" || len(input.Declarations) == 0 && name < input.File {
					input.File = name
				}
				input.Unbound = false
				out.inputs[reader.Reader] = input
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

// Inputs yields qualified one-sided input identities and their source-relative files.
func (s *Set) Inputs() iter.Seq2[manifest.NamedResource, InputChange] {
	return func(yield func(manifest.NamedResource, InputChange) bool) {
		if s == nil {
			return
		}
		for id, file := range s.inputs {
			if !yield(id, file) {
				return
			}
		}
	}
}

// Sources yields the source identities carrying immutable comparison evidence.
func (s *Set) Sources() iter.Seq[manifest.NamedResource] {
	return func(yield func(manifest.NamedResource) bool) {
		if s == nil {
			return
		}
		if _, ok := s.sources[manifest.BootstrapSourceID]; !ok && !yield(manifest.BootstrapSourceID) {
			return
		}
		for id := range s.sources {
			if !yield(id) {
				return
			}
		}
	}
}

// ForSource returns source-relative changes, exact admitted artifacts and their source declarations.
func (s *Set) ForSource(source manifest.NamedResource) (*Set, [2]*store.SourceArtifact, [2]manifest.BaseManifest) {
	if s == nil {
		return nil, [2]*store.SourceArtifact{}, [2]manifest.BaseManifest{}
	}
	if evidence, ok := s.sources[source]; ok {
		return evidence.changes, evidence.artifacts, evidence.declarations
	}
	if source == manifest.BootstrapSourceID {
		return s, s.origins, [2]manifest.BaseManifest{}
	}
	return nil, [2]*store.SourceArtifact{}, [2]manifest.BaseManifest{}
}

type comparisonKey struct {
	source, owner, input manifest.NamedResource
	use                  string
	position             int
}

type declarationComparison struct {
	objects    [2]manifest.BaseManifest
	references [2]manifest.Reference
	changed    bool
}

// WithDeclarations freezes observed authoritative declaration and reference comparisons before dispatch.
// Only completed producer output is prepared comparison authority; raw producer-owned input is excluded.
// Missing preparation is unknown; the returned set retains no Store or captured read set.
func (s *Set) WithDeclarations(ctx context.Context, before, after *store.Store, prepared [2]map[manifest.NamedResource]store.Child) (*Set, error) {
	if s == nil {
		return nil, nil
	}
	out := *s
	out.declarations = map[comparisonKey]declarationComparison{}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if before == nil || after == nil {
		return out.freezeSources(), nil
	}
	stores := [2]*store.Store{before, after}
	objects := [2]map[manifest.NamedResource]store.Child{}
	sources := [2]map[manifest.NamedResource]manifest.NamedResource{}
	for side, st := range stores {
		objects[side], sources[side] = map[manifest.NamedResource]store.Child{}, map[manifest.NamedResource]manifest.NamedResource{}
		for _, obj := range st.Objects() {
			id := obj.Named()
			if st.GetDeclaration(id) != obj {
				continue
			}
			_, doc, emission := st.GetEmission(id)
			emission.Finish()
			objects[side][id] = store.Child{Obj: obj, Doc: doc}
		}
		for id, child := range prepared[side] {
			if _, known := objects[side][id]; !known {
				objects[side][id] = child
			}
		}
		for id, child := range objects[side] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			obj := child.Obj
			source, ambiguous := manifest.BootstrapSourceID, false
			found := false
			for _, production := range st.Productions(id, nil) {
				ref := production.Source
				if ref == (manifest.NamedResource{}) {
					ref = manifest.BootstrapSourceID
				}
				if found && ref != source {
					ambiguous = true
					break
				}
				source, found = ref, true
			}
			if ambiguous {
				delete(objects[side], id)
				continue
			}
			_, snapshots, _ := s.ForSource(source)
			if snapshots[side] == nil || snapshots[side] != st.GetArtifact(source, store.ArtifactContent) {
				delete(objects[side], id)
				continue
			}
			objects[side][id] = store.Child{Obj: obj, Doc: manifest.DeepCopyMap(child.Doc)}
			sources[side][id] = source
		}
	}
	var copied map[*Set]*Set
	for side, declarations := range objects {
		for id := range declarations {
			if _, present := objects[1-side][id]; present {
				continue
			}
			source := sources[side][id]
			delta, _, _ := s.ForSource(source)
			if delta == nil {
				continue
			}
			for _, production := range stores[side].Productions(id, nil) {
				if production.Source != source || production.File == "" || !delta.Contains(production.File) {
					continue
				}
				if copied == nil {
					copied = map[*Set]*Set{}
				}
				local := copied[delta]
				if local == nil {
					clone := *delta
					clone.inputs = maps.Clone(delta.inputs)
					if clone.inputs == nil {
						clone.inputs = map[manifest.NamedResource]InputChange{}
					}
					local = &clone
					copied[delta] = local
				}
				local.inputs[id] = InputChange{File: production.File}
			}
		}
	}
	if len(copied) != 0 {
		out.sources = maps.Clone(s.sources)
		for source, evidence := range out.sources {
			if local := copied[evidence.changes]; local != nil {
				evidence.changes = local
				out.sources[source] = evidence
			}
		}
		if local := copied[s]; local != nil {
			out.inputs = local.inputs
		}
	}
	for id, left := range objects[0] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		right, ok := objects[1][id]
		if !ok || sources[0][id] != sources[1][id] {
			continue
		}
		source := sources[0][id]
		equal, err := manifest.OutputEqual(left.Obj, right.Obj, [2]map[string]any{left.Doc, right.Doc})
		if err != nil {
			return nil, err
		}
		out.declarations[comparisonKey{source: source, owner: id, input: id}] = declarationComparison{objects: [2]manifest.BaseManifest{left.Obj, right.Obj}, changed: !equal}
		refs := [2][]manifest.Reference{}
		for side, child := range [2]store.Child{left, right} {
			for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
				refs[side] = append(refs[side], ref.Clone())
			}
		}
		for _, ref := range refs[0] {
			if !ref.ContentInput() || ref.Selector != nil {
				continue
			}
			peer := slices.IndexFunc(refs[1], func(other manifest.Reference) bool { return referencesEqual(ref, other) })
			if peer < 0 {
				continue
			}
			inputs := [2]store.Child{objects[0][ref.NamedResource], objects[1][ref.NamedResource]}
			if inputs[0].Obj == nil || inputs[1].Obj == nil {
				continue
			}
			changed := false
			if ref.Key != "" {
				values := [2]string{}
				present := [2]bool{}
				valid := true
				for side, input := range inputs {
					data, ok := input.Obj.(manifest.Data)
					if !ok {
						valid = false
						break
					}
					var err error
					values[side], present[side], err = data.Value(ref.Key)
					if err != nil {
						valid = false
						break
					}
				}
				if !valid || !present[0] || !present[1] {
					continue
				}
				changed = values[0] != values[1] || present[0] != present[1]
			} else {
				equal, err := manifest.OutputEqual(inputs[0].Obj, inputs[1].Obj, [2]map[string]any{inputs[0].Doc, inputs[1].Doc})
				if err != nil {
					return nil, err
				}
				changed = !equal
			}
			out.declarations[comparisonKey{source: source, owner: id, input: ref.NamedResource, use: ref.Use, position: ref.Position}] = declarationComparison{objects: [2]manifest.BaseManifest{left.Obj, right.Obj}, references: [2]manifest.Reference{ref, refs[1][peer]}, changed: changed}
		}
	}
	return out.freezeSources(), nil
}

func (s *Set) freezeSources() *Set {
	snapshots := map[*store.SourceArtifact]*store.SourceArtifact{}
	s.origins = freezeArtifacts(s.origins, snapshots)
	s.sources = maps.Clone(s.sources)
	locals := map[*Set]*Set{}
	for source, evidence := range s.sources {
		evidence.artifacts = freezeArtifacts(evidence.artifacts, snapshots)
		if evidence.changes != nil {
			original := evidence.changes
			local := locals[original]
			if local == nil {
				copied := *original
				copied.origins = freezeArtifacts(copied.origins, snapshots)
				copied.sources, copied.groups, copied.declarations = nil, nil, nil
				local = &copied
				locals[original] = local
			}
			evidence.changes = local
		}
		s.sources[source] = evidence
	}
	s.groupSources()
	return s
}

func freezeArtifacts(inputs [2]*store.SourceArtifact, snapshots map[*store.SourceArtifact]*store.SourceArtifact) [2]*store.SourceArtifact {
	for side, input := range inputs {
		if input == nil {
			continue
		}
		if snapshot := snapshots[input]; snapshot != nil {
			inputs[side] = snapshot
			continue
		}
		snapshot := *input
		snapshot.Reads, snapshot.ContentFingerprint = nil, nil
		snapshot.Metadata = maps.Clone(input.Metadata)
		snapshot.Warnings = slices.Clone(input.Warnings)
		for i := range snapshot.Warnings {
			snapshot.Warnings[i].Detail = slices.Clone(snapshot.Warnings[i].Detail)
		}
		inputs[side] = &snapshot
		snapshots[input] = &snapshot
	}
	return inputs
}

// OutputChanged reads a frozen verdict only while the same local declaration and reference apply.
// Source artifact applicability MUST be checked by the caller against ForSource first.
func (s *Set) OutputChanged(side manifest.TreeSide, source manifest.NamedResource, ref manifest.Reference, current manifest.BaseManifest) (changed, known bool) {
	if s == nil || current == nil || side > manifest.TreeHead {
		return false, false
	}
	fact, ok := s.declarations[comparisonKey{source: source, owner: current.Named(), input: ref.NamedResource, use: ref.Use, position: ref.Position}]
	if !ok || current != fact.objects[side] {
		return false, false
	}
	if ref.Use != "" && !referencesEqual(ref, fact.references[side]) {
		return false, false
	}
	return fact.changed, true
}

// OppositeDeclaration returns the paired declaration while the same local declaration applies.
// Source artifact applicability MUST be checked by the caller against ForSource first.
func (s *Set) OppositeDeclaration(side manifest.TreeSide, source manifest.NamedResource, current manifest.BaseManifest) manifest.BaseManifest {
	if s == nil || current == nil || side > manifest.TreeHead {
		return nil
	}
	id := current.Named()
	fact := s.declarations[comparisonKey{source: source, owner: id, input: id}]
	if current != fact.objects[side] {
		return nil
	}
	return fact.objects[manifest.TreeHead-side]
}

func referencesEqual(a, b manifest.Reference) bool {
	left, right := a.DependencyRef, b.DependencyRef
	left.ReadyExpr, right.ReadyExpr = nil, nil
	if left != right || (a.ReadyExpr == nil) != (b.ReadyExpr == nil) || a.ReadyExpr != nil && *a.ReadyExpr != *b.ReadyExpr {
		return false
	}
	return a.Use == b.Use && a.Position == b.Position && a.Optional == b.Optional && a.Content == b.Content && a.Gate == b.Gate && a.ArtifactStages == b.ArtifactStages && a.Ordering == b.Ordering && a.ChartPath == b.ChartPath && a.Key == b.Key && a.NamespaceDefault == b.NamespaceDefault && a.Selector == b.Selector && slices.Equal(a.Files, b.Files)
}
