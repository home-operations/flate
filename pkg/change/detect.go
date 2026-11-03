package change

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

const changeCapacityDivisor = 8

// Set carries immutable bootstrap-path changes and qualified source evidence.
// Completed sets are immutable and safe to share; CollectInputs is construction-only.
type Set struct {
	oneSided     [2]map[string]map[manifest.NamedResource]string
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

// NewSet constructs a path-only Set without Detect's one-sided input identities.
func NewSet(paths []string) *Set {
	out := &Set{paths: make(map[string]struct{}, len(paths))}
	for _, p := range paths {
		out.paths[filepath.ToSlash(p)] = struct{}{}
	}
	return out
}

type readerPair struct {
	before, after any
	exclusions    [2]string
}

// Qualify compares available admitted source pairs before reconciliation.
// Supplied changes retain their bootstrap coordinates and are never mutated.
func Qualify(ctx context.Context, changes *Set, before, after *store.Store) (*Set, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	stores := [2]*store.Store{before, after}
	ids := sourceIDs(stores)
	out := &Set{sources: make(map[manifest.NamedResource]sourceDelta, len(ids))}
	compared := map[readerPair]*Set{}
	for _, id := range slices.SortedFunc(maps.Keys(ids), manifest.NamedResource.Compare) {
		evidence := sourceEvidence(stores, id)
		if !evidence.readable() {
			if id == manifest.BootstrapSourceID && changes == nil {
				return nil, manifest.NewError(manifest.ErrInput, nil, "both admitted inputs required")
			}
			continue
		}
		delta, err := changes.qualifyDelta(ctx, id, &evidence, compared)
		if err != nil {
			return nil, err
		}
		evidence.changes = delta
		if len(delta.sources) != 0 || len(delta.groups) != 0 || len(delta.declarations) != 0 {
			local := *delta
			local.sources, local.groups, local.declarations = nil, nil, nil
			evidence.changes = &local
		}
		out.sources[id] = evidence
		if id == manifest.BootstrapSourceID {
			out.paths, out.origins, out.oneSided = delta.paths, evidence.artifacts, delta.oneSided
		}
	}
	if out.paths == nil && changes != nil {
		out.paths = changes.paths
	}
	out.groupSources()
	return out, nil
}

func sourceIDs(stores [2]*store.Store) map[manifest.NamedResource]struct{} {
	ids := map[manifest.NamedResource]struct{}{manifest.BootstrapSourceID: {}}
	for _, st := range stores {
		for _, obj := range st.Objects() {
			if _, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.SourceArtifact); ok {
				ids[obj.Named()] = struct{}{}
			}
		}
	}
	return ids
}

func sourceEvidence(stores [2]*store.Store, id manifest.NamedResource) sourceDelta {
	var evidence sourceDelta
	for i, st := range stores {
		evidence.artifacts[i], _ = st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
		evidence.declarations[i] = st.GetObject(id)
	}
	return evidence
}

func (e *sourceDelta) readable() bool {
	a, b := e.artifacts[0], e.artifacts[1]
	return a != nil && b != nil && a.FS != nil && b.FS != nil
}

func (s *Set) qualifyDelta(
	ctx context.Context,
	id manifest.NamedResource,
	evidence *sourceDelta,
	compared map[readerPair]*Set,
) (*Set, error) {
	var previous sourceDelta
	if s != nil {
		previous = s.sources[id]
	}
	if previous.changes != nil && previous.artifacts == evidence.artifacts &&
		previous.declarations == evidence.declarations {
		return previous.changes, nil
	}
	if id == manifest.BootstrapSourceID && s != nil && previous.changes == nil {
		return s, nil
	}
	pair := evidence.readerPair()
	if delta := compared[pair]; delta != nil {
		return delta, nil
	}
	equal, err := evidence.contentEqual(ctx, id)
	if err != nil {
		return nil, err
	}
	var delta *Set
	if equal {
		delta = &Set{}
	} else {
		delta, err = Detect(ctx, evidence.artifacts[0], evidence.artifacts[1])
		if err != nil {
			return nil, fmt.Errorf("compare source %s: %w", id, err)
		}
	}
	compared[pair] = delta
	return delta, nil
}

func (e *sourceDelta) readerPair() readerPair {
	a, b := e.artifacts[0], e.artifacts[1]
	pair := readerPair{before: a, after: b}
	for i, input := range e.artifacts {
		exclusion, err := filepath.Rel(input.LocalPath, e.artifacts[1-i].LocalPath)
		exclusion = filepath.ToSlash(exclusion)
		if err == nil && fs.ValidPath(exclusion) && exclusion != "." {
			pair.exclusions[i] = exclusion
		}
	}
	if reflect.ValueOf(a.FS).Comparable() && reflect.ValueOf(b.FS).Comparable() {
		pair.before, pair.after = a.FS, b.FS
	}
	return pair
}

func (e *sourceDelta) contentEqual(ctx context.Context, id manifest.NamedResource) (bool, error) {
	a, b := e.artifacts[0], e.artifacts[1]
	selected := id != manifest.BootstrapSourceID
	equal := selected && a.Content != "" && a.Content == b.Content && a.Key != "" && a.Key == b.Key &&
		a.Revision == b.Revision &&
		a.Digest == b.Digest
	if !selected || equal || a.ContentFingerprint == nil || b.ContentFingerprint == nil {
		return equal, nil
	}
	var fingerprints [2]string
	for i, input := range e.artifacts {
		fingerprint, err := input.ContentFingerprint(ctx, input.FS)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return false, err
			}
			return false, manifest.NewError(manifest.ErrInput, fmt.Errorf("fingerprint source %s: %w", id, err), "")
		}
		fingerprints[i] = fingerprint
	}
	return fingerprints[0] != "" && fingerprints[0] == fingerprints[1], nil
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
// to be [filepath.ToSlash]-normalized.
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
	bef, aft, err := scanInputs(ctx, before, after, digests)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]struct{}, len(aft)/changeCapacityDivisor)

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
	if err := hashJobs(ctx, before, after, jobs); err != nil {
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
		return nil, fmt.Errorf("context: %w", err)
	}
	return out, nil
}

type hashJob struct {
	name    string
	changed bool
}

func scanInputs(
	ctx context.Context,
	before, after *store.SourceArtifact,
	digests [2]map[string]string,
) (map[string]fileMeta, map[string]fileMeta, error) {
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
		return nil, nil, fmt.Errorf("wait: %w", err)
	}
	return bef, aft, nil
}

func hashJobs(ctx context.Context, before, after *store.SourceArtifact, jobs []hashJob) error {
	hg, hashCtx := errgroup.WithContext(ctx)
	const hashWorkers = 8
	for worker := range min(hashWorkers, len(jobs)) {
		hg.Go(func() error {
			for i := worker; i < len(jobs); i += hashWorkers {
				if err := hashCtx.Err(); err != nil {
					return fmt.Errorf("context: %w", err)
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
		return fmt.Errorf("wait: %w", err)
	}
	return nil
}

type inputReader func([]byte) (int, error)

func (r inputReader) Read(p []byte) (int, error) { return r(p) }

// CollectInputs records a one-sided path and its declared content.
// Before identifies the baseline side; WithReaders resolves its exact read dependents.
// Call only while constructing an unpublished Set, never concurrently, and discard
// the set on error. Completed sets returned by Detect or baseline resolution MUST
// remain immutable.
func (s *Set) CollectInputs(ctx context.Context, input fs.FS, name string, before bool) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	side := 1
	if before {
		side = 0
	}
	if s.oneSided[side] == nil {
		s.oneSided[side] = make(map[string]map[manifest.NamedResource]string)
	}
	s.oneSided[side][name] = nil
	declared, err := collectInput(ctx, input, name)
	if err != nil {
		return err
	}
	s.oneSided[side][name] = declared
	return nil
}

func collectInput(ctx context.Context, input fs.FS, name string) (map[manifest.NamedResource]string, error) {
	var declared map[manifest.NamedResource]string
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yaml", ".yml", ".json":
	default:
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	f, err := input.Open(name)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("open input %s: %w", name, err), "")
	}
	defer func() { _ = f.Close() }()
	var terminal, readErr error
	reader := checkedInputReader(ctx, f, &terminal, &readErr)
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
		return nil, err
	}
	if decodeErr != nil {
		for _, doc := range docs {
			manifest.ReleaseDoc(doc.Document)
		}
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	for _, occurrence := range docs {
		doc := occurrence.Document
		id := manifest.DocIdentity(doc)
		if id.Kind != "" && id.Name != "" && manifest.DocAPIVersion(doc) != "" &&
			!manifest.HasEnvsubstReference(id.Name) &&
			!manifest.HasEnvsubstReference(id.Namespace) {
			if declared == nil {
				declared = map[manifest.NamedResource]string{}
			}
			fingerprint := manifest.Fingerprint(manifest.WithoutOwnershipLabels(doc))
			if previous, present := declared[id]; present && previous != fingerprint {
				fingerprint = ""
			}
			declared[id] = fingerprint
		}
		manifest.ReleaseDoc(doc)
	}
	return declared, ctx.Err()
}

func checkedInputReader(ctx context.Context, f fs.File, terminal, readErr *error) inputReader {
	return inputReader(func(p []byte) (int, error) {
		if localErr := ctx.Err(); localErr != nil {
			*terminal = localErr
			return 0, fmt.Errorf("context: %w", localErr)
		}
		n, localErr := f.Read(p)
		*terminal = localErr
		if localErr != nil &&
			localErr != io.EOF { //nolint:errorlint // io.Reader requires an unwrapped EOF; preserve other read errors.
			*readErr = localErr
		}
		if localErr := ctx.Err(); localErr != nil {
			*terminal = localErr
			return 0, fmt.Errorf("context: %w", localErr)
		}
		return n, localErr
	})
}

// WithReaders derives affected inputs from raw facts and current discovery observations.
// The receiver and both discovery indexes remain immutable.
func (s *Set) WithReaders(
	ctx context.Context,
	before, after *manifest.DiscoveryIndex,
	inputs [2]map[manifest.NamedResource]inputprojection.Projection,
) (*Set, error) {
	if s == nil {
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	indexes := [2]*manifest.DiscoveryIndex{before, after}
	bindings := *s
	bindings.declarations = maps.Clone(s.declarations)
	origins := s.origins
	if bootstrap, ok := s.sources[manifest.BootstrapSourceID]; ok {
		origins = bootstrap.artifacts
	}
	if len(s.sources) == 0 {
		out := s.withReaders(
			indexes,
			&bindings,
			&readerComparison{artifacts: origins, source: manifest.BootstrapSourceID, inputs: inputs},
		)
		out.declarations = bindings.declarations
		return out, nil
	}
	out := *s
	out.sources = maps.Clone(s.sources)
	out.inputs = nil
	for ref, evidence := range s.sources {
		qualified := evidence.changes.withReaders(
			indexes,
			&bindings,
			&readerComparison{artifacts: evidence.artifacts, source: ref, inputs: inputs},
		)
		evidence.changes = qualified
		out.sources[ref] = evidence
		if ref == manifest.BootstrapSourceID {
			out.inputs = evidence.changes.inputs
		}
	}
	out.declarations = bindings.declarations
	out.groupSources()
	return &out, nil
}

type readerComparison struct {
	inputs    [2]map[manifest.NamedResource]inputprojection.Projection
	source    manifest.NamedResource
	artifacts [2]*store.SourceArtifact
}
