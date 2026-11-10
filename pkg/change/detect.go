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

	"github.com/go-git/go-git/v5/plumbing"

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
func Qualify(ctx context.Context, changes *Set, stores [2]*store.Store, work manifest.WorkScope) (*Set, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
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
		delta := changes.qualifyDelta(id, &evidence)
		var err error
		if delta == nil {
			delta, err = evidence.compare(ctx, id, compared, work)
		}
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

func (s *Set) qualifyDelta(id manifest.NamedResource, evidence *sourceDelta) *Set {
	var previous sourceDelta
	if s != nil {
		previous = s.sources[id]
	}
	if previous.changes != nil && previous.artifacts == evidence.artifacts &&
		previous.declarations == evidence.declarations {
		return previous.changes
	}
	if id == manifest.BootstrapSourceID && s != nil && previous.changes == nil {
		return s
	}
	return nil
}

func (e *sourceDelta) compare(
	ctx context.Context,
	id manifest.NamedResource,
	compared map[readerPair]*Set,
	work manifest.WorkScope,
) (*Set, error) {
	pair := e.readerPair()
	if delta := compared[pair]; delta != nil {
		return delta, nil
	}
	equal, err := e.contentEqual(ctx, id)
	if err != nil {
		return nil, err
	}
	var delta *Set
	if equal && e.immutableArtifact() {
		delta = &Set{}
	} else {
		delta, err = detect(ctx, e.artifacts, DetectConfig{Work: work.Source(id)}, equal)
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
	if id == manifest.BootstrapSourceID {
		return false, nil
	}
	if e.immutableArtifact() {
		return true, nil
	}
	if !e.hasFingerprints() {
		return false, nil
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

func (e *sourceDelta) immutableArtifact() bool {
	a, b := e.artifacts[0], e.artifacts[1]
	identity := a.Content != "" && a.Content == b.Content && a.Key != "" && a.Key == b.Key
	return identity && a.Revision == b.Revision && a.Digest == b.Digest
}

func (e *sourceDelta) hasFingerprints() bool {
	return e.artifacts[0].ContentFingerprint != nil && e.artifacts[1].ContentFingerprint != nil
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

// DetectConfig borrows candidate evidence, which MUST remain immutable until Detect returns.
type DetectConfig struct {
	Work manifest.WorkScope
	// Nil discovers all paths; an empty map compares no candidate content.
	// True preserves read failures from uncertain checkout observations.
	Candidates map[string]bool
}

// FileContext supplies source coordinates without granting execution membership.
type FileContext struct {
	Root, Origin, ToIndex string
	Input                 *store.SourceArtifact
	Paired                bool
}

// FileChanged preserves absent comparison evidence as unchanged.
func (s *Set) FileChanged(location FileContext, file string) bool {
	if s == nil {
		return false
	}
	if location.Paired && location.Input != nil {
		if location.ToIndex == "." {
			return s.Contains(filepath.ToSlash(filepath.Clean(file)))
		}
		relative, err := filepath.Rel(location.Origin, filepath.Join(location.Root, filepath.FromSlash(file)))
		return err == nil && s.Contains(filepath.ToSlash(relative))
	}
	return s.Contains(file)
}

// Detect compares admitted filesystems under each side's Flux ignore policy.
func Detect(ctx context.Context, before, after *store.SourceArtifact, config DetectConfig) (*Set, error) {
	return detect(ctx, [2]*store.SourceArtifact{before, after}, config, false)
}

func detect(
	ctx context.Context,
	inputs [2]*store.SourceArtifact,
	config DetectConfig,
	contentEqual bool,
) (*Set, error) {
	before, after := inputs[0], inputs[1]
	if before == nil || after == nil || before.FS == nil || after.FS == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "both admitted inputs required")
	}
	digests := completedDigests(inputs)
	scanned, err := scanInputs(ctx, inputs, digests, config)
	if err != nil {
		return nil, err
	}
	comparison := compareScannedFiles(
		&scanned,
		comparisonEvidence{digests: digests, candidates: config.Candidates, contentEqual: contentEqual},
	)
	if err := hashJobs(ctx, inputs, comparison.jobs); err != nil {
		return nil, err
	}
	for _, job := range comparison.jobs {
		if job.changed {
			comparison.paths[job.name] = struct{}{}
		}
	}
	out := &Set{paths: comparison.paths, origins: inputs}
	slices.Sort(comparison.oneSided)
	for _, name := range comparison.oneSided {
		_, onAfter := scanned.after.entries[name]
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
	name         string
	present      [2]bool
	changed      bool
	readRequired bool
}

type scannedInputs struct{ before, after fileScan }

func scanInputs(
	ctx context.Context,
	inputs [2]*store.SourceArtifact,
	digests [2]map[string]string,
	config DetectConfig,
) (scannedInputs, error) {
	before, after := inputs[0], inputs[1]
	beforeWork := config.Work.Record(manifest.WorkPreparation, before.Key)
	afterWork := config.Work.Record(manifest.WorkPreparation, after.Key)
	var bef, aft fileScan
	eg, scanCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		bef, err = scanTree(
			scanCtx,
			inputs,
			scanEvidence{digests: digests[0], candidates: config.Candidates},
			beforeWork,
		)
		return err
	})
	eg.Go(func() error {
		var err error
		aft, err = scanTree(
			scanCtx,
			[2]*store.SourceArtifact{after, before},
			scanEvidence{digests: digests[1], candidates: config.Candidates},
			afterWork,
		)
		return err
	})
	if err := eg.Wait(); err != nil {
		return scannedInputs{}, fmt.Errorf("wait: %w", err)
	}
	return scannedInputs{before: bef, after: aft}, nil
}

func hashJobs(ctx context.Context, inputs [2]*store.SourceArtifact, jobs []hashJob) error {
	hg, hashCtx := errgroup.WithContext(ctx)
	const hashWorkers = 8
	for worker := range min(hashWorkers, len(jobs)) {
		hg.Go(func() error {
			for i := worker; i < len(jobs); i += hashWorkers {
				if err := hashCtx.Err(); err != nil {
					return fmt.Errorf("context: %w", err)
				}
				if err := jobs[i].compare(hashCtx, inputs); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := hg.Wait(); err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	return nil
}

func (j *hashJob) compare(ctx context.Context, inputs [2]*store.SourceArtifact) error {
	var identities [2]string
	for side, input := range inputs {
		if !j.present[side] {
			continue
		}
		if j.readRequired {
			sum, err := hashFile(ctx, input.FS, j.name)
			if err != nil {
				return err
			}
			identities[side] = string(sum[:])
			continue
		}
		source, native := input.FS.(*gittree.FS)
		if !native {
			var err error
			source, _, err = gittree.New(
				ctx,
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: input.FS},
			)
			if err != nil {
				return err
			}
		}
		observed, err := source.FreshInputs(ctx, []string{j.name}, map[string]fs.FileMode{j.name: 0})
		if err != nil {
			return err
		}
		identity, present := observed.Files[j.name]
		if !present {
			return fmt.Errorf("read %s: %w", j.name, fs.ErrNotExist)
		}
		identities[side] = identity
	}
	j.changed = j.changed || identities[0] != identities[1]
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

func declaresInputs(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yaml", ".yml", ".json":
		return true
	default:
		return false
	}
}

func collectInput(ctx context.Context, input fs.FS, name string) (map[manifest.NamedResource]string, error) {
	if !declaresInputs(name) {
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
	declared := inputDeclarations(docs)
	return declared, ctx.Err() //nolint:wrapcheck // Preserve this final cancellation identity and diagnostic.
}

//nolint:gocritic // The reader callback writes the caller error state after this function returns.
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
		if cancelErr := ctx.Err(); cancelErr != nil {
			*terminal = cancelErr
			return 0, fmt.Errorf("context: %w", cancelErr)
		}
		return n, localErr //nolint:wrapcheck // Keep io.Reader EOF exact and other read errors unchanged.
	})
}

// WithReaders derives affected inputs from raw facts and current discovery observations.
// The receiver and both discovery indexes remain immutable.
func (s *Set) WithReaders(
	ctx context.Context,
	indexes [2]*manifest.DiscoveryIndex,
	inputs [2]map[manifest.NamedResource]inputprojection.Projection,
	scope manifest.WorkScope,
) (*Set, error) {
	if s == nil {
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
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
			scope.Source(manifest.BootstrapSourceID).Record(manifest.WorkPreparation, "readers"),
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
			scope.Source(ref).Record(manifest.WorkPreparation, "readers"),
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

type fileComparison struct {
	paths    map[string]struct{}
	jobs     []hashJob
	oneSided []string
}

type comparisonEvidence struct {
	digests      [2]map[string]string
	candidates   map[string]bool
	contentEqual bool
}

func compareScannedFiles(scanned *scannedInputs, evidence comparisonEvidence) fileComparison {
	before, after := scanned.before.entries, scanned.after.entries
	out := fileComparison{paths: make(map[string]struct{}, len(after)/changeCapacityDivisor)}
	for name, right := range after {
		left, present := before[name]
		if !present {
			if scanned.includesOneSided(name, evidence.candidates) {
				out.addOneSided(name, [2]bool{false, true}, evidence)
			}
			continue
		}
		uncertain, candidate := evidence.candidates[name]
		if evidence.candidates != nil && !candidate {
			continue
		}
		changed := !sameMode(left.mode, right.mode) || left.size != right.size
		switch {
		case uncertain:
			out.jobs = append(
				out.jobs,
				hashJob{name: name, present: [2]bool{true, true}, changed: changed, readRequired: true},
			)
		case changed || evidence.digests[0] != nil && evidence.digests[0][name] != evidence.digests[1][name]:
			out.paths[name] = struct{}{}
		case evidence.digests[0] == nil && !evidence.contentEqual:
			out.jobs = append(out.jobs, hashJob{name: name, present: [2]bool{true, true}})
		}
	}
	for name := range before {
		if _, present := after[name]; present {
			continue
		}
		if scanned.includesOneSided(name, evidence.candidates) {
			out.addOneSided(name, [2]bool{true, false}, evidence)
		}
	}
	return out
}

func (s *scannedInputs) includesOneSided(name string, candidates map[string]bool) bool {
	_, candidate := candidates[name]
	if candidates == nil || candidate {
		return true
	}
	return s.before.policy.Match(name, false) != s.after.policy.Match(name, false)
}

func (c *fileComparison) addOneSided(name string, present [2]bool, evidence comparisonEvidence) {
	c.paths[name] = struct{}{}
	c.oneSided = append(c.oneSided, name)
	if evidence.candidates[name] && !declaresInputs(name) {
		c.jobs = append(c.jobs, hashJob{name: name, present: present, changed: true, readRequired: true})
	}
}

// Flux artifacts normalize all executable regular-file permissions to one class.
func sameMode(a, b fs.FileMode) bool {
	if a.Type() != b.Type() {
		return false
	}
	return !a.IsRegular() || (a.Perm()&0o111 != 0) == (b.Perm()&0o111 != 0)
}

func inputDeclarations(docs []manifest.DocOccurrence) map[manifest.NamedResource]string {
	var declared map[manifest.NamedResource]string
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
	return declared
}
