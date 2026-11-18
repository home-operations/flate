package change

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/go-git/go-git/v6/plumbing"

	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

const changeCapacityDivisor = 8

// Set carries immutable bootstrap-path changes and qualified source evidence.
// Completed sets MUST remain immutable while readers share them.
type Set struct {
	origins      [2]*store.SourceArtifact
	inputs       map[manifest.NamedResource]InputChange
	paths        map[string][2]filePresence
	sources      map[manifest.NamedResource]sourceDelta
	groups       []sourceGroup
	declarations map[comparisonKey]declarationComparison
}

type filePresence uint8

const (
	fileUnknown filePresence = iota
	fileAbsent
	filePresent
)

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

func (s *Set) recordPresence(scanned *scannedInputs) {
	for side, scan := range [2]fileScan{scanned.before, scanned.after} {
		for file, presence := range s.paths {
			presence[side] = fileAbsent
			if _, present := scan.entries[file]; present {
				presence[side] = filePresent
			}
			s.paths[file] = presence
		}
	}
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
		switch {
		case evidence.readable():
		case id == manifest.BootstrapSourceID && changes == nil:
			return nil, manifest.NewError(manifest.ErrInput, nil, "both admitted inputs required")
		default:
			continue
		}
		delta, err := changes.qualifySource(ctx, &evidence, id, work)
		if delta == nil && err == nil {
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
			out.paths, out.origins = delta.paths, evidence.artifacts
		}
	}
	if out.paths == nil && changes != nil {
		out.paths = make(map[string][2]filePresence, len(changes.paths))
		for file := range changes.paths {
			out.paths[file] = [2]filePresence{}
		}
	}
	out.groupSources()
	return out, nil
}

func (s *Set) qualifySource(
	ctx context.Context,
	evidence *sourceDelta,
	id manifest.NamedResource,
	work manifest.WorkScope,
) (*Set, error) {
	if delta := s.qualifyDelta(id, evidence); delta != nil {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context: %w", err)
		}
		return delta, nil
	}
	if s == nil || id != manifest.BootstrapSourceID || s.sources[id].changes != nil {
		return nil, nil //nolint:nilnil // Existing qualified sources use their admitted comparison.
	}
	candidates := make(map[string]bool, len(s.paths))
	for file := range s.paths {
		candidates[file] = false
	}
	digests := completedDigests(evidence.artifacts)
	scanned, err := scanInputs(ctx, evidence.artifacts,
		[2]scanEvidence{{digests: digests[0], candidates: candidates}, {digests: digests[1], candidates: candidates}},
		[2]*manifest.WorkRecord{
			work.Source(manifest.BootstrapSourceID).Record(manifest.WorkValidation, evidence.artifacts[0].Key),
			work.Source(manifest.BootstrapSourceID).Record(manifest.WorkValidation, evidence.artifacts[1].Key),
		})
	if err != nil {
		return nil, err
	}
	comparison := compareScannedFiles(
		&scanned,
		comparisonEvidence{digests: digests, candidates: candidates, contentEqual: true},
	)
	for file := range s.paths {
		comparison.paths[file] = [2]filePresence{}
	}
	delta := &Set{paths: comparison.paths, origins: evidence.artifacts}
	delta.recordPresence(&scanned)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	return delta, nil
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
	if id == manifest.BootstrapSourceID && s != nil && previous.changes == nil && s.origins == evidence.artifacts {
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
	if s == nil || len(s.paths) == 0 {
		return nil
	}
	paths := make([]string, 0, len(s.paths))
	for path := range s.paths {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	return paths
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

// Present reads scan membership without filesystem access.
func (s *Set) Present(
	side manifest.TreeSide,
	file string,
) (present, known bool) { //nolint:nonamedreturns // Both booleans describe distinct presence authority.
	if s == nil || side > manifest.TreeHead {
		return false, false
	}
	return s.present(int(side), file)
}

func (s *Set) present(
	side int,
	file string,
) (present, known bool) { //nolint:nonamedreturns // Both booleans describe distinct presence authority.
	state := s.paths[filepath.ToSlash(file)][side]
	return state == filePresent, state != fileUnknown
}

// DetectConfig borrows candidate evidence, which MUST remain immutable until Detect returns.
type DetectConfig struct {
	Work manifest.WorkScope
	// Nil discovers all paths; an empty map compares no candidate content.
	// True preserves read failures from uncertain checkout observations.
	Candidates map[string]bool
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
	scanned, err := scanInputs(
		ctx,
		inputs,
		[2]scanEvidence{
			{digests: digests[0], candidates: config.Candidates},
			{digests: digests[1], candidates: config.Candidates},
		},
		[2]*manifest.WorkRecord{
			config.Work.Record(manifest.WorkPreparation, before.Key),
			config.Work.Record(manifest.WorkPreparation, after.Key),
		},
	)
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
			comparison.paths[job.name] = [2]filePresence{}
		}
	}
	out := &Set{paths: comparison.paths, origins: inputs}
	out.recordPresence(&scanned)
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
	evidence [2]scanEvidence,
	work [2]*manifest.WorkRecord,
) (scannedInputs, error) {
	before, after := inputs[0], inputs[1]
	var bef, aft fileScan
	eg, scanCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		bef, err = scanTree(
			scanCtx,
			inputs,
			evidence[0],
			work[0],
		)
		return err
	})
	eg.Go(func() error {
		var err error
		aft, err = scanTree(
			scanCtx,
			[2]*store.SourceArtifact{after, before},
			evidence[1],
			work[1],
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
				return manifest.NewError(manifest.ErrInput, err, "")
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
	paths map[string][2]filePresence
	jobs  []hashJob
}

type comparisonEvidence struct {
	digests      [2]map[string]string
	candidates   map[string]bool
	contentEqual bool
}

func compareScannedFiles(scanned *scannedInputs, evidence comparisonEvidence) fileComparison {
	before, after := scanned.before.entries, scanned.after.entries
	out := fileComparison{paths: make(map[string][2]filePresence, len(after)/changeCapacityDivisor)}
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
			out.paths[name] = [2]filePresence{}
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
	c.paths[name] = [2]filePresence{}
	if evidence.candidates == nil && evidence.digests[0] == nil || evidence.candidates[name] {
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
