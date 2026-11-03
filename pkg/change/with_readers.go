package change

import (
	"cmp"
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"iter"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

type readerScope struct {
	changes        *Set
	bindings       *Set
	evidence       *readerComparison
	indexes        [2]*manifest.DiscoveryIndex
	roots, origins [2]string
	usable         [2]bool
	qualified      [2]map[manifest.NamedResource]bool
	contents       [2]map[string]map[manifest.NamedResource]string
	out            *Set
}

func (s *Set) withReaders(indexes [2]*manifest.DiscoveryIndex, bindings *Set, evidence *readerComparison) *Set {
	r := readerScope{changes: s, bindings: bindings, evidence: evidence, indexes: indexes}
	r.initialize()
	r.resetDeclaredKnown()
	r.contents = s.oneSided
	if r.completeSources() {
		r.collectDeclaredContents()
		r.compareDeclarations()
	}
	out := *s
	r.out = &out
	r.out.inputs = nil
	r.collectVisibility()
	r.routeReaders()
	return r.out
}

func (r *readerScope) initialize() {
	_, bootstrap, _ := r.bindings.ForSource(manifest.BootstrapSourceID)
	for side, input := range bootstrap {
		if input != nil {
			r.roots[side] = input.LocalPath
		}
	}
	for side, input := range r.evidence.artifacts {
		if input == nil {
			continue
		}
		r.origins[side] = input.OriginPath(input.LocalPath)
		r.usable[side] = input.FS != nil && input.Reads.Valid()
	}
}

func (r *readerScope) qualifiedSource(side int, actual manifest.NamedResource) bool {
	if actual == (manifest.NamedResource{}) {
		actual = manifest.BootstrapSourceID
	}
	if actual == r.evidence.source {
		return true
	}
	if known, present := r.qualified[side][actual]; present {
		return known
	}
	_, admitted, _ := r.bindings.ForSource(actual)
	left, right := r.evidence.artifacts[side], admitted[side]
	known := r.usable[side] && right != nil && right.FS != nil && right.Reads.Valid() &&
		(left.InputAlias || right.InputAlias || r.origins[side] == r.roots[side]) &&
		r.origins[side] == right.OriginPath(right.LocalPath)
	if r.qualified[side] == nil {
		r.qualified[side] = map[manifest.NamedResource]bool{}
	}
	r.qualified[side][actual] = known
	return known
}

func (r *readerScope) qualifiedRecord(side int, record *manifest.DiscoveryRecord) bool {
	if len(record.Productions) == 0 {
		return r.qualifiedSource(side, manifest.BootstrapSourceID)
	}
	for i := range record.Productions {
		if r.qualifiedSource(side, record.Productions[i].Source) {
			return true
		}
	}
	return false
}

func (r *readerScope) resetDeclaredKnown() {
	for key, comparison := range r.bindings.declarations { //nolint:gocritic // Map iteration preserves a value snapshot without an extra lookup.
		if key.source == r.evidence.source {
			comparison.declaredKnown = false
			r.bindings.declarations[key] = comparison
		}
	}
}

func (r *readerScope) completeSources() bool {
	complete := [2]bool{}
	for side, index := range r.indexes {
		if index == nil || !r.usable[side] {
			continue
		}
		for actual, known := range index.CompleteSources {
			complete[side] = complete[side] || known && r.qualifiedSource(side, actual)
		}
	}
	return complete[0] && complete[1]
}

func (r *readerScope) fileAt(side int, file string) string {
	if r.roots[side] == "" || r.evidence.artifacts[side] == nil || r.evidence.artifacts[side].LocalPath == "" {
		return file
	}
	if r.origins[side] == r.roots[side] {
		return filepath.ToSlash(filepath.Clean(file))
	}
	relative, err := filepath.Rel(r.origins[side], filepath.Join(r.roots[side], filepath.FromSlash(file)))
	if err != nil {
		return ""
	}
	return filepath.ToSlash(relative)
}

func (r *readerScope) collectDeclaredContents() {
	for side, index := range r.indexes {
		r.contents[side] = maps.Clone(r.contents[side])
		for id, record := range index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
			file := r.fileAt(side, record.File)
			if !r.changes.Contains(file) || !r.qualifiedRecord(side, &record) {
				continue
			}
			if _, known := r.changes.oneSided[side][file]; known || record.Declared == "" {
				continue
			}
			if r.contents[side] == nil {
				r.contents[side] = map[string]map[manifest.NamedResource]string{}
			}
			if r.contents[side][file] == nil {
				r.contents[side][file] = map[manifest.NamedResource]string{}
			}
			authored := record.Authored
			if authored == (manifest.NamedResource{}) {
				authored = id
			}
			r.contents[side][file][authored] = record.Declared
		}
	}
}

func (r *readerScope) compareDeclarations() {
	if r.bindings.declarations == nil {
		r.bindings.declarations = map[comparisonKey]declarationComparison{}
	}
	for side, index := range r.indexes {
		for id, record := range index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
			r.compareDeclared(side, id, &record)
		}
	}
}

func (r *readerScope) compareDeclared(side int, id manifest.NamedResource, record *manifest.DiscoveryRecord) {
	if !r.qualifiedRecord(side, record) {
		return
	}
	peer, indexed := r.indexes[1-side].Lookup(id)
	present := indexed && r.qualifiedRecord(1-side, &peer)
	key := comparisonKey{source: r.evidence.source, owner: id, input: id}
	comparison := r.bindings.declarations[key]
	comparison.inputContent[side], comparison.inputKnown[side] = r.evidence.inputs[side][id]
	comparison.inputContent[1-side], comparison.inputKnown[1-side] = r.evidence.inputs[1-side][id]
	r.bindings.declarations[key] = comparison
	file, peerFile := r.fileAt(side, record.File), r.fileAt(1-side, peer.File)
	if !r.changes.Contains(file) && (!present || !r.changes.Contains(peerFile)) {
		return
	}
	authored := record.Authored
	if authored == (manifest.NamedResource{}) {
		authored = id
	}
	fingerprint := r.contents[side][file][authored]
	if fingerprint == "" {
		return
	}
	comparison.declared = [2]string{}
	comparison.implicitNamespace = [2]bool{}
	comparison.declared[side] = fingerprint
	comparison.implicitNamespace[side] = record.Authored != (manifest.NamedResource{}) &&
		record.Authored.Namespace == ""
	if present {
		peerAuthored := peer.Authored
		if peerAuthored == (manifest.NamedResource{}) {
			peerAuthored = id
		}
		comparison.declared[1-side] = r.contents[1-side][peerFile][peerAuthored]
		if comparison.declared[1-side] == "" {
			return
		}
		comparison.implicitNamespace[1-side] = peer.Authored != (manifest.NamedResource{}) &&
			peer.Authored.Namespace == ""
	} else if !indexed && !r.observeOppositeDeclaration(side, authored, &comparison) {
		return
	}

	comparison.declaredKnown = true
	r.bindings.declarations[key] = comparison
}

func (r *readerScope) observeOppositeDeclaration(
	side int,
	authored manifest.NamedResource,
	comparison *declarationComparison,
) bool {
	observed := false
	for _, declarations := range r.changes.oneSided[1-side] {
		fingerprint, found := declarations[authored]
		if !found {
			continue
		}
		if observed && comparison.declared[1-side] != fingerprint {
			comparison.declared[1-side] = ""
			break
		}
		comparison.declared[1-side], observed = fingerprint, true
		comparison.implicitNamespace[1-side] = authored.Namespace == ""
	}
	return !observed || comparison.declared[1-side] != ""
}

func (r *readerScope) coordinate(side int, name string) string {
	if input := r.evidence.artifacts[side]; input != nil && r.roots[side] != "" {
		if r.origins[side] == r.roots[side] {
			return filepath.ToSlash(filepath.Clean(name))
		}
		relative, err := filepath.Rel(r.roots[side], filepath.Join(r.origins[side], filepath.FromSlash(name)))
		if err != nil {
			return ""
		}
		return filepath.ToSlash(relative)
	}
	return name
}

func (r *readerScope) collectVisibility() {
	type domain struct{ root, ignore string }
	domains := map[domain][2]bool{}
	visible := [2]map[manifest.NamedResource]string{{}, {}}
	for side, index := range r.indexes {
		if index == nil {
			continue
		}
		for _, observation := range index.Visibility {
			if !r.qualifiedSource(side, observation.Source) {
				continue
			}
			key := domain{observation.Root, observation.Ignore}
			observed := domains[key]
			observed[side] = observation.Complete
			domains[key] = observed
			if !observation.Complete {
				continue
			}
			mergeVisible(visible[side], &observation)
		}
	}
	observed := [2]bool{len(domains) != 0, len(domains) != 0}
	for _, complete := range domains {
		for side := range observed {
			observed[side] = observed[side] && complete[side]
		}
	}
	for side, identities := range visible {
		r.publishVisible(side, identities, visible[1-side], observed[1-side])
	}
}

func mergeVisible(visible map[manifest.NamedResource]string, observation *manifest.InputVisibility) {
	for id, files := range observation.Inputs {
		for _, file := range files {
			if previous, ok := visible[id]; !ok || file < previous {
				visible[id] = file
			}
		}
	}
}

func (r *readerScope) publishVisible(side int, identities, opposite map[manifest.NamedResource]string, observed bool) {
	for id, file := range identities {
		if _, present := opposite[id]; present || !observed {
			continue
		}
		input := InputChange{File: file, Unbound: true}
		for reader := range r.indexes[side].ReadersOfFile(r.coordinate(side, file)) {
			if r.qualifiedSource(side, reader.Source) {
				input.Unbound = false
				break
			}
		}
		if r.out.inputs == nil {
			r.out.inputs = map[manifest.NamedResource]InputChange{}
		}
		r.out.inputs[id] = input
	}
}

func (r *readerScope) routeReaders() {
	for side, index := range r.indexes {
		if index == nil {
			continue
		}
		files := make(map[string]string, len(r.changes.paths))
		for name := range r.changes.paths {
			files[r.coordinate(side, name)] = name
		}
		for id, record := range index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
			r.routeRecord(side, id, &record, files)
		}
		r.routeOneSided(side, index)
	}
}

func (r *readerScope) routeOneSided(side int, index *manifest.DiscoveryIndex) {
	for name := range r.changes.oneSided[side] {
		for reader := range index.ReadersOfFile(r.coordinate(side, name)) {
			r.routeInputReader(side, name, &reader)
		}
	}
}

func (r *readerScope) routeInputReader(side int, name string, reader *manifest.FileReader) {
	if reader.Materialize || !r.qualifiedSource(side, reader.Source) {
		return
	}
	if reader.Declaration != (manifest.NamedResource{}) {
		if changed, known := r.bindings.declaredChanged(
			r.evidence.source,
			reader.Declaration,
		); known && !changed {
			return
		}
	}
	if r.out.inputs == nil {
		r.out.inputs = make(map[manifest.NamedResource]InputChange)
	}
	input := r.out.inputs[reader.Reader]
	if input.File == "" || len(input.Declarations) == 0 && name < input.File {
		input.File = name
	}
	input.Unbound = false
	r.out.inputs[reader.Reader] = input
}

type readerRoute struct {
	scope  *readerScope
	side   int
	id     manifest.NamedResource
	record *manifest.DiscoveryRecord
	files  map[string]string
	input  InputChange
}

func (r *readerScope) routeRecord(
	side int,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	files map[string]string,
) {
	if !r.qualifiedRecord(side, record) {
		return
	}
	route := readerRoute{scope: r, side: side, id: id, record: record, files: files}
	route.collect()
	if route.input.File == "" {
		return
	}
	if r.out.inputs == nil {
		r.out.inputs = map[manifest.NamedResource]InputChange{}
	}
	input := r.out.inputs[id]
	input.File, input.Unbound = route.input.File, false
	input.Declarations = append(slices.Clone(input.Declarations), route.input.Declarations...)
	slices.Sort(input.Declarations)
	input.Declarations = slices.Compact(input.Declarations)
	input.Productions = append(slices.Clone(input.Productions), route.input.Productions...)
	slices.SortFunc(input.Productions, func(a, b manifest.Production) int {
		return cmp.Or(
			a.Owner.Compare(b.Owner),
			a.Authored.Compare(b.Authored),
			a.Source.Compare(b.Source),
			strings.Compare(a.File, b.File),
		)
	})
	input.Productions = slices.Compact(input.Productions)
	r.out.inputs[id] = input
}

func (r *readerRoute) collect() {
	pending := []manifest.NamedResource{r.id}
	seen := map[manifest.NamedResource]bool{}
	for len(pending) != 0 {
		owner := pending[0]
		pending = pending[1:]
		if seen[owner] {
			continue
		}
		seen[owner] = true
		record, found := r.scope.indexes[r.side].Lookup(owner)
		if !found || !r.scope.qualifiedRecord(r.side, &record) {
			continue
		}
		pending = r.visit(owner, &record, pending)
	}
}

func (r *readerRoute) visit(
	owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	pending []manifest.NamedResource,
) []manifest.NamedResource {
	if owner != r.id && owner.KindInfo().Render && !owner.ReportHidden() {
		r.input.Productions = append(
			r.input.Productions,
			manifest.Production{
				Owner:       owner,
				Declaration: owner,
				Authored:    r.record.Authored,
				Source:      r.scope.evidence.source,
				File:        r.record.File,
			},
		)
	}
	comparison, known := r.scope.ownerComparison(owner)
	if known && !comparison.changed {
		return pending
	}
	if !r.recordFile(owner, record, &comparison, known) {
		return pending
	}
	for _, file := range record.Inputs {
		if changed, found := r.files[file]; found && (!known || comparison.changed) &&
			(r.input.File == "" || changed < r.input.File) {
			r.input.File = changed
		}
	}
	pending = append(pending, record.Parent, record.BuilderOwner)
	for i := range record.Productions {
		production := &record.Productions[i]
		pending = append(pending, production.Owner, production.Declaration)
	}
	return pending
}

func (r *readerRoute) recordFile(
	owner manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	comparison *declarationComparison,
	known bool,
) bool {
	if record.File == "" {
		return true
	}
	file := record.File
	if artifact := r.scope.evidence.artifacts[r.side]; artifact != nil && r.scope.roots[r.side] != "" {
		file = r.scope.fileAt(r.side, file)
		if !filepath.IsLocal(file) {
			return false
		}
	}
	r.input.Declarations = append(r.input.Declarations, file)
	changed, found := r.files[record.File]
	if !found {
		return true
	}
	declaredChanged, declaredKnown := r.scope.bindings.declaredChanged(r.scope.evidence.source, owner)
	if known && comparison.changed || !known && (!declaredKnown || declaredChanged) {
		if r.input.File == "" || changed < r.input.File {
			r.input.File = changed
		}
	}
	return true
}

func (r *readerScope) ownerComparison(owner manifest.NamedResource) (declarationComparison, bool) {
	comparison, known := r.bindings.declarations[comparisonKey{source: r.evidence.source, owner: owner, input: owner}]
	known = known && comparison.objects[0] != nil && comparison.objects[1] != nil
	if known {
		return comparison, true
	}
	for alias := range r.bindings.Sources() {
		if alias == r.evidence.source || !r.qualifiedSource(0, alias) || !r.qualifiedSource(1, alias) {
			continue
		}
		paired, present := r.bindings.declarations[comparisonKey{source: alias, owner: owner, input: owner}]
		if present && paired.objects[0] != nil && paired.objects[1] != nil {
			return paired, true
		}
	}
	return comparison, false
}

type fileMeta struct {
	size int64
}

// Excluded directories MUST still be traversed so deeper .sourceignore re-includes
// survive, matching github.com/fluxcd/pkg/sourceignore v0.19.0 LoadIgnorePatterns.
func scanTree(
	ctx context.Context,
	input, opposite *store.SourceArtifact,
	digests map[string]string,
) (map[string]fileMeta, error) {
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

	files, err := scanFiles(ctx, scoped, oppositeName, out)
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

func scanFiles(ctx context.Context, scoped fs.FS, oppositeName string, out map[string]fileMeta) ([]string, error) {
	files := []string{}
	err := fs.WalkDir(scoped, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if localErr := ctx.Err(); localErr != nil {
			return fmt.Errorf("context: %w", localErr)
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
			return fmt.Errorf("stat %s: %w", name, err)
		}
		out[name] = fileMeta{size: info.Size()}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk: %w", err)
	}
	return files, nil
}

type hashBuffer struct {
	hash  hash.Hash
	bytes [32 * 1024]byte
	sum   [sha256.Size]byte
}

//nolint:gochecknoglobals // Process-wide reuse keeps file hashing allocation-free.
var hashBuffers = sync.Pool{New: func() any {
	return &hashBuffer{hash: sha256.New()}
}}

func hashFile(ctx context.Context, input fs.FS, name string) ([sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("context: %w", err)
	}
	f, err := input.Open(name)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("open %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()
	buf := hashBuffers.Get().(*hashBuffer) //nolint:errcheck // The pool stores only hashBuffer pointers.
	defer hashBuffers.Put(buf)
	h := buf.hash
	h.Reset()
	for {
		if err := ctx.Err(); err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("context: %w", err)
		}
		n, err := f.Read(buf.bytes[:])
		if n > 0 {
			_, _ = h.Write(buf.bytes[:n])
		}
		if err == io.EOF { //nolint:errorlint // io.Reader requires an unwrapped EOF; preserve other read errors.
			break
		}
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("read %s: %w", name, err)
		}
	}
	h.Sum(buf.sum[:0])
	return buf.sum, nil
}

// Inputs yields qualified one-sided inputs matching a name, or all inputs for an empty name.
func (s *Set) Inputs(input manifest.NamedResource) iter.Seq2[manifest.NamedResource, InputChange] {
	return func(yield func(manifest.NamedResource, InputChange) bool) { s.yieldInputs(input, yield) }
}

func (s *Set) yieldInputs(input manifest.NamedResource, yield func(manifest.NamedResource, InputChange) bool) {
	if s == nil {
		return
	}
	if input.Name != "" && input.Namespace != "" {
		if changed, known := s.inputs[input]; known && !yield(input, changed) {
			return
		}
		unqualified := input
		unqualified.Namespace = ""
		if changed, known := s.inputs[unqualified]; known {
			yield(unqualified, changed)
		}
		return
	}
	for id, file := range s.inputs {
		if input.Name != "" && (id.Group != input.Group || id.Kind != input.Kind || id.Name != input.Name) {
			continue
		}
		if !yield(id, file) {
			return
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
	inputContent      [2]inputprojection.Projection
	inputKnown        [2]bool
	objects           [2]manifest.BaseManifest
	references        [2]manifest.Reference
	changed           bool
	declared          [2]string
	declaredKnown     bool
	implicitNamespace [2]bool
}

func (s *Set) declaredChanged(source, id manifest.NamedResource) (bool, bool) {
	if s == nil {
		return false, false
	}
	fact := s.declarations[comparisonKey{source: source, owner: id, input: id}]
	return fact.declared[0] != fact.declared[1], fact.declaredKnown
}

// DeclarationChanged compares complete declared content and the current prepared document.
// A nil document grants no authority over preparation or additional build inputs.
func (s *Set) DeclarationChanged(
	side manifest.TreeSide,
	source, id manifest.NamedResource,
	document map[string]any,
) (bool, bool) {
	changed, known := s.declaredChanged(source, id)
	if !known || changed || document == nil || side > manifest.TreeHead {
		return changed, known
	}
	fact := s.declarations[comparisonKey{source: source, owner: id, input: id}]
	// Namespace defaulting MUST NOT change identical authored content.
	if _, namespace := manifest.DocMetadata(
		document,
	); fact.implicitNamespace[side] && namespace != "" &&
		namespace == id.Namespace {
		document = maps.Clone(document)
		metadata := maps.Clone(
			document["metadata"].(map[string]any), //nolint:errcheck // ObjectDocument always creates metadata as a map.
		)
		delete(metadata, "namespace")
		document["metadata"] = metadata
	}
	current := manifest.Fingerprint(manifest.WithoutOwnershipLabels(document))
	if current == "" {
		return false, false
	}
	return current != fact.declared[side], true
}
