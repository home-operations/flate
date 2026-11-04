package change

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// WithDeclarations freezes observed authoritative declaration and reference comparisons before dispatch.
// Only completed producer output is prepared comparison authority; raw producer-owned input is excluded.
// Missing preparation is unknown; the returned set retains no Store or captured read set.
func (s *Set) WithDeclarations(
	ctx context.Context,
	input DeclarationInputs,
) (*Set, error) {
	if s == nil {
		return nil, nil //nolint:nilnil // A nil result denotes successful absence.
	}
	out := *s
	out.declarations = maps.Clone(s.declarations)
	if out.declarations == nil {
		out.declarations = map[comparisonKey]declarationComparison{}
	}
	for key, comparison := range out.declarations { //nolint:gocritic // Map iteration preserves a value snapshot without an extra lookup.
		comparison.objects, comparison.references, comparison.changed = [2]manifest.BaseManifest{}, [2]manifest.Reference{}, false
		out.declarations[key] = comparison
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	if input.Stores[0] == nil || input.Stores[1] == nil {
		return out.freezeSources(), nil
	}
	d := declarationScope{changes: s, stores: input.Stores, out: &out, work: input.Work}
	for side := range d.stores {
		if err := d.capture(ctx, side, input.Prepared[side]); err != nil {
			return nil, err
		}
	}
	d.markOneSided()
	d.applyCopied()
	for id, left := range d.objects[0] {
		if err := d.compare(ctx, id, &left); err != nil {
			return nil, err
		}
	}
	return out.freezeSources(), nil
}

type declarationScope struct {
	work    manifest.WorkScope
	changes *Set
	stores  [2]*store.Store
	objects [2]map[manifest.NamedResource]store.Child
	sources [2]map[manifest.NamedResource]manifest.NamedResource
	copied  map[*Set]*Set
	out     *Set
}

func (d *declarationScope) capture(
	ctx context.Context,
	side int,
	prepared map[manifest.NamedResource]store.Child,
) error {
	st := d.stores[side]
	d.objects[side], d.sources[side] = map[manifest.NamedResource]store.Child{}, map[manifest.NamedResource]manifest.NamedResource{}
	for _, obj := range st.Objects() {
		id := obj.Named()
		if st.GetDeclaration(id) != obj {
			continue
		}
		_, doc, emission := st.GetEmission(id)
		emission.Finish()
		d.objects[side][id] = store.Child{Obj: obj, Doc: doc}
	}
	for id, child := range prepared {
		if _, known := d.objects[side][id]; !known {
			d.objects[side][id] = child
		}
	}
	for id, child := range d.objects[side] {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		source, known := d.declarationSource(side, id)
		if !known {
			delete(d.objects[side], id)
			continue
		}
		d.objects[side][id] = store.Child{Obj: child.Obj, Doc: manifest.DeepCopyMap(child.Doc)}
		d.sources[side][id] = source
	}
	return nil
}

func (d *declarationScope) declarationSource(side int, id manifest.NamedResource) (manifest.NamedResource, bool) {
	st := d.stores[side]
	source, found := manifest.BootstrapSourceID, false
	{
		productionValues := st.Productions(id, nil)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]

			ref := production.Source
			if ref == (manifest.NamedResource{}) {
				ref = manifest.BootstrapSourceID
			}
			if found && ref != source {
				return source, false
			}
			source, found = ref, true
		}
	}
	_, snapshots, _ := d.changes.ForSource(source)
	return source, snapshots[side] != nil && snapshots[side] == st.GetArtifact(source, store.ArtifactContent)
}

func (d *declarationScope) markOneSided() {
	for side, declarations := range d.objects {
		for id := range declarations {
			if _, present := d.objects[1-side][id]; present {
				continue
			}
			source := d.sources[side][id]
			delta, _, _ := d.changes.ForSource(source)
			if delta == nil {
				continue
			}
			key := comparisonKey{source: source, owner: id, input: id}
			comparison := d.out.declarations[key]
			comparison.objects[side], comparison.changed = declarations[id].Obj, true
			d.out.declarations[key] = comparison
			{
				productionValues2 := d.stores[side].Productions(id, nil)
				for productionIndex2 := range productionValues2 {
					production := productionValues2[productionIndex2]

					if production.Source != source || production.File == "" || !delta.Contains(production.File) {
						continue
					}
					d.copyInput(delta, id, production.File)
				}
			}
		}
	}
}

func (d *declarationScope) copyInput(delta *Set, id manifest.NamedResource, file string) {
	if d.copied == nil {
		d.copied = map[*Set]*Set{}
	}
	local := d.copied[delta]
	if local == nil {
		clone := *delta
		clone.inputs = maps.Clone(delta.inputs)
		if clone.inputs == nil {
			clone.inputs = map[manifest.NamedResource]InputChange{}
		}
		local = &clone
		d.copied[delta] = local
	}
	local.inputs[id] = InputChange{File: file}
}

func (d *declarationScope) applyCopied() {
	if len(d.copied) == 0 {
		return
	}
	d.out.sources = maps.Clone(d.changes.sources)
	for source, evidence := range d.out.sources {
		if local := d.copied[evidence.changes]; local != nil {
			evidence.changes = local
			d.out.sources[source] = evidence
		}
	}
	if local := d.copied[d.changes]; local != nil {
		d.out.inputs = local.inputs
	}
}

func (d *declarationScope) compare(ctx context.Context, id manifest.NamedResource, left *store.Child) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	if kind := manifest.KindOf(left.Obj); kind.Runnable && !kind.Render {
		return nil
	}
	right, ok := d.objects[1][id]
	if !ok || d.sources[0][id] != d.sources[1][id] {
		return nil
	}
	children := [2]store.Child{*left, right}
	if !d.validSourcePaths(ctx, &children) {
		return nil
	}
	equal, err := manifest.OutputEqual(left.Obj, right.Obj, [2]map[string]any{left.Doc, right.Doc})
	if err != nil {
		return err
	}
	key := comparisonKey{source: d.sources[0][id], owner: id, input: id}
	comparison := d.out.declarations[key]
	comparison.objects, comparison.changed = [2]manifest.BaseManifest{left.Obj, right.Obj}, !equal
	d.out.declarations[key] = comparison
	return d.compareReferences(id, &children, d.sameInputs(&comparison))
}

func (d *declarationScope) validSourcePaths(_ context.Context, children *[2]store.Child) bool {
	for side, child := range *children {
		ref, path, applies := child.Obj.SourcePath()
		if !applies {
			continue
		}
		input, _ := d.stores[side].GetArtifact(ref, store.ArtifactContent).(*store.SourceArtifact)
		if input == nil || input.FS == nil {
			continue
		}
		info, err := fs.Stat(input.FS, manifest.NormalizeClaimBase(path))
		if err != nil || !info.IsDir() {
			return false
		}
	}
	return true
}

func (d *declarationScope) sameInputs(comparison *declarationComparison) bool {
	if !comparison.inputKnown[0] || !comparison.inputKnown[1] {
		return false
	}
	projections := comparison.inputContent
	same := manifest.ValueEqual(projections[0].Content, projections[1].Content)
	for side, projection := range projections {
		for input, captured := range projection.Inputs {
			data, ok := d.objects[side][input].Obj.(manifest.Data)
			if !ok || !manifest.ValueEqual(data, captured) {
				same = false
			}
		}
	}
	return same
}

func (d *declarationScope) compareReferences(
	id manifest.NamedResource,
	children *[2]store.Child,
	sameInputs bool,
) error {
	work := d.work.Source(d.sources[0][id]).Record(manifest.WorkPreparation, "declarations")
	refs := [2][]manifest.Reference{}
	for side, child := range *children {
		for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
			work.Add(&manifest.WorkCounts{ReaderEdges: 1})
			refs[side] = append(refs[side], ref.Clone())
		}
	}
	for i := range refs[0] {
		ref := &refs[0][i]
		if !ref.ContentInput() || ref.Selector != nil {
			continue
		}
		peer := slices.IndexFunc(
			refs[1],
			func(other manifest.Reference) bool {
				return referencesEqual(ref, &other)
			},
		)
		if peer < 0 {
			continue
		}
		inputs := [2]store.Child{d.objects[0][ref.NamedResource], d.objects[1][ref.NamedResource]}
		comparison, err := referenceChanged(ref, &inputs)
		if err != nil {
			return err
		}
		if !comparison.Known {
			continue
		}
		if sameInputs {
			comparison.Changed = false
		}
		d.out.declarations[comparisonKey{source: d.sources[0][id], owner: id, input: ref.NamedResource, use: ref.Use, position: ref.Position}] = declarationComparison{
			objects:    [2]manifest.BaseManifest{children[0].Obj, children[1].Obj},
			references: [2]manifest.Reference{*ref, refs[1][peer]},
			changed:    comparison.Changed,
		}
	}
	return nil
}

// Comparison grants a change verdict only when Known is true.
type Comparison struct{ Changed, Known bool }

func referenceChanged(ref *manifest.Reference, inputs *[2]store.Child) (Comparison, error) {
	if inputs[0].Obj == nil || inputs[1].Obj == nil {
		return Comparison{}, nil
	}
	if _, left := inputs[0].Obj.(manifest.Data); !left {
		return Comparison{}, nil
	}
	if _, right := inputs[1].Obj.(manifest.Data); !right {
		return Comparison{}, nil
	}
	if ref.Key != "" {
		values := [2]string{}
		present := [2]bool{}
		for side, input := range *inputs {
			data, ok := input.Obj.(manifest.Data)
			if !ok {
				return Comparison{}, nil
			}
			value, err := data.Value(ref.Key)
			if err != nil {
				return Comparison{}, nil //nolint:nilerr // Unreadable values leave comparison unknown.
			}
			values[side], present[side] = value.Value, value.Found
		}
		if !present[0] || !present[1] {
			return Comparison{}, nil
		}
		return Comparison{Changed: values[0] != values[1] || present[0] != present[1], Known: true}, nil
	}
	equal, err := manifest.OutputEqual(inputs[0].Obj, inputs[1].Obj, [2]map[string]any{inputs[0].Doc, inputs[1].Doc})
	if err != nil {
		return Comparison{}, err
	}
	return Comparison{Changed: !equal, Known: true}, nil
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

func freezeArtifacts(
	inputs [2]*store.SourceArtifact,
	snapshots map[*store.SourceArtifact]*store.SourceArtifact,
) [2]*store.SourceArtifact {
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
//
//nolint:gocritic // OutputChanged preserves the SDK reference value snapshot.
func (s *Set) OutputChanged(
	side manifest.TreeSide,
	source manifest.NamedResource,
	ref manifest.Reference,
	current manifest.BaseManifest,
) Comparison {
	if s == nil || current == nil || side > manifest.TreeHead {
		return Comparison{}
	}
	fact, ok := s.declarations[comparisonKey{source: source, owner: current.Named(), input: ref.NamedResource, use: ref.Use, position: ref.Position}]
	if !ok || current != fact.objects[side] || fact.objects[manifest.TreeHead-side] == nil {
		return Comparison{}
	}
	if ref.Use != "" && !referencesEqual(&ref, &fact.references[side]) {
		return Comparison{}
	}
	return Comparison{Changed: fact.changed, Known: true}
}

// OppositeDeclaration returns the opposite declaration while the local identity and declaration apply.
// Source artifact applicability MUST be checked by the caller against ForSource first.
//
//nolint:ireturn // Declarations retain their heterogeneous manifest types.
func (s *Set) OppositeDeclaration(
	side manifest.TreeSide,
	source, id manifest.NamedResource,
	current manifest.BaseManifest,
) manifest.BaseManifest {
	if s == nil || side > manifest.TreeHead {
		return nil
	}
	fact := s.declarations[comparisonKey{source: source, owner: id, input: id}]
	if current != fact.objects[side] {
		return nil
	}
	return fact.objects[manifest.TreeHead-side]
}

func referencesEqual(a, b *manifest.Reference) bool {
	left, right := a.DependencyRef, b.DependencyRef
	left.ReadyExpr, right.ReadyExpr = nil, nil
	if left != right || (a.ReadyExpr == nil) != (b.ReadyExpr == nil) ||
		a.ReadyExpr != nil && *a.ReadyExpr != *b.ReadyExpr {
		return false
	}
	return a.Use == b.Use && a.Position == b.Position && a.Optional == b.Optional && a.Content == b.Content &&
		a.Gate == b.Gate &&
		a.ArtifactStages == b.ArtifactStages &&
		a.Ordering == b.Ordering &&
		a.ChartPath == b.ChartPath &&
		a.Key == b.Key &&
		a.NamespaceDefault == b.NamespaceDefault &&
		a.Selector == b.Selector &&
		slices.Equal(a.Files, b.Files)
}
