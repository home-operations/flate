package loader

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) buildProductions(
	ctx context.Context,
	repoRoot string,
	emittedFiles map[string][]manifest.NamedResource,
) error {
	if repoRoot == "" {
		return nil
	}
	b := &selfProduceBuilder{
		idx: l.Index, store: l.Store, reader: l.documents,
		emittedFiles: emittedFiles, locations: l.locations,
	}
	var generators []generatorRecord
	incomplete := map[manifest.NamedResource][]string{}
	var declarations productionDeclarations
	prepared := map[string][]store.Child{}
	preparedOwners := map[manifest.NamedResource]manifest.NamedResource{}
	pinned := l.pinnedProductionOwners()
	remaining := map[string]struct{}{}
	l.scanRemainingGenerators(b, remaining)
	record := func(ctx context.Context, v graphVisit) error {
		if b.scanExclusions && new(l.documents).scanExcluded(v.file) {
			return nil
		}
		if !l.recordProductionInput(&v, repoRoot, pinned, incomplete) {
			return nil
		}
		if v.document != nil {
			delete(remaining, filepath.Dir(v.file))
		}
		if v.role != kustomizeinput.Resource {
			return nil
		}
		if l.documents.cache[v.file].selected {
			l.recordPreparedProductions(b, &v, prepared[v.coordinate], preparedOwners)
			return nil
		}
		if v.document != nil {
			l.recordGraphGenerators(&v, &generators)
			return nil
		}
		return declarations.recordVisit(ctx, l, b, new(v))
	}
	l.prepareBuiltProductions(b, repoRoot, prepared, preparedOwners)
	if err := declarations.recordContributions(ctx, l, b, record); err != nil {
		return err
	}
	if l.documents.compact {
		l.recordContributionRoots()
	}

	l.warnIncompleteProductions(incomplete)

	for _, dir := range l.unreachedGeneratorRoots(repoRoot, remaining) {
		root := graphVisit{root: repoRoot, dir: dir}
		walks := l.visitScope(new(root), false)
		if err := declarations.recordStandalone(ctx, b, record, walks); err != nil {
			return err
		}
	}
	l.recordUnownedProductions(b)
	if b.err != nil {
		return b.err
	}
	return l.addGenerated(ctx, generators)
}

func (l *Loader) addGenerated(ctx context.Context, generators []generatorRecord) error {
	{
		rValues := generators
		for rIndex := range rValues {
			r := rValues[rIndex]
			// Generator names are indexed before suffix hashing, including failed inputs.
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: r.namespace, Name: r.args.Name}
			if r.isConfigMap {
				id.Kind = manifest.KindConfigMap
			}
			record := l.Index.Records[id]
			if record.File == "" {
				record.File = l.fileCoordinate(r.file)
			}
			p := manifest.Production{Owner: r.owner, Source: r.source, File: l.fileCoordinate(r.file)}
			if ks, ok := l.Store.Get[*manifest.Kustomization](r.owner); ok {
				p.Source = ks.SourceID()
			}
			if p.Source.Name == "" || p.Source.Kind == "" {
				p.Source = manifest.BootstrapSourceID
			}
			if !slices.Contains(record.Productions, p) {
				record.Productions = append(record.Productions, p)
			}
			l.Index.Records[id] = record
			if l.documents.compact {
				continue
			}
			child, err := r.materialize(ctx, &l.Options)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, errRemoteInputs) {
					return err
				}
				slog.Debug("loader: skipping generator input", "file", r.file, "generator", r.args.Name, "error", err)
				continue
			}
			record.ReferenceData = append(
				record.ReferenceData,
				//nolint:errcheck // Generator materialization must panic if it produces a non-Data object.
				manifest.ReferenceData{Production: p, OfflineData: child.Obj.(manifest.Data), Warnings: child.Warnings},
			)
			l.Index.Records[id] = record
		}
	}
	return nil
}

type graphContext struct {
	root, dir, namespace, fallback string
	namePrefix, nameSuffix         string
	pluginRoot                     string
	role                           kustomizeinput.Role
	deferMaterialization           bool
	checkMetadata                  bool
	deferDecode                    bool
	entryResources                 bool
	requiresBuild                  bool
}

type graphVisit struct {
	graphContext

	file, origin       string
	localFile          string
	prefix, coordinate string
	owner              manifest.NamedResource
	source             manifest.NamedResource
	input              int
	localBase          string
	incomplete         string
	document           *types.Kustomization
	readable           bool
	membershipOnly     bool
}

func (v *graphVisit) localCoordinate() string {
	if v.localBase == "" || v.coordinate == v.prefix {
		return v.localBase
	}
	local := strings.TrimPrefix(v.coordinate, v.prefix+"/")
	if v.localBase == "." {
		return local
	}
	return path.Join(v.localBase, local)
}

type producerDeclaration struct {
	obj      manifest.BaseManifest
	fact     *authoredOccurrence
	identity manifest.NamedResource
	authored manifest.NamedResource
	visit    graphVisit
}

type selfProduceBuilder struct {
	reader         documentReader
	idx            *manifest.DiscoveryIndex
	emittedFiles   map[string][]manifest.NamedResource
	store          *store.Store
	locations      map[manifest.NamedResource]authoredOccurrence
	scanExclusions bool
	err            error
}

func (b *selfProduceBuilder) recordProduced(
	ctx context.Context,
	vArg *graphVisit,
	declarations []producerDeclaration,
	l *Loader,
) ([]producerDeclaration, error) {
	pr := productionRecorder{visit: *vArg}
	pr.coordinate = cmp.Or(pr.visit.localCoordinate(), pr.visit.coordinate)
	pr.declarations = declarations
	record := func(child parsedObject, version string) {
		pr.record(b, l, new(child), version)
	}
	reader := b.reader
	if cached := reader.cache[pr.visit.file]; cached.objects != nil {
		reader.intent = cached.coverage
	}
	var objects []parsedObject
	var err error
	if reader.compact {
		objects, err = reader.censusFile(ctx, pr.visit.file, manifest.ParseDocOptions{})
	} else {
		objects, err = reader.fileObjects(pr.visit.file, manifest.ParseDocOptions{})
	}
	if err != nil {
		return pr.declarations, err
	}

	{
		objValues := objects
		for objIndex := range objValues {
			obj := objValues[objIndex]
			if obj.outcome.Err == nil {
				record(obj, obj.version)
			}
		}
	}
	return pr.declarations, pr.projectionErr
}

func (b *selfProduceBuilder) joinProductions(declarations []producerDeclaration, scope []graphVisit) {
	if len(declarations) == 0 {
		return
	}
	var candidates []manifest.NamedResource
	{
		vValues4 := scope
		for vIndex4 := range vValues4 {
			v := vValues4[vIndex4]
			if v.role != kustomizeinput.Resource || v.document != nil || !v.readable ||
				b.scanExclusions && new(b.reader).scanExcluded(v.file) {
				continue
			}
			record := func(child store.Child) {
				b.collectProducerReferences(&child, &v, &candidates)
			}
			reader := b.reader
			if cached := reader.cache[v.file]; cached.objects != nil {
				reader.intent = cached.coverage
			}
			objects, err := reader.fileObjects(v.file, manifest.ParseDocOptions{})
			if err != nil {
				if b.err == nil {
					b.err = err
				}
				continue
			}
			{
				objValues2 := objects
				for objIndex2 := range objValues2 {
					obj := objValues2[objIndex2]
					if obj.outcome.Err == nil {
						record(obj.Child)
					}
				}
			}
		}
	}
	slices.SortFunc(candidates, manifest.NamedResource.Compare)
	candidates = slices.Compact(candidates)
	{
		declarationValues := declarations
		for declarationIndex := range declarationValues {
			declaration := declarationValues[declarationIndex]
			b.recordProducer(new(declaration), candidates)
		}
	}
}

func (b *selfProduceBuilder) recordProducer(declarationArg *producerDeclaration, candidates []manifest.NamedResource) {
	declaration := *declarationArg
	obj, authored, v := declaration.obj, declaration.authored, declaration.visit
	projected := declaration.identity
	if v.owner == (manifest.NamedResource{}) &&
		(manifest.KindOf(obj).Runnable || declaration.fact != nil && projected.KindInfo().Runnable) {
		v.owner = projected
	}
	var targets []manifest.ProducerTarget
	if declaration.fact != nil {
		targets = declaration.fact.facts.authoredTargets
	} else {
		targets = manifest.ProducerTargets(obj, candidates...)
	}
	for _, target := range targets {
		target.NamedResource = manifest.Resolve(target.NamedResource, v.namespace, v.fallback)
		if target.Namespace == "" || target.Name == "" || manifest.HasEnvsubstReference(target.Name) ||
			manifest.HasEnvsubstReference(target.Namespace) {
			continue
		}
		p := manifest.Production{
			Owner:       v.owner,
			Declaration: projected,
			Authored:    authored,
			Source:      v.source,
			File:        cmp.Or(v.localCoordinate(), v.coordinate),
		}
		r := b.idx.Records[target.NamedResource]
		if !slices.Contains(r.Productions, p) {
			r.Productions = append(r.Productions, p)
		}
		if len(target.DeclaredKeys) != 0 &&
			!slices.ContainsFunc(
				r.ReferenceData,
				func(candidate manifest.ReferenceData) bool { return candidate.Production == p },
			) {
			r.ReferenceData = append(
				r.ReferenceData,
				manifest.ReferenceData{Production: p, DeclaredKeys: target.DeclaredKeys},
			)
		}
		b.idx.Records[target.NamedResource] = r
		if v.owner != (manifest.NamedResource{}) && (declaration.fact == nil || !declaration.fact.facts.uncertain) {
			b.recordDeclarationProduction(authored, projected, &p)
		}
	}
}

func appendUniqueProducer[T comparable](dst []T, v T) []T {
	if slices.Contains(dst, v) {
		return dst
	}
	return append(dst, v)
}

func (b *selfProduceBuilder) collectProducerReferences(
	child *store.Child,
	v *graphVisit,
	candidates *[]manifest.NamedResource,
) {
	obj := child.Obj
	if obj == nil {
		return
	}
	ns := manifest.Resolve(obj.Named(), v.namespace, v.fallback).Namespace
	if ns != obj.Named().Namespace {
		obj = obj.WithNamespace(ns)
	}
	if v.deferMaterialization && !b.reader.cache[v.file].loaded && b.store.GetObject(obj.Named()) == nil {
		return
	}
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		*candidates = append(*candidates, ref.NamedResource)
	}
}

func (b *selfProduceBuilder) recordDeclarationProduction(
	authored manifest.NamedResource,
	projected manifest.NamedResource,
	production *manifest.Production,
) {
	for _, producer := range []manifest.NamedResource{authored, projected} {
		r := b.idx.Records[producer]
		if !slices.Contains(r.Productions, *production) {
			r.Productions = append(r.Productions, *production)
		}
		b.idx.Records[producer] = r
	}
}

func (b *selfProduceBuilder) recordApplication(l *Loader, declaration *producerDeclaration) {
	if declaration.visit.owner == (manifest.NamedResource{}) || declaration.visit.owner == declaration.identity {
		return
	}
	object := declaration.obj
	if object == nil && declaration.fact != nil {
		object = manifest.CatalogObject(declaration.fact.facts.catalog, declaration.identity)
	}
	application, valid := manifest.ApplicationFor(object)
	input, _ := b.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if !valid || !AppliesToEntry(b.store, &application, input, l.entryPath) {
		return
	}
	relative, _ := filepath.Rel(l.SourceRoot, l.entryPath)
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), manifest.NamedResource{}).
		Named()
	production := manifest.Production{
		Owner:       declaration.visit.owner,
		Declaration: declaration.identity,
		Authored:    declaration.authored,
		Source:      declaration.visit.source,
		File:        cmp.Or(declaration.visit.localCoordinate(), declaration.visit.coordinate),
	}
	record := b.idx.Records[entry]
	if !slices.Contains(record.Productions, production) {
		record.Productions = append(record.Productions, production)
		b.idx.Records[entry] = record
	}
}
