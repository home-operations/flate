package loader

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

type productionRecorder struct {
	visit         graphVisit
	coordinate    string
	declarations  []producerDeclaration
	projectionErr error
}

func (pr *productionRecorder) record(b *selfProduceBuilder, l *Loader, childArg *parsedObject, version string) {
	production, include := pr.projectObject(b, l, childArg, version)
	if !include {
		return
	}
	production.publish(pr, b, l)
}

func (pr *productionRecorder) recordFact(b *selfProduceBuilder, l *Loader, fact *authoredOccurrence, version string) {
	if !fact.indexed() && !fact.Identity.CanDeclareGeneratedEntry() {
		return
	}
	o, err := l.projectOccurrence(fact, &pr.visit, version)
	if err != nil {
		pr.projectionErr = err
		return
	}
	if !o.indexed() {
		return
	}
	id := o.Identity
	declaration := producerDeclaration{fact: &o, identity: id, authored: o.Authored, visit: pr.visit}
	declaration.project(!o.facts.uncertain && declaration.hasApplicationOwner())
	if o.facts.uncertain {
		b.recordProducer(&declaration, nil)
		return
	}

	o.requiresBuild = o.requiresBuild || pr.visit.requiresBuild || b.locations[id].requiresBuild
	r := b.idx.Records[id]
	if r.Authored == (manifest.NamedResource{}) {
		r.Authored, r.Path, r.File = o.Authored, pr.visit.file, pr.coordinate
		for ref := range o.references() {
			if discoveryReference(new(ref)) {
				r.Refs = append(r.Refs, ref.NamedResource)
			}
		}
	}
	p := manifest.Production{
		Owner:    pr.visit.owner,
		Authored: o.Authored,
		Path:     pr.visit.file,
		File:     pr.coordinate,
		Source:   pr.visit.source,
	}
	if pr.visit.owner != (manifest.NamedResource{}) && id.KindInfo().Runnable && b.store.GetObject(id) == nil {
		p.Path = ""
	}
	projectOccurrenceProduction(&o, &p)
	if !slices.Contains(r.Productions, p) {
		r.Productions = append(r.Productions, p)
	}
	b.idx.Records[id] = r
	if r.Path == pr.visit.file && r.Authored == o.Authored {
		b.locations[id] = o
	}
	pr.recordDeclaration(b, &declaration)
	b.recordApplication(l, &declaration)
	if id.KindInfo().Structural {
		b.emittedFiles[pr.coordinate] = appendUniqueProducer(b.emittedFiles[pr.coordinate], pr.visit.owner)
	}
}

type objectProduction struct {
	fact               *authoredOccurrence
	object             manifest.BaseManifest
	kind               manifest.KindInfo
	resolved           manifest.NamedResource
	identity           manifest.NamedResource
	substituteDisabled bool
}

func (pr *productionRecorder) projectObject(
	b *selfProduceBuilder,
	l *Loader,
	childArg *parsedObject,
	version string,
) (objectProduction, bool) {
	child := *childArg
	switch {
	case pr.projectionErr != nil, child.outcome.Err != nil, child.Obj == nil && child.fact == nil:
		return objectProduction{}, false
	case child.fact != nil:
		pr.recordFact(b, l, child.fact, version)
		return objectProduction{}, false
	}
	obj, info := child.Obj, manifest.KindOf(child.Obj)
	_, _, hasPath := obj.SourcePath()
	if !info.Tracked && !hasPath && b.reader.cache[pr.visit.file].loaded && len(manifest.ProducerTargets(obj)) == 0 {
		return objectProduction{}, false
	}
	resolved := manifest.Resolve(obj.Named(), pr.visit.namespace, pr.visit.fallback)
	production := objectProduction{
		object: obj, kind: info, resolved: resolved, identity: resolved,
		substituteDisabled: child.substituteDisabled,
	}
	occurrence, known := l.authoredOccurrence(pr.visit.file, obj.Named())
	contextual := known && l.contextualDeclaration(pr.visit.file, obj.Named())
	if contextual {
		projected, err := l.projectOccurrence(occurrence.fact, &pr.visit, version)
		if err != nil {
			pr.projectionErr = err
			return objectProduction{}, false
		}
		if !projected.facts.uncertain {
			production.fact, production.identity = &projected, projected.Identity
			return production, true
		}
		if !pr.visit.deferMaterialization {
			declaration := producerDeclaration{
				fact: &projected, visit: pr.visit, identity: projected.Identity, authored: projected.Authored,
			}
			declaration.project(false)
			b.recordProducer(&declaration, nil)
		}
		return objectProduction{}, false
	}
	if owner, _ := b.idx.Lookup(pr.visit.owner); pr.visit.owner == obj.Named() || owner.Authored == obj.Named() {
		production.identity = pr.visit.owner
	} else if info.Runnable && (pr.visit.namePrefix != "" || pr.visit.nameSuffix != "") {
		production.identity, pr.projectionErr = l.projectIdentity(
			obj.Named(),
			new(pr.visit),
			version,
			manifest.KindOf(obj).Namespaced(),
		)
		if pr.projectionErr != nil {
			return objectProduction{}, false
		}
	}
	return production, true
}

func (op *objectProduction) publish(pr *productionRecorder, b *selfProduceBuilder, l *Loader) {
	r := b.idx.Records[op.identity]
	declaration := producerDeclaration{
		obj: op.object, fact: op.fact, identity: op.identity, authored: op.object.Named(),
		visit: pr.visit, substituteDisabled: op.substituteDisabled,
	}
	declaration.project(
		r.Authored == (manifest.NamedResource{}) || op.identity.Namespace != op.resolved.Namespace,
	)
	if r.Authored == (manifest.NamedResource{}) {
		r.Authored, r.Path, r.File = op.object.Named(), pr.visit.file, pr.coordinate
		if op.identity.Name != op.object.Named().Name {
			r.Path = ""
		}
		if op.fact != nil {
			r.Refs = authoredReferences(op.fact)
		} else {
			r.Refs = discoveryReferences(nil, declaration.obj)
		}
	}
	p := manifest.Production{
		Owner:    pr.visit.owner,
		Authored: op.object.Named(),
		Path:     pr.visit.file,
		File:     pr.coordinate,
		Source:   pr.visit.source,
	}
	if p.Materializer(op.identity) != (manifest.NamedResource{}) && op.kind.Runnable &&
		b.store.GetObject(op.identity) == nil {
		p.Path = ""
	}
	if owner := b.store.GetObject(pr.visit.owner); owner != nil {
		if source, _, applies := owner.SourcePath(); applies {
			p.Source = source
		}
	}
	if op.fact != nil {
		projectOccurrenceProduction(op.fact, &p)
	}
	if !slices.Contains(r.Productions, p) {
		r.Productions = append(r.Productions, p)
	}
	b.idx.Records[op.identity] = r
	b.recordApplication(l, &declaration)
	if !pr.visit.deferMaterialization ||
		op.kind.Runnable && (b.reader.cache[pr.visit.file].loaded || b.store.GetObject(op.identity) != nil) {
		pr.recordDeclaration(b, &declaration)
	}
	if op.kind.Structural {
		b.emittedFiles[pr.coordinate] = appendUniqueProducer(b.emittedFiles[pr.coordinate], pr.visit.owner)
	}
}

func (pr *productionRecorder) recordDeclaration(b *selfProduceBuilder, declaration *producerDeclaration) {
	if declaration.needsCandidates {
		pr.declarations = append(pr.declarations, *declaration)
		return
	}
	b.recordProducer(declaration, nil)
}
