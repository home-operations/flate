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
	o := *fact
	if !o.facts.indexed() {
		return
	}
	projected, err := l.projectOccurrence(&o, &pr.visit, version)
	if err != nil {
		pr.projectionErr = err
		return
	}
	o = projected
	id := o.Identity
	if o.facts.uncertain {
		b.recordProducer(&producerDeclaration{
			fact: &o, visit: pr.visit, identity: id, authored: o.Authored,
		}, nil)
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
	declaration := producerDeclaration{fact: &o, identity: id, authored: o.Authored, visit: pr.visit}
	b.recordProducer(&declaration, nil)
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

//nolint:gocognit // Physical and contextual declarations MUST retain one producer publication decision.
func (pr *productionRecorder) projectObject(
	b *selfProduceBuilder,
	l *Loader,
	childArg *parsedObject,
	version string,
) (objectProduction, bool) {
	child := *childArg
	if pr.projectionErr != nil || child.outcome.Err != nil || child.Obj == nil && child.fact == nil {
		return objectProduction{}, false
	}
	obj, info := child.Obj, manifest.KindOf(child.Obj)
	if child.fact != nil {
		pr.recordFact(b, l, child.fact, version)
		return objectProduction{}, false
	}
	_, _, hasPath := obj.SourcePath()
	if !info.Tracked && !hasPath && b.reader.cache[pr.visit.file].loaded && len(manifest.ProducerTargets(obj)) == 0 {
		return objectProduction{}, false
	}
	resolved := manifest.Resolve(obj.Named(), pr.visit.namespace, pr.visit.fallback)
	production := objectProduction{
		object: obj, kind: info, resolved: resolved, identity: resolved,
		substituteDisabled: child.substituteDisabled,
	}
	if occurrence, known := l.authoredOccurrence(
		pr.visit.file,
		obj.Named(),
	); known &&
		l.contextualDeclaration(pr.visit.file, obj.Named()) {
		projected, err := l.projectOccurrence(occurrence.fact, &pr.visit, version)
		if err != nil {
			pr.projectionErr = err
			return objectProduction{}, false
		}
		if projected.facts.uncertain {
			if !pr.visit.deferMaterialization {
				b.recordProducer(
					&producerDeclaration{
						fact:     &projected,
						visit:    pr.visit,
						identity: projected.Identity,
						authored: projected.Authored,
					},
					nil,
				)
			}
			return objectProduction{}, false
		}
		production.fact, production.identity = &projected, projected.Identity
		return production, true
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
	projected := op.object
	needsCandidates := manifest.ProducerNeedsCandidates(op.object)
	if op.identity.Namespace != op.object.Named().Namespace &&
		(r.Authored == (manifest.NamedResource{}) || needsCandidates || op.identity.Namespace != op.resolved.Namespace) {
		projected = op.object.WithNamespace(op.identity.Namespace)
	}
	if r.Authored == (manifest.NamedResource{}) {
		r.Authored, r.Path, r.File = op.object.Named(), pr.visit.file, pr.coordinate
		if op.identity.Name != op.object.Named().Name {
			r.Path = ""
		}
		if op.fact != nil {
			r.Refs = authoredReferences(op.fact)
		} else {
			r.Refs = discoveryReferences(nil, projected)
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
	if ks, ok := b.store.Get[*manifest.Kustomization](pr.visit.owner); ok {
		p.Source = ks.EffectiveSourceID()
	}
	if op.fact != nil {
		projectOccurrenceProduction(op.fact, &p)
	}
	if !slices.Contains(r.Productions, p) {
		r.Productions = append(r.Productions, p)
	}
	b.idx.Records[op.identity] = r
	b.recordApplication(
		l,
		&producerDeclaration{
			obj:      projected,
			fact:     op.fact,
			identity: op.identity,
			authored: op.object.Named(),
			visit:    pr.visit,
		},
	)
	op.recordDeclaration(pr, b, projected, needsCandidates)
	if op.kind.Structural {
		b.emittedFiles[pr.coordinate] = appendUniqueProducer(b.emittedFiles[pr.coordinate], pr.visit.owner)
	}
}

func (op *objectProduction) recordDeclaration(
	pr *productionRecorder,
	b *selfProduceBuilder,
	projected manifest.BaseManifest,
	needsCandidates bool,
) {
	if !pr.visit.deferMaterialization ||
		op.kind.Runnable && (b.reader.cache[pr.visit.file].loaded || b.store.GetObject(op.identity) != nil) {
		declaration := producerDeclaration{
			obj:                projected,
			identity:           op.identity,
			authored:           op.object.Named(),
			visit:              pr.visit,
			substituteDisabled: op.substituteDisabled,
		}
		if needsCandidates {
			pr.declarations = append(pr.declarations, declaration)
		} else {
			declaration.fact = op.fact
			b.recordProducer(new(declaration), nil)
		}
	}
}
