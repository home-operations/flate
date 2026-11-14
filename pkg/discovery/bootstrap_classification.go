package discovery

import (
	"cmp"
	"context"
	"net/url"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6/plumbing/transport"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

type sourceRequest struct {
	object    manifest.BaseManifest
	reference manifest.Reference
	reads     *store.ReadSet
	work      manifest.WorkScope
	scoped    bool
	producers []occurrenceKey
	entry     bool
	projected bool
}

type entrySourceGroup struct {
	reference manifest.Reference
	producers []occurrenceKey
	object    manifest.BaseManifest
	document  map[string]any
	refs      []manifest.NamedResource
	scoped    bool
	required  bool
	entry     bool
	projected bool
}

func entrySourceGroupFor(
	groups map[manifest.NamedResource]*entrySourceGroup,
	id manifest.NamedResource,
) *entrySourceGroup {
	group := groups[id]
	if group == nil {
		group = &entrySourceGroup{}
		groups[id] = group
	}
	return group
}

func addEntrySource(
	groups map[manifest.NamedResource]*entrySourceGroup,
	id, requester manifest.NamedResource,
	reference *manifest.Reference,
) {
	group := entrySourceGroupFor(groups, id)
	group.required = true
	if reference != nil && (reference.ReadsSourceTree() || reference.RequiresDeclaration) {
		requiresDeclaration := group.reference.RequiresDeclaration || reference.RequiresDeclaration
		group.reference = *reference
		group.reference.RequiresDeclaration = requiresDeclaration
	}
	if requester != (manifest.NamedResource{}) && !slices.Contains(group.refs, requester) {
		group.refs = append(group.refs, requester)
	}
}

func (g *entrySourceGroup) project(declaration manifest.BaseManifest) {
	if g.object != nil || declaration == nil {
		return
	}
	g.object, g.projected = declaration, true
}

func sourceReference(object manifest.BaseManifest, id manifest.NamedResource) manifest.Reference {
	if object != nil {
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if ref.BindsSource() && ref.NamedResource == id {
				return ref
			}
		}
	}
	return manifest.Reference{}
}

func (a *Admission) classifySource(id manifest.NamedResource, request *sourceRequest) entryResolution {
	previous := a.resolvedSources.snapshot()[id]
	entry := request.entry || previous.entry && previous.input == a.input
	candidate := entryResolution{object: request.object, input: a.input, entry: entry, producers: request.producers}
	if id == manifest.BootstrapSourceID {
		candidate.artifact = a.input
		switch {
		case candidate.object != nil:
			return candidate
		case previous.input == a.input:
			return previous
		}
		candidate.object, _ = id.BindLocal(a.input.LocalPath)
		candidate.synthetic = true
		return candidate
	}
	var object manifest.BaseManifest
	switch request.reads {
	case nil:
		object = a.store.GetDeclaration(id)
	default:
		object = request.reads.GetDeclaration(id)
		candidate.object = cmp.Or(candidate.object, request.reads.GetObject(id), previous.object)
	}
	switch {
	case !request.scoped && object != nil && !previous.ownsDeclaration(object):
		candidate.object = object
	case candidate.object == nil && !request.scoped && a.loader != nil && a.loader.IsFileIndexed(id):
		candidate.object = a.loader.CatalogObject(id)
	}
	if candidate.object == nil && !request.scoped && previous.projected {
		candidate.object = previous.object
	}
	candidate.projected = request.projected && candidate.object == request.object ||
		previous.projected && candidate.object == previous.object
	behavior, ok := candidate.object.(entrySource)
	switch {
	case candidate.projected && !ok:
		candidate.entry = false
		return candidate
	case previous.ownsDeclaration(candidate.object):
		previous.entry = candidate.entry
		return a.classifyMissingSource(id, request, &candidate, &previous)
	case previous.reuseClassification(&candidate, request, a.loaderIndex()):
		return previous
	case candidate.object == nil:
		return a.classifyMissingSource(id, request, &candidate, &previous)
	case !ok:
		return candidate
	}
	_, descriptor := behavior.BootstrapAlias(a.input.LocalPath)
	if _, match := a.remotes[normalizeGitURL(descriptor.URL)]; !match && !candidate.entry {
		return candidate
	}
	candidate.git, candidate.pinned = descriptor, descriptor.BootstrapPinned()
	return candidate
}

func (r *entryResolution) sameClassification(candidate *entryResolution) bool {
	return r.object == candidate.object && r.entry == candidate.entry && r.projected == candidate.projected
}

func (a *Admission) appliesEntry(application *manifest.EntryApplication) bool {
	if a.input == nil || a.path == "" || a.store == nil || !application.Source.CanBindLocal() {
		return false
	}
	artifact, _ := a.store.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	resolved := entryResolution{object: a.store.GetDeclaration(application.Source), artifact: artifact}
	return a.applicationApplies(application, &resolved)
}

func (a *Admission) applicationApplies(application *manifest.EntryApplication, resolved *entryResolution) bool {
	selected := a.resolvedSources.snapshot()[application.Source]
	if selected.input == a.input && selected.artifact != nil {
		resolved = &selected
	}
	return loader.AppliesToEntry(resolved, application, a.input, a.path)
}

// A source MUST NOT replace its construction input with its own rendered declaration.
func (r *entryResolution) reuseClassification(
	candidate *entryResolution,
	request *sourceRequest,
	index *manifest.DiscoveryIndex,
) bool {
	if candidate.object != nil && r.sameClassification(candidate) {
		switch {
		case r.input == candidate.input:
			return true
		case r.artifact != nil && r.artifact.InputAlias:
			r.input, r.artifact = candidate.input, nil
			return true
		}
	}
	if request.reads == nil || r.entry != candidate.entry || r.projected != candidate.projected {
		return false
	}
	if r.synthetic || r.artifact == nil || r.input != candidate.input || r.sameClassification(candidate) {
		return false
	}
	id := r.object.Named()
	return slices.ContainsFunc(request.reads.Productions(id, index), func(production manifest.Production) bool {
		return production.Source == id && production.Owner != (manifest.NamedResource{})
	})
}

func (a *Admission) classifyMissingSource(
	id manifest.NamedResource,
	request *sourceRequest,
	candidate *entryResolution,
	previous *entryResolution,
) entryResolution {
	*candidate = entryResolution{input: a.input, producers: candidate.producers}
	if request.reference.RequiresDeclaration {
		return *candidate
	}
	if !request.scoped && previous.input == a.input {
		candidate.producers = previous.producers
	}
	var artifact store.Artifact
	if request.reads != nil && request.reads.ID() != id {
		artifact = request.reads.GetArtifact(id, store.ArtifactContent)
	} else {
		artifact = a.store.GetArtifact(id, store.ArtifactContent)
	}
	owned, _ := artifact.(*store.SourceArtifact)
	if artifact != nil && !owned.SameMaterial(previous.artifact) && owned.Reads.Valid() {
		return *candidate
	}
	if a.sourceProduced(id, request) || len(candidate.producers) != 0 {
		return *candidate
	}
	if previous.synthetic {
		if previous.input != a.input {
			previous.input, previous.artifact = a.input, nil
		}
		return *previous
	}
	candidate.synthetic = request.reference.ReadsSourceTree() && id.CanBindLocal()
	return *candidate
}

func (a *Admission) sourceProduced(id manifest.NamedResource, request *sourceRequest) bool {
	var productions []manifest.Production
	if request.reads != nil {
		productions = request.reads.Productions(id, a.loaderIndex())
	} else {
		productions = a.store.Productions(id, a.loaderIndex())
	}
	for i := range productions {
		production := &productions[i]
		if production.Declaration != id &&
			(production.Declaration != (manifest.NamedResource{}) || production.Owner != (manifest.NamedResource{})) {
			return true
		}
	}
	if a.loader == nil || request.scoped {
		return false
	}
	for _, projection := range a.loader.SourceFacts(manifest.BootstrapSourceID, request.reads).Context.TemplateProjections {
		for _, target := range projection.Targets {
			if target.NamedResource == id {
				return true
			}
		}
	}
	return false
}

func (a *Admission) loaderIndex() *manifest.DiscoveryIndex {
	if a.loader != nil {
		return a.loader.Index
	}
	return nil
}

func (d *Session) entrySourceGroups(
	ctx context.Context,
	allowMissing bool,
) (map[manifest.NamedResource]*entrySourceGroup, error) {
	// Sparse group maps MUST hold pointers to avoid reserving inline storage for empty slots.
	groups := d.sourceGroups
	if groups == nil {
		groups = map[manifest.NamedResource]*entrySourceGroup{}
		d.sourceGroups = groups
	}
	for _, group := range groups {
		*group = entrySourceGroup{refs: group.refs[:0]}
	}
	addEntrySource(groups, manifest.BootstrapSourceID, manifest.NamedResource{}, nil)
	for kind := range manifest.Kinds() {
		if !kind.IsSource() {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			if _, ok := obj.(entrySource); ok {
				addEntrySource(groups, obj.Named(), manifest.NamedResource{}, nil)
			}
		}
	}
	if d.loader != nil {
		for id := range d.loader.Index.Files() {
			application, valid := manifest.ApplicationFor(d.loader.CatalogObject(id))
			if valid && d.admission().appliesEntry(&application) {
				entrySourceGroupFor(groups, application.Source).entry = true
			}
		}
		if err := d.addLoadedSources(ctx, groups, allowMissing); err != nil {
			return nil, err
		}
	}
	if allowMissing {
		d.addMissingSources(groups)
	}
	return groups, nil
}

func (d *Session) addLoadedSources(
	ctx context.Context,
	groups map[manifest.NamedResource]*entrySourceGroup,
	allowMissing bool,
) error {
	restricted := !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{})
	for child, err := range d.loader.Sources(ctx) {
		if err != nil {
			return err
		}
		id := child.Obj.Named()
		evidence := d.loader.RootEvidence(id, nil, true)
		if evidence.Occurrence.RequiresBuild && evidence.Record.Builder == "" {
			continue
		}
		group := entrySourceGroupFor(groups, id)
		group.object, group.document = child.Obj, child.Doc
		root := selection.Root(evidence)
		group.projected = !root.Admitted && evidence.Occurrence.Declaration == (selection.OccurrenceID{}) &&
			evidence.Record.Builder == "" && d.cfg.Store.GetDeclaration(id) == nil
		for ref := range manifest.References(child.Obj, manifest.ReferenceRelations) {
			if ref.BindsSource() && ref.RequiresDeclaration {
				addEntrySource(groups, ref.NamedResource, id, &ref)
			}
		}
		group.required = group.required || restricted && root.Admitted
	}
	d.addPathSources(groups, allowMissing)
	return nil
}

func (d *Session) addMissingSources(groups map[manifest.NamedResource]*entrySourceGroup) {
	for kind := range manifest.Kinds() {
		if !kind.Runnable {
			continue
		}
		for _, obj := range d.cfg.Store.ListObjects(kind.Group, kind.Kind) {
			for ref := range manifest.References(obj, manifest.ReferenceRelations) {
				if ref.IsSource() && ref.NamedResource.KindInfo().IsSource() {
					addEntrySource(groups, ref.NamedResource, obj.Named(), &ref)
				}
			}
		}
	}
	d.addMissingIndexedSources(groups)
}

func (d *Session) addMissingIndexedSources(groups map[manifest.NamedResource]*entrySourceGroup) {
	if d.loader == nil {
		return
	}
	for id := range d.loader.Index.Files() {
		if !id.KindInfo().Runnable || d.cfg.Store.GetObject(id) != nil ||
			!slices.ContainsFunc(d.loader.Index.Records[id].Refs, func(ref manifest.NamedResource) bool {
				return ref.KindInfo().IsSource()
			}) {
			continue
		}
		evidence := d.loader.RootEvidence(id, nil, true)
		prepared := !evidence.Occurrence.RequiresBuild || evidence.Record.Builder != ""
		eager := d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{}) &&
			evidence.Scope.EntryKustomize && evidence.Scope.Eligibility != selection.RootInactive && prepared
		if !prepared || !selection.Root(evidence).Admitted && !eager {
			continue
		}
		for ref := range d.loader.References(id) {
			if ref.IsSource() && ref.NamedResource.KindInfo().IsSource() {
				addEntrySource(groups, ref.NamedResource, id, &ref)
			}
		}
	}
}

func (d *Session) addPathSources(groups map[manifest.NamedResource]*entrySourceGroup, allowMissing bool) {
	childValues := d.loader.PathObjects()
	for childIndex := range childValues {
		child := childValues[childIndex]
		if child.RequiresBuild {
			continue
		}

		application, valid := manifest.ApplicationFor(partitionObject(&child))
		if valid {
			group := entrySourceGroupFor(groups, application.Source)
			group.entry = group.entry || d.admission().appliesEntry(&application)
		}
		if application.Declaration != nil {
			group := entrySourceGroupFor(groups, application.Source)
			group.project(application.Declaration)
			group.required = group.required || group.entry
			continue
		}
		ref, _, _ := child.SourcePath()
		group := groups[ref]
		declared := group != nil && group.object != nil
		if allowMissing || declared || d.cfg.Store.GetObject(ref) != nil {
			reference := sourceReference(partitionObject(&child), ref)
			addEntrySource(groups, ref, child.Identity, &reference)
		}
	}
}

func normalizeGitURL(raw string) string {
	raw = strings.TrimSpace(raw)
	endpoint, err := transport.ParseURL(raw)
	if err != nil {
		return ""
	}
	switch strings.ToLower(endpoint.Scheme) {
	case "http", "https", "ssh", "git":
	default:
		return ""
	}
	host, remotePath := strings.Trim(endpoint.Host, "[]"), endpoint.Path
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return ""
		}
		host, remotePath = parsed.Hostname(), parsed.Path
	}
	if host == "" || strings.ContainsAny(host, "/\\") || remotePath == "" {
		return ""
	}
	remotePath = strings.TrimSuffix(remotePath, "/")
	remotePath = strings.TrimSuffix(remotePath, ".git")
	remotePath = strings.TrimPrefix(remotePath, "/")
	if remotePath == "" {
		return ""
	}
	return strings.ToLower(host + "/" + remotePath)
}
