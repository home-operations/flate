package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entryApplication interface {
	EntryApplication() (manifest.EntryApplication, bool)
}

type entryQualification struct {
	loader     *loader.Loader
	trees      *kustomize.TreeCache
	candidates []manifest.NamedResource
	current    manifest.NamedResource
	receipt    *manifest.RenderReceipt
	documents  map[manifest.NamedResource]map[string]any
	visited    map[manifest.NamedResource]bool
	observed   bool
}

func (d *Session) needsEntry(changes *change.Set) bool {
	if changes == nil || !d.cfg.Selection.Changed {
		return false
	}
	delta, _, _ := changes.ForSource(manifest.BootstrapSourceID)
	for id, record := range d.loader.Index.Files() {
		if record.Path == "" || record.Parent != (manifest.NamedResource{}) || !d.cfg.Selection.Matches(id) || !delta.Contains(record.File) || !safepath.Contains(d.cfg.Path, record.Path) {
			continue
		}
		if slices.ContainsFunc(record.Productions, func(p manifest.Production) bool {
			return p.Owner != (manifest.NamedResource{}) && p.Owner != id && !p.Owner.ReportHidden()
		}) {
			continue
		}
		return true
	}
	return false
}

func (d *Session) qualifyEntry(ctx context.Context, admission *Admission) (kustomize.EntryEvidence, error) {
	q := &entryQualification{loader: d.loader, trees: d.cfg.Trees, visited: map[manifest.NamedResource]bool{}}
	admission.qualification = q
	relative, _ := filepath.Rel(d.cfg.Input.LocalPath, d.cfg.Path)
	root := manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), manifest.NamedResource{})
	evidence := d.cfg.Trees.PreviousEntry(ctx, kustomize.RenderRequest{
		Source: d.cfg.Input, ApplyIgnore: true, SubPath: root.Path, Spec: root,
		WipeSecrets: d.cfg.WipeSecrets, StrictSubstitutions: d.cfg.StrictSubstitutions, SkipKinds: d.cfg.SkipKinds, DependencySubstitutions: d.substitutions,
	})
	if evidence.Application != nil {
		obj, _, _, err := manifest.ParseDoc(evidence.Application, manifest.ParseDocOptions{WipeSecrets: d.cfg.WipeSecrets})
		if err == nil && obj != nil {
			admission.addApplication(d.cfg.Store, obj, true)
			q.receipt = evidence.Receipt
			return evidence, nil
		}
	}
	for id, record := range d.loader.Index.Files() {
		if record.Path == "" || !safepath.Contains(d.cfg.Path, record.Path) {
			continue
		}
		object := d.loader.CatalogObject(id)
		if object == nil {
			continue
		}
		if record.Parent == (manifest.NamedResource{}) && !d.loader.AdmitsRoot(object, record.Path) {
			continue
		}
		if _, _, applies := object.SourcePath(); applies {
			continue
		}
		if _, producer := object.(interface {
			EmissionPolicy() manifest.EmissionPolicy
		}); producer {
			q.candidates = append(q.candidates, id)
		}
	}
	slices.SortFunc(q.candidates, manifest.NamedResource.Compare)
	return kustomize.EntryEvidence{}, ctx.Err()
}

// EntryProducer returns the next producer needed to qualify an otherwise unknown entry applier.
func (a *Admission) EntryProducer() manifest.NamedResource {
	if a == nil || a.qualification == nil || a.entryBound.Load() {
		return manifest.NamedResource{}
	}
	q := a.qualification
	if q.current != (manifest.NamedResource{}) && !q.observed {
		return manifest.NamedResource{}
	}
	for len(q.candidates) != 0 {
		id := q.candidates[0]
		q.candidates[0], q.candidates = manifest.NamedResource{}, q.candidates[1:]
		if q.visited[id] {
			continue
		}
		q.visited[id] = true
		q.current, q.observed = id, false
		return id
	}
	a.qualification = nil
	return manifest.NamedResource{}
}

func (a *Admission) captureApplication(ctx context.Context, st *store.Store, owner manifest.BaseManifest) (*manifest.RenderReceipt, error) {
	q := a.qualification
	if q == nil {
		return nil, nil
	}
	if q.receipt != nil {
		return q.receipt, nil
	}
	document := q.documents[owner.Named()]
	if document == nil {
		return nil, nil
	}
	var files []string
	seen := map[manifest.NamedResource]bool{}
	queue := []manifest.NamedResource{q.current}
	for len(queue) != 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		record, _ := q.loader.Index.Lookup(id)
		if record.File != "" {
			files = append(files, record.File)
		}
		files = append(files, record.Inputs...)
		for _, production := range st.Productions(id, q.loader.Index) {
			if production.Owner != (manifest.NamedResource{}) {
				queue = append(queue, production.Owner)
			}
			if production.Declaration != (manifest.NamedResource{}) {
				queue = append(queue, production.Declaration)
			}
		}
		for ref := range q.loader.References(id) {
			queue = append(queue, ref.NamedResource)
		}
	}
	for file, readers := range q.loader.Index.Readers {
		if slices.ContainsFunc(readers, func(reader manifest.FileReader) bool {
			return reader.Exact && !reader.Materialize && seen[reader.Reader]
		}) {
			files = append(files, file)
		}
	}
	for i, file := range files {
		absolute := filepath.Join(a.input.LocalPath, filepath.FromSlash(file))
		if safepath.Contains(a.input.LocalPath, absolute) {
			continue
		}
		mapped := false
		for id := range a.sources {
			artifact, _ := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			if artifact == nil || !artifact.InputAlias || !safepath.Contains(artifact.LocalPath, absolute) {
				continue
			}
			origin := artifact.OriginPath(absolute)
			if safepath.Contains(a.input.OriginPath(a.input.LocalPath), origin) {
				relative, _ := filepath.Rel(a.input.OriginPath(a.input.LocalPath), origin)
				files[i], mapped = filepath.ToSlash(relative), true
				break
			}
		}
		if !mapped {
			return nil, nil
		}
	}
	slices.Sort(files)
	output, ok := st.GetArtifact(q.current, store.ArtifactContent).(*store.Emission)
	if !ok {
		return nil, nil
	}
	return q.trees.ApplicationReceipt(ctx, a.input, kustomize.EntryEvidence{Application: document, Owner: q.current, Records: st.ChildFootprint(q.current, output.Children, q.loader.Index)}, slices.Compact(files))
}

func (d *Session) applications() []manifest.BaseManifest {
	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	add := func(obj manifest.BaseManifest) {
		if application, ok := obj.(entryApplication); ok {
			if _, valid := application.EntryApplication(); valid && objects[obj.Named()] == nil {
				objects[obj.Named()] = obj
			}
		}
	}
	for _, child := range d.loader.PathObjects() {
		obj := d.cfg.Store.GetObject(child.Identity)
		if obj == nil {
			obj = d.loader.CatalogObject(child.Identity)
		}
		add(obj)
	}
	for id, record := range d.loader.Index.Records {
		obj := record.Declaration
		if obj == nil && objects[id] == nil {
			obj = d.loader.CatalogObject(id)
			if obj == nil {
				obj = d.loader.IndexedObject(id, record).Obj
			}
		}
		add(obj)
	}
	var applications []manifest.BaseManifest
	for _, id := range slices.SortedFunc(maps.Keys(objects), manifest.NamedResource.Compare) {
		applications = append(applications, objects[id])
	}
	return applications
}

type applicationEvidence struct {
	object      manifest.BaseManifest
	application manifest.EntryApplication
	source      entryResolution
	history     bool
	next        *applicationEvidence
}

// AddApplication captures published application evidence without resolving or rendering it.
func (a *Admission) AddApplication(st *store.Store, obj manifest.BaseManifest) {
	a.addApplication(st, obj, false)
}

func (a *Admission) addApplication(st *store.Store, obj manifest.BaseManifest, history bool) {
	if a == nil || a.entryBound.Load() {
		return
	}
	applier, ok := obj.(entryApplication)
	if !ok {
		return
	}
	application, valid := applier.EntryApplication()
	if !valid {
		return
	}
	evidence := &applicationEvidence{object: obj, application: application, source: a.applicationSource(st, application), history: history}
	for {
		evidence.next = a.applications.Load()
		if a.applications.CompareAndSwap(evidence.next, evidence) {
			return
		}
	}
}

// AddHistory admits application evidence only from validated accepted render history.
func (a *Admission) AddHistory(st *store.Store, id manifest.NamedResource, record manifest.DiscoveryRecord) {
	if record.Application == "" {
		return
	}
	var document map[string]any
	if err := json.Unmarshal([]byte(record.Application), &document); err != nil {
		return
	}
	obj, _, _, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	if err == nil && obj != nil && obj.Named() == id {
		a.addApplication(st, obj, true)
	}
}

func (a *Admission) applicationSource(st *store.Store, application manifest.EntryApplication) entryResolution {
	resolved := entryResolution{object: st.GetObject(application.Source)}
	resolved.artifact, _ = st.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	if resolved.object == nil {
		resolved.object = a.sources[application.Source].object
	}
	if resolved.object == nil {
		resolved.object = application.Declaration
	}
	return resolved
}

func (a *Admission) qualifyEntry(ctx context.Context, st *store.Store, objects []manifest.BaseManifest) ([]manifest.BaseManifest, error) {
	repoRoot := a.input.LocalPath
	var appliers []manifest.BaseManifest
	qualify := func(obj manifest.BaseManifest, application manifest.EntryApplication, resolved entryResolution) error {
		artifact := resolved.artifact
		if artifact == nil {
			var err error
			resolved, err = a.resolve(ctx, application.Source, resolved.object, []sourceWitness{{id: obj.Named(), path: application.Path}})
			if err != nil {
				return err
			}
			artifact = resolved.artifact
		}
		if artifact == nil {
			return nil
		}
		root, pinned, external := loader.SourceView(resolved, application.Source, repoRoot)
		if pinned || external {
			return nil
		}
		target := filepath.Join(root, manifest.NormalizeClaimBase(application.Path))
		if safepath.Contains(root, target) && artifact.OriginPath(target) == a.input.OriginPath(a.path) {
			appliers = append(appliers, obj)
		}
		return nil
	}
	for _, obj := range objects {
		applier, ok := obj.(entryApplication)
		if !ok {
			continue
		}
		application, valid := applier.EntryApplication()
		if valid {
			if err := qualify(obj, application, a.applicationSource(st, application)); err != nil {
				return nil, err
			}
		}
	}
	for evidence := a.applications.Load(); evidence != nil; evidence = evidence.next {
		if !evidence.history {
			if current := st.GetObject(evidence.object.Named()); current == nil || !manifest.Equal(current, evidence.object) {
				continue
			}
		}
		if err := qualify(evidence.object, evidence.application, evidence.source); err != nil {
			return nil, err
		}
	}
	slices.SortStableFunc(appliers, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
	return slices.CompactFunc(appliers, func(a, b manifest.BaseManifest) bool { return a.Named() == b.Named() }), nil
}

func (r entryResolution) GetObject(manifest.NamedResource) manifest.BaseManifest {
	return r.object
}

func (r entryResolution) GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact {
	if r.artifact == nil {
		return nil
	}
	return r.artifact
}

func (a *Admission) entry(ctx context.Context, st *store.Store, objects []manifest.BaseManifest) (*manifest.Kustomization, manifest.BaseManifest, error) {
	appliers, err := a.qualifyEntry(ctx, st, objects)
	if err != nil {
		return nil, nil, err
	}
	relative, _ := filepath.Rel(a.input.LocalPath, a.path)
	if len(appliers) == 0 {
		found := false
		for _, filename := range manifest.KustomizeBuilderFilenames {
			_, err := fs.ReadFile(a.input.FS, filepath.ToSlash(filepath.Join(relative, filename)))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inspect entry package: %w", err), "")
			}
			if err == nil {
				found = true
				break
			}
		}
		if !found {
			return nil, nil, nil
		}
	}
	var owner manifest.BaseManifest
	var applier manifest.NamedResource
	if len(appliers) != 0 {
		owner = appliers[0]
		applier = owner.Named()
	}
	return manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), applier), owner, nil
}

// BindEntry qualifies the admitted entry against the current declaration fixed point.
// It returns immutable producer metadata; callers admit the producer before publication.
func (a *Admission) BindEntry(ctx context.Context, st *store.Store) (*manifest.Kustomization, Binding, error) {
	if a == nil {
		return nil, Binding{}, nil
	}
	if q := a.qualification; q != nil && q.current != (manifest.NamedResource{}) && !q.observed {
		output, ok := st.GetArtifact(q.current, store.ArtifactContent).(*store.Emission)
		if !ok || !output.Complete || !output.OutputReads.Valid() {
			status, _ := st.GetStatus(q.current)
			switch status.Outcome() {
			case store.OutcomeFailed, store.OutcomeSkipped, store.OutcomeSuspended, store.OutcomeUnchanged:
				q.observed = true
			}
			record, _ := q.loader.Index.Lookup(q.current)
			parents := []manifest.NamedResource{record.Parent, record.BuilderOwner}
			for _, production := range st.Productions(q.current, q.loader.Index) {
				parents = append(parents, production.Materializer(q.current))
			}
			for _, owner := range parents {

				parent, accepted := st.GetArtifact(owner, store.ArtifactContent).(*store.Emission)
				if accepted && parent.Complete && parent.OutputReads.Valid() && !slices.ContainsFunc(parent.Children, func(child store.Child) bool { return child.Obj.Named() == q.current }) {
					q.observed = true
				}
			}
			return nil, Binding{}, ctx.Err()
		}
		if output.Complete && output.OutputReads.Valid() {
			q.observed = true
			q.documents = map[manifest.NamedResource]map[string]any{}
			for _, child := range output.Children {
				if applier, ok := child.Obj.(entryApplication); ok {
					if _, valid := applier.EntryApplication(); valid {
						a.addApplication(st, child.Obj, true)
						q.documents[child.Obj.Named()] = child.Doc
					}
				}
				if _, _, applies := child.Obj.SourcePath(); !applies && !q.visited[child.Obj.Named()] {
					if _, producer := child.Obj.(interface {
						EmissionPolicy() manifest.EmissionPolicy
					}); producer && !slices.Contains(q.candidates, child.Obj.Named()) {
						q.candidates = append(q.candidates, child.Obj.Named())
					}
				}
			}
		}
	}
	root, owner, err := a.entry(ctx, st, st.ListObjects("", ""))
	if err != nil || root == nil {
		return nil, Binding{}, err
	}
	if owner != nil {
		root.EntryReceipt, err = a.captureApplication(ctx, st, owner)
		if err != nil {
			return nil, Binding{}, err
		}
	}
	key := root.Named()
	if candidate, ok := owner.(*manifest.Kustomization); ok && len(candidate.DependsOn) == 0 && (candidate.PostBuild == nil || len(candidate.PostBuild.SubstituteFrom) == 0) {
		if current := st.GetObject(candidate.Named()); current == nil || manifest.Equal(current, candidate) {
			root = candidate
		}
	}
	a.entryBound.Store(true)
	a.applications.Store(nil)
	a.qualification = nil
	source := root.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	return root, Binding{Producer: root.Named(), Source: source, Key: key, Path: manifest.NormalizeClaimBase(root.Path)}, nil
}
