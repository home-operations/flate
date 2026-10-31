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

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entryApplication interface {
	EntryApplication() (manifest.EntryApplication, bool)
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
	root, owner, err := a.entry(ctx, st, st.ListObjects("", ""))
	if err != nil || root == nil {
		return nil, Binding{}, err
	}
	key := root.Named()
	if candidate, ok := owner.(*manifest.Kustomization); ok && len(candidate.DependsOn) == 0 && (candidate.PostBuild == nil || len(candidate.PostBuild.SubstituteFrom) == 0) {
		if current := st.GetObject(candidate.Named()); current == nil || manifest.Equal(current, candidate) {
			root = candidate
		}
	}
	a.entryBound.Store(true)
	a.applications.Store(nil)
	source := root.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	return root, Binding{Producer: root.Named(), Source: source, Key: key, Path: manifest.NormalizeClaimBase(root.Path)}, nil
}
