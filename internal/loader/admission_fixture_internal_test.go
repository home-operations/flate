package loader

import (
	"iter"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type objects interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	Objects() []manifest.BaseManifest
}

type role uint8

const (
	absent role = iota
	kept
	primary
)

type admissionFixture struct {
	tb         testing.TB
	mu         sync.Mutex
	identities []manifest.NamedResource
	pending    *selection.Admission
	loader     *Loader
	objs       objects
	history    map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}
}

type admissionConfig struct {
	changes   *change.Set
	index     *manifest.DiscoveryIndex
	inventory objects
	root      string
}

func newAdmission(tb testing.TB, cfg admissionConfig) *admissionFixture {
	tb.Helper()
	changes, index, inventory, root := cfg.changes, cfg.index, cfg.inventory, cfg.root
	st, ok := inventory.(*store.Store)
	if !ok {
		st = store.New()
		if inventory != nil {
			st.AddObjects(inventory.Objects())
		}
	}
	if root == "" {
		if input, ok := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact); ok {
			root = input.LocalPath
		}
	}
	l := New(st)
	l.SourceRoot = root
	l.mountSources()
	l.Index = index
	if l.Index == nil {
		l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	}
	l.Options.Changes = changes
	configureSelection(tb, l, selection.Request{Changed: changes != nil})
	if changes != nil {
		_, snapshots, _ := changes.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range snapshots {
			if snapshot != nil && snapshot == st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) {
				l.side, l.sideKnown = manifest.TreeSide(side), true
			}
		}
	}
	f := &admissionFixture{tb: tb, loader: l, objs: inventory}
	if changes != nil {
		f.prepare(selection.PrepareRequest{Invocation: true}, false)
	}
	return f
}

//nolint:gocritic // Every proposal MUST retain an independent immutable caller request.
func (f *admissionFixture) prepare(request selection.PrepareRequest, commit bool) []manifest.NamedResource {
	f.tb.Helper()
	if !f.Enabled() {
		return nil
	}
	request.History = f.history
	proposal, err := prepareSelection(f.tb.Context(), f.tb, f.loader, request)
	if err != nil {
		f.tb.Fatal(err)
	}
	needed := slices.Clone(proposal.Needed)
	if commit {
		var committed bool
		proposal, committed, err = f.loader.commitSelection(f.tb.Context(), f.tb, proposal, selection.Execution)
		if err != nil || !committed {
			f.tb.Fatalf("fixture commit=%v: %v", committed, err)
		}
		needed = append(needed, proposal.Needed...)
		slices.SortFunc(needed, manifest.NamedResource.Compare)
		needed = slices.Compact(needed)
	} else {
		f.pending = proposal
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	before := slices.Clone(f.identities)
	f.identities = append(f.identities, needed...)
	slices.SortFunc(f.identities, manifest.NamedResource.Compare)
	f.identities = slices.Compact(f.identities)
	var added []manifest.NamedResource
	for _, id := range needed {
		if !slices.Contains(before, id) {
			added = append(added, id)
		}
	}
	return added
}

func (f *admissionFixture) Enabled() bool {
	return f != nil && f.loader != nil && f.loader.Options.Session != nil
}

func (f *admissionFixture) Reconcile(id manifest.NamedResource) bool {
	if !f.Enabled() {
		return true
	}
	f.mu.Lock()
	pending := f.pending
	f.mu.Unlock()
	if pending != nil {
		return slices.Contains(pending.Needed, id)
	}
	return f.loader.Options.Session.Snapshot().Reconcile(id)
}

func (f *admissionFixture) Output(id manifest.NamedResource) bool {
	if !f.Enabled() {
		return true
	}
	f.mu.Lock()
	pending := f.pending
	f.mu.Unlock()
	if pending != nil {
		return slices.Contains(pending.Selected, id)
	}
	return f.loader.Options.Session.Snapshot().Output(id)
}

func (f *admissionFixture) role(id manifest.NamedResource) role {
	if f.Output(id) {
		return primary
	}
	if f.Reconcile(id) {
		return kept
	}
	return absent
}

func (f *admissionFixture) KeepNames() []string {
	f.mu.Lock()
	ids := slices.Clone(f.identities)
	f.mu.Unlock()
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		if f.Reconcile(id) {
			names = append(names, id.String())
		}
	}
	slices.Sort(names)
	return names
}

func (f *admissionFixture) KeepNamespaces() map[string]struct{} {
	f.mu.Lock()
	ids := slices.Clone(f.identities)
	f.mu.Unlock()
	namespaces := map[string]struct{}{}
	for _, id := range ids {
		if id.Namespace != "" && f.Reconcile(id) {
			namespaces[id.Namespace] = struct{}{}
		}
	}
	return namespaces
}

func (f *admissionFixture) settle() {
	f.tb.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pending == nil {
		return
	}
	_, committed, err := f.loader.commitSelection(f.tb.Context(), f.tb, f.pending, selection.Execution)
	if err != nil || !committed {
		f.tb.Fatalf("fixture initial commit=%v: %v", committed, err)
	}
	f.pending = nil
}

func (f *admissionFixture) selectRoots(ids []manifest.NamedResource) {
	f.tb.Helper()
	f.mu.Lock()
	known := slices.Clone(f.identities)
	f.mu.Unlock()
	roots := slices.Clone(ids)
	for _, id := range known {
		if f.Output(id) {
			roots = append(roots, id)
		}
	}
	if f.pending != nil {
		f.pending.Retire()
		f.pending = nil
	}
	configureSelection(f.tb, f.loader, selection.Request{Roots: roots})
	f.prepare(selection.PrepareRequest{Invocation: true}, false)
	f.settle()
}

func (f *admissionFixture) AddEmitted(
	parent manifest.NamedResource,
	incoming *store.Child,
	reader any,
) []manifest.NamedResource {
	child := *incoming
	if !f.Enabled() {
		return nil
	}
	f.settle()
	child.Productions = append(slices.Clone(child.Productions), manifest.Production{Owner: parent})
	reads, _ := reader.(*store.ReadSet)
	return f.prepare(selection.PrepareRequest{Owner: parent, Generation: []store.Child{child}, Reads: reads}, true)
}

func (f *admissionFixture) RefineHistory(
	records map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord],
	complete map[manifest.NamedResource]bool,
) {
	f.history = map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{}
	for owner, records := range records {
		if records != nil {
			var rows []struct {
				id     manifest.NamedResource
				record manifest.DiscoveryRecord
			}
			//nolint:gocritic // The iterator transfers immutable records into a replayable snapshot.
			for id, record := range records {
				rows = append(rows, struct {
					id     manifest.NamedResource
					record manifest.DiscoveryRecord
				}{id, record})
			}
			records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				for i := range rows {
					if !yield(rows[i].id, rows[i].record) {
						return
					}
				}
			}
		}
		f.history[owner] = struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{records, complete[owner]}
	}
	f.identities = nil
	if f.pending != nil {
		f.pending.Retire()
		f.pending = nil
	}
	f.prepare(selection.PrepareRequest{Invocation: true}, false)
}

func sourceChanges(changes *change.Set, source manifest.NamedResource) *change.Set {
	delta, _, _ := changes.ForSource(source)
	return delta
}

func inputMap(changes *change.Set) map[manifest.NamedResource]string {
	var inputs map[manifest.NamedResource]string
	for id, changed := range changes.Inputs(manifest.NamedResource{}) {
		if inputs == nil {
			inputs = make(map[manifest.NamedResource]string)
		}
		inputs[id] = changed.File
	}
	return inputs
}

func testInput(root string) *store.SourceArtifact {
	if root == "" {
		return nil
	}
	return &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
}

func projectedProducerForTest(object manifest.BaseManifest) manifest.TemplateProjection {
	projection, _ := manifest.ProjectProducer(object, nil)
	return projection
}

func (f *admissionFixture) roles() map[manifest.NamedResource]role {
	f.mu.Lock()
	ids := slices.Clone(f.identities)
	f.mu.Unlock()
	roles := map[manifest.NamedResource]role{}
	for _, id := range ids {
		if membership := f.role(id); membership != absent {
			roles[id] = membership
		}
	}
	return roles
}
