package loader

import (
	"iter"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
	tb      testing.TB
	mu      sync.Mutex
	keep    map[manifest.NamedResource]role
	last    *admissionClosure
	loader  *Loader
	objs    objects
	history map[manifest.NamedResource]struct {
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
		if wrapped, ok := inventory.(interface{ fixtureStore() *store.Store }); ok {
			st = wrapped.fixtureStore()
		} else {
			st = store.New()
			if inventory != nil {
				st.AddObjects(inventory.Objects())
			}
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
	l.Options.Selection = manifest.Selection{Changed: changes != nil}
	l.Options.Changes = changes
	if changes != nil {
		l.Options.Filter = change.NewFilter()
	}
	if changes != nil {
		_, snapshots, _ := changes.ForSource(manifest.BootstrapSourceID)
		for side, snapshot := range snapshots {
			if snapshot != nil && snapshot == st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) {
				l.side, l.sideKnown = manifest.TreeSide(side), true
			}
		}
	}
	f := &admissionFixture{tb: tb, loader: l, objs: inventory, keep: map[manifest.NamedResource]role{}}
	if changes != nil {
		f.prepare(store.Child{}, nil, false)
	}
	return f
}

func (f *admissionFixture) prepare(child store.Child, reads *store.ReadSet, commit bool) []manifest.NamedResource {
	f.tb.Helper()
	proposal := &Admission{}
	_, _, _, proposal.Revision = f.loader.Options.Filter.Membership(manifest.NamedResource{})
	var reader admissionReader = f.loader.Store
	if instrumented, ok := f.objs.(admissionReader); ok {
		reader = instrumented
	}
	if reads != nil {
		reader = reads
	}
	c := admissionClosure{loader: f.loader, objects: reader, incoming: child, proposal: proposal, history: f.history}
	proposal, err := c.evaluate(f.tb.Context(), reads)
	if err != nil {
		f.tb.Fatal(err)
	}
	if commit {
		_, retry := f.loader.Options.Filter.AddEmitted(proposal.Needed, proposal.Selected, proposal.Dirty, proposal.Revision)
		if retry {
			return f.prepare(child, reads, commit)
		}
	}
	f.mu.Lock()
	f.last = &c
	defer f.mu.Unlock()
	var added []manifest.NamedResource
	for _, id := range proposal.Needed {
		if f.keep[id] == absent {
			added = append(added, id)
		}
		f.keep[id] = max(f.keep[id], kept)
	}
	for _, id := range proposal.Selected {
		f.keep[id] = primary
	}

	return added
}

func (f *admissionFixture) Enabled() bool {
	return f != nil && f.loader != nil && f.loader.Options.Filter.Enabled()
}
func (f *admissionFixture) ShouldReconcile(id manifest.NamedResource) bool {
	if !f.Enabled() {
		return true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keep[id] != absent
}
func (f *admissionFixture) KeepNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := make([]string, 0, len(f.keep))
	for id := range f.keep {
		names = append(names, id.String())
	}
	slices.Sort(names)
	return names
}
func (f *admissionFixture) KeepNamespaces() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	namespaces := map[string]struct{}{}
	for id := range f.keep {
		if id.Namespace != "" {
			namespaces[id.Namespace] = struct{}{}
		}
	}
	return namespaces
}
func (f *admissionFixture) AddEmitted(parent manifest.NamedResource, child store.Child, reader any) []manifest.NamedResource {
	f.mu.Lock()
	var needs, selected []manifest.NamedResource
	for id, role := range f.keep {
		needs = append(needs, id)
		if role == primary {
			selected = append(selected, id)
		}
	}
	f.mu.Unlock()
	_, _, _, revision := f.loader.Options.Filter.Membership(manifest.NamedResource{})
	f.loader.Options.Filter.AddEmitted(needs, selected, selected, revision)
	child.Productions = append(slices.Clone(child.Productions), manifest.Production{Owner: parent})
	reads, _ := reader.(*store.ReadSet)
	return f.prepare(child, reads, true)
}
func (f *admissionFixture) RefineHistory(records map[manifest.NamedResource]iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], complete map[manifest.NamedResource]bool) {
	f.history = map[manifest.NamedResource]struct {
		Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
		Complete bool
	}{}
	for owner, records := range records {
		f.history[owner] = struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{records, complete[owner]}
	}
	clear(f.keep)
	f.prepare(store.Child{}, nil, false)
}
func sourceChanges(changes *change.Set, source manifest.NamedResource) *change.Set {
	delta, _, _ := changes.ForSource(source)
	return delta
}

func inputMap(changes *change.Set) map[manifest.NamedResource]string {
	var inputs map[manifest.NamedResource]string
	for id, changed := range changes.Inputs() {
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
