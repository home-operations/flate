package loader

import (
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestCatalog_FreezeAndClose(t *testing.T) {
	t.Parallel()
	for _, restricted := range []bool{false, true} {
		t.Run(strconv.FormatBool(restricted), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var denied atomic.Bool
			var calls atomic.Int64
			files := catalogReadFS{FS: fstest.MapFS{
				"declarations/child.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							"child",
							"apps",
							"  path: ./payload\n  sourceRef: {kind: GitRepository, name: local, namespace: sources}\n",
						),
					),
				},
				"payload/kustomization.yaml": {Data: []byte("resources: []\n")},
				"generator/kustomization.yaml": {
					Data: []byte("configMapGenerator:\n- name: opaque\n  files: [literal.yaml]\n"),
				},
				"generator/literal.yaml": {
					Data: []byte(testutil.KustomizationYAML("literal", "apps", "  path: ./hidden\n")),
				},
			}, denied: &denied, calls: &calls}
			input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files}
			st := store.New()
			source := &manifest.GitRepository{Name: "local", Namespace: "sources"}
			external := &manifest.GitRepository{Name: "remote", Namespace: "sources"}
			st.AddObject(
				&manifest.GitRepository{
					Name:      manifest.BootstrapSourceID.Name,
					Namespace: manifest.BootstrapSourceID.Namespace,
				},
			)
			st.AddObject(source)
			st.AddObject(external)
			st.SetArtifact(manifest.BootstrapSourceID, input)
			st.SetArtifact(source.Named(), input)
			l := New(st)
			l.SourceRoot = root
			child := testutil.KustomizationID("apps", "child")
			request := selection.Request{}
			if restricted {
				request.Roots = []manifest.NamedResource{child}
			}
			var err error
			l.Options.Session, err = selection.New(st, l, request)
			assert.NoError(t, err)
			assert.Equal(t, l.Options.Session != nil, true)
			assert.Equal(t, l.Options.Session.Snapshot().Output(manifest.NamedResource{}), !restricted)
			_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			construction := l.RootEvidence(child, nil, true)
			assert.Equal(t, construction.Captured, false)
			assert.Equal(t, construction.Scope.NamespaceEligible, true)
			assert.Equal(t, selection.Root(construction).Independent, true)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			st.SetDiscoveryIndex(l.Index)
			object := l.CatalogObject(child)
			assert.FatalIf(t, object == nil, "missing retained child declaration")
			literal := testutil.KustomizationID("apps", "literal")
			assert.Equal(t, l.CatalogObject(literal), manifest.BaseManifest(nil))
			factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
			owner := &manifest.Kustomization{
				Name:            "owner",
				Namespace:       "apps",
				Path:            "./declarations",
				TargetNamespace: "apps",
			}
			owner.SourceRef.Kind, owner.SourceRef.Name, owner.SourceRef.Namespace = source.Named().Kind, source.Name, source.Namespace
			st.AddObject(factory)
			unrelated := owner.Clone()
			unrelated.Path = "./elsewhere"
			st.RecordEmitted(
				factory.Named(),
				&store.Emission{Children: []store.Child{{Obj: unrelated}}, Complete: true},
				nil,
			)
			catalog := l.DetachCatalog()
			assert.Equal(t, l.DetachCatalog(), catalog)
			var before store.ReadSet
			before.Init(st, manifest.NamedResource{}, nil)
			defer before.Retire()
			assert.Equal(t, selection.Root(catalog.RootEvidence(child, &before, true)).Independent, true)
			var liveBefore store.ReadSet
			liveBefore.Init(st, manifest.NamedResource{}, nil)
			defer liveBefore.Retire()
			assert.Equal(t, selection.Root(l.RootEvidence(child, &liveBefore, true)).Independent, true)
			assert.Equal(t, l.RootEvidence(child, nil, true).Captured, true)
			assert.Equal(t, catalog.RootEvidence(child, nil, true).Captured, false)
			st.RecordEmitted(
				factory.Named(),
				&store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true},
				nil,
			)
			assert.Equal(t, before.Valid(), false)
			assert.Equal(t, liveBefore.Valid(), false)
			assert.Equal(t, l.RootEvidence(child, &liveBefore, true).Stale, true)
			var liveCovered store.ReadSet
			liveCovered.Init(st, manifest.NamedResource{}, nil)
			defer liveCovered.Retire()
			liveEvidence := l.RootEvidence(child, &liveCovered, true)
			assert.Equal(t, selection.Root(liveEvidence).Admitted, false)
			assert.Equal(t, selection.Root(liveEvidence).Independent, false)
			assert.Equal(t, selection.Root(liveEvidence).Owner, owner.Named())
			assert.Equal(t, selection.Root(l.RootEvidence(child, nil, true)).Independent, false)
			l.Close()
			denied.Store(true)
			count := calls.Load()
			assert.Equal(t, catalog.CatalogObject(child), object)
			assert.Equal(t, catalog.CatalogObject(literal), manifest.BaseManifest(nil))
			var covered store.ReadSet
			covered.Init(st, manifest.NamedResource{}, nil)
			defer covered.Retire()
			evidence := catalog.RootEvidence(child, &covered, true)
			assert.Equal(t, evidence.Record.Path, filepath.Join(root, "declarations", "child.yaml"))
			assert.Equal(t, evidence.Scope.Initial, true)
			assert.Equal(t, selection.Root(evidence).Admitted, false)
			assert.Equal(t, selection.Root(evidence).Independent, false)
			assert.Equal(t,
				cmp.Diff(
					liveEvidence, evidence,
					cmp.Exporter(func(reflect.Type) bool { return true }),
				),
				"",
			)
			assert.Equal(t, catalog.SourceExternal(source.Named(), &covered), false)
			assert.Equal(t, catalog.SourceExternal(external.Named(), &covered), true)
			assert.Equal(t, covered.Valid(), true)
			st.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
			assert.Equal(t, covered.Valid(), false)
			assert.Equal(t, liveCovered.Valid(), false)
			var restored store.ReadSet
			restored.Init(st, manifest.NamedResource{}, nil)
			defer restored.Retire()
			result := selection.Root(catalog.RootEvidence(child, &restored, true))
			assert.Equal(t, result.Admitted, true)
			assert.Equal(t, result.Independent, true)
			assert.Equal(t, result.Entry, true)
			assert.Equal(t, restored.Valid(), true)
			replacement := *input
			replacement.Content = "replacement"
			st.SetArtifact(source.Named(), &replacement)
			assert.Equal(t, restored.Valid(), false)
			var current store.ReadSet
			current.Init(st, manifest.NamedResource{}, nil)
			defer current.Retire()
			assert.Equal(t, selection.Root(catalog.RootEvidence(child, &current, true)).Independent, true)
			assert.Equal(t, current.Valid(), true)
			assert.Equal(t, calls.Load(), count)
			for _, doc := range catalog.files {
				assert.Equal(t, len(doc.objects), 0)
				assert.Equal(t, doc.body == nil, true)
				assert.Equal(t, doc.builtInput == nil, true)
			}
			catalog.Close()
			assert.Equal(t, reflect.ValueOf(*catalog).IsZero(), true)
			catalog.Close()
		})
	}
}

type catalogReadFS struct {
	fs.FS

	denied *atomic.Bool
	calls  *atomic.Int64
}

func (f catalogReadFS) Open(name string) (fs.File, error) {
	f.calls.Add(1)
	if f.denied.Load() {
		return nil, fs.ErrPermission
	}
	return f.FS.Open(name)
}

func TestSession_PendingRootsCoveringOwner(t *testing.T) {
	t.Parallel()
	l, _, factory, owner, child := admissionMembershipFixture(t)
	defer l.Close()
	l.Store.SetDiscoveryIndex(l.Index)
	proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	_, committed, err := l.commitSelection(t.Context(), t, proposal, selection.Execution)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, l.Options.Session.Snapshot().Output(child), true)
	assert.Equal(t, l.Store.DeleteObject(child), true)
	assert.Equal(t, l.Store.GetObject(child), manifest.BaseManifest(nil))
	catalog := l.DetachCatalog()
	defer catalog.Close()

	capture := func(independent bool) [2]*store.ReadSet {
		reads := new(store.ReadSet)
		reads.Init(l.Store, manifest.NamedResource{}, nil)
		t.Cleanup(reads.Retire)
		evidence := l.RootEvidence(child, reads, true)
		deferred := l.RootEvidence(child, reads, false)
		physical := evidence
		physical.CoveringOwners = nil
		assert.Equal(t, cmp.Diff(physical, deferred, cmp.Exporter(func(reflect.Type) bool { return true })), "")
		result := selection.Root(evidence)
		assert.Equal(t, result.Admitted, independent)
		assert.Equal(t, result.Independent, independent)
		if !independent {
			assert.Equal(t, result.Owner, owner.Named())
		}
		pending := l.Options.Session.PendingRoots()
		if pending != nil {
			t.Cleanup(pending.Retire)
			assert.Diff(t, maps.Collect(pending.Required()), map[manifest.NamedResource]bool{child: false})
			assert.Equal(t, pending.Valid(), true)
		}
		assert.Equal(t, pending != nil, independent)
		detached := new(store.ReadSet)
		detached.Init(l.Store, manifest.NamedResource{}, nil)
		t.Cleanup(detached.Retire)
		assert.Equal(t,
			cmp.Diff(
				evidence, catalog.RootEvidence(child, detached, true),
				cmp.Exporter(func(reflect.Type) bool { return true }),
			),
			"",
		)
		return [2]*store.ReadSet{reads, detached}
	}
	before := capture(true)
	pending := l.Options.Session.PendingRoots()
	assert.FatalIf(t, pending == nil, "selected missing declaration did not request acquisition")
	defer pending.Retire()
	l.Store.RecordEmitted(factory.Named(), &store.Emission{
		Children: []store.Child{{Obj: owner}}, Complete: true,
	}, nil)
	for _, reads := range before {
		assert.Equal(t, reads.Valid(), false)
	}
	assert.Equal(t, pending.Valid(), false)
	covered := capture(false)

	replacement := owner.Clone()
	replacement.Path = "./elsewhere"
	l.Store.RecordEmitted(factory.Named(), &store.Emission{
		Children: []store.Child{{Obj: replacement}}, Complete: true,
	}, nil)
	for _, reads := range covered {
		assert.Equal(t, reads.Valid(), false)
	}
	moved := capture(true)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{
		Children: []store.Child{{Obj: owner}}, Complete: true,
	}, nil)
	for _, reads := range moved {
		assert.Equal(t, reads.Valid(), false)
	}
	covered = capture(false)
	l.Store.RecordEmitted(factory.Named(), &store.Emission{Complete: true}, nil)
	for _, reads := range covered {
		assert.Equal(t, reads.Valid(), false)
	}
	capture(true)
}

type catalogObjectPrecedenceCase struct {
	name       string
	native     bool
	builder    bool
	record     bool
	candidate  bool
	occurrence bool
	compact    bool
	projected  bool
	live       bool
	want       string
}

func TestCatalogObject_Precedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []catalogObjectPrecedenceCase{
		{name: "native entry", native: true, builder: true, record: true, candidate: true, occurrence: true, want: "native"},
		{name: "builder declaration", builder: true, record: true, candidate: true, occurrence: true, want: "record"},
		{name: "object occurrence", occurrence: true, record: true, candidate: true, want: "occurrence"},
		{name: "compact occurrence", occurrence: true, compact: true, record: true, candidate: true, want: "occurrence"},
		{name: "projected occurrence", occurrence: true, compact: true, projected: true, want: "occurrence"},
		{name: "record before candidate", record: true, candidate: true, want: "record"},
		{name: "candidate", candidate: true, want: "candidate"},
		{name: "live store only", live: true, want: "live"},
		{name: "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCatalogObjectPrecedenceCase(t, &tc)
		})
	}
}

func testCatalogObjectPrecedenceCase(t *testing.T, tc *catalogObjectPrecedenceCase) {
	t.Helper()

	l := New(store.New())
	defer l.Close()
	l.candidates = map[manifest.NamedResource]store.Child{}
	l.locations = map[manifest.NamedResource]authoredOccurrence{}
	id := testutil.KustomizationID("apps", "child")
	objects := map[string]*manifest.Kustomization{}
	for _, name := range []string{"native", "record", "candidate", "occurrence", "live"} {
		objects[name] = &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "./" + name}
	}
	record := manifest.DiscoveryRecord{}
	if tc.record {
		record.Declaration = objects["record"]
	}
	if tc.builder {
		record.Builder = "parent"
	}
	l.Index.Records[id] = record
	if tc.native {
		l.nativeEntry = objects["native"]
	}
	if tc.candidate {
		l.candidates[id] = store.Child{Obj: objects["candidate"]}
	}
	if tc.occurrence {
		object := objects["occurrence"]
		authored := id
		if tc.projected {
			object = object.WithNamespace("").(*manifest.Kustomization)
			authored = object.Named()
		}
		occurrence := authoredOccurrence{
			Identity: authored, Authored: authored,
			object: object, facts: &occurrenceFacts{namespaced: true},
		}
		if tc.compact {
			occurrence.object, occurrence.facts.catalog = nil, object
		}
		l.locations[authored] = occurrence
	}
	if tc.live {
		l.Store.AddObject(objects["live"])
	}
	catalog := l.DetachCatalog()
	defer catalog.Close()
	for _, surface := range []struct {
		name   string
		object manifest.BaseManifest
		want   string
	}{
		{name: "loader", object: l.CatalogObject(id), want: tc.want},
		{name: "catalog", object: catalog.CatalogObject(id), want: tc.want},
	} {
		if tc.live && surface.name == "catalog" {
			// A store-only declaration has no retained catalog representation.
			surface.want = ""
		}
		if surface.want == "" {
			assert.Equal(t, surface.object, manifest.BaseManifest(nil))
			continue
		}
		if surface.object == nil {
			t.Errorf("%s lost %s declaration", surface.name, surface.want)
			continue
		}
		assert.Equal(t, surface.object.Named(), id)
		assert.Equal(t, manifest.Equal(surface.object, objects[surface.want]), true)
	}
	assert.Equal(t, objects["occurrence"].Named(), id)
}

type rootEvidenceStaticClassificationCase struct {
	name        string
	object      manifest.BaseManifest
	identity    manifest.NamedResource
	cached      bool
	native      bool
	indexed     bool
	eligible    bool
	independent bool
}

func TestRootEvidence_StaticClassification(t *testing.T) {
	t.Parallel()
	native := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
	for _, tc := range []rootEvidenceStaticClassificationCase{
		{
			name: "explicit namespace", indexed: true, eligible: true, independent: true,
			object: &manifest.ConfigMap{Name: "values", Namespace: "apps"},
		},
		{
			name: "missing namespace", indexed: true,
			object: &manifest.ConfigMap{Name: "values"},
		},
		{
			name: "projected cached namespace", indexed: true, cached: true, eligible: true, independent: true,
			object:   &manifest.ConfigMap{Name: "values"},
			identity: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"},
		},
		{
			name: "unindexed namespace projection", indexed: true, eligible: true,
			object:   &manifest.ConfigMap{Name: "values"},
			identity: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"},
		},
		{
			name: "cluster scope", indexed: true, eligible: true, independent: true,
			object: &manifest.RawObject{APIVersion: "v1", Kind: "Namespace", Name: "apps"},
		},
		{name: "native entry", object: native, native: true, eligible: true, independent: true},
		{
			name: "record without declaration", indexed: true,
			identity: testutil.KustomizationID("apps", "unknown"),
		},
		{name: "unknown identity", identity: testutil.KustomizationID("apps", "unknown")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRootEvidenceStaticClassificationCase(t, &tc, native)
		})
	}
}

func testRootEvidenceStaticClassificationCase(
	t *testing.T,
	tc *rootEvidenceStaticClassificationCase,
	native *manifest.Kustomization,
) {
	t.Helper()

	root := t.TempDir()
	l := New(store.New())
	defer l.Close()
	l.SourceRoot = root
	admitForTest(l.Store, root)
	l.locations = map[manifest.NamedResource]authoredOccurrence{}
	id := tc.identity
	if id == (manifest.NamedResource{}) {
		id = tc.object.Named()
	}
	if tc.native {
		l.nativeEntry, l.entry = native, entryKustomize
	}
	if tc.indexed {
		authored := id
		if tc.object != nil {
			authored = tc.object.Named()
			occurrence := compactOccurrence(tc.object, manifest.TemplateProjection{})
			occurrence.Declaration = occurrenceID{Source: root, File: "declaration.yaml"}
			occurrence.object = tc.object
			l.locations[authored] = occurrence
		}
		file := filepath.Join(root, "declaration.yaml")
		record := manifest.DiscoveryRecord{
			Path: file, File: "declaration.yaml", Authored: authored, Declaration: tc.object,
		}
		key := authored
		if tc.cached {
			key = id
		}
		l.Index.Records[key] = record
		l.documents.cache[file] = document{
			eligible: selection.RootEligible, initial: true, loaded: true, rootKnown: true,
		}
		if tc.cached {
			scope := rootAdmission{}
			view := l.physicalView()
			evidence := view.rootScope(l, &rootScopeInput{identity: authored, record: record, admission: scope})
			l.roots.set(id, &evidence)
		}
	}
	l.Store.SetDiscoveryIndex(l.Index)
	construction := l.RootEvidence(id, nil, true)
	assert.Equal(t, construction.Captured, false)
	assert.Equal(t, construction.Scope.NamespaceEligible, tc.eligible)
	assert.Equal(t, selection.Root(construction).Independent, tc.independent)
	var reads store.ReadSet
	reads.Init(l.Store, manifest.NamedResource{}, nil)
	defer reads.Retire()
	captured := l.RootEvidence(id, &reads, true)
	assert.Equal(t, captured.Captured, true)
	assert.Equal(t, captured.Scope.NamespaceEligible, tc.eligible)
	assert.Equal(t, selection.Root(captured).Independent, tc.independent)
	l.frozen = true
	assert.Equal(t, l.RootEvidence(id, nil, true).Captured, true)
	catalog := l.DetachCatalog()
	defer catalog.Close()
	static := catalog.RootEvidence(id, nil, true)
	assert.Equal(t, static.Captured, false)
	assert.Equal(t, static.Scope.NamespaceEligible, tc.eligible)
	assert.Equal(t, selection.Root(static).Independent, tc.independent)
	l.Close()
	var detachedReads store.ReadSet
	detachedReads.Init(l.Store, manifest.NamedResource{}, nil)
	defer detachedReads.Retire()
	detached := catalog.RootEvidence(id, &detachedReads, true)
	assert.Equal(t, detached.Scope.NamespaceEligible, tc.eligible)
	assert.Equal(t, selection.Root(detached).Independent, tc.independent)
	assert.Equal(t,
		cmp.Diff(
			captured, detached,
			cmp.Exporter(func(reflect.Type) bool { return true }),
		),
		"",
	)
}

type countedCatalogOwner struct {
	*manifest.Kustomization

	inventories *atomic.Int64
}

func (o *countedCatalogOwner) SourcePath() (manifest.NamedResource, string, bool) {
	var callers [16]uintptr
	count := runtime.Callers(2, callers[:])
	frames := runtime.CallersFrames(callers[:count])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, ".rootCoveringFacts") ||
			strings.HasSuffix(frame.Function, ".collectOwners") {
			o.inventories.Add(1)
			break
		}
		if !more {
			break
		}
	}
	return o.Kustomization.SourcePath()
}

type countedCatalogFacts struct {
	*Loader

	completed int
	deferred  int
}

func (f *countedCatalogFacts) RootEvidence(
	id manifest.NamedResource,
	reads *store.ReadSet,
	completeOwners bool,
) selection.RootEvidence {
	if completeOwners {
		f.completed++
	} else {
		f.deferred++
	}
	return f.Loader.RootEvidence(id, reads, completeOwners)
}

func TestSession_RootOwnerInventoryShared(t *testing.T) {
	t.Parallel()
	for _, mode := range []struct{ changed, empty bool }{{}, {changed: true}, {empty: true}, {changed: true, empty: true}} {
		t.Run(fmt.Sprintf("changed_%t_empty_%t", mode.changed, mode.empty), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{"payload/kustomization.yaml": {Data: []byte("resources: []\n")}}
			roots := make([]manifest.NamedResource, 0, 3)
			for i := range 3 {
				name := fmt.Sprintf("child-%d", i)
				files["declarations/"+name+".yaml"] = &fstest.MapFile{
					Data: []byte(testutil.KustomizationYAML(name, "apps", "  path: ./payload\n")),
				}
				roots = append(roots, testutil.KustomizationID("apps", name))
			}
			st := store.New()
			st.AddObject(
				&manifest.GitRepository{
					Name:      manifest.BootstrapSourceID.Name,
					Namespace: manifest.BootstrapSourceID.Namespace,
				},
			)
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: files},
			)
			l := New(st)
			defer l.Close()
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Roots: roots, Changed: mode.changed})
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			st.SetDiscoveryIndex(l.Index)
			factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
			st.AddObject(factory)
			var inventories atomic.Int64
			owner := &countedCatalogOwner{
				Kustomization: &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./elsewhere"},
				inventories:   &inventories,
			}

			if !mode.empty {
				st.RecordEmitted(
					factory.Named(),
					&store.Emission{Children: []store.Child{{Obj: owner}}, Complete: true},
					nil,
				)
			}
			facts := &countedCatalogFacts{Loader: l}
			l.Options.Session, err = selection.New(st, facts, selection.Request{Roots: roots, Changed: mode.changed})
			assert.NoError(t, err)
			var preparation *selection.Preparation
			defer func() { preparation.Retire() }()
			request := selection.PrepareRequest{Invocation: true, Preparation: &preparation}
			inventories.Store(0)
			proposal, err := l.Options.Session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			defer proposal.Retire()

			expected := int64(1)
			if mode.empty {
				expected = 0
			}
			assert.Equal(t, inventories.Load(), expected)
			assert.Equal(t, facts.completed, 0)
			assert.Equal(t, facts.deferred >= len(roots), true)
			replacement := &countedCatalogOwner{Kustomization: owner.Kustomization.Clone(), inventories: &inventories}
			replacement.Path = "./declarations"
			st.RecordEmitted(
				factory.Named(),
				&store.Emission{Children: []store.Child{{Obj: replacement}}, Complete: true},
				nil,
			)
			inventories.Store(0)
			proposal, err = l.Options.Session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, inventories.Load(), int64(1))
			assert.Equal(t, facts.completed, 0)
			assert.Equal(t, slices.Contains(proposal.Needed, replacement.Named()), true)
			for _, id := range roots {
				var reads store.ReadSet
				reads.Init(st, manifest.NamedResource{}, nil)
				result := selection.Root(l.RootEvidence(id, &reads, true))
				assert.Equal(t, result.Independent, false)
				assert.Equal(t, result.Owner, replacement.Named())
				reads.Retire()
			}
		})
	}
}
