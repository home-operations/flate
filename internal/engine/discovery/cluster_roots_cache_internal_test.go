package discovery

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type registrationContext struct {
	context.Context //nolint:containedctx // The fixture overrides Err at the registration boundary.

	onRegistration func()
	checks         int
}

func (c *registrationContext) Err() error {
	const registrationCheck = 2
	c.checks++
	if c.checks == registrationCheck {
		c.onRegistration()
	}
	return c.Context.Err()
}

func TestFollowOutput_StalePreparationReturnsOriginalChildren(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	d := newDiscoverer(&Config{Input: input, Path: root, Store: st, Trees: testTrees(t)})
	t.Cleanup(d.Close)
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "."}
	original := selection.OccurrenceFacts{
		Identity: testutil.KustomizationID("apps", "authored"), RequiresBuild: true,
	}
	built := &manifest.Kustomization{Name: "built", Namespace: "apps"}
	frame := &declarationPreparation{
		owner: owner, source: input, complete: true, children: []store.Child{{Obj: built}},
	}
	frame.captureSupport(st, manifest.BootstrapSourceID)
	d.prepared = map[manifest.NamedResource]*declarationPreparation{owner.Named(): frame}
	occurrence := selection.OccurrenceFacts{
		Declaration: selection.OccurrenceID{Source: root, File: "parent.yaml"},
		Identity:    owner.Named(), Object: owner, Mounted: input,
	}
	planner := entryPlanner{
		session: d,
		inputs:  map[string]*store.SourceArtifact{root: input},
		objects: map[occurrenceKey]store.Child{occurrenceIdentity(&occurrence): {VisibleGeneration: true}},
		projections: map[occurrenceProjection][]selection.OccurrenceFacts{
			{path: root, namespace: owner.EmissionPolicy().Namespace, input: input}: {original},
		},
	}
	ctx := &registrationContext{Context: t.Context(), onRegistration: func() {
		next := *input
		next.Content = "replacement"
		st.SetArtifact(manifest.BootstrapSourceID, &next)
	}}
	assert.Equal(t, frame.support.Valid(), true)
	children, err := planner.followOutput(
		ctx,
		input,
		&rootApplication{visit: &rootVisit{occurrence: &occurrence}, object: owner},
		root,
	)
	assert.NoError(t, err)
	assert.Equal(t, d.prepared[owner.Named()], frame)
	assert.Equal(t, frame.complete, true)
	assert.Equal(t, frame.support.Valid(), false)
	assert.Diff(t, children, []selection.OccurrenceFacts{original})
}

func TestPlan_LocalWorkloadTracing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		changes *change.Set
	}{
		{name: "full"},
		{name: "changed only", changes: new(change.Set)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var reads atomic.Int64
			files := fstest.MapFS{
				"entry/sync/kustomization.yaml": {Data: []byte("resources: [sync.yaml]\n")},
				"entry/sync/sync.yaml": {Data: []byte(testutil.KustomizationYAML("entry", "apps",
					"  path: ./entry\n"))},
				"entry/apps/workload.yaml": {Data: []byte(testutil.KustomizationYAML("workload", "apps",
					"  path: ./workload\n"))},
				"workload/object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
			}
			input := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				FS:        entryReadFS{FS: files, reads: map[string]*atomic.Int64{"workload/object.yaml": &reads}},
			}
			scan := filepath.Join(root, "entry")
			entries, err := Plan(t.Context(), Config{
				Input: input, Path: scan,
				Selection: selection.Request{Changed: tc.changes != nil}, Changes: tc.changes,
			})
			assert.NoError(t, err)
			assert.FatalIf(t, len(entries) != 1, "entries = %+v", entries)
			assert.Equal(t, entries[0].ID, manifest.ClusterID("entry"))
			assert.Equal(t, entries[0].Path, scan)
			assert.Equal(t, entries[0].aggregate, false)
			assert.Equal(t, reads.Load() > 0, tc.changes == nil)
		})
	}
}

func TestPlan_TracksDisjointWorkloadsForMultipleRoots(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"workload/object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
	}
	for _, name := range []string{"first", "second"} {
		entry := "entry/" + name
		files[entry+"/sync/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [sync.yaml]\n")}
		files[entry+"/sync/sync.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML(name, "apps", "  path: ./"+entry+"\n")),
		}
		files[entry+"/apps/workload.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML(name+"-workload", "apps", "  path: ./workload\n")),
		}
	}
	var reads atomic.Int64
	session, err := Begin(t.Context(), testConfig(t, Config{
		Input: &store.SourceArtifact{
			Provider:  store.ProviderInput,
			LocalPath: root,
			FS:        entryReadFS{FS: files, reads: map[string]*atomic.Int64{"workload/object.yaml": &reads}},
		},
		Store: store.New(), Path: filepath.Join(root, "entry"),
		Selection: selection.Request{Changed: true}, Changes: new(change.Set),
	}))
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	_, err = session.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	assert.NoError(t, session.loadPlan(t.Context()))
	assert.Equal(t, reads.Load(), 0)
	var roots []clusterRoot
	for {
		roots, err = session.clusterRoots(t.Context())
		if !errors.Is(err, errPendingSources) {
			break
		}
		assert.NoError(t, constructTestSources(t.Context(), session))
	}
	assert.NoError(t, err)
	assert.FatalIf(t, len(roots) != 2, "roots = %+v", roots)
	assert.Equal(t, reads.Load() > 0, true)
	entries := clusterEntries(roots, session.cfg.Input, session.cfg.Path)
	assert.FatalIf(t, len(entries) != 2, "entries = %+v", entries)
	for i, name := range []string{"first", "second"} {
		assert.Equal(t, len(roots[i].qualifying), 1)
		assert.FatalIf(t, len(roots[i].anchors) != 1, "expected one anchor for %s", name)
		assert.Equal(t, roots[i].anchors[0], bootstrapAnchor{target: "entry/" + name})
		assert.Equal(t, slices.ContainsFunc(roots[i].Occurrences, func(o selection.OccurrenceFacts) bool {
			return o.Declaration.File == "workload/object.yaml"
		}), true)
		entry := &entries[i]
		assert.Equal(t, entry.Path, filepath.Join(root, "entry", name))
		var inputs entryInputs
		inputs.collectEntryOperands(entry, roots, "entry/"+name, false)
		assert.Diff(t, inputs.publicOperands, []manifest.NamedResource{testutil.KustomizationID("apps", name)})
	}
}

func TestPlan_IndirectAnchorSeedOrder(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"first seed first", "anchor seed first"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			for i, side := range []string{"base", "current"} {
				t.Run(side, func(t *testing.T) {
					t.Parallel()
					testIndirectAnchorSeedOrder(t, order == "anchor seed first", manifest.TreeSide(i))
				})
			}
		})
	}
}

func testIndirectAnchorSeedOrder(t *testing.T, reverse bool, side manifest.TreeSide) {
	t.Helper()
	root := t.TempDir()
	files := fstest.MapFS{
		"outside/a.yaml": {Data: []byte(testutil.KustomizationYAML("a", "apps",
			"  path: ./shared\n"))},
		"entry/bootstrap/b.yaml": {Data: []byte(testutil.KustomizationYAML("b", "apps",
			"  path: ./shared\n"))},
		"entry/kustomization.yaml": {Data: []byte("resources: [bootstrap/b.yaml, marker.yaml]\n")},
		"entry/marker.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker}\ndata: {side: base}\n"),
		},
		"shared/return.yaml": {Data: []byte(testutil.KustomizationYAML("return", "apps",
			"  path: ./entry\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))},
		"orphan/root.yaml": {Data: []byte(testutil.KustomizationYAML("orphan", "apps",
			"  path: ./workload\n"))},
		"workload/object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n")},
	}
	current := maps.Clone(files)
	current["entry/marker.yaml"] = &fstest.MapFile{Data: []byte(
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker}\ndata: {side: current}\n",
	)}
	inputs := [2]*store.SourceArtifact{
		{Provider: store.ProviderInput, LocalPath: root, FS: files},
		{Provider: store.ProviderInput, LocalPath: root, FS: current},
	}
	changes, err := change.Detect(
		t.Context(),
		inputs[manifest.TreeBase],
		inputs[manifest.TreeHead],
		change.DetectConfig{},
	)
	assert.NoError(t, err)
	d, err := Begin(t.Context(), Config{
		Path: root, Input: inputs[side],
		Store: store.New(), Selection: selection.Request{Changed: true}, Changes: changes,
	})
	assert.NoError(t, err)
	t.Cleanup(d.Close)
	assert.NoError(t, constructTestSources(t.Context(), d))
	assert.NoError(t, d.loadPlan(t.Context()))
	first, err := d.loader.Occurrences(t.Context(), filepath.Join(root, "outside"), "")
	assert.NoError(t, err)
	second, err := d.loader.Occurrences(t.Context(), filepath.Join(root, "entry", "bootstrap"), "")
	assert.NoError(t, err)
	seeds := append(slices.Clone(first), second...)
	assert.FatalIf(t, len(seeds) != 2, "seeds = %+v", seeds)
	if reverse {
		slices.Reverse(seeds)
	}
	orphan, err := d.loader.Occurrences(t.Context(), filepath.Join(root, "orphan"), "")
	assert.NoError(t, err)
	d.planner = &entryPlanner{
		session: d, inputs: map[string]*store.SourceArtifact{root: d.cfg.Input},
		coordinates: map[selection.OccurrenceID]bool{}, seen: map[occurrenceKey]rootVisitState{},
		roots: []clusterRoot{{Path: "entry/bootstrap", seeds: seeds}, {Path: "orphan", seeds: orphan}},
	}
	roots, err := d.clusterRoots(t.Context())
	assert.NoError(t, err)
	entries := clusterEntries(roots, d.cfg.Input, root)
	assert.FatalIf(t, len(entries) != 2, "entries = %+v", entries)
	assert.Equal(t, entries[0].ID, manifest.ClusterID("entry"))
	assert.Equal(t, entries[0].target, filepath.Join(root, "entry"))
	assert.Equal(t, entries[1].ID, manifest.ClusterID("orphan"))
	for _, entry := range entries {
		var members []manifest.NamedResource
		for _, i := range entry.roots {
			for j := range roots[i].Occurrences {
				occurrence := &roots[i].Occurrences[j]
				members = append(members, occurrence.Identity)
			}
		}
		slices.SortFunc(members, manifest.NamedResource.Compare)
		if entry.ID == manifest.ClusterID("orphan") {
			assert.Diff(t, members, []manifest.NamedResource{
				{Kind: "ConfigMap", Name: "workload"}, testutil.KustomizationID("apps", "orphan"),
			})
			continue
		}
		assert.Diff(t, members, []manifest.NamedResource{
			{Kind: "ConfigMap", Name: "marker"},
			testutil.KustomizationID("apps", "a"),
			testutil.KustomizationID("apps", "b"), testutil.KustomizationID("apps", "return"),
		})
	}
}

func TestFollowTarget_WorkloadDeferral(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path          string
		anchored, seed, pin bool
		unknownScan         bool
		deferWorkload       bool
	}{
		{name: "disjoint workload", path: "workload", anchored: true, deferWorkload: true},
		{name: "unanchored root", path: "workload"},
		{name: "seed", path: "workload", anchored: true, seed: true},
		{name: "pinned source", path: "workload", anchored: true, pin: true},
		{name: "above scan", path: ".", anchored: true},
		{name: "at scan", path: "entry", anchored: true},
		{name: "path case differs", path: "ENTRY", anchored: true, deferWorkload: runtime.GOOS != "windows"},
		{name: "below scan", path: "entry/child", anchored: true},
		{name: "sibling prefix", path: "entry-other", anchored: true, deferWorkload: true},
		{name: "unknown scan origin", path: "entry", anchored: true, unknownScan: true, deferWorkload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root}
			planner := entryPlanner{
				roots: []clusterRoot{{Path: "entry/sync"}},
				session: &Session{cfg: Config{
					Input: input, Path: filepath.Join(root, "entry"), Changes: new(change.Set),
				}},
			}
			if tc.anchored {
				planner.roots[0].anchors = []bootstrapAnchor{{target: "entry"}}
			}
			if tc.unknownScan {
				planner.session.cfg.Path = ""
			}
			if tc.pin {
				input = &store.SourceArtifact{
					Provider:  store.ProviderCommit,
					LocalPath: filepath.Join(root, "pin"),
					LocalRoot: root,
					Content:   "pinned",
				}
			}
			want, local := filepath.Join(input.LocalPath, tc.path), !tc.pin
			if tc.deferWorkload {
				want, local = "", false
			}
			target, targetLocal := planner.followTarget(&planner.roots[0], input, tc.path, tc.seed)
			assert.Equal(t, target, want)
			assert.Equal(t, targetLocal, local)
		})
	}
}

func TestEntryReach_CapturesSelectedDescendant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, application   string
		selected, visible   bool
		reaches, descendant bool
	}{
		{name: "ancestor", application: ".", selected: true, visible: true, reaches: true, descendant: true},
		{name: "equivalent spelling", application: "apps" + string(filepath.Separator) + ".", selected: true, visible: true, reaches: true},
		{name: "case only", application: "APPS", selected: true, visible: true, reaches: runtime.GOOS == "windows"},
		{name: "exact entry", application: "apps", selected: true, visible: true, reaches: true},
		{name: "unselected exact entry without generation", application: "apps", reaches: true},
		{name: "unselected equivalent entry without generation", application: "apps" + string(filepath.Separator) + ".", reaches: true},
		{name: "unselected ancestor without generation", application: "."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			entry := filepath.Join(root, "apps")
			occurrence := selection.OccurrenceID{Source: root, File: "apps/selected.yaml"}
			target := entryTarget{origin: entry}
			if tc.selected {
				target = entryTarget{
					origin: filepath.Join(entry, "selected.yaml"), occurrence: occurrence, coverEntry: true,
				}
			}
			reach := entryReach{
				entry: entry, targets: []entryTarget{target},
				packageReaches: func(_ *applicationEvidence, path string) bool { return path == target.origin },
			}
			evidence := applicationEvidence{visibleGeneration: tc.visible}
			origin := root + string(filepath.Separator) + tc.application
			assert.Equal(t, reach.includes(origin, &evidence, true), tc.reaches)
			assert.Equal(t, evidence.descendant, tc.descendant)
			assert.Equal(t, slices.Contains(evidence.reaches, occurrence), tc.selected && tc.reaches)
		})
	}
}

func TestAdmission_ReachesEntryAcrossMounts(t *testing.T) {
	t.Parallel()
	root, checkout, alias := t.TempDir(), t.TempDir(), t.TempDir()
	files := fstest.MapFS{}
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: checkout, LocalRoot: root, FS: files}
	st := store.New()
	source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
	st.SetArtifact(source.Named(), &store.SourceArtifact{
		Provider: store.ProviderAlias, LocalPath: alias, LocalRoot: root, FS: files,
	})
	admission := Admission{input: input, path: filepath.Join(checkout, "apps"), store: st}
	for _, tc := range []struct {
		path string
		want bool
	}{
		{path: "apps", want: true},
		{path: "APPS", want: runtime.GOOS == "windows"},
		{path: "apps-other"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			application := manifest.EntryApplication{Source: source.Named(), Path: tc.path}
			assert.Equal(t, admission.reachesEntry(&application, nil), tc.want)
		})
	}
}

func TestPlan_CatalogSourceScope(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	planner := entryPlanner{
		session: d, coordinates: map[selection.OccurrenceID]bool{}, seen: map[occurrenceKey]rootVisitState{},
	}
	mounts := map[string]*store.SourceArtifact{
		"left": input,
		"right": {
			Provider: store.ProviderAlias, LocalPath: t.TempDir(), LocalRoot: root, FS: input.FS,
		},
	}
	mounts["right-sibling"] = mounts["right"]
	for _, side := range []string{"left", "right", "right-sibling"} {
		source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
		mount := mounts[side]
		planner.catalog.occurrences = append(planner.catalog.occurrences, selection.OccurrenceFacts{
			Declaration: selection.OccurrenceID{Source: mount.LocalPath, File: side + "/source.yaml"},
			Identity:    source.Named(), Object: source, Mounted: mount,
		})
	}
	for _, side := range []string{"left", "right"} {
		owner := &manifest.Kustomization{Name: "root", Namespace: "apps"}
		owner.Path = "./" + side
		mount := mounts[side]
		seed := d.loader.ProducerFacts(owner).Occurrence
		seed.Declaration = selection.OccurrenceID{Source: mount.LocalPath, File: side + "/sync.yaml"}
		seed.Mounted = mount
		candidate := clusterRoot{Path: side, seeds: []selection.OccurrenceFacts{seed}}
		assert.NoError(t, planner.start(t.Context(), &candidate))
		id := planner.catalog.occurrences[0].Identity
		selected, _ := planner.catalogSource(id)
		assert.FatalIf(t, selected == nil, "missing source for %s", side)
		assert.Equal(t, selected.Declaration.File, side+"/source.yaml")
		repeated, _ := planner.catalogSource(id)
		assert.Equal(t, repeated == selected, true)
	}
}

func TestPlan_CatalogScopeWithoutMount(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"apps/sync.yaml":          {Data: []byte(testutil.KustomizationYAML("sync", "apps", "  path: ./apps\n"))},
		"apps-sibling/other.yaml": {Data: []byte(testutil.KustomizationYAML("other", "apps", "  path: ./elsewhere\n"))},
	}
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	d := newDiscoverer(&Config{Path: root, Input: input, Store: st})
	t.Cleanup(d.Close)
	d.loader.SourceRoot = root
	_, err := d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, d.beginRootPlan(t.Context()))
	assert.FatalIf(t, len(d.planner.roots) != 2, "roots = %+v", d.planner.roots)
	assert.Equal(t, d.planner.roots[0].Path, "apps")
	assert.Equal(t, d.planner.roots[0].seeds[0].Mounted, (*store.SourceArtifact)(nil))

	mapped := *input
	mapped.LocalRoot = t.TempDir()
	d.cfg.Input = &mapped
	assert.NoError(t, d.planner.start(t.Context(), &d.planner.roots[0]))
	occurrences := d.planner.catalogView.occurrences
	assert.FatalIf(t, len(occurrences) != 1, "catalog = %+v", occurrences)
	assert.Equal(t, occurrences[0].Declaration.File, "apps/sync.yaml")

	secondMount := &store.SourceArtifact{
		Provider: store.ProviderAlias, LocalPath: t.TempDir(), LocalRoot: mapped.LocalRoot, FS: files,
	}
	mountedRoot := d.planner.roots[0]
	mountedRoot.seeds = slices.Clone(mountedRoot.seeds)
	mountedSeed := &mountedRoot.seeds[0]
	mountedSeed.Declaration.Source = secondMount.LocalPath
	mountedSeed.Mounted = secondMount
	d.planner.catalog.occurrences = slices.Clone(d.planner.catalog.occurrences)
	for i := range d.planner.catalog.occurrences {
		occurrence := &d.planner.catalog.occurrences[i]
		occurrence.Declaration.Source = secondMount.LocalPath
		occurrence.Mounted = secondMount
	}
	target, contains := configuredApplicationTarget(&mapped, "./apps", mountedSeed)
	assert.Equal(t, target, filepath.Join(mapped.LocalRoot, "apps"))
	assert.Equal(t, contains, true)
	assert.NoError(t, d.planner.start(t.Context(), &mountedRoot))
	occurrences = d.planner.catalogView.occurrences
	assert.FatalIf(t, len(occurrences) != 1, "mounted catalog = %+v", occurrences)
	assert.Equal(t, occurrences[0].Declaration, mountedSeed.Declaration)

	d.cfg.Trees = testTrees(t)
	d.planner.directoryInventory = false
	d.planner.roots[0].seeds[0].RequiresBuild = true
	assert.Equal(t, d.planner.rootRequiresPreparation(&d.planner.roots[0], &mapped), true)
	mountedSeed.RequiresBuild = true
	assert.Equal(t, d.planner.rootRequiresPreparation(&mountedRoot, &mapped), true)
	mountedSibling := *mountedSeed
	mountedSibling.Declaration.File = "apps-sibling/other.yaml"
	mountedRoot.seeds = []selection.OccurrenceFacts{mountedSibling}
	assert.Equal(t, d.planner.rootRequiresPreparation(&mountedRoot, &mapped), false)

	d.cfg.Path = filepath.Join(root, "apps")
	seed := &d.planner.roots[0].seeds[0]
	application := manifest.EntryApplication{Path: "apps"}
	parent := clusterRoot{Path: "."}
	assert.Equal(t, d.planner.bootstrapRoot(&parent, seed, &mapped, &application), true)
	sibling := d.planner.roots[1].seeds[0]
	assert.Equal(t, d.planner.bootstrapRoot(&parent, &sibling, &mapped, &application), false)
}

func TestPlan_CatalogNamespaceProjection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("namespace: apps\nresources: [owner.yaml]\n")},
		"owner.yaml":         {Data: []byte(testutil.KustomizationYAML("owner", "", "  path: ./leaf\n"))},
	}}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, d.beginRootPlan(t.Context()))
	assert.FatalIf(t, len(d.planner.catalog.occurrences) != 1, "catalog = %+v", d.planner.catalog.occurrences)
	occurrence := &d.planner.catalog.occurrences[0]
	projected := occurrence.EffectiveObject()
	assert.Equal(t, projected.Named().Namespace, "apps")
	assert.Equal(t, occurrence.EffectiveObject() == projected, true)
	assert.Equal(t, d.packages[0].Occurrences[0].Object.Named().Namespace, "")
}

func TestPlan_ProjectedGeneratedApplication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, syncPath, syncName, wantName string
		prepare                            bool
	}{
		{name: "omitted path"},
		{name: "missing directory", syncPath: "./missing", wantName: "apps"},
		{name: "authored root", syncPath: ".", wantName: "apps", prepare: true},
		{name: "explicit sync name", syncPath: "./missing", syncName: "content", wantName: "content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			var syncName string
			if tc.syncName != "" {
				syncName = "    name: " + tc.syncName + "\n"
			}
			var syncPath string
			if tc.syncPath != "" {
				syncPath = "    path: " + tc.syncPath + "\n"
			}
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
				"kustomization.yaml": {Data: []byte("namespace: apps\nresources: [instance.yaml]\n")},
				"instance.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance}
spec:
  sync:
    kind: GitRepository
    url: https://fixture.invalid/checkout.git
    ref: refs/heads/main
` + syncPath + syncName)},
			}}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			d := newDiscoverer(&Config{Path: root, Input: input, Store: st, Trees: testTrees(t)})
			t.Cleanup(d.Close)
			configureSelection(t, d.loader, selection.Request{Changed: true})
			d.loader.SourceRoot = root
			_, err := d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			occurrences, err := d.loader.Occurrences(t.Context(), root, "")
			assert.NoError(t, err)
			assert.Equal(t, len(occurrences), 1)
			occurrence := &occurrences[0]
			assert.Equal(t, occurrence.Object, manifest.BaseManifest(nil))
			assert.Equal(t, occurrence.Authored.Namespace, "")
			assert.Equal(t, occurrence.Identity.Namespace, "apps")
			assert.Equal(t, occurrence.Catalog.Named().Namespace, "")
			assert.Equal(t, st.GetDeclaration(occurrence.Identity), manifest.BaseManifest(nil))

			occurrence.RequiresBuild = true
			planner := entryPlanner{session: d, directoryInventory: true}
			assert.Equal(
				t,
				planner.rootRequiresPreparation(&clusterRoot{Path: ".", seeds: occurrences}, input),
				tc.prepare,
			)
			source, path, valid := occurrence.SourcePath()
			assert.Equal(t, valid, tc.syncPath != "")
			assert.Equal(t, path, tc.syncPath)
			var expected manifest.NamedResource
			if tc.syncPath != "" {
				expected = manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindGitRepository,
					Namespace: "apps",
					Name:      tc.wantName,
				}
			}
			assert.Equal(t, source, expected)
			application, applies := occurrence.EntryApplication()
			assert.Equal(t, applies, valid)
			assert.Equal(t, application, manifest.EntryApplication{Source: expected, Path: tc.syncPath})
		})
	}
}

func TestPlan_ProducerEvidenceReuse(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	var reads atomic.Int64
	files := fstest.MapFS{"producer.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec: {}
`)}}
	input := &store.SourceArtifact{
		Provider:  store.ProviderInput,
		LocalPath: root,
		FS:        entryReadFS{FS: files, reads: map[string]*atomic.Int64{"producer.yaml": &reads}},
	}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	packages, err := d.loader.Packages(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, len(packages), 1)
	scope := rootScope{occurrences: slices.Clone(packages[0].Occurrences)}
	planner := entryPlanner{session: d, inputs: map[string]*store.SourceArtifact{root: input}}
	assert.NoError(t, planner.prepareScope(t.Context(), &scope))
	assert.Equal(t, len(scope.occurrences[0].TemplateObjects), 0)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "content",
	}
	reference := aliasRequestReference(t, d.cfg.Store, id)
	visit := rootVisit{occurrence: &scope.occurrences[0], scope: &scope}
	_, err = planner.source(t.Context(), &clusterRoot{}, &rootApplication{visit: &visit}, &reference)
	assert.Equal(t, errors.Is(err, errPendingSources), true)
	assert.NoError(t, constructTestSources(t.Context(), d))
	before := reads.Load()
	assert.Equal(t, len(planner.objects), 1)
	planner.parsed = d.loader.ParsedInputs()
	parsed := planner.parsed
	for range 2 {
		_, sourceErr := planner.source(t.Context(), &clusterRoot{}, &rootApplication{visit: &visit}, &reference)
		assert.NoError(t, sourceErr)
	}
	assert.Equal(t, reads.Load(), before)
	assert.Equal(t, planner.parsed == parsed, true)
	assert.Equal(t, len(planner.objects), 1)
	witness := scope.occurrences[0]
	witness.Witness = new(int)
	scope = rootScope{occurrences: []selection.OccurrenceFacts{witness}}
	assert.NoError(t, planner.prepareScope(t.Context(), &scope))
	assert.Equal(t, len(planner.objects), 2)
	assert.Equal(t, planner.parsed == nil, true)
	replaced := &store.SourceArtifact{
		Provider:  store.ProviderInput,
		LocalPath: root,
		FS: fstest.MapFS{"producer.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: apps}
spec:
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: added}
    spec: {url: oci://registry.invalid/added}
`)}},
	}
	planner.inputs[root] = replaced
	replacement := scope.occurrences[0]
	replacement.Witness = new(int)
	replacement.Identity.Namespace = "other"
	scope = rootScope{occurrences: []selection.OccurrenceFacts{replacement}}
	assert.NoError(t, planner.prepareScope(t.Context(), &scope))
	assert.Equal(t, len(planner.objects), 3)
	assert.Equal(t, planner.parsed == nil, true)
	id = manifest.NamedResource{
		Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository,
		Namespace: "other", Name: "added",
	}
	_, producers := scope.source(id)
	assert.Equal(t, len(producers), 1)
	id.Namespace = "apps"
	_, producers = scope.source(id)
	assert.Equal(t, len(producers), 0)
}

func TestPlan_ResolvedEntryDeclarations(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps", `  path: ./entry
  postBuild:
    substitute: {SYNC_PATH: ./entry, SYNC_NAME: content}
`))},
		"entry/kustomization.yaml": {Data: []byte("resources: [instance.yaml, child.yaml]\n")},
		"entry/instance.yaml": {Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: instance, namespace: apps}
spec:
  sync:
    kind: GitRepository
    url: https://fixture.invalid/checkout.git
    ref: refs/heads/main
    path: ${SYNC_PATH}
    name: ${SYNC_NAME}
`)},
		"entry/child.yaml": {Data: []byte(testutil.KustomizationYAML("child", "apps", `  path: ${SYNC_PATH}
  sourceRef: {kind: GitRepository, name: "${SYNC_NAME}"}
`))},
	}}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	d := newDiscoverer(&Config{Path: root, Input: input, Store: st, Trees: testTrees(t)})
	t.Cleanup(d.Close)
	configureSelection(t, d.loader, selection.Request{Changed: true})
	d.loader.SourceRoot = root
	_, err := d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, d.loader.FinalizeIndex(t.Context(), root, false))

	source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
	want := manifest.EntryApplication{Source: source.Named(), Path: "./entry"}
	child, known := d.loader.Occurrence(testutil.KustomizationID("apps", "child"))
	assert.Equal(t, known, true)
	assert.Equal(t, child.Declaration.File, "entry/child.yaml")
	authored, _ := manifest.ApplicationFor(child.EffectiveObject())
	assert.FatalIf(t, authored == want, "checkout fixture did not distinguish authored and resolved application")

	scope := rootScope{occurrences: []selection.OccurrenceFacts{child}}
	planner := entryPlanner{session: d, inputs: map[string]*store.SourceArtifact{root: input}}
	assert.Equal(t, planner.scopedEntry(source.Named(), source, &scope), entryAccepted)

	instance, known := d.loader.Occurrence(manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "instance",
	})
	assert.Equal(t, known, true)
	generated, err := planner.scopeGeneration(&instance, instance.EffectiveObject())
	assert.NoError(t, err)
	assert.FatalIf(t, len(generated) != 2, "generated %d declarations, want 2", len(generated))
	assert.Equal(t, generated[0].Identity, want.Source)
	application, applies := generated[1].EntryApplication()
	assert.Equal(t, applies, true)
	assert.Equal(t, application, want)
}
