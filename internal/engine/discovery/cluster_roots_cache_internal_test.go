package discovery

import (
	"errors"
	"path/filepath"
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

func TestFollowTarget_WorkloadDeferral(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path          string
		anchored, seed, pin bool
		deferWorkload       bool
	}{
		{name: "disjoint workload", path: "workload", anchored: true, deferWorkload: true},
		{name: "unanchored root", path: "workload"},
		{name: "seed", path: "workload", anchored: true, seed: true},
		{name: "pinned source", path: "workload", anchored: true, pin: true},
		{name: "above scan", path: ".", anchored: true},
		{name: "below scan", path: "entry/child", anchored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{LocalPath: root}
			planner := entryPlanner{
				roots: []clusterRoot{{Path: "entry/sync"}},
				session: &Session{cfg: Config{
					Input: input, Path: filepath.Join(root, "entry"), Changes: new(change.Set),
				}},
			}
			if tc.anchored {
				planner.roots[0].anchors = []bootstrapAnchor{{target: "entry"}}
			}
			if tc.pin {
				input = &store.SourceArtifact{LocalPath: filepath.Join(root, "pin"), LocalRoot: root, Content: "pinned"}
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

func TestPlan_CatalogSourceScope(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	t.Cleanup(d.Close)
	planner := entryPlanner{
		session: d, coordinates: map[selection.OccurrenceID]bool{}, seen: map[occurrenceKey]bool{},
	}
	for _, side := range []string{"left", "right"} {
		source := &manifest.GitRepository{Name: "content", Namespace: "apps"}
		planner.catalog.occurrences = append(planner.catalog.occurrences, selection.OccurrenceFacts{
			Declaration: selection.OccurrenceID{Source: root, File: side + "/source.yaml"},
			Identity:    source.Named(), Object: source,
		})
	}
	for _, side := range []string{"left", "right"} {
		owner := &manifest.Kustomization{Name: "root", Namespace: "apps"}
		owner.Path = "./" + side
		candidate := clusterRoot{Path: side, seeds: []selection.OccurrenceFacts{{
			Declaration: selection.OccurrenceID{Source: root, File: side + "/sync.yaml"},
			Identity:    owner.Named(), Object: owner,
		}}}
		assert.NoError(t, planner.start(t.Context(), &candidate))
		id := planner.catalog.occurrences[0].Identity
		selected, _ := planner.catalogSource(id)
		assert.FatalIf(t, selected == nil, "missing source for %s", side)
		assert.Equal(t, selected.Declaration.File, side+"/source.yaml")
		repeated, _ := planner.catalogSource(id)
		assert.Equal(t, repeated == selected, true)
	}
}

func TestPlan_CatalogNamespaceProjection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
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
	projected := occurrenceObject(occurrence)
	assert.Equal(t, projected.Named().Namespace, "apps")
	assert.Equal(t, occurrenceObject(occurrence) == projected, true)
	assert.Equal(t, d.packages[0].Occurrences[0].Object.Named().Namespace, "")
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
	_, err = planner.source(t.Context(), &clusterRoot{}, &visit, &reference)
	assert.Equal(t, errors.Is(err, errPendingSources), true)
	assert.NoError(t, constructTestSources(t.Context(), d))
	before := reads.Load()
	assert.Equal(t, len(planner.objects), 1)
	planner.parsed = d.loader.ParsedInputs()
	parsed := planner.parsed
	for range 2 {
		_, sourceErr := planner.source(t.Context(), &clusterRoot{}, &visit, &reference)
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
