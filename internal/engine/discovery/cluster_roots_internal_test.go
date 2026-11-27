package discovery

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestClusterEntries_BootstrapPackage(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name, scan, builder string
		packagePath         string
		explicitAncestor    bool
		grouped             bool
	}{
		{
			name: "repository root", scan: ".", builder: "cluster/flux/bootstrap/kustomization.yaml",
			packagePath: "cluster/flux/bootstrap",
		},
		{
			name: "sync directory", scan: "cluster/flux", builder: "cluster/flux/bootstrap/kustomization.yaml",
			packagePath: "cluster/flux/bootstrap",
		},
		{
			name: "sync builder", scan: ".", builder: "cluster/flux/kustomization.yaml",
			packagePath: "cluster/flux",
		},
		{
			name: "grouped roots", scan: ".", builder: "cluster/flux/bootstrap/kustomization.yaml",
			packagePath: "cluster/flux/bootstrap", grouped: true,
		},
		{
			name: "explicit ancestor", scan: "cluster", builder: "cluster/flux/bootstrap/kustomization.yaml",
			explicitAncestor: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{tt.builder: {}}}
			roots := []clusterRoot{{
				Path: "cluster/flux/bootstrap", packageBuild: &plannedPackage{},
				anchors:    []bootstrapAnchor{{target: "cluster/flux", source: manifest.BootstrapSourceID}},
				qualifying: []selection.OccurrenceID{{Source: root}},
			}}
			wantPackages := 1
			if tt.grouped {
				secondary := roots[0]
				secondary.Path = "cluster/flux/secondary"
				secondary.packageBuild = &plannedPackage{}
				roots = append(roots, secondary)
				wantPackages++
			}
			scan := filepath.Join(root, tt.scan)
			entries := clusterEntries(roots, input, scan)
			assert.FatalIf(t, len(entries) != 1, "entries = %+v", entries)
			assert.Equal(t, entries[0].ID, manifest.ClusterID("cluster/flux"))
			assert.Equal(t, entries[0].Path, scan)
			relative := "cluster/flux"
			target := filepath.Join(root, relative)
			if tt.explicitAncestor {
				relative, target = tt.scan, ""
			}
			assert.Equal(t, entries[0].target, target)
			prepared := entryInputs{input: *input}
			prepared.collectEntryOperands(&entries[0], roots, relative, false)
			assert.Equal(t, len(prepared.packages), wantPackages)
			assert.Equal(t, entries[0].packagePath, tt.packagePath)
			entries[0].prepared = &entryInputs{input: *input, parsed: new(loader.ParsedInputs)}
			session := newDiscoverer(&Config{Input: input, Path: scan, Store: store.New(), Entry: &entries[0]})
			t.Cleanup(session.Close)
			file, known := session.cachedEntryBuilder(input, target)
			assert.Equal(t, known, !tt.explicitAncestor)
			wantBuilder := ""
			if tt.packagePath == "cluster/flux" {
				wantBuilder = tt.builder
			}
			assert.Equal(t, file, wantBuilder)
		})
	}
}

const bootstrapLayoutBuilder = "resources: [cm.yaml, other.yaml]\n"

func TestCompareEntryApplications_BootstrapLayouts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, builder, value string
		wantFirst            string
		wantBound, wantError bool
	}{
		{name: "equal", builder: bootstrapLayoutBuilder, value: "old", wantFirst: "inventory"},
		{name: "reordered", builder: "resources: [other.yaml, cm.yaml]\n", value: "old", wantFirst: "other"},
		{
			name: "different", builder: bootstrapLayoutBuilder, value: "new",
			wantFirst: "inventory", wantBound: true,
		},
		{
			name: "failed inventory", value: "old", wantBound: true, wantError: true,
			builder: bootstrapLayoutBuilder + "patches:\n- target: {kind: ConfigMap}\n  patch: |-\n" +
				"    - op: replace\n      path: /missing/key\n      value: rejected\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, reverse := range []bool{false, true} {
				left := bootstrapLayoutSession(t, "entry/bootstrap", "old", bootstrapLayoutBuilder)
				right := bootstrapLayoutSession(t, "entry", tc.value, tc.builder)
				first, second := left, right
				if reverse {
					first, second = right, left
				}
				assert.NoError(t, first.CompareEntryApplications(t.Context(), second, new(change.Set), true))
				assertBootstrapLayoutBinding(t, left, "inventory", bindingExpectation{bound: tc.wantBound})
				assertBootstrapLayoutBinding(t, right, tc.wantFirst, bindingExpectation{
					bound: tc.wantBound, failed: tc.wantError,
				})
			}
		})
	}
	t.Run("canceled second capture", testCanceledBootstrapCapture)
}

type bindingExpectation struct {
	bound, failed bool
}

func assertBootstrapLayoutBinding(t *testing.T, side *Session, wantFirst string, want bindingExpectation) {
	t.Helper()
	relative, err := filepath.Rel(side.cfg.Input.LocalPath, side.entryCoordinates.packagePath)
	assert.NoError(t, err)
	path := filepath.ToSlash(relative)
	id := manifest.NewEntryKustomization(manifest.BootstrapSourceID, path, manifest.NamedResource{}).Named()
	frame := side.prepared[id]
	assert.FatalIf(t, frame == nil, "missing inventory for %s", path)
	if wantFirst != "" {
		assert.FatalIf(t, len(frame.children) != 2, "expected two inventory documents for %s", path)
		assert.Equal(t, frame.children[0].Obj.Named().Name, wantFirst)
	}
	key, bound := store.Bindings(side.loader.Index).Slot(id)
	assert.Equal(t, bound, want.bound)
	if bound {
		wantKey := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).Named()
		assert.Equal(t, side.binding.Producer, id)
		assert.Equal(t, side.binding.Path, path)
		assert.Equal(t, side.binding.Source, manifest.BootstrapSourceID)
		assert.Equal(t, key, wantKey)
		assert.Equal(t, side.binding.Failure != nil, want.failed)
	}
}

func testCanceledBootstrapCapture(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		first := bootstrapLayoutSession(t, "entry/bootstrap", "old", bootstrapLayoutBuilder)
		second := bootstrapLayoutSession(t, "entry", "old", bootstrapLayoutBuilder)
		if reverse {
			first, second = second, first
		}
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		var captured *store.ReadSet
		prepare := second.cfg.PrepareDocuments
		second.cfg.PrepareDocuments = func(owner manifest.BaseManifest, docs []manifest.DocOccurrence) {
			captured = first.entryAdmission.qualification.proposal
			assert.FatalIf(t, captured == nil, "the first capture must finish before cancellation")
			artifact := captured.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			assert.FatalIf(t, artifact == nil, "the first proposal must hold the bootstrap artifact")
			cancel()
			prepare(owner, docs)
		}
		err := first.CompareEntryApplications(ctx, second, new(change.Set), true)
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		assert.FatalIf(t, captured == nil, "the first capture must finish before cancellation")
		assert.Equal(t, captured.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent), nil)
		assert.Equal(t, first.entryAdmission.qualification.proposal, nil)
		assert.Equal(t, second.entryAdmission.qualification.proposal, nil)
		assert.Equal(t, first.binding, Binding{})
		assert.Equal(t, second.binding, Binding{})
	}
}

func TestSameEntryInventory_DuplicateIdentity(t *testing.T) {
	t.Parallel()
	object := &manifest.ConfigMap{Name: "inventory"}
	first := store.Child{Obj: object, Doc: map[string]any{"value": "first"}}
	second := store.Child{Obj: object, Doc: map[string]any{"value": "second"}}
	for _, tc := range []struct {
		name  string
		right []store.Child
		equal bool
	}{
		{name: "same occurrences", right: []store.Child{second, first, first}, equal: true},
		{name: "different counts", right: []store.Child{second, first, second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			left, right := new(declarationPreparation), new(declarationPreparation)
			left.children, right.children = []store.Child{first, second, first}, tc.right
			assert.Equal(t, left.sameEntryInventory(right), tc.equal)
			assert.Equal(t, right.sameEntryInventory(left), tc.equal)
			assert.Equal(t, left.children[0].Doc["value"], "first")
			assert.Equal(t, right.children[0].Doc["value"], "second")
		})
	}
}

func TestCompareEntryApplications_QualifiedBootstrapOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		reverse bool
	}{
		{name: "application first"},
		{name: "application second", reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			left := bootstrapLayoutSession(t, "entry/bootstrap", "'${version}'", "resources: [cm.yaml]\n")
			right := bootstrapLayoutSession(t, "entry", "'${version}'", "resources: [cm.yaml]\n")
			app := &manifest.Kustomization{Name: "bootstrap", Namespace: "apps", Path: "./entry/bootstrap"}
			app.PostBuild = &kustomizev1.PostBuild{
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "values"}},
			}
			left.cfg.Store.AddObject(app)
			left.admission().AddApplication(left.cfg.Store, app)
			first, second := left, right
			if tc.reverse {
				first, second = right, left
			}
			assert.NoError(t, first.CompareEntryApplications(t.Context(), second, new(change.Set), true))
			assert.Equal(t, left.binding, Binding{})
			assert.Equal(t, right.binding, Binding{})
			frame := left.prepared[app.Named()]
			assert.FatalIf(t, frame == nil || frame.complete, "expected an incomplete application inventory")

			left.cfg.Store.AddObject(&manifest.ConfigMap{
				Name: "values", Namespace: "apps", Data: map[string]any{"version": "qualified"},
			})
			assert.NoError(t, first.CompareEntryApplications(t.Context(), second, new(change.Set), true))
			assert.Equal(t, left.binding.Producer, app.Named())
			frame = left.prepared[app.Named()]
			assert.FatalIf(
				t, frame == nil || !frame.complete || len(frame.children) != 1,
				"missing qualified inventory",
			)
			assert.Equal(t, frame.children[0].Doc["data"].(map[string]any)["version"], "qualified")
			literal := manifest.NewEntryKustomization(
				manifest.BootstrapSourceID, "entry", manifest.NamedResource{},
			).Named()
			assert.Equal(t, right.binding.Producer, literal)
			assert.Equal(t, left.binding.Key, right.binding.Key)
		})
	}
}

func bootstrapLayoutSession(t *testing.T, pkg, value, builder string) *Session {
	t.Helper()
	root := t.TempDir()
	input := &store.SourceArtifact{
		LocalPath: root, Provider: store.ProviderInput, FS: fstest.MapFS{
			pkg + "/kustomization.yaml": {Data: []byte(builder)},
			pkg + "/cm.yaml": {Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: inventory}\ndata: {version: " + value + "}\n",
			)},
			pkg + "/other.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other}\n")},
		},
	}
	path := filepath.Join(root, "entry")
	entry := &Entry{
		Path: path, Input: input, target: path, packagePath: pkg,
		prepared: &entryInputs{input: *input, parsed: new(loader.ParsedInputs)},
	}
	cfg := testConfig(t, Config{
		Input: input, Path: path, Store: store.New(), Entry: entry,
		Selection: selection.Request{Changed: true}, Changes: new(change.Set),
	})
	session, err := BeginTest(t.Context(), cfg)
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	ready, err := session.Load(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, ready, true)
	assert.Equal(t, session.entryScanPath(), path)
	return session
}

func TestClusterRoots_PinnedOwnership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		claims [][2]string
		want   []string
	}{
		{name: "unclaimed", want: []string{"a", "b", "c"}},
		{name: "retired claims", claims: [][2]string{{"a", "b"}, {"b", "c"}}, want: []string{"a", "c"}},
		{name: "owner sorts later", claims: [][2]string{{"c", "a"}}, want: []string{"b", "c"}},
		{name: "mutual owner with downstream", claims: [][2]string{{"b", "c"}, {"c", "b"}, {"b", "a"}}, want: []string{"b"}},
		{name: "mutual ownership", claims: [][2]string{{"a", "b"}, {"b", "a"}}, want: []string{"a", "c"}},
		{name: "self ownership", claims: [][2]string{{"a", "a"}}, want: []string{"a", "b", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			roots := []clusterRoot{{Path: "a"}, {Path: "b"}, {Path: "c"}}
			for i := range roots {
				roots[i].pinned.Claims = map[string][]manifest.NamedResource{}
				for _, claim := range tc.claims {
					if roots[i].Path != claim[0] {
						continue
					}
					prefix := claim[1] + "/"
					roots[i].pinned.Claims[prefix] = append(
						roots[i].pinned.Claims[prefix],
						manifest.NamedResource{Kind: manifest.KindKustomization, Name: claim[0]},
					)
				}
			}
			d := &Session{
				cfg: Config{Input: &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir()}},
			}
			paths := func(roots []clusterRoot) []string {
				result := make([]string, len(roots))
				for i := range roots {
					result[i] = roots[i].Path
				}
				slices.Sort(result)
				return result
			}
			assert.Diff(t, paths(d.unpinnedRoots(slices.Clone(roots))), tc.want)
			slices.Reverse(roots)
			assert.Diff(t, paths(d.unpinnedRoots(roots)), tc.want)
		})
	}
}

func TestClusterRoots_PartitionAnchors(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{LocalPath: string(filepath.Separator)}
	root := clusterRoot{
		anchors: []bootstrapAnchor{
			{target: "apps"},
			{target: "apps/nested"},
			{target: "apps/./nested"},
			{target: "K-only"},
			{target: "."},
			{target: "K"},
			{target: "K"},
			{target: "K/K"},
			{target: "K/K/a"},
		},
	}
	declarations := []selection.OccurrenceID{
		{Source: input.LocalPath, File: "apps", Document: 1},
		{Source: input.LocalPath, File: "apps/nested/objects.yaml", Document: 1},
		{Source: input.LocalPath, File: "apps/nested/objects.yaml", Document: 2},
		{Source: input.LocalPath, File: "apps-other/object.yaml", Document: 1},
		{Source: input.LocalPath, File: "K-only/object.yaml", Document: 1},
		{Source: input.LocalPath, File: "K/file.yaml", Document: 1},
		{Source: input.LocalPath, File: "K/K/a/object.yaml", Document: 1},
	}
	for _, declaration := range declarations {
		root.seeds = append(root.seeds, selection.OccurrenceFacts{Declaration: declaration})
	}
	partitions := root.partitionAnchors(input)
	assert.Equal(t, len(partitions), len(root.anchors))
	wantPartitions := [][]selection.OccurrenceID{
		declarations[:1], declarations[1:3], nil, nil, declarations[3:5],
		declarations[5:6], nil, nil, declarations[6:],
	}
	if runtime.GOOS == "windows" {
		wantPartitions[3] = declarations[4:5]
		wantPartitions[4] = declarations[3:4]
		wantPartitions[5] = nil
		wantPartitions[6] = declarations[5:6]
	}
	for i, want := range wantPartitions {
		assert.Equal(t, partitions[i].Path, root.anchors[i].target)
		var got []selection.OccurrenceID
		for j := range partitions[i].seeds {
			got = append(got, partitions[i].seeds[j].Declaration)
		}
		assert.Diff(t, got, want)
	}
}

func TestPlan_ClusterRootsWithAppBases(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"main", "test", "utility"} {
		testutil.WriteFile(t, root, "kubernetes/clusters/"+name+"/apps.yaml",
			testutil.KustomizationYAML("cluster-apps", "flux-system", "  path: ./kubernetes/apps/"+name+"\n"))
		testutil.WriteFile(t, root, "kubernetes/apps/"+name+"/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n",
		)
	}
	for _, name := range []string{"create", "vibecraft"} {
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/base/"+name+"/kustomization.yaml",
			"resources: [source.yaml, cm.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/base/"+name+"/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: "+name+"}\nspec: {url: oci://fixture.invalid/"+name+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"kubernetes/apps/base/"+name+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n",
		)
	}
	for range 2 {
		entries, err := Plan(t.Context(), Config{Path: root})
		assert.NoError(t, err)
		paths := make([]string, 0, len(entries))
		for _, entry := range entries {
			paths = append(paths, entry.Path)
		}
		assert.Diff(
			t,
			paths,
			[]string{
				filepath.Join(root, "kubernetes", "clusters", "main"),
				filepath.Join(root, "kubernetes", "clusters", "test"),
				filepath.Join(root, "kubernetes", "clusters", "utility"),
			},
		)
	}
}

func TestPlan_InterleavedCompatiblePackages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"x", "y", "z"} {
		testutil.WriteFile(t, root, "a/"+name+"/kustomization.yaml", "resources: [objects.yaml]\n")
		var contents strings.Builder
		contents.WriteString(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: " + name + "}\nspec: {url: oci://fixture.invalid/" + name + "}\n",
		)
		for _, identity := range []string{"X", "Z"} {
			if name == "x" && identity == "Z" || name == "z" && identity == "X" {
				continue
			}
			contents.WriteString(
				"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + strings.ToLower(identity) + "}\n",
			)
		}
		testutil.WriteFile(t, root, "a/"+name+"/objects.yaml", contents.String())
	}
	entries, err := Plan(t.Context(), Config{Path: root})
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].Path, root)
	assert.Equal(t, entries[0].aggregate, true)
}

func TestPlan_DependentNamespacePackages(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, namespace := range []string{"storage", "workloads"} {
		spec := "  path: ./apps/" + namespace + "/app\n"
		if namespace == "workloads" {
			spec += "  dependsOn: [{name: storage, namespace: storage}]\n"
		}
		testutil.WriteFile(t, root, "apps/"+namespace+"/kustomization.yaml", "resources: [ks.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/"+namespace+"/ks.yaml",
			testutil.KustomizationYAML(namespace, namespace, spec),
		)
		testutil.WriteFile(t, root, "apps/"+namespace+"/app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/"+namespace+"/app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+namespace+"}\n",
		)
	}
	for range 2 {
		entries, err := Plan(t.Context(), Config{Path: filepath.Join(root, "apps"), RepoRoot: root})
		assert.NoError(t, err)
		ids := make([]manifest.ClusterID, len(entries))
		for i := range entries {
			ids[i] = entries[i].ID
		}
		assert.Diff(t, ids, []manifest.ClusterID{"apps"})
		assert.FatalIf(t, len(entries) != 1, "namespace packages produced %d entries", len(entries))
		assert.Equal(t, entries[0].Path, filepath.Join(root, "apps"))
		assert.Equal(t, entries[0].aggregate, true)
	}
}

func TestPlan_AncestorBootstrapSiblingApplication(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{}
	write := func(path, body string) { files[path] = &fstest.MapFile{Data: []byte(body)} }
	write(
		"entry/flux-system/kustomization.yaml",
		"resources: [sync.yaml]\npatches:\n- target: {kind: Kustomization, name: flux-system}\n  patch: |-\n    - op: add\n      path: /spec/prune\n      value: true\n",
	)
	write(
		"entry/flux-system/sync.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: flux-system}\nspec: {url: 'https://fixture.invalid/repo'}\n---\n"+
			testutil.KustomizationYAML(
				"flux-system",
				"flux-system",
				"  path: ./entry\n  sourceRef: {kind: GitRepository, name: cluster}\n",
			),
	)
	write(
		"entry/apps/ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./workloads\n  sourceRef: {kind: GitRepository, name: cluster}\n  dependsOn: [{name: unavailable}]\n",
		),
	)
	write("workloads/kustomization.yaml", "resources: [cm.yaml]\n")
	write("workloads/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n")
	cfg := testConfig(
		t,
		Config{
			Input:    &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files},
			Path:     filepath.Join(root, "entry"),
			RepoRoot: root,
			SelfURLs: []string{"https://fixture.invalid/repo"},
		},
	)
	entries, err := Plan(t.Context(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].ID, manifest.ClusterID("entry"))
	assert.Equal(t, entries[0].aggregate, false)
	cfg.Input, cfg.Entry, cfg.Path, cfg.Store = entries[0].Input, &entries[0], entries[0].Path, store.New()
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	defer result.Close()
	apps := testutil.KustomizationID("flux-system", "apps")
	record, found := result.Index.Lookup(apps)
	assert.Equal(t, found, true)
	assert.Equal(t, record.Authored, apps)
	assert.Equal(t, record.Parent, testutil.KustomizationID("flux-system", "flux-system"))
	assert.Equal(t, result.Binding.Producer, testutil.KustomizationID("flux-system", "flux-system"))
}

func TestPlan_CustomBootstrapSource(t *testing.T) {
	t.Parallel()
	for _, scan := range []string{".", "clusters/dev", "clusters/dev/flux-system"} {
		t.Run(scan, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			source := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "fleet",
			}
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
				"clusters/dev/flux-system/kustomization.yaml": {Data: []byte("resources: [sync.yaml]\n")},
				"clusters/dev/flux-system/sync.yaml": {Data: []byte(
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
						"metadata: {name: fleet, namespace: flux-system}\nspec: {url: https://fixture.invalid/fleet.git}\n---\n" +
						testutil.KustomizationYAML("flux-system", "flux-system",
							"  path: ./clusters/dev\n  sourceRef: {kind: GitRepository, name: fleet}\n"),
				)},
				"clusters/dev/apps.yaml": {Data: []byte(testutil.KustomizationYAML("apps", "flux-system",
					"  path: ./workloads\n  sourceRef: {kind: GitRepository, name: fleet}\n"))},
				"workloads/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"workloads/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n"),
				},
			}}
			entries, err := Plan(t.Context(), testConfig(t, Config{Input: input, Path: filepath.Join(root, scan)}))
			assert.NoError(t, err)
			assert.FatalIf(t, len(entries) != 1, "got %d planned entries, want one", len(entries))
			binding := entries[0].prepared.sources[source]
			assert.FatalIf(t, binding.artifact == nil, "no local binding for %s", source)
			assert.Equal(t, binding.object.Named(), source)
			assert.Equal(t, binding.entry, true)
			assert.Equal(t, binding.input, input)
			assert.Equal(t, (binding.artifact.Provider == store.ProviderAlias), true)
			assert.Equal(t, binding.artifact.OriginPath(binding.artifact.LocalPath), root)
			assert.Equal(t, binding.artifact.LocalPath == root, false)
		})
	}
}

func TestEntryPlanner_BootstrapAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		path          string
		file          string
		requiresBuild bool
		competing     bool
		bound         bool
	}{
		{name: "authored", path: "./tenants/x", file: "tenants/x/sync.yaml", bound: true},
		{name: "competing source", path: "./tenants/x", file: "tenants/x/sync.yaml", competing: true},
		{name: "omitted path", file: "tenants/x/sync.yaml"},
		{name: "requires build", path: "./tenants/x", file: "tenants/x/sync.yaml", requiresBuild: true},
		{name: "target outside checkout", path: "./tenants/../../", file: "tenants/x/sync.yaml"},
		{name: "declaration outside target", path: "./tenants/y", file: "tenants/x/sync.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			st := store.New()
			fleet := &manifest.GitRepository{Name: "fleet", Namespace: manifest.DefaultNamespace}
			fleet.URL = "https://fixture.invalid/fleet.git"
			owner := &manifest.Kustomization{
				Name: "sync", Namespace: fleet.Namespace, Path: tc.path,
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository, Name: fleet.Name,
				},
			}
			occurrences := []selection.OccurrenceFacts{{
				Identity: owner.Named(), Catalog: owner, RequiresBuild: tc.requiresBuild,
				Declaration: selection.OccurrenceID{Source: root, File: tc.file},
			}}
			cfg := testConfig(t, Config{Store: st, Input: input, Path: filepath.Join(root, "tenants", "x", "nested")})
			d := newDiscoverer(&cfg)
			t.Cleanup(d.Close)
			admission := d.admission()

			existing := &manifest.GitRepository{Name: "existing", Namespace: fleet.Namespace}
			var binding *store.SourceArtifact
			if tc.competing {
				binding = &store.SourceArtifact{
					LocalPath: t.TempDir(), LocalRoot: root, FS: input.FS, Provider: store.ProviderAlias,
				}
				st.AddObject(existing)
				st.SetArtifact(existing.Named(), binding)
				other := *owner
				other.Name, other.SourceRef.Name = "existing", existing.Name
				occurrences = append(occurrences, selection.OccurrenceFacts{
					Identity: other.Named(), Object: &other,
					Declaration: selection.OccurrenceID{Source: root, File: "tenants/x/existing.yaml"},
				})
			}

			planner := entryPlanner{session: d, scope: rootScope{occurrences: occurrences}}
			application, valid := owner.EntryApplication()
			assert.Equal(t, valid, true)
			entry := planner.scopedEntry(application.Source, fleet, &planner.scope)
			resolved, err := admission.resolve(t.Context(), fleet.Named(), &sourceRequest{
				object: fleet, entry: entry == entryAccepted,
			})
			assert.NoError(t, err)
			assert.Equal(t, resolved.artifact != nil, tc.bound)
			if tc.competing {
				assert.Equal(t, st.GetArtifact(existing.Named(), store.ArtifactContent), store.Artifact(binding))
			}
		})
	}
}

type entryPlannerSyncCase struct {
	name, file, path string
	mounted, reject  bool
	want             bool
}

func TestEntryPlanner_SyncScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []entryPlannerSyncCase{
		{name: "mounted", file: "other/instance.yaml", path: "./elsewhere", mounted: true, want: true},
		{name: "rejected", file: "entry/instance.yaml", path: "./entry", reject: true},
		{name: "replaced pair", file: "other/instance.yaml", path: "./elsewhere"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEntryPlannerSyncScope(t, &tc)
		})
	}
}

func testEntryPlannerSyncScope(t *testing.T, tc *entryPlannerSyncCase) {
	t.Helper()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	st := store.New()
	cfg := testConfig(t, Config{Store: st, Input: input, RepoRoot: root, Path: filepath.Join(root, "entry")})
	d := newDiscoverer(&cfg)
	t.Cleanup(d.Close)
	d.resolvedSources = &entrySourcePublisher{store: st, mutable: map[manifest.NamedResource]entryResolution{}}

	var source manifest.BaseManifest = &manifest.OCIRepository{
		Name: "sync", Namespace: "apps", URL: "oci://fixture.invalid/repo",
	}
	if !tc.mounted && !tc.reject {
		source = &manifest.GitRepository{Name: "sync", Namespace: "apps", URL: "https://fixture.invalid/repo"}
	}
	id := source.Named()
	app := manifest.NewEntryKustomization(id, tc.path, manifest.NamedResource{})
	app.Name, app.Namespace = "sync", "apps"
	previousApp := *app
	previousApp.Path = "./entry"
	previous := &syncSource{source: source, application: &previousApp}
	mount := &store.SourceArtifact{Provider: store.ProviderAlias, LocalPath: t.TempDir(), LocalRoot: root, FS: input.FS}
	d.resolvedSources.record(id, &entryResolution{
		object: source, input: input, artifact: mount, entry: true, projected: true,
		sync: previous, provider: store.ProviderAlias,
	})
	selected := selection.OccurrenceFacts{
		Identity: id, Object: source, Declaration: selection.OccurrenceID{Source: root, File: tc.file},
	}
	if tc.mounted {
		selected.Declaration.Source, selected.Mounted = mount.LocalPath, mount
	}
	application := selected
	application.Identity, application.Object = app.Named(), app
	scope := rootScope{occurrences: []selection.OccurrenceFacts{selected, application}}
	if tc.reject {
		other := &manifest.GitRepository{Name: "other", Namespace: "apps"}
		st.AddObject(other)
		st.SetArtifact(other.Named(), input)
		narrow := manifest.NewEntryKustomization(other.Named(), "./entry/nested", manifest.NamedResource{})
		narrow.Name, narrow.Namespace = "narrow", "apps"
		scope.occurrences = append(scope.occurrences, selection.OccurrenceFacts{
			Identity: narrow.Named(), Object: narrow,
			Declaration: selection.OccurrenceID{Source: root, File: "entry/nested/ks.yaml"},
		})
	}
	sourceChild := store.Child{Obj: source, VisibleGeneration: true}
	planner := entryPlanner{
		session: d,
		inputs:  map[string]*store.SourceArtifact{selected.Declaration.Source: selected.Mounted},
		objects: map[occurrenceKey]store.Child{
			occurrenceIdentity(&selected):    sourceChild,
			occurrenceIdentity(&application): {Obj: app, VisibleGeneration: true},
		},
	}
	_, err := planner.source(t.Context(), &clusterRoot{}, &rootVisit{scope: &scope, occurrence: &application},
		&manifest.Reference{NamedResource: id})
	assert.Equal(t, errors.Is(err, errPendingSources), tc.want)
	if !tc.want {
		assert.NoError(t, err)
	}
	binding := d.resolvedSources.snapshot()[id]
	assert.Equal(t, binding.artifact != nil, tc.want)
	if tc.want {
		assert.Equal(t, binding.sync, previous)
	} else {
		assert.Equal(t, binding.sync == nil, true)
	}
}

func TestPlan_ExternalURLBootstrapSource(t *testing.T) {
	t.Parallel()
	for _, scan := range []string{".", "tenants/x"} {
		t.Run(scan, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "external",
			}
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
				"tenants/x/ks.yaml": {Data: []byte(testutil.KustomizationYAML("sync", source.Namespace,
					"  path: ./\n  sourceRef: {kind: GitRepository, name: external}\n",
				))},
				"tenants/x/source.yaml": {Data: []byte(
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
						"metadata: {name: external, namespace: flux-system}\nspec: {url: https://other.invalid/other.git}\n",
				)},
			}}
			entries, err := Plan(t.Context(), testConfig(t, Config{Input: input, Path: filepath.Join(root, scan)}))
			assert.NoError(t, err)
			assert.FatalIf(t, len(entries) != 1, "got %d planned entries, want one", len(entries))
			binding := entries[0].prepared.sources[source]
			assert.FatalIf(t, binding.artifact == nil, "no checkout binding for %s", source)
			assert.Equal(t, (binding.artifact.Provider == store.ProviderAlias), true)
			assert.Equal(t, binding.artifact.OriginPath(binding.artifact.LocalPath), root)
		})
	}
}
