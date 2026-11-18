package discovery

import (
	"cmp"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAggregateInput_PackageEvidence(t *testing.T) {
	t.Parallel()
	for _, authored := range []bool{false, true} {
		name := "generated"
		if authored {
			name = "authored"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const root = "/virtual/aggregate"
			files := fstest.MapFS{
				"package/kustomization.yaml": {Data: []byte("resources: [object.yaml]\nnamespace: transformed\n")},
				"package/object.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: package}\n"),
				},
				"plain.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: plain}\n"),
				},
			}
			if authored {
				files["kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [plain.yaml]\n")}
			}
			input := &store.SourceArtifact{
				LocalPath: root, LocalRoot: "/virtual/origin", Content: "immutable-original", FS: files,
			}
			packages := []loader.PackageEvidence{
				{Path: "package", Independent: true},
				{Path: ".", Independent: true, Occurrences: []selection.OccurrenceFacts{
					{Declaration: selection.OccurrenceID{Source: root, File: "plain.yaml"}},
					{Declaration: selection.OccurrenceID{Source: root, File: "plain.yaml", Document: 1}},
				}},
			}
			aggregate, err := aggregateInput(input, root, packages)
			assert.NoError(t, err)
			assert.Equal(t, input.Content, "immutable-original")
			file := filepath.Join(root, "plain.yaml")
			assert.Equal(t, aggregate.OriginPath(file), input.OriginPath(file))
			data, err := fs.ReadFile(aggregate.FS, "kustomization.yaml")
			assert.NoError(t, err)
			var document kustypes.Kustomization
			assert.NoError(t, yaml.Unmarshal(data, &document))
			if authored {
				assert.Equal(t, aggregate, input)
				assert.Diff(t, document.Resources, []string{"plain.yaml"})
			} else {
				assert.Equal(t, aggregate.Content, "")
				assert.Equal(t, aggregate.ContentFingerprint == nil, true)
				assert.Diff(t, document.Resources, []string{"package", "plain.yaml"})
				_, err := fs.Stat(input.FS, "kustomization.yaml")
				assert.Equal(t, err != nil, true)
			}
		})
	}
}

func TestPlan_BootstrapRootClusters(t *testing.T) {
	t.Parallel()
	for _, style := range []string{"gotk-sync", "equivalent", "same-named instances", "disjoint identities", "rootless", "child applications"} {
		t.Run(style, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := bootstrapRootFiles(style)
			for range 2 {
				entries, err := Plan(t.Context(), Config{
					Input: &store.SourceArtifact{LocalPath: root, FS: files}, Path: root,
					SelfURLs: []string{"https://fixture.invalid/repo"},
				})
				assert.NoError(t, err)
				if style == "rootless" || style == "child applications" {
					assert.Equal(t, len(entries), 1)
					assert.Equal(t, entries[0].Path, root)
					assert.Equal(t, entries[0].aggregate, true)
					_, builderErr := fs.Stat(entries[0].Input.FS, "kustomization.yaml")
					assert.NoError(t, builderErr)
				} else {
					assert.Equal(t, len(entries), 2)
					for i, side := range []string{"first", "second"} {
						assert.Equal(t, entries[i].ID, manifest.ClusterID(side))
						assert.Equal(t, entries[i].Path, filepath.Join(root, side))
					}
				}
			}
		})
	}
}

func TestPlan_RequestedApplication(t *testing.T) {
	t.Parallel()
	for _, exists := range []bool{false, true} {
		t.Run(fmt.Sprintf("target exists %t", exists), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{
				"root.yaml": {
					Data: []byte(
						"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps, namespace: apps}\nspec: {path: ./apps, sourceRef: {kind: GitRepository, name: cluster}}\n",
					),
				},
			}
			if exists {
				files["apps/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
			}
			entries, err := Plan(
				t.Context(),
				Config{Input: &store.SourceArtifact{LocalPath: root, FS: files}, Path: root},
			)
			assert.NoError(t, err)
			assert.Equal(t, len(entries), 1)
			assert.Equal(t, entries[0].ID, manifest.ClusterID("."))
			assert.Equal(t, entries[0].Path, root)
			assert.Equal(t, entries[0].aggregate, false)
		})
	}
}

func TestClusterRoots_ListOccurrences(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"input.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: List\nitems:\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: same}\n- apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: same}\n",
			),
		},
	}})
	l := loader.New(st)
	l.SourceRoot = root
	configureSelection(t, l, selection.Request{Changed: true})
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	occurrences, err := l.Occurrences(t.Context(), root, "")
	assert.NoError(t, err)
	assert.Equal(t, len(occurrences), 2)
	assert.Equal(t, occurrences[0].Declaration == occurrences[1].Declaration, false)
	entries := clusterEntries([]clusterRoot{
		{Path: "a", Occurrences: occurrences[:1]},
		{Path: "b", Occurrences: occurrences[1:]},
	}, &store.SourceArtifact{LocalPath: root}, root)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].aggregate, true)
}

func TestAggregateInput_PreservesSymlinks(t *testing.T) {
	t.Parallel()
	const root = "/virtual/aggregate-links"
	builder := tree.NewBuilder(nil)
	assert.NoError(
		t,
		builder.AddFile("plain.yaml", []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: plain}\n"), 0o644),
	)
	assert.NoError(t, builder.AddSymlink("linked.yaml", "plain.yaml"))
	authored, err := builder.Build()
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, Content: "authored", FS: authored}
	aggregate, err := aggregateInput(input, root, []loader.PackageEvidence{
		{Path: ".", Independent: true, Occurrences: []selection.OccurrenceFacts{
			{Declaration: selection.OccurrenceID{Source: root, File: "linked.yaml"}},
		}},
	})
	assert.NoError(t, err)
	target, err := fs.ReadLink(aggregate.FS, "linked.yaml")
	assert.NoError(t, err)
	assert.Equal(t, target, "plain.yaml")
	info, err := fs.Lstat(aggregate.FS, "linked.yaml")
	assert.NoError(t, err)
	assert.Equal(t, info.Mode()&fs.ModeSymlink != 0, true)
	_, err = fs.Stat(authored, "kustomization.yaml")
	assert.Equal(t, err != nil, true)
}

func TestPlan_BootstrapRootClustersLatePreparation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	cfg := testConfig(t, Config{
		Path: root, SelfURLs: []string{"https://fixture.invalid/repo"},
		Input: &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"producer/kustomization.yaml": {
				Data: []byte(
					"resources: [settings.yaml, instance.yaml]\nreplacements:\n- source: {kind: ConfigMap, name: settings, fieldPath: data.target}\n  targets:\n  - select: {kind: FluxInstance, name: late}\n    fieldPaths: [spec.sync.path]\n",
				),
			},
			"producer/settings.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {target: './'}\n",
				),
			},
			"producer/instance.yaml": {
				Data: []byte(
					"apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: late, namespace: apps}\nspec:\n  sync: {kind: GitRepository, name: cluster, url: 'https://fixture.invalid/repo', ref: 'refs/heads/main', path: './not-the-entry'}\n",
				),
			},
		}},
	})
	entries, err := Plan(t.Context(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	assert.Equal(t, entries[0].ID, manifest.ClusterID("."))
	assert.Equal(t, entries[0].aggregate, true)
	cfg.Input, cfg.Entry, cfg.Path, cfg.Store = entries[0].Input, &entries[0], entries[0].Path, store.New()
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	defer result.Close()
	assert.Equal(
		t,
		manifest.DiagnosticFor(result.Binding.Producer, cfg.Store.GetObject(result.Binding.Producer)).Resource,
		manifest.NamedResource{
			Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "late",
		},
	)
	assert.Equal(t, result.Binding.Path, "")
}

func TestPlan_BootstrapRootPatchErrors(t *testing.T) {
	t.Parallel()
	const replicaPatch = "op: replace\n      path: /spec/replicas\n      value: 2"
	for _, tc := range []struct {
		name, transforms, patch, volumes, cause string
	}{
		{name: "valid replica patch", patch: replicaPatch},
		{
			name: "invalid output sort order", transforms: "sortOptions: {order: unknown}\n",
			patch: replicaPatch, cause: "the field 'sortOptions.order' must be one of [fifo, legacy]",
		},
		{
			name: "name reference on authored volumes", patch: replicaPatch, volumes: "4",
			cause: "spec/template/spec/volumes/configMap/name",
		},
		{name: "incorrect path key", patch: "op: replace\n      Path: /spec/replicas\n      value: 2", cause: "missing path"},
		{name: "failed patch test", patch: "op: test\n      path: /spec/replicas\n      value: 9", cause: "test failed"},
		{
			name: "common labels on patched template", transforms: "commonLabels: {app: fixture}\n",
			patch: "op: replace\n      path: /spec/template\n      value: broken", cause: "expected sequence or mapping node",
		},
		{
			name: "labels on patched selector", transforms: "labels: [{pairs: {app: fixture}, includeSelectors: true}]\n",
			patch: "op: replace\n      path: /spec/selector\n      value: 4", cause: "expected sequence or mapping node",
		},
		{
			name:  "name reference on patched volumes",
			patch: "op: replace\n      path: /spec/template/spec/volumes\n      value: 4",
			cause: "spec/template/spec/volumes/configMap/name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := bootstrapRootFiles("disjoint identities")
			files["first/kustomization.yaml"] = &fstest.MapFile{Data: []byte(
				"resources: [root.yaml, deployment.yaml]\n" + tc.transforms + "patches:\n- target: {kind: Deployment}\n  patch: |-\n    - " +
					tc.patch + "\n",
			)}
			volumes := cmp.Or(tc.volumes, "[{name: settings, configMap: {name: settings}}]")
			files["first/deployment.yaml"] = &fstest.MapFile{Data: []byte(
				`apiVersion: apps/v1
kind: Deployment
metadata: {name: app, namespace: apps}
spec:
  replicas: 1
  selector:
    matchLabels: {app: fixture}
  template:
    metadata:
      labels: {app: fixture}
    spec:
      containers: [{name: app, image: app}]
      volumes: ` + volumes + "\n",
			)}
			entries, err := Plan(t.Context(), testConfig(t, Config{
				Input: &store.SourceArtifact{LocalPath: root, FS: files}, Path: root,
			}))
			if tc.cause == "" {
				assert.NoError(t, err)
				assert.Equal(t, len(entries), 2)
				return
			}
			assert.FatalIf(
				t, len(entries) != 0 || err == nil || !strings.Contains(err.Error(), tc.cause),
				"entries = %d, plan error = %v", len(entries), err,
			)
		})
	}
}

func bootstrapRootFiles(style string) fstest.MapFS {
	files := fstest.MapFS{}
	for _, side := range []string{"first", "second"} {
		files[side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [root.yaml, cm.yaml]\n")}
		files[side+"/cm.yaml"] = &fstest.MapFile{
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + side + "}\n"),
		}
		name := "same"
		if style == "disjoint identities" {
			name = side
		}
		document := "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: " + name + ", namespace: apps}\nspec: {path: ./" + side + "}\n"
		if style == "child applications" {
			delete(files, side+"/kustomization.yaml")
			document = "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: " + side + ", namespace: apps}\nspec: {path: ./" + side + "/app, sourceRef: {kind: GitRepository, name: cluster}}\n"
			files[side+"/app/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
			files[side+"/app/cm.yaml"] = &fstest.MapFile{
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + side + "-child}\n"),
			}
		}
		if style == "same-named instances" {
			document = "apiVersion: fluxcd.controlplane.io/v1\nkind: FluxInstance\nmetadata: {name: same, namespace: apps}\nspec:\n  sync: {kind: GitRepository, name: cluster, url: 'https://fixture.invalid/repo', ref: 'refs/heads/main', path: './" + side + "'}\n"
		}
		if style == "gotk-sync" {
			document = "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: 'https://fixture.invalid/repo'}\n---\napiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: same, namespace: apps}\nspec: {path: ./" + side + ", sourceRef: {kind: GitRepository, name: cluster}}\n"
		}
		if style == "rootless" {
			document = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + side + "-root}\n"
		}
		files[side+"/root.yaml"] = &fstest.MapFile{Data: []byte(document)}
	}
	return files
}
