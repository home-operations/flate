package change

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func optionalSupplierPair(tb testing.TB, namespace string) (*Set, [2]*store.Store, [2]string, *manifest.GitRepository) {
	tb.Helper()
	var stores [2]*store.Store
	var roots [2]string
	source := &manifest.GitRepository{Name: "supplier", Namespace: "infra"}
	for side := range stores {
		root := tb.TempDir()
		roots[side] = root
		st := store.New()
		stores[side] = st
		st.AddObject(source)
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{
			"flux/consumer.yaml":      {Data: []byte("unchanged consumer")},
			"apps/kustomization.yaml": {Data: []byte("resources: [output.yaml]\n")},
			"apps/output.yaml":        {Data: []byte("unchanged output")},
		}})
		files := fstest.MapFS{"inputs/kustomization.yaml": {Data: []byte("resources: []\n")}}
		if side == 0 {
			files["inputs/kustomization.yaml"].Data = []byte("resources: [optional.yaml]\n")
			files["inputs/optional.yaml"] = &fstest.MapFile{Data: fmt.Appendf(nil, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: %q}\ndata: {VALUE: available}\n", namespace)}
		}
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, ".supplier"), LocalRoot: root, Key: fmt.Sprint(side), FS: files})
	}
	changes, err := Qualify(tb.Context(), nil, stores[0], stores[1])
	if err != nil {
		tb.Fatal(err)
	}
	return changes, stores, roots, source
}

func TestFilter_CrossSourceOptionalInputPairedSides(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"exact_removed", "deferred_consumer", "wrong_namespace", "unrelated_blank_namespace", "empty_supplier_collision", "artifact_replaced", "declaration_replaced", "stale_bootstrap_alternative"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			namespace := "apps"
			if mode == "wrong_namespace" {
				namespace = "other"
			}
			if mode == "unrelated_blank_namespace" {
				namespace = ""
			}
			changes, stores, roots, source := optionalSupplierPair(t, namespace)
			input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: namespace, Name: "optional"}
			assert.Equal(t, changes.sources[source.Named()].changes.inputs[input], "inputs/optional.yaml")
			assert.Equal(t, len(changes.inputs), 0)
			assert.Equal(t, changes.Len(), 0)
			assert.Diff(t, changes.sources[source.Named()].changes.Paths(), []string{"inputs/kustomization.yaml", "inputs/optional.yaml"})
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "optional", Optional: true}}}}
			sibling := consumer.Clone()
			sibling.Name = "sibling"
			sibling.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "unrelated", Optional: true}}}
			if mode == "empty_supplier_collision" {
				evidence := changes.sources[source.Named()]
				evidence.changes = NewSet(nil)
				changes.sources[source.Named()] = evidence
				changes.inputs = map[manifest.NamedResource]string{input: "inputs/optional.yaml"}
			}
			if mode == "stale_bootstrap_alternative" {
				bootstrap := changes.sources[source.Named()]
				bootstrap.declarations = [2]manifest.BaseManifest{}
				changes.sources[manifest.BootstrapSourceID] = bootstrap
			}
			changes.groupSources()
			for side, st := range stores {
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					consumer.Named(): {File: "flux/consumer.yaml", Refs: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "optional"}}},
					sibling.Named():  {File: "flux/sibling.yaml"},
				}}
				if mode != "deferred_consumer" {
					st.AddObject(consumer)
				}
				st.AddObject(sibling)
				if mode == "artifact_replaced" {
					art := *st.GetArtifact(source.Named()).(*store.SourceArtifact)
					art.Revision = "replacement"
					st.SetArtifact(source.Named(), &art)
				}
				if mode == "declaration_replaced" {
					replacement := *source
					replacement.URL = "https://example.test/replaced"
					st.AddObject(&replacement)
				}
				filter := NewFilter(changes, index, st, roots[side])
				want := mode == "exact_removed" || mode == "deferred_consumer" || mode == "stale_bootstrap_alternative"
				if got := filter.ShouldReconcile(consumer.Named()); got != want {
					t.Errorf("side %d consumer selected=%t, want %t: %v", side, got, want, filter.KeepNames())
				}
				assert.Equal(t, filter.ShouldReconcile(sibling.Named()), false)
			}
		})
	}
}

func TestFilter_DeferredSourceValuesPairedSides(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"deferred", "present", "unchanged_collision"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			var stores [2]*store.Store
			var indexes [2]*manifest.DiscoveryIndex
			var roots [2]string
			source := &manifest.GitRepository{Name: "values", Namespace: "apps"}
			parent := testutil.KustomizationID("flux-system", "parent")
			release := testutil.HelmReleaseID("apps", "demo")
			sibling := testutil.HelmReleaseID("apps", "sibling")
			for side := range stores {
				root := t.TempDir()
				roots[side] = root
				testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "flux-system", "  path: ./apps\n  targetNamespace: apps\n"))
				testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [hr.yaml, sibling.yaml]\n")
				testutil.WriteFile(t, root, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: values}
      valuesFiles: [config/prod.yaml]
`)
				testutil.WriteFile(t, root, "apps/sibling.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: sibling, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: unrelated}}\n")
				st := store.New()
				stores[side] = st
				st.AddObject(source)
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
				value := side
				if state == "unchanged_collision" {
					value = 0
				}
				st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, ".selected"), LocalRoot: root, Key: fmt.Sprint(side), FS: fstest.MapFS{"config/prod.yaml": {Data: fmt.Appendf(nil, "value: %d\n", value)}}})
				l := loader.New(st)
				l.SourceRoot = root
				_, err := l.Load(t.Context(), root)
				assert.NoError(t, err)
				assert.NoError(t, l.FinalizeIndex(t.Context(), root))
				indexes[side] = l.Index
				r, ok := l.Index.Lookup(release)
				if !ok || r.Parent != parent || !slices.ContainsFunc(r.Productions, func(p manifest.Production) bool { return p.Owner == parent }) || st.GetObject(release) != nil {
					t.Fatalf("SETUP side %d: release must be indexed with its contributing parent and absent from Store: %+v", side, r)
				}
				if state == "present" {
					st.AddObject(&manifest.HelmRelease{Name: release.Name, Namespace: release.Namespace, Chart: manifest.HelmChart{Source: source.Named()}, ChartValuesFiles: []string{"config/prod.yaml"}})
				}
			}
			bootstrap := NewSet(nil)
			if state == "unchanged_collision" {
				bootstrap = NewSet([]string{".selected/config/prod.yaml"})
			}
			changes, err := Qualify(t.Context(), bootstrap, stores[0], stores[1])
			assert.NoError(t, err)
			want := state != "unchanged_collision"
			assert.Equal(t, changes.sources[source.Named()].changes.Contains("config/prod.yaml"), want)
			assert.Equal(t, changes.Len(), bootstrap.Len())
			for side, st := range stores {
				filter := NewFilter(changes, indexes[side], st, roots[side])
				for _, id := range []manifest.NamedResource{release, parent} {
					if got := filter.ShouldReconcile(id); got != want {
						t.Errorf("SELECTION side %d: %s selected=%t, want %t", side, id, got, want)
					}
				}
				assert.Equal(t, filter.ShouldReconcile(sibling), false)
			}
		})
	}
}

func TestFilter_ReadSourceAlternatives(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"changed_first", "changed_last", "read_only_collision", "structural_collision"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			reader := &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: ".selected"}
			var stores [2]*store.Store
			var indexes [2]*manifest.DiscoveryIndex
			var roots [2]string
			for side := range stores {
				root := t.TempDir()
				roots[side] = root
				st := store.New()
				stores[side] = st
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)})
				var reads []manifest.FileReader
				for _, name := range []string{"unchanged", "changed"} {
					source := &manifest.GitRepository{Name: name, Namespace: "apps"}
					value := 0
					if name == "changed" && (mode == "changed_first" || mode == "changed_last") {
						value = side
					}
					st.AddObject(source)
					st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, ".selected"), LocalRoot: root, Key: fmt.Sprintf("%s-%d", name, side), FS: fstest.MapFS{"input.yaml": {Data: fmt.Appendf(nil, "value: %d\n", value)}}})
					reads = append(reads, manifest.FileReader{Reader: reader.Named(), Source: source.Named()})
				}
				if mode == "changed_first" {
					slices.Reverse(reads)
				}
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{reader.Named(): {File: "flux/reader.yaml"}}, Readers: map[string][]manifest.FileReader{".selected/input.yaml": reads}}
				indexes[side] = index
				owners, _ := index.OwnersOfFile(".selected/input.yaml")
				assert.Diff(t, owners, []manifest.NamedResource{reader.Named()})
				if mode == "structural_collision" {
					st.AddObject(reader)
					index.Claims = map[string][]manifest.NamedResource{".selected/": {reader.Named()}}
				}
			}
			bootstrap := NewSet(nil)
			if mode == "read_only_collision" || mode == "structural_collision" {
				bootstrap = NewSet([]string{".selected/input.yaml"})
			}
			changes, err := Qualify(t.Context(), bootstrap, stores[0], stores[1])
			assert.NoError(t, err)
			for side, st := range stores {
				filter := NewFilter(changes, indexes[side], st, roots[side])
				assert.Equal(t, filter.ShouldReconcile(reader.Named()), mode != "read_only_collision")
			}
		})
	}
}
