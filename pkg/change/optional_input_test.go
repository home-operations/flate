package change_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestOptionalInput_AppearanceRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"directory", "committed"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			writeOptionalInputCluster(t, root, manifest.KindConfigMap)
			repo := testutil.GitInit(t, root)
			options := &git.CommitOptions{Author: &object.Signature{
				Name: "test", Email: "test@example.test", When: time.Unix(0, 0),
			}}
			base := testutil.GitCommitAll(t, repo, "base", options)
			testutil.WriteFile(t, root, "flux/optional.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: optional, namespace: flux-system}
data: {VALUE: available}
`)
			var inputs *baseline.Result
			var err error
			if route == "directory" {
				before := t.TempDir()
				writeOptionalInputCluster(t, before, manifest.KindConfigMap)
				inputs, err = baseline.ResolvePaths(t.Context(), root, before)
			} else {
				testutil.GitCommitAll(t, repo, "optional input appears", options)
				cache := cachetest.New(t, t.TempDir(), sourcecache.New)
				inputs, err = baseline.AutoResolve(t.Context(), root, base.String(), cache)
			}
			assert.NoError(t, err)
			if _, err := fs.ReadFile(inputs.BaseInput.FS, "flux/optional.yaml"); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("baseline optional input: got %v, want not found", err)
			}
			changes, err := change.Detect(t.Context(), inputs.BaseInput, inputs.Source)
			assert.NoError(t, err)
			assert.Diff(t, changes.Paths(), []string{"flux/optional.yaml"})

			st := store.New()
			discovered, err := discovery.Run(t.Context(), discovery.Config{
				Input: inputs.Source, Path: inputs.ScanPath, Store: st,
				SelfURLs: []string{"https://example.test/cluster.git"},
			})
			assert.NoError(t, err)
			app := testutil.KustomizationID("flux-system", "app")
			cluster := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository,
				Namespace: "flux-system", Name: "cluster",
			}
			input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"}
			if st.GetObject(app) == nil || st.GetObject(cluster) == nil {
				t.Fatal("discovery did not admit app and cluster")
			}
			record, ok := discovered.Index.Lookup(input)
			if !ok || record.File != "flux/optional.yaml" {
				t.Fatalf("optional input discovery record: %+v, found=%t", record, ok)
			}
			owners, _ := discovered.Index.OwnersOfFile(record.File)
			if slices.Contains(owners, app) {
				t.Fatal("optional input is inside app's rendered ownership")
			}
			optional := false
			for ref := range manifest.References(st.GetObject(app)) {
				optional = optional || ref.NamedResource == input && ref.Use == "substituteFrom" && ref.Optional
			}
			if !optional {
				t.Fatal("app does not declare the optional substitution input")
			}
			empty, err := change.Detect(t.Context(), inputs.Source, inputs.Source)
			assert.NoError(t, err)
			unchanged := change.NewFilter(empty, discovered.Index, st)
			if unchanged.ShouldReconcile(app) || unchanged.ShouldReconcile(cluster) {
				t.Fatal("unchanged input selects app or cluster")
			}
			filter := change.NewFilter(changes, discovered.Index, st)
			if !filter.ShouldReconcile(input) {
				t.Fatal("changed optional input was not selected")
			}
			for _, id := range []manifest.NamedResource{app, cluster} {
				if !filter.ShouldReconcile(id) {
					t.Errorf("optional input appearance did not select %s", id)
				}
			}
		})
	}
}

func writeOptionalInputCluster(t *testing.T, root, kind string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  path: ./apps/app
  sourceRef: {kind: GitRepository, name: cluster}
  postBuild:
    substituteFrom: [{kind: `+kind+`, name: optional, optional: true}]
`)
	testutil.WriteFile(t, root, "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "apps/app/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered, namespace: flux-system}
data: {value: "${VALUE:-fallback}"}
`)
}

func TestFilter_OptionalInputChanges(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, beforeName, namespace, operation string
		want                                         bool
	}{
		{"configmap appears", "ConfigMap", "optional", "flux-system", "appear", true},
		{"secret appears", "Secret", "optional", "flux-system", "appear", true},
		{"configmap edits", "ConfigMap", "optional", "flux-system", "edit", true},
		{"secret edits", "Secret", "optional", "flux-system", "edit", true},
		{"configmap removes", "ConfigMap", "optional", "flux-system", "remove", true},
		{"secret removes", "Secret", "optional", "flux-system", "remove", true},
		{"unrelated removal", "ConfigMap", "unrelated", "flux-system", "remove", false},
		{"wrong namespace removal", "ConfigMap", "optional", "other", "remove", false},
		{"empty namespace removal", "ConfigMap", "optional", "", "remove", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, after := t.TempDir(), t.TempDir()
			writeOptionalInputCluster(t, before, tc.kind)
			writeOptionalInputCluster(t, after, tc.kind)
			body := fmt.Sprintf("apiVersion: v1\nkind: %s\nmetadata: {name: %s, namespace: %q}\ndata: {VALUE: b2xk}\n", tc.kind, tc.beforeName, tc.namespace)
			if tc.operation != "appear" {
				testutil.WriteFile(t, before, "flux/optional.yaml", body)
			}
			if tc.operation != "remove" {
				testutil.WriteFile(t, after, "flux/optional.yaml", body+"immutable: false\n")
			}
			inputs, err := baseline.ResolvePaths(t.Context(), after, before)
			assert.NoError(t, err)
			changes, err := change.Detect(t.Context(), inputs.BaseInput, inputs.Source)
			assert.NoError(t, err)
			f, index := optionalInputFilter(t, inputs.Source, changes)
			app := testutil.KustomizationID("flux-system", "app")
			cluster := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
			for _, id := range []manifest.NamedResource{app, cluster} {
				assert.Equal(t, f.ShouldReconcile(id), tc.want)
			}
			if tc.operation == "remove" {
				for _, name := range []string{"optional", "unrelated"} {
					_, present := index.Lookup(manifest.NamedResource{Kind: tc.kind, Namespace: tc.namespace, Name: name})
					assert.Equal(t, present, false)
				}
			}
			child := &manifest.Kustomization{Name: "emitted", Namespace: "flux-system"}
			f.AddEmitted(app, child)
			assert.Equal(t, f.ShouldReconcile(child.Named()), tc.want)
		})
	}
}

func TestOptionalInput_PairedSides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, inputName string
		projected       bool
	}{
		{"optional", "optional", false},
		{"unrelated", "unrelated", false},
		{"projected namespace", "optional", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, after := t.TempDir(), t.TempDir()
			inputPath, namespace := "flux/optional.yaml", "flux-system"
			for _, root := range []string{before, after} {
				writeOptionalInputCluster(t, root, manifest.KindConfigMap)
				testutil.WriteFile(t, root, "flux/other.yaml", testutil.KustomizationYAML("other", "other", "  path: ./apps/other\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n"))
				if tc.projected {
					testutil.WriteFile(t, root, "flux/inputs.yaml", testutil.KustomizationYAML("inputs", "flux-system", "  path: ./inputs\n  targetNamespace: flux-system\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
					inputPath, namespace = "inputs/optional.yaml", ""
				}
			}
			testutil.WriteFile(t, after, inputPath, fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s, namespace: %q}\ndata: {VALUE: after}\n", tc.inputName, namespace))
			inputs, err := baseline.ResolvePaths(t.Context(), after, before)
			assert.NoError(t, err)
			for _, pair := range [][2]*store.SourceArtifact{{inputs.BaseInput, inputs.Source}, {inputs.Source, inputs.BaseInput}} {
				changes, err := change.Detect(t.Context(), pair[0], pair[1])
				assert.NoError(t, err)
				for _, side := range pair {
					f, _ := optionalInputFilter(t, side, changes)
					assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "app")), tc.inputName == "optional")
					assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("other", "other")), tc.projected)
					assert.Equal(t, f.ShouldReconcile(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}), tc.inputName == "optional")
				}
			}
		})
	}
}

func TestFilter_OrderingOnlySiblingExcluded(t *testing.T) {
	t.Parallel()
	before, after := t.TempDir(), t.TempDir()
	for _, root := range []string{before, after} {
		writeOptionalInputCluster(t, root, manifest.KindConfigMap)
		body, err := fs.ReadFile(os.DirFS(root), "flux/cluster.yaml")
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "flux/cluster.yaml", string(body)+"  dependsOn: [{name: sibling}]\n")
		testutil.WriteFile(t, root, "flux/sibling.yaml", testutil.KustomizationYAML("sibling", "flux-system", "  path: ./apps/sibling\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
		testutil.WriteFile(t, root, "apps/sibling/kustomization.yaml", "resources: []\n")
	}
	testutil.WriteFile(t, after, "flux/sibling.yaml", testutil.KustomizationYAML("sibling", "flux-system", "  path: ./apps/sibling\n  prune: true\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
	inputs, err := baseline.ResolvePaths(t.Context(), after, before)
	assert.NoError(t, err)
	changes, err := change.Detect(t.Context(), inputs.BaseInput, inputs.Source)
	assert.NoError(t, err)
	f, _ := optionalInputFilter(t, inputs.Source, changes)
	assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "sibling")), true)
	assert.Equal(t, f.ShouldReconcile(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}), true)
	assert.Equal(t, f.ShouldReconcile(testutil.KustomizationID("flux-system", "app")), false)
}

func optionalInputFilter(t *testing.T, input *store.SourceArtifact, changes *change.Set) (*change.Filter, *manifest.DiscoveryIndex) {
	t.Helper()
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{Input: input, Store: st, SelfURLs: []string{"https://example.test/cluster.git"}})
	assert.NoError(t, err)
	return change.NewFilter(changes, result.Index, st), result.Index
}
