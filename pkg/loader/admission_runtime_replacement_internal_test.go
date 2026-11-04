package loader

import (
	"slices"
	"sync"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAddEmitted_AlreadySelectedSourceRetained(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	child := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "apps",
				Name:      "new-chart",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "new-chart",
			Namespace: "apps",
		},
	}
	old := *child.Clone()
	old.ChartRef.Name = "old-chart"
	old.Chart = manifest.HelmChart{}
	oldSource := old.ChartSource()
	objects := filtertest.MapLister{old.Named(): &old}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"parent.yaml", "old-chart.yaml"}),
			index: new(filterFixture{files: map[manifest.NamedResource]string{
				parent: "parent.yaml", oldSource: "old-chart.yaml",
			}, objects: objects}).index(),
			inventory: objects,
			root:      "",
		},
	)
	assert.Equal(t, f.ShouldReconcile(oldSource), true)
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), false)

	added := f.AddEmitted(parent, store.Child{Obj: child}, f.objs)

	assert.Equal(t, len(added), 1)
	for _, id := range []manifest.NamedResource{child.Named(), child.ChartSource()} {
		assert.Equal(t, f.ShouldReconcile(id), true)
		assert.Equal(t, slices.Contains(added, id), id == child.ChartSource())
	}
	assert.Equal(t, f.ShouldReconcile(oldSource), true)
	assert.Equal(t, slices.Contains(added, oldSource), false)
}

func TestAddEmitted_ConcurrentReplacementReturnsEachIDOnce(t *testing.T) {
	t.Parallel()
	parent := testutil.KustomizationID("apps", "parent")
	child := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindOCIRepository,
				Namespace: "apps",
				Name:      "new-chart",
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindOCIRepository,
			Name:      "new-chart",
			Namespace: "apps",
		},
	}
	old := *child.Clone()
	old.ChartRef.Name = "old-chart"
	old.Chart = manifest.HelmChart{}
	objects := filtertest.MapLister{old.Named(): &old}
	f := newAdmission(
		t,
		admissionConfig{
			changes: change.NewSet([]string{"parent.yaml"}),
			index: new(filterFixture{
				files:   map[manifest.NamedResource]string{parent: "parent.yaml"},
				objects: objects,
			}).index(),
			inventory: objects,
			root:      "",
		},
	)
	for _, id := range []manifest.NamedResource{child.Named(), child.ChartSource(), old.ChartSource()} {
		assert.Equal(t, f.ShouldReconcile(id), false)
	}
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	var results [2][]manifest.NamedResource
	ready.Add(len(results))
	for i := range results {
		joined.Go(func() {
			ready.Done()
			<-start
			results[i] = f.AddEmitted(parent, store.Child{Obj: child}, f.objs)
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()

	returned := make(map[manifest.NamedResource]int)
	for _, ids := range results {
		for _, id := range ids {
			returned[id]++
		}
	}
	assert.Diff(t, returned, map[manifest.NamedResource]int{child.Named(): 1, child.ChartSource(): 1})
	assert.Equal(t, f.ShouldReconcile(child.Named()), true)
	assert.Equal(t, f.ShouldReconcile(child.ChartSource()), true)
	assert.Equal(t, f.ShouldReconcile(old.ChartSource()), false)
}

// TestFilter_KeepByNameDoesNotLeakAcrossNamespaces pins the
// asymmetric-fallback contract: a fully-namespaced keep entry must
// NOT match a fully-namespaced lookup that happens to share
// (Kind, Name). Without this, a kept
// Kustomization.kustomize.toolkit.fluxcd.io/cluster-infra/external-secrets would silently scope-in
// an unrelated Kustomization.kustomize.toolkit.fluxcd.io/database/external-secrets (and likewise
// for the runtime AddEmitted / Add paths), broadening changed-only
// mode in ways the user can't see. The empty-namespace bridge in
// TestFilter_ShouldReconcileEmptyNamespaceFallback is preserved
// because it indexes only entries whose Namespace is empty.
func TestFilter_KeepByNameDoesNotLeakAcrossNamespaces(t *testing.T) {
	t.Parallel()
	before, after := t.TempDir(), t.TempDir()
	testutil.WriteFile(
		t,
		before,
		"changed.yaml",
		"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: same}\n",
	)
	testutil.WriteFile(
		t,
		after,
		"changed.yaml",
		"apiVersion: foreign.example/v1\nkind: Kustomization\nmetadata: {name: same}\n",
	)
	changes, err := change.Detect(t.Context(), testInput(before), testInput(after), manifest.WorkScope{})
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"changed.yaml"})
	for _, tc := range []struct {
		name, group, namespace, otherGroup, otherNamespace string
	}{
		{"namespace", manifest.FluxKustomizeDomain, "cluster-infra", manifest.FluxKustomizeDomain, "database"},
		{"real changed", manifest.FluxKustomizeDomain, "apps", "foreign.example", "apps"},
		{"foreign changed", "foreign.example", "apps", manifest.FluxKustomizeDomain, "apps"},
		{"real bare changed", manifest.FluxKustomizeDomain, "", "foreign.example", "apps"},
		{"foreign bare changed", "foreign.example", "", manifest.FluxKustomizeDomain, "apps"},
		{"core bare changed", "", "", manifest.FluxKustomizeDomain, "apps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kept := manifest.NamedResource{
				Group:     tc.group,
				Kind:      manifest.KindKustomization,
				Namespace: tc.namespace,
				Name:      "same",
			}
			collider := manifest.NamedResource{
				Group:     tc.otherGroup,
				Kind:      kept.Kind,
				Namespace: tc.otherNamespace,
				Name:      kept.Name,
			}
			files := map[manifest.NamedResource]string{kept: "changed.yaml", collider: "unchanged.yaml"}
			f := newAdmission(
				t,
				admissionConfig{
					changes:   changes,
					index:     new(filterFixture{files: files}).index(),
					inventory: filtertest.EmptyLister(),
					root:      "",
				},
			)
			assert.Equal(t, f.ShouldReconcile(kept), true)
			assert.Equal(t, f.ShouldReconcile(collider), false)
			projected := kept
			projected.Namespace = "effective"
			assert.Equal(t, f.ShouldReconcile(projected), false)
			child := kept
			child.Name, child.Namespace = "emitted", ""
			assert.Equal(t, len(f.AddEmitted(collider, store.Child{Obj: refObj(child)}, f.objs)), 0)
			assert.Equal(t, f.ShouldReconcile(child), false)
			f.AddEmitted(kept, store.Child{Obj: refObj(child)}, f.objs)
			assert.Equal(t, f.ShouldReconcile(child), true)
			child.Namespace = "effective"
			assert.Equal(t, f.ShouldReconcile(child), false)
			child.Group = tc.otherGroup
			child.Namespace = tc.otherNamespace
			if tc.group != tc.otherGroup {
				assert.Equal(t, f.ShouldReconcile(child), false)
			}
		})
	}
}
