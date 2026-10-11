package loader

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var writeFile = testutil.WriteFile

func TestApplyNamespaceInheritance_GraphProjection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		scans []string
		want  []string
	}{
		{name: "standalone", files: map[string]string{
			"apps/kustomization.yaml": "namespace: media\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"media"}},
		{name: "outer_overlay", files: map[string]string{
			"apps/kustomization.yaml": "namespace: outer\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"outer"}},
		{name: "flux_target", files: map[string]string{
			"flux/ks.yaml":            "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps, namespace: flux-system}\nspec: {path: ./apps, targetNamespace: media}\n",
			"apps/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"media"}},
		{name: "shared_base", files: map[string]string{
			"flux/red.yaml":           "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: red, namespace: flux-system}\nspec: {path: ./red}\n",
			"flux/blue.yaml":          "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: blue, namespace: flux-system}\nspec: {path: ./blue}\n",
			"red/kustomization.yaml":  "namespace: red\nresources: [../base]\n",
			"blue/kustomization.yaml": "namespace: blue\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "red", "blue"}, want: []string{"red", "blue"}},
		{name: "cross_tree", files: map[string]string{
			"apps/kustomization.yaml": "namespace: games\nresources: [ks.yaml]\n",
			"apps/ks.yaml":            "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: games}\nspec: {path: ./base}\n",
			"base/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"apps", "base"}, want: []string{"games"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, deferred := range []bool{false, true} {
				root := t.TempDir()
				for file, body := range tc.files {
					testutil.WriteFile(t, root, file, body)
				}
				hrDir := "apps"
				if _, ok := tc.files["base/kustomization.yaml"]; ok {
					hrDir = "base"
				}
				testutil.WriteFile(t, root, hrDir+"/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app}
spec:
  chartRef: {kind: OCIRepository, name: chart}
  valuesFrom: [{kind: ConfigMap, name: values}, {kind: Secret, name: optional, optional: true}]
`)
				l := loaderFixture(store.New(), nil)
				l.SourceRoot, l.Options.DiscoveryOnly = root, deferred
				for _, scan := range tc.scans {
					if _, err := l.Load(t.Context(), filepath.Join(root, scan)); err != nil {
						t.Fatal(err)
					}
				}
				l.ApplyNamespaces(root)
				for _, ns := range tc.want {
					id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: ns, Name: "app"}
					r, ok := l.Index.Lookup(id)
					if !ok || len(r.Refs) != 3 {
						t.Fatalf("deferred=%t: %s record=%+v present=%t", deferred, id, r, ok)
					}
					for _, ref := range r.Refs {
						if ref.Namespace != ns {
							t.Fatalf("%s ref=%s", id, ref)
						}
					}
					if !deferred && l.Store.GetObject(id) == nil {
						t.Fatalf("missing projected %s", id)
					}
				}
				if _, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "app"}); ok {
					t.Fatal("stale raw consumer record")
				}
				if _, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "inner", Name: "app"}); ok {
					t.Fatal("inner base namespace must not override its overlay")
				}
			}
		})
	}
}

func TestApplyDefaultNamespaces_LateFallback(t *testing.T) {
	s := store.New()
	rs := &manifest.ResourceSet{Name: "apps"}
	rsip := &manifest.ResourceSetInputProvider{Name: "apps-inputs"}
	hc := &manifest.HelmChartSource{Name: "chart"}
	s.AddObject(rs)
	s.AddObject(rsip)
	s.AddObject(hc)
	sourceFiles := map[manifest.NamedResource]string{
		rs.Named():   "resourceset.yaml",
		rsip.Named(): "rsip.yaml",
		hc.Named():   "chart.yaml",
	}

	defaultNamespacesForTest(s, sourceFiles)

	for _, id := range []manifest.NamedResource{
		{Kind: manifest.KindResourceSet, Namespace: manifest.DefaultNamespace, Name: "apps"},
		{Kind: manifest.KindResourceSetInputProvider, Namespace: manifest.DefaultNamespace, Name: "apps-inputs"},
		{Kind: manifest.KindHelmChart, Namespace: manifest.DefaultNamespace, Name: "chart"},
	} {
		if got := s.GetObject(id); got == nil {
			t.Fatalf("expected %s after defaulting; sourceFiles=%v", id, sourceFiles)
		}
		if _, ok := sourceFiles[id]; !ok {
			t.Fatalf("sourceFiles missing defaulted id %s; sourceFiles=%v", id, sourceFiles)
		}
	}
}
