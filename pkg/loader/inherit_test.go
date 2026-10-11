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
		name   string
		hrDir  string
		pinned bool
		files  map[string]string
		scans  []string
		want   []string
	}{
		{name: "pinned_namespace", pinned: true, files: map[string]string{
			"flux/ks.yaml":            "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: pinned, namespace: flux-system}\nspec: {path: ./apps, targetNamespace: apps, sourceRef: {kind: GitRepository, name: cluster}}\n",
			"apps/kustomization.yaml": "namespace: dirty\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"apps"}},
		{name: "standalone", files: map[string]string{
			"apps/kustomization.yaml": "namespace: media\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"media"}},
		{name: "outer_overlay", files: map[string]string{
			"apps/kustomization.yaml": "namespace: outer\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"outer"}},
		{name: "three_layer_overlay", hrDir: "leaf", files: map[string]string{
			"apps/kustomization.yaml": "namespace: outer\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: middle\nresources: [../leaf]\n",
			"leaf/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"apps"}, want: []string{"outer"}},
		{name: "flux_target", files: map[string]string{
			"flux/ks.yaml":            "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps, namespace: flux-system}\nspec: {path: ./apps, targetNamespace: media}\n",
			"apps/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"media"}},
		{name: "nested_claim_fallback", hrDir: "apps/nested", files: map[string]string{
			"flux/outer.yaml":                "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: outer, namespace: flux-system}\nspec: {path: ./apps}\n",
			"flux/inner.yaml":                "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: inner, namespace: flux-system}\nspec: {path: ./apps/nested, targetNamespace: shifted}\n",
			"apps/kustomization.yaml":        "resources: [nested]\n",
			"apps/nested/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"shifted"}},
		{name: "shared_base", files: map[string]string{
			"flux/red.yaml":           "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: red, namespace: flux-system}\nspec: {path: ./red}\n",
			"flux/blue.yaml":          "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: blue, namespace: flux-system}\nspec: {path: ./blue}\n",
			"red/kustomization.yaml":  "namespace: red\nresources: [../base]\n",
			"blue/kustomization.yaml": "namespace: blue\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "red", "blue"}, want: []string{"red", "blue"}},
		{name: "shared_fallbacks", files: map[string]string{
			"flux/red.yaml":           "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: red, namespace: red}\nspec: {path: ./base}\n",
			"flux/blue.yaml":          "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: blue, namespace: blue}\nspec: {path: ./base}\n",
			"base/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "base"}, want: []string{"red", "blue"}},
		{name: "cross_tree", files: map[string]string{
			"apps/kustomization.yaml": "namespace: games\nresources: [ks.yaml]\n",
			"apps/ks.yaml":            "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: games}\nspec: {path: ./base, dependsOn: [{name: prerequisite}]}\n",
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
				if tc.hrDir != "" {
					hrDir = tc.hrDir
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
				if tc.pinned {
					artifact := t.TempDir()
					testutil.WriteFile(t, artifact, "apps/kustomization.yaml", "resources: []\n")
					l.Store.SetArtifact(manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}, &store.SourceArtifact{LocalRoot: root, LocalPath: artifact})
					l.documents.read(filepath.Join(artifact, "apps"))
				}
				for _, scan := range tc.scans {
					if _, err := l.Load(t.Context(), filepath.Join(root, scan)); err != nil {
						t.Fatal(err)
					}
				}
				l.ApplyNamespaces(root)
				if tc.name == "cross_tree" {
					ks, ok := l.Store.Get[*manifest.Kustomization](manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "games", Name: "games"})
					if !ok || len(ks.DependsOn) != 1 || ks.DependsOn[0].Namespace != "games" {
						t.Fatalf("projected dependsOn namespace: %+v", ks)
					}
				}
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
					if !deferred {
						hr, ok := l.Store.Get[*manifest.HelmRelease](id)
						if !ok || hr.Chart.RepoNamespace != ns {
							t.Fatalf("projected %s chart source namespace: %+v", id, hr)
						}
					}
				}
				if _, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "app"}); ok {
					t.Fatal("stale raw consumer record")
				}
				if _, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "flux-system", Name: "app"}); ok {
					t.Fatal("ancestor fallback retained a stale consumer identity")
				}
				if _, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "inner", Name: "app"}); ok {
					t.Fatal("inner base namespace must not override its overlay")
				}
			}
		})
	}
}

func TestApplyNamespaceInheritance_SourceKinds(t *testing.T) {
	for _, tc := range []struct {
		kind, spec string
	}{
		{manifest.KindBucket, "{bucketName: fixtures, endpoint: minio.invalid}"},
		{manifest.KindGitRepository, "{url: https://example.invalid/repo.git}"},
		{manifest.KindHelmChart, "{chart: demo, sourceRef: {kind: HelmRepository, name: repo}}"},
		{manifest.KindExternalArtifact, "{}"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "namespace: apps\nresources: [source.yaml]\n")
			testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: "+tc.kind+"\nmetadata: {name: source}\nspec: "+tc.spec+"\n")
			l := loaderFixture(store.New(), nil)
			l.SourceRoot, l.Options.DiscoveryOnly = root, true
			if _, err := l.Load(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			raw := manifest.NamedResource{Kind: tc.kind, Name: "source"}
			original := l.Store.GetObject(raw)
			if original == nil {
				t.Fatal("source not loaded")
			}
			l.ApplyNamespaces(root)
			projected := raw
			projected.Namespace = "apps"
			if l.Store.GetObject(projected) == nil {
				t.Fatalf("projected source %s missing", projected)
			}
			if _, ok := l.Index.Lookup(projected); !ok {
				t.Fatalf("projected source %s not indexed", projected)
			}
			if original.Named() != raw || l.Store.GetObject(raw) != nil {
				t.Fatal("raw source must remain immutable and leave the store")
			}
			if _, ok := l.Index.Lookup(raw); ok {
				t.Fatal("stale raw source record")
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
