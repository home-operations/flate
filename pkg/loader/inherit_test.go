package loader

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

var writeFile = testutil.WriteFile

func TestApplyNamespaceInheritance_GraphProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		hrDir  string
		pinned bool
		files  map[string]string
		scans  []string
		want   []string
		wantKS []string
		alias  bool
	}{
		{name: "named_ready", files: map[string]string{
			"flux/kustomization.yaml": "resources: [ks.yaml, ../apps]\n",
			"flux/ks.yaml":            testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps, targetNamespace: media, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"apps/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"media"}},
		{name: "claim_only", files: map[string]string{
			"flux/ks.yaml": testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps}\n"),
		}, scans: []string{"flux", "apps"}, want: []string{""}},
		{name: "self_claim", files: map[string]string{
			"apps/kustomization.yaml": "resources: [ks.yaml, hr.yaml]\n",
			"apps/ks.yaml":            testutil.KustomizationYAML("owner", "", "  {path: ./apps, targetNamespace: media}\n"),
		}, scans: []string{"apps"}, want: []string{"media"}},
		{name: "pinned_namespace", pinned: true, files: map[string]string{
			"flux/ks.yaml":            testutil.KustomizationYAML("pinned", "flux-system", "  {path: ./apps, targetNamespace: apps, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"apps/kustomization.yaml": "namespace: dirty\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"apps"}},
		{name: "alias_namespace", alias: true, files: map[string]string{
			"flux/ks.yaml":            testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps, targetNamespace: apps, sourceRef: {kind: GitRepository, name: cluster}}\n"),
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
			"flux/ks.yaml":            testutil.KustomizationYAML("apps", "flux-system", "  {path: ./apps, targetNamespace: media}\n"),
			"apps/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"media"}},
		{name: "nested_claim_fallback", hrDir: "apps/nested", files: map[string]string{
			"flux/outer.yaml":                testutil.KustomizationYAML("outer", "flux-system", "  {path: ./apps}\n"),
			"flux/inner.yaml":                testutil.KustomizationYAML("inner", "flux-system", "  {path: ./apps/nested, targetNamespace: shifted}\n"),
			"apps/kustomization.yaml":        "resources: [nested]\n",
			"apps/nested/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "apps"}, want: []string{"shifted"}},
		{name: "shared_base", files: map[string]string{
			"flux/red.yaml":           testutil.KustomizationYAML("red", "flux-system", "  {path: ./red}\n"),
			"flux/blue.yaml":          testutil.KustomizationYAML("blue", "flux-system", "  {path: ./blue}\n"),
			"red/kustomization.yaml":  "namespace: red\nresources: [../base]\n",
			"blue/kustomization.yaml": "namespace: blue\nresources: [../base]\n",
			"base/kustomization.yaml": "namespace: inner\nresources: [hr.yaml]\n",
		}, scans: []string{"flux", "red", "blue"}, want: []string{"red", "blue"}},
		{name: "shared_fallbacks", files: map[string]string{
			"flux/red.yaml":           testutil.KustomizationYAML("red", "red", "  {path: ./base, targetNamespace: red}\n"),
			"flux/blue.yaml":          testutil.KustomizationYAML("blue", "blue", "  {path: ./base}\n"),
			"base/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"flux", "base"}, want: []string{"", "red"}},
		{name: "shared_transformer_contexts", files: map[string]string{
			"apps/kustomization.yaml":              "resources: [hr.yaml, ks.yaml]\ntransformers: [./transformers]\n",
			"apps/transformers/kustomization.yaml": "namespace: myns\nresources: [../../transformers]\n",
			"transformers/kustomization.yaml":      "resources: [transformer.yaml]\n",
			"transformers/transformer.yaml":        namespaceTransformer,
			"apps/ks.yaml":                         testutil.KustomizationYAML("myapp", "flux-system", "  {path: ./output, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"other/kustomization.yaml":             "namespace: other\nresources: [../apps/ks.yaml]\n",
		}, scans: []string{"apps", "other"}, want: []string{"myns"}, wantKS: []string{"myns", "other"}},
		{name: "mixed_transformer_contexts", files: map[string]string{
			"apps/kustomization.yaml":              "resources: [hr.yaml, ks.yaml]\ntransformers: [./transformers]\n",
			"apps/transformers/kustomization.yaml": "namespace: myns\nresources: [../../transformers]\n",
			"transformers/kustomization.yaml":      "resources: [transformer.yaml]\n",
			"transformers/transformer.yaml":        namespaceTransformer,
			"apps/ks.yaml":                         testutil.KustomizationYAML("myapp", "flux-system", "  {path: ./output, sourceRef: {kind: GitRepository, name: cluster}}\n"),
			"other/kustomization.yaml":             "resources: [../apps/ks.yaml]\n",
		}, scans: []string{"apps", "other"}, want: []string{"myns"}, wantKS: []string{"flux-system", "myns"}},
		{name: "self_claim_transformer_fallback", files: map[string]string{
			"apps/kustomization.yaml":              "resources: [hr.yaml, ks.yaml]\ntransformers: [./transformers]\n",
			"apps/transformers/kustomization.yaml": "namespace: myns\nresources: [../../transformers]\n",
			"transformers/kustomization.yaml":      "resources: [transformer.yaml]\n",
			"transformers/transformer.yaml":        namespaceTransformer,
			"apps/ks.yaml":                         testutil.KustomizationYAML("myapp", "flux-system", "  {path: ./apps, sourceRef: {kind: GitRepository, name: cluster}}\n"),
		}, scans: []string{"apps"}, want: []string{"myns"}, wantKS: []string{"myns"}},
		{name: "cross_tree", files: map[string]string{
			"apps/kustomization.yaml": "namespace: games\nresources: [ks.yaml]\n",
			"apps/ks.yaml":            testutil.KustomizationYAML("games", "", "  {path: ./base, dependsOn: [{name: prerequisite}]}\n"),
			"base/kustomization.yaml": "resources: [hr.yaml]\n",
		}, scans: []string{"apps", "base"}, want: []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			hrDoc := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app}
spec:
  chartRef: {kind: OCIRepository, name: chart}
  valuesFrom: [{kind: ConfigMap, name: values}, {kind: Secret, name: optional, optional: true}]
`
			testutil.WriteFile(t, root, hrDir+"/hr.yaml", hrDoc)
			l := loaderFixture(store.New(), nil)
			l.SourceRoot = root
			admitForTest(l.Store, root)
			if tc.alias {
				location, err := gitrepo.Locate(t.Context(), root, root)
				if err != nil {
					t.Fatal(err)
				}
				input, err := gittree.AdmitDirectory(t.Context(), location)
				assert.NoError(t, err)
				l.Store.SetArtifact(manifest.BootstrapSourceID, input)
				view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: input.FS})
				assert.NoError(t, err)
				id := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}
				l.Store.AddObject(&manifest.GitRepository{Name: id.Name, Namespace: id.Namespace, URL: "file://" + root})
				l.Store.SetArtifact(id, &store.SourceArtifact{Kind: id.Kind, LocalRoot: root, LocalPath: filepath.Join(filepath.Dir(root), ".flate-sources", "alias", id.Namespace, id.Name), InputAlias: true, FS: view})
			}
			if tc.name == "self_claim_transformer_fallback" {
				for _, namespace := range []string{"flux-system", "myns"} {
					source := &manifest.GitRepository{Name: "cluster", Namespace: namespace}
					l.Store.AddObject(source)
					l.Store.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
				}
			}
			if tc.pinned {
				artifact := t.TempDir()
				testutil.WriteFile(t, artifact, "apps/kustomization.yaml", "resources: [hr.yaml]\n")
				testutil.WriteFile(t, artifact, "apps/hr.yaml", hrDoc)
				l.Store.SetArtifact(manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "cluster"}, &store.SourceArtifact{LocalRoot: root, Key: "pinned", FS: os.DirFS(artifact), LocalPath: artifact})
				l.documents = readerForTest(root)
				l.documents.views[artifact] = os.DirFS(artifact)
				l.documents.read(filepath.Join(artifact, "apps"))
			}
			for _, scan := range tc.scans {
				if _, err := l.Load(t.Context(), filepath.Join(root, scan)); err != nil {
					t.Fatal(err)
				}
				if tc.name == "named_ready" && scan == "flux" {
					l.ApplyNamespaces(root)
					if _, ok := l.Index.Lookup(testutil.HelmReleaseID("", "app")); !ok {
						t.Fatalf("unresolved source records before loading its path: %+v", l.Index.Records)
					}
					source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
					l.Store.AddObject(source)
					l.Store.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
				}
			}
			if tc.pinned || tc.alias {
				ks := l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)[0]
				artifact := l.Store.GetArtifact(ks.SourceID()).(*store.SourceArtifact)
				if _, err := l.Load(t.Context(), filepath.Join(artifact.LocalPath, "apps")); err != nil {
					t.Fatal(err)
				}
			}
			var original, snapshot *manifest.Kustomization
			var raw manifest.DiscoveryRecord
			if len(tc.wantKS) != 0 {
				id := testutil.KustomizationID("flux-system", "myapp")
				original, _ = l.Store.Get[*manifest.Kustomization](id)
				if original == nil {
					t.Fatal("authored Kustomization missing")
				}
				snapshot = original.Clone()
				raw, _ = l.Index.Lookup(id)
			}
			l.ApplyNamespaces(root)
			if original != nil {
				if diff := cmp.Diff(original, snapshot, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
					t.Fatalf("authored object mutated (-want +got): %s", diff)
				}
				if diff := cmp.Diff(l.authoredObject(raw.Path, raw.Authored), snapshot, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
					t.Fatalf("authored object mutated (-want +got): %s", diff)
				}
				assert.Diff(t, l.recordNamespaces(raw, "myns"), tc.wantKS)
				got, want := map[manifest.NamedResource][]manifest.NamedResource{}, map[manifest.NamedResource][]manifest.NamedResource{}
				for id, r := range l.Index.Files() {
					if r.Path == raw.Path {
						assert.Equal(t, r.Authored, raw.Authored)
						got[id] = r.Refs
					}
				}
				for _, ns := range tc.wantKS {
					id := testutil.KustomizationID(ns, "myapp")
					want[id] = []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: ns, Name: "cluster"}}
				}
				assert.Diff(t, got, want)
			}
			if tc.name == "self_claim" {
				id := testutil.KustomizationID("", "owner")
				if _, ok := l.Index.Lookup(id); !ok || l.Store.GetObject(id) == nil {
					t.Fatal("Kustomization inherited its own targetNamespace as its identity")
				}
				l.FinalizeIndex(root)
				id.Namespace = "media"
				if _, ok := l.Index.Lookup(id); ok {
					t.Fatal("own coverage invented a second Kustomization identity")
				}
			}
			if tc.pinned {
				l.FinalizeIndex(root)
				r, _ := l.Index.Lookup(testutil.HelmReleaseID("apps", "app"))
				readers := l.Index.Readers[r.File]
				if len(readers) != 1 || readers[0] != (testutil.KustomizationID("flux-system", "pinned")) {
					t.Fatalf("committed source file %q readers = %v", r.File, readers)
				}
			}
			if tc.name == "cross_tree" {
				ks, ok := l.Store.Get[*manifest.Kustomization](testutil.KustomizationID("games", "games"))
				if !ok {
					t.Fatal("projected Kustomization missing")
				}
				var deps []manifest.NamedResource
				for ref := range manifest.References(ks) {
					if ref.Use == "dependsOn" {
						deps = append(deps, ref.NamedResource)
					}
				}
				if len(deps) != 1 || deps[0].Namespace != "games" {
					t.Fatalf("projected dependsOn namespace: %+v", deps)
				}
			}
			for _, ns := range tc.want {
				id := testutil.HelmReleaseID(ns, "app")
				r, ok := l.Index.Lookup(id)
				if !ok || len(r.Refs) != 3 {
					t.Fatalf("%s record=%+v present=%t", id, r, ok)
				}
				for _, ref := range r.Refs {
					if ref.Namespace != ns {
						t.Fatalf("%s ref=%s", id, ref)
					}
				}
				if l.Store.GetObject(id) != nil {
					t.Fatalf("%s admitted before promotion", id)
				}

			}
			if _, ok := l.Index.Lookup(testutil.HelmReleaseID("", "app")); ok && !slices.Contains(tc.want, "") {
				t.Fatal("stale raw consumer record")
			}
			if _, ok := l.Index.Lookup(testutil.HelmReleaseID("flux-system", "app")); ok {
				t.Fatal("ancestor fallback retained a stale consumer identity")
			}
			if _, ok := l.Index.Lookup(testutil.HelmReleaseID("inner", "app")); ok {
				t.Fatal("inner base namespace must not override its overlay")
			}
		})
	}

	t.Run("subtree_availability", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("parent", "flux-system", "  {path: ./apps, targetNamespace: media, sourceRef: {kind: OCIRepository, name: source}}\n"))
		testutil.WriteFile(t, root, "kubernetes/apps/kustomization.yaml", "resources: [child.yaml, explicit.yaml]\n")
		for _, name := range []string{"child", "explicit"} {
			namespace := ""
			if name == "explicit" {
				namespace = "control"
			}
			testutil.WriteFile(t, root, "kubernetes/apps/"+name+".yaml", testutil.KustomizationYAML(name, namespace, "  {path: ./output, sourceRef: {kind: OCIRepository, name: source, namespace: flux-system}}\n"))
		}
		st := store.New()
		l := loaderFixture(st, nil)
		l.SourceRoot = root
		admitForTest(st, root)
		for _, dir := range []string{"flux", "kubernetes/apps"} {
			if _, err := l.Load(t.Context(), filepath.Join(root, dir)); err != nil {
				t.Fatal(err)
			}
		}
		owner, _ := st.Get[*manifest.Kustomization](testutil.KustomizationID("flux-system", "parent"))
		if owner == nil {
			t.Fatal("parent missing")
		}
		l.ApplyNamespaces(root)
		if _, _, external := SourceView(st, owner.SourceID(), root); !external {
			t.Fatal("unavailable source borrowed bootstrap coordinates")
		}
		for _, id := range []manifest.NamedResource{
			testutil.KustomizationID("", "child"),
			testutil.KustomizationID("control", "explicit"),
		} {
			if _, ok := l.Index.Lookup(id); !ok {
				t.Fatalf("unavailable source changed raw identity %s", id)
			}
		}
		input := st.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
		view, err := fs.Sub(input.FS, "kubernetes")
		assert.NoError(t, err)
		source, err := owner.SourceID().DecodeIdentity()
		assert.NoError(t, err)
		st.AddObject(source)
		mount := filepath.Join(filepath.Dir(root), ".flate-sources", "subtree", "flux-system", "source")
		st.SetArtifact(source.Named(), &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalRoot: filepath.Join(root, "kubernetes"), LocalPath: mount, InputAlias: true, FS: view})
		if got, _, external := SourceView(st, owner.SourceID(), root); external || got != mount {
			t.Fatalf("available source = %s, external=%t", got, external)
		}
		if _, err := l.Load(t.Context(), filepath.Join(mount, "apps")); err != nil {
			t.Fatal(err)
		}
		raw := map[string]manifest.DiscoveryRecord{}
		for _, name := range []string{"child", "explicit"} {
			id := testutil.KustomizationID("", name)
			if name == "explicit" {
				id.Namespace = "control"
			}
			record, ok := l.Index.Lookup(id)
			file, err := filepath.Rel(root, filepath.Join(mount, "apps", name+".yaml"))
			assert.NoError(t, err)
			if !ok || record.File != filepath.ToSlash(file) || record.Authored != id {
				t.Fatalf("raw %s record = %+v", id, record)
			}
			raw[name] = record
		}
		l.ApplyNamespaces(root)
		for _, name := range []string{"child", "explicit"} {
			id := testutil.KustomizationID("media", name)
			record, ok := l.Index.Lookup(id)
			if !ok || st.GetObject(id) == nil || record.File != raw[name].File || record.Path != filepath.Join(mount, "apps", name+".yaml") || record.Path != raw[name].Path {
				t.Fatalf("projected %s record=%+v, raw=%+v", id, record, raw[name])
			}
		}
		l.FinalizeIndex(root)
		for _, name := range []string{"child", "explicit"} {
			id := testutil.KustomizationID("media", name)
			record, ok := l.Index.Lookup(id)
			if !ok || record.Parent != owner.Named() || record.File != "kubernetes/apps/"+name+".yaml" || record.Path != raw[name].Path {
				t.Fatalf("finalized %s record=%+v", id, record)
			}
			if readers := slices.Collect(l.Index.ReadersOfFile(record.File)); !slices.Contains(readers, owner.Named()) {
				t.Fatalf("subtree readers = %v", readers)
			}
		}
	})

	t.Run("stale_declaration", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeFile(t, root, "apps/kustomization.yaml", "namespace: immich\nresources: [ks.yaml]\n")
		writeFile(t, root, "apps/ks.yaml", testutil.KustomizationYAML("immich-db", "pg", "  {path: ./output, targetNamespace: pg}\n"))
		l := loaderFixture(store.New(), nil)
		l.SourceRoot = root
		if _, err := l.Load(t.Context(), filepath.Join(root, "apps")); err != nil {
			t.Fatal(err)
		}
		l.ApplyNamespaces(root)
		l.FinalizeIndex(root)
		canonical := testutil.KustomizationID("immich", "immich-db")
		ks, ok := l.Store.Get[*manifest.Kustomization](canonical)
		if !ok || ks.TargetNamespace != "pg" {
			t.Fatalf("canonical declaration = %+v, want targetNamespace pg", ks)
		}
		if _, ok := l.Index.Lookup(canonical); !ok {
			t.Fatal("canonical declaration missing from finalized index")
		}
		stale := testutil.KustomizationID("pg", "immich-db")
		_, indexed := l.Index.Lookup(stale)
		if l.Store.GetObject(stale) != nil || indexed {
			t.Fatal("stale pg/immich-db declaration survived projection")
		}
	})
}

func TestApplyNamespaceInheritance_SourceKinds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, spec string
	}{
		{manifest.KindBucket, "{bucketName: fixtures, endpoint: minio.invalid}"},
		{manifest.KindGitRepository, "{url: https://example.invalid/repo.git}"},
		{manifest.KindHelmChart, "{chart: demo, sourceRef: {kind: HelmRepository, name: repo}}"},
		{manifest.KindExternalArtifact, "{}"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "namespace: apps\nresources: [source.yaml]\n")
			testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: "+tc.kind+"\nmetadata: {name: source}\nspec: "+tc.spec+"\n")
			l := loaderFixture(store.New(), nil)
			l.SourceRoot = root
			if _, err := l.Load(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			raw := manifest.NamedResource{Group: manifest.SourceDomain, Kind: tc.kind, Name: "source"}
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

func TestApplyNamespaceInheritance_LateFallback(t *testing.T) {
	t.Parallel()
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("owned_%t", owned), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/objects.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: apps}
spec: {}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: inputs}
spec: {type: Static}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: chart}
spec: {chart: app, sourceRef: {kind: HelmRepository, name: repo}}
`)
			l := loaderFixture(store.New(), nil)
			l.SourceRoot = root
			if owned {
				l.Store.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "owner", Path: "./apps"})
			}
			if _, err := l.Load(t.Context(), filepath.Join(root, "apps")); err != nil {
				t.Fatal(err)
			}
			l.ApplyNamespaces(root)
			for _, obj := range l.Store.ListObjects("", "") {
				if obj.Named().Kind == manifest.KindKustomization {
					continue
				}
				want := ""
				if obj.Named().Namespace != want {
					t.Errorf("%s namespace = %q, want %q", obj.Named().Kind, obj.Named().Namespace, want)
				}
			}
		})
	}
}

func TestRecordNamespaces_GeneratedCoverage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		role     kustomizeinput.Role
		deferred bool
		want     string
	}{
		{"resource", kustomizeinput.Resource, false, "inner"},
		{"implicit resource", kustomizeinput.Resource, true, "authored"},
		{"configuration", kustomizeinput.Configuration, false, "authored"},
		{"terminal", kustomizeinput.File, false, "authored"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := loaderFixture(store.New(), nil)
			l.ensureScopes()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "flux-system"}
			l.Store.AddObject(owner)
			l.scopes.visits["child.yaml"] = []graphVisit{{
				namespace: "inner", deferMaterialization: tc.deferred, role: tc.role, owner: owner.Named(),
			}}
			r := manifest.DiscoveryRecord{Path: "child.yaml", Authored: testutil.HelmReleaseID("authored", "child")}
			assert.Diff(t, l.recordNamespaces(r, ""), []string{tc.want})
		})
	}
}
