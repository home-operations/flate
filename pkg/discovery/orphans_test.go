package discovery

import (
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPromoteOrphans_IndexedEvidence(t *testing.T) {
	t.Parallel()
	const root = "/virtual/orphan-evidence"
	input := fstest.MapFS{
		"a.yaml":         {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: default}\ndata: {VALUE: X}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\ndata: {VALUE: A}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling, namespace: default}\ndata: {VALUE: S}\n")},
		"z.yaml":         {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\ndata: {VALUE: Z}\n")},
		"generator.yaml": {Data: []byte("apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: default, uid: authored, annotations: {source.extensions.fluxcd.io/reconcile: disabled}}\nspec: {sources: [{alias: repo, kind: GitRepository, name: input}]}\n")},
	}
	x := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "x"}
	shared := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "shared"}
	sibling := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "sibling"}
	generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "default", Name: "generator"}
	for _, tc := range []struct {
		name      string
		order     []manifest.NamedResource
		chartPath string
	}{
		{name: "x_then_shared", order: []manifest.NamedResource{x, shared}},
		{name: "shared_then_x", order: []manifest.NamedResource{shared, x}},
		{name: "discovery"},
		{name: "root chart", chartPath: "."},
		{name: "nested chart", chartPath: "charts/demo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			view := fstest.MapFS{}
			for name, file := range input {
				view[path.Join(tc.chartPath, name)] = file
			}
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: view})
			l := loader.New(st)
			if tc.chartPath != "" {
				view[path.Join(tc.chartPath, "Chart.yaml")] = &fstest.MapFile{Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n")}
				source := &manifest.GitRepository{Name: "source", Namespace: "default"}
				release := &manifest.HelmRelease{Name: "demo", Namespace: "default", Chart: manifest.HelmChart{Name: tc.chartPath, Source: source.Named()}}
				st.AddObjects([]manifest.BaseManifest{source, release})
				st.SetArtifact(source.Named(), st.GetArtifact(manifest.BootstrapSourceID))
				l.Index.Records[release.Named()] = manifest.DiscoveryRecord{File: "release.yaml", Refs: []manifest.NamedResource{source.Named()}}
				if err := l.FinalizeIndex(t.Context(), root); err != nil {
					t.Fatal(err)
				}
				record, _ := l.Index.Lookup(release.Named())
				if !slices.Contains(record.Inputs, path.Join(tc.chartPath, "a.yaml")) {
					t.Fatalf("chart did not read independent declarations: %+v", record.Inputs)
				}
			}
			for _, id := range []manifest.NamedResource{x, shared, sibling} {
				file := "a.yaml"
				if id == shared {
					file = "z.yaml"
				}
				file = path.Join(tc.chartPath, file)
				l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: root + "/" + file, File: file}
			}
			file := path.Join(tc.chartPath, "generator.yaml")
			l.Index.Records[generator] = manifest.DiscoveryRecord{Authored: generator, Path: root + "/" + file, File: file}
			d := &discoverer{cfg: Config{Store: st}, loader: l}
			if tc.order == nil {
				d.promoteOrphans(t.Context(), nil)
			} else {
				for _, id := range tc.order {
					record, _ := l.Index.Lookup(id)
					admit := func(obj manifest.BaseManifest) bool { return d.loader.AdmitsRoot(obj.Named(), record.Path) }
					if !loader.Promote(t.Context(), st, id, loader.PromoteOptions{Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File}, Admit: admit}) {
						t.Fatalf("promotion failed for %s", id)
					}
				}
				d.promoteOrphans(t.Context(), nil)
			}
			obj, doc, attempt := st.GetEmission(generator)
			attempt.Finish()
			if obj == nil || doc == nil || doc["metadata"].(map[string]any)["uid"] != "authored" || doc["metadata"].(map[string]any)["annotations"].(map[string]any)["source.extensions.fluxcd.io/reconcile"] != "disabled" {
				t.Fatalf("orphan generator lost authored document: %#v", doc)
			}
			for _, want := range []struct {
				id    manifest.NamedResource
				value string
			}{{x, "X"}, {shared, "Z"}, {sibling, "S"}} {
				obj, ok := st.GetObject(want.id).(*manifest.ConfigMap)
				if !ok {
					t.Fatalf("missing ConfigMap %s", want.id)
				}
				if got := obj.Data["VALUE"]; got != want.value {
					t.Errorf("%s VALUE=%v, want indexed %s", want.id, got, want.value)
				}
			}
		})
	}
}

func TestPromoteOrphans_ChecksEachSiblingsProducer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/bundle.yaml", testutil.KustomizationYAML("self", "authored", "  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n")+"---\n"+
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: child, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
---
apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps, uid: authored}
spec: {sources: [{alias: repo, kind: GitRepository, name: source}]}
`)
	st := store.New()
	admitDiscoveryForTest(st, dir)
	l := loader.New(st)
	self := testutil.KustomizationID("apps", "self")
	child := testutil.HelmReleaseID("apps", "child")
	generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator"}
	for _, id := range []manifest.NamedResource{self, child, generator} {
		record := manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "apps/bundle.yaml"), File: "apps/bundle.yaml"}
		if id == self {
			record.Authored.Namespace = "authored"
		}
		l.Index.Records[id] = record
	}
	d := &discoverer{
		cfg: Config{Store: st, WipeSecrets: true}, loader: l,
	}
	l.Index.Claims = map[string][]manifest.NamedResource{"apps/": {self}}
	d.promoteOrphans(t.Context(), nil)
	if st.GetObject(self) == nil {
		t.Error("self-excluded orphan must be admitted")
	}
	if st.GetObject(child) != nil {
		t.Error("orphan promotion must not admit a parent-owned sibling")
	}
	if st.GetObject(generator) != nil {
		t.Error("orphan promotion admitted a parent-owned generator")
	}
}

func TestPromoteOrphans_GraphOwnedSiblingsAndSelf(t *testing.T) {
	t.Parallel()
	t.Run("omitted mixed file", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		body := testutil.KustomizationYAML("disabled", "apps", "  path: ./target\n") + "---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: sibling, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: chart}}\n"
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
		testutil.WriteFile(t, root, "apps/disabled.yaml", body)
		testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: []\n")
		st := store.New()
		l := loader.New(st)
		l.SourceRoot = root
		if _, err := l.Load(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		l.Admit(root)
		if err := l.FinalizeIndex(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		d := discoverer{cfg: Config{Store: st}, loader: l}
		d.promoteOrphans(t.Context(), nil)
		for _, id := range []manifest.NamedResource{testutil.KustomizationID("apps", "disabled"), testutil.HelmReleaseID("apps", "sibling")} {
			if st.GetObject(id) != nil {
				t.Fatalf("omitted file promoted %s", id)
			}
		}
	})

	dir := t.TempDir()
	body := testutil.KustomizationYAML("self", "apps", "  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n") + "---\n" +
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: child, namespace: apps}
spec: {chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: source}}}}
`
	testutil.WriteFile(t, dir, "bundle.yaml", body)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../bundle.yaml]\n")
	st := store.New()
	admitDiscoveryForTest(st, dir)
	docs, err := manifest.SplitDocs([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	ks, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	if err != nil {
		t.Fatal(err)
	}
	st.AddObject(ks)
	source := &manifest.GitRepository{Name: "source", Namespace: ks.Named().Namespace}
	st.AddObject(source)
	st.SetArtifact(source.Named(), st.GetArtifact(manifest.BootstrapSourceID))
	l := loader.New(st)
	l.Options.WipeSecrets = false
	if err := l.FinalizeIndex(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	self := ks.Named()
	st.DeleteObject(self)
	child := testutil.HelmReleaseID("apps", "child")
	for _, id := range []manifest.NamedResource{self, child} {
		l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml"), File: "bundle.yaml"}
	}
	d := &discoverer{cfg: Config{Store: st}, loader: l}
	d.promoteOrphans(t.Context(), nil)
	if st.GetObject(self) == nil {
		t.Error("self-owned Kustomization must be admitted")
	}
	if st.GetObject(child) != nil {
		t.Error("graph-owned sibling bypassed per-document admission")
	}
}

func TestPromoteOrphans_SecretPrecedence(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"standalone", "siblings", "failed", "preexisting", "graph-owned", "unknown-file"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			st := store.New()
			admitDiscoveryForTest(st, dir)
			l := loader.New(st)
			l.Claims(dir)
			d := &discoverer{cfg: Config{Store: st, WipeSecrets: true}, loader: l}
			ids := []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "secure", Name: "a"}}
			if scenario == "siblings" {
				ids = append(ids, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "secure", Name: "b"})
			}
			var body string
			var saved []*manifest.Secret
			for _, id := range ids {
				body += "---\napiVersion: v1\nkind: Secret\nmetadata: {name: " + id.Name + ", namespace: secure}\nstringData: {HOST: fixture-value}\n"
				l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml"), File: "bundle.yaml"}
				if scenario == "unknown-file" {
					r := l.Index.Records[id]
					r.File = ""
					l.Index.Records[id] = r
				}
				value := "..PLACEHOLDER_HOST.."
				if scenario == "preexisting" {
					value = "preexisting-value"
				}
				obj := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"HOST": value}}
				saved = append(saved, obj)
				if scenario != "unknown-file" {
					doc := obj.Snapshot()
					doc["metadata"].(map[string]any)["uid"] = "saved-secret"
					st.AddObject(obj, doc)
				}
			}
			testutil.WriteFile(t, dir, "bundle.yaml", body)
			if scenario == "graph-owned" {
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../bundle.yaml]\n")
				ksDoc, err := manifest.SplitDocs([]byte(testutil.KustomizationYAML("owner", "flux-system", "  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n")))
				if err != nil {
					t.Fatal(err)
				}
				ks, _, _, err := manifest.ParseDoc(ksDoc[0], manifest.ParseDocOptions{DefaultNamespace: ""})
				if err != nil {
					t.Fatal(err)
				}
				st.AddObject(ks)
				source := &manifest.GitRepository{Name: "source", Namespace: ks.Named().Namespace}
				st.AddObject(source)
				st.SetArtifact(source.Named(), st.GetArtifact(manifest.BootstrapSourceID))
				l.SourceRoot = dir
				if _, err := l.Load(t.Context(), dir); err != nil {
					t.Fatal(err)
				}
				if err := l.FinalizeIndex(t.Context(), dir); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "failed" {
				if err := os.Remove(filepath.Join(dir, "bundle.yaml")); err != nil {
					t.Fatal(err)
				}
			}
			candidates := ids
			if scenario == "preexisting" || scenario == "unknown-file" {
				candidates = nil
			}
			d.promoteOrphans(t.Context(), candidates)
			for i, id := range ids {
				got, ok := st.GetObject(id).(*manifest.Secret)
				if scenario == "unknown-file" {
					if ok {
						t.Errorf("Secret without a source-file entry admitted: %+v", got)
					}
					continue
				}
				if !ok {
					t.Fatalf("Secret %s missing", id)
				}
				if scenario == "standalone" || scenario == "siblings" {
					if got.StringData["HOST"] != "fixture-value" {
						t.Errorf("%s = %+v, want real file value", id, got)
					}
				} else if got != saved[i] {
					t.Errorf("%s must preserve exact immutable object", id)
				}
				_, doc, emission := st.GetEmission(id)
				emission.Finish()
				if doc == nil {
					t.Fatal("promoted or restored secret lost its authoritative document")
				}
				if scenario == "failed" || scenario == "preexisting" || scenario == "graph-owned" {
					if doc["metadata"].(map[string]any)["uid"] != "saved-secret" {
						t.Fatalf("saved secret metadata lost: %v", doc)
					}
				}
				wantSaved := "..PLACEHOLDER_HOST.."
				if scenario == "preexisting" {
					wantSaved = "preexisting-value"
				}
				if saved[i].StringData["HOST"] != wantSaved {
					t.Error("saved immutable Secret mutated")
				}
			}
		})
	}
}
