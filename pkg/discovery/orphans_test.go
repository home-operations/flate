package discovery

import (
	"os"
	"path/filepath"
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
		"a.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: default}\ndata: {VALUE: X}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\ndata: {VALUE: A}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling, namespace: default}\ndata: {VALUE: S}\n")},
		"z.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\ndata: {VALUE: Z}\n")},
	}
	x := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "x"}
	shared := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "shared"}
	sibling := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "sibling"}
	for _, tc := range []struct {
		name  string
		order []manifest.NamedResource
	}{
		{"x_then_shared", []manifest.NamedResource{x, shared}},
		{"shared_then_x", []manifest.NamedResource{shared, x}},
		{"discovery", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input})
			l := loader.New(st)
			for _, id := range []manifest.NamedResource{x, shared, sibling} {
				file := "a.yaml"
				if id == shared {
					file = "z.yaml"
				}
				l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: root + "/" + file, File: file}
			}
			d := &discoverer{cfg: Config{Store: st}, loader: l}
			if tc.order == nil {
				d.promoteOrphans(t.Context(), nil)
			} else {
				for _, id := range tc.order {
					record, _ := l.Index.Lookup(id)
					admit := func(obj manifest.BaseManifest) bool { return d.admitOrphan(obj.Named(), record.Path) }
					if !loader.Promote(t.Context(), st, id, loader.PromoteOptions{Record: manifest.Production{Authored: record.Authored, Path: record.Path, File: record.File}, Admit: admit}) {
						t.Fatalf("promotion failed for %s", id)
					}
				}
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
`)
	st := store.New()
	admitDiscoveryForTest(st, dir)
	l := loader.New(st)
	self := testutil.KustomizationID("apps", "self")
	child := testutil.HelmReleaseID("apps", "child")
	for _, id := range []manifest.NamedResource{self, child} {
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
}

func TestPromoteOrphans_GraphOwnedSiblingsAndSelf(t *testing.T) {
	t.Parallel()
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
					st.AddObject(obj)
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
