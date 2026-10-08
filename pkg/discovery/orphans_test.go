package discovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPromoteOrphans_ChecksEachSiblingsProducer(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/bundle.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: self, namespace: apps}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: child, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
`)
	st := store.New()
	l := loader.New(st)
	l.Existence = loader.NewExistenceIndex()
	self := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "self"}
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "child"}
	for _, id := range []manifest.NamedResource{self, child} {
		l.Existence.Record(id, filepath.Join(dir, "apps/bundle.yaml"))
	}
	d := &discoverer{
		cfg: Config{Store: st, WipeSecrets: true}, loader: l,
		sourceFiles: map[manifest.NamedResource]string{self: "apps/bundle.yaml", child: "apps/bundle.yaml"},
	}
	d.promoteOrphans([]loader.KSPathPrefix{{ID: self, Prefix: "apps/"}}, nil, l.Existence.All(), nil)
	if st.GetObject(self) == nil {
		t.Error("self-excluded orphan must be admitted")
	}
	if st.GetObject(child) != nil {
		t.Error("orphan promotion must not admit a parent-owned sibling")
	}
}

func TestPromoteOrphans_GraphOwnedSiblingsAndSelf(t *testing.T) {
	dir := t.TempDir()
	body := `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: self, namespace: apps}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: child, namespace: apps}
spec: {chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: source}}}}
`
	testutil.WriteFile(t, dir, "bundle.yaml", body)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../bundle.yaml]\n")
	st := store.New()
	docs, err := manifest.SplitDocs([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	ks, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st.AddObject(ks)
	idx := loader.BuildSelfProduceIndex(st, dir, nil, false)
	self := ks.Named()
	st.DeleteObject(self)
	child := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "child"}
	l := loader.New(st)
	l.Existence = loader.NewExistenceIndex()
	for _, id := range []manifest.NamedResource{self, child} {
		l.Existence.Record(id, filepath.Join(dir, "bundle.yaml"))
	}
	d := &discoverer{cfg: Config{Store: st}, loader: l, sourceFiles: map[manifest.NamedResource]string{self: "bundle.yaml", child: "bundle.yaml"}}
	d.promoteOrphans(nil, idx, l.Existence.All(), nil)
	if st.GetObject(self) == nil {
		t.Error("self-owned Kustomization must be admitted")
	}
	if st.GetObject(child) != nil {
		t.Error("graph-owned sibling bypassed per-document admission")
	}
}

func TestPromoteOrphans_SecretPrecedence(t *testing.T) {
	for _, scenario := range []string{"standalone", "siblings", "failed", "preexisting", "graph-owned", "unknown-file"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			st := store.New()
			l := loader.New(st)
			l.Existence = loader.NewExistenceIndex()
			d := &discoverer{cfg: Config{Store: st, WipeSecrets: true}, loader: l, sourceFiles: map[manifest.NamedResource]string{}}
			ids := []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "secure", Name: "a"}}
			if scenario == "siblings" {
				ids = append(ids, manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "secure", Name: "b"})
			}
			var body string
			var saved []*manifest.Secret
			for _, id := range ids {
				body += "---\napiVersion: v1\nkind: Secret\nmetadata: {name: " + id.Name + ", namespace: secure}\nstringData: {HOST: fixture-value}\n"
				l.Existence.Record(id, filepath.Join(dir, "bundle.yaml"))
				if scenario != "unknown-file" {
					d.sourceFiles[id] = "bundle.yaml"
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
			var idx *loader.SelfProduceIndex
			if scenario == "graph-owned" {
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../bundle.yaml]\n")
				ksDoc, err := manifest.SplitDocs([]byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: source}}
`))
				if err != nil {
					t.Fatal(err)
				}
				ks, err := manifest.ParseDoc(ksDoc[0], manifest.ParseDocOptions{})
				if err != nil {
					t.Fatal(err)
				}
				st.AddObject(ks)
				idx = loader.BuildSelfProduceIndex(st, dir, nil, true)
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
			d.promoteOrphans(nil, idx, l.Existence.All(), candidates)
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
