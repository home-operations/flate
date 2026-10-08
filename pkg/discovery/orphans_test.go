package discovery

import (
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
	d.promoteOrphans([]loader.KSPathPrefix{{ID: self, Prefix: "apps/"}})
	if st.GetObject(self) == nil {
		t.Error("self-excluded orphan must be admitted")
	}
	if st.GetObject(child) != nil {
		t.Error("orphan promotion must not admit a parent-owned sibling")
	}
}
