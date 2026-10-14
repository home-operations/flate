package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOrchestrator_TransformerTargetNamespace(t *testing.T) {
	for _, earlyHR := range []bool{true, false} {
		t.Run(fmt.Sprintf("hr_before_parent_emission_%v", earlyHR), func(t *testing.T) {
			dir := t.TempDir()

			// Shared builtin NamespaceTransformer (kubernetes/transformers/).
			testutil.WriteFile(t, dir, "transformers/kustomization.yaml", "resources:\n  - ./transformer.yaml\n")
			testutil.WriteFile(t, dir, "transformers/transformer.yaml", `apiVersion: builtin
kind: NamespaceTransformer
metadata:
  name: apply-ns
  namespace: .invalid
fieldSpecs:
  - path: metadata/name
    kind: Namespace
    create: true
  - path: spec/targetNamespace
    group: kustomize.toolkit.fluxcd.io
    kind: Kustomization
    create: true
`)

			// Namespace overlay: pulls in the transformer subtree whose
			// `namespace:` directive feeds the NamespaceTransformer.
			testutil.WriteFile(t, dir, "apps/myapp/kustomization.yaml", `resources:
  - ./namespace.yaml
  - ./myapp/ks.yaml
transformers:
  - ./transformers
`)
			testutil.WriteFile(t, dir, "apps/myapp/namespace.yaml", `apiVersion: v1
kind: Namespace
metadata:
  name: .invalid
`)
			testutil.WriteFile(t, dir, "apps/myapp/transformers/kustomization.yaml", `namespace: myns
resources:
  - ../../../transformers
`)

			testutil.WriteFile(t, dir, "apps/myapp/myapp/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: myapp
spec:
  path: ./apps/myapp/myapp/app
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
			testutil.WriteFile(t, dir, "apps/myapp/myapp/app/kustomization.yaml", "resources: [helmrelease.yaml, producer.yaml]\n")
			testutil.WriteFile(t, dir, "apps/myapp/myapp/app/helmrelease.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: myapp
spec:
  chart:
    spec:
      chart: charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  valuesFrom:
  - {kind: Secret, name: missing-values}
  - {kind: Secret, name: missing-values, valuesKey: other.yaml}
`)

			testutil.WriteFile(t, dir, "apps/myapp/myapp/app/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: myapp-values}
spec: {target: {name: missing-values}}
`)
			testutil.WriteFile(t, dir, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "charts/app/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered}
data: {greeting: fallback}
`)
			testutil.WriteFile(t, dir, "flux/root.yaml", ksYAML("apps", "apps/myapp", "")+`  patches:
  - target: {kind: Kustomization}
    patch: |-
      apiVersion: kustomize.toolkit.fluxcd.io/v1
      kind: Kustomization
      metadata: {name: _}
      spec:
        patches:
        - target: {kind: HelmRelease}
          patch: |-
            apiVersion: helm.toolkit.fluxcd.io/v2
            kind: HelmRelease
            metadata: {name: _}
            spec:
              install: {crds: CreateReplace}
              upgrade: {crds: CreateReplace}
`)

			o, err := New(Config{Path: dir, RepoRoot: dir, CacheDir: t.TempDir(), WipeSecrets: true, Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(o.Stop)
			if err := o.Bootstrap(t.Context()); err != nil {
				t.Fatal(err)
			}
			o.configureControllers()
			o.hrc.Start(t.Context())
			raw := ksID("myapp")
			parent := raw
			parent.Namespace = "myns"
			hr := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "myns", Name: "myapp"}
			dispatch := func(id manifest.NamedResource) <-chan struct{} {
				done := make(chan struct{})
				o.tasks.Go(t.Context(), "namespace transformer", func(ctx context.Context) {
					defer close(done)
					var blocked []manifest.NamedResource
					if id.Kind == manifest.KindHelmRelease {
						blocked = o.hrc.ReconcileNode(ctx, id, manifest.DrainNone)
					} else {
						blocked = o.ksc.ReconcileNode(ctx, id, manifest.DrainNone)
					}
					if len(blocked) != 0 {
						t.Errorf("unexpected blocked %s: %v", id, blocked)
					}
				})
				return done
			}
			<-dispatch(ksID("apps"))
			<-dispatch(raw)
			gate, release := make(chan struct{}), make(chan struct{})
			unsub := o.store.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				info := payload.(store.StatusInfo)
				if id == parent && ((earlyHR && info.Message == "rendering") || (!earlyHR && info.Status == store.StatusReady)) {
					close(gate)
					<-release
				}
			}, false)
			// Hold the real parent before or after it publishes the patched HR while
			// the second task worker attempts the competing HR reconcile.
			parentDone := dispatch(parent)
			<-gate
			<-dispatch(hr)
			close(release)
			<-parentDone
			unsub()
			<-dispatch(hr)
			o.tasks.BlockTillDone()
			count := 0
			for _, w := range o.store.Warnings() {
				if w.Category == manifest.WarnOmittedValues && strings.Contains(w.Message, "missing-values") {
					count = w.Count
				}
			}
			if count != 1 {
				t.Errorf("warning count = %d, want 1", count)
			}
			if o.store.GetObject(raw) != nil || o.index.Records[hr].Parent != parent {
				t.Errorf("bootstrap identity or parent differs: stale=%v parent=%s", o.store.GetObject(raw), o.index.Records[hr].Parent)
			}
			gotHR, ok := o.store.Get[*manifest.HelmRelease](hr)
			if !ok || gotHR.CRDsPolicy != "CreateReplace" {
				t.Errorf("patched release missing: %+v", gotHR)
			}
			rendered, ok := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "myns", Name: "rendered"})
			if !ok || rendered.Data["greeting"] != "fallback" || o.store.GetArtifact(hr) == nil {
				t.Errorf("rendered values or artifact missing: %+v", rendered)
			}
			if o.store.GetObject(manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "myapp"}) != nil {
				t.Error("empty-namespace HelmRelease present")
			}
		})
	}
}
