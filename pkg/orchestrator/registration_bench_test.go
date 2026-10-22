package orchestrator

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// The late provider depends on both optional readers completing. The emitted
// name is an input expression, so discovery cannot supply a producer edge.
func BenchmarkDirtyRerun_DynamicEmissionChain(b *testing.B) {
	root := b.TempDir()
	testutil.WriteFile(b, root, "source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git"}
`)
	testutil.WriteFile(b, root, "consumer.yaml", ksYAML("consumer", "app", "")+`  postBuild:
    substituteFrom: [{kind: ConfigMap, name: late-settings, optional: true}]
`)
	testutil.WriteFile(b, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(b, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: ks-output, namespace: flux-system}\ndata: {value: '${VALUE:=early}'}\n")
	testutil.WriteFile(b, root, "release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: consumer, namespace: flux-system}
spec:
  chart:
    spec:
      chart: charts/app
      sourceRef: {kind: GitRepository, name: flux-system}
  values: {VALUE: early}
  valuesFrom: [{kind: ConfigMap, name: late-settings, valuesKey: HELM_VALUES, optional: true}]
`)
	testutil.WriteFile(b, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(b, root, "charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hr-output, namespace: flux-system}\ndata: {value: {{ .Values.VALUE | quote }}}\n")
	testutil.WriteFile(b, root, "sets.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: emitter, namespace: flux-system}
spec:
  inputsFrom:
    - kind: ResourceSetInputProvider
      selector: {matchLabels: {color: blue}}
  resourcesTemplate: |
    << if inputs >>
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: '<< inputs.name >>', namespace: flux-system}
    data:
      VALUE: '<< inputs.VALUE >>'
      HELM_VALUES: 'VALUE: << inputs.VALUE >>'
    << end >>
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: late-provider, namespace: flux-system}
spec:
  dependsOn:
    - {apiVersion: kustomize.toolkit.fluxcd.io/v1, kind: Kustomization, name: consumer}
    - {apiVersion: helm.toolkit.fluxcd.io/v2, kind: HelmRelease, name: consumer}
  resourcesTemplate: |
    apiVersion: fluxcd.controlplane.io/v1
    kind: ResourceSetInputProvider
    metadata: {name: late, namespace: flux-system, labels: {color: blue}}
    spec: {type: Static, defaultValues: {name: late-settings, VALUE: late}}
`)
	for _, workers := range []int{2, 16, runtime.NumCPU() * 4} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			cfg := Config{Path: root, RepoRoot: root, CacheDir: b.TempDir(), WipeSecrets: true, Concurrency: workers}
			var renders int64
			b.ReportAllocs()
			for b.Loop() {
				o, err := New(b.Context(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				if err := o.Bootstrap(b.Context()); err != nil {
					b.Fatal(err)
				}
				var actualRenders atomic.Int64
				unsubRender := o.store.AddListener(store.EventArtifactUpdated, func(_ manifest.NamedResource, payload any) {
					if _, ok := payload.(*store.Emission); ok {
						actualRenders.Add(1)
					}
				}, false)
				err = o.Run(b.Context())
				unsubRender()
				if err != nil {
					b.Fatal(err)
				}
				if failed := o.store.FailedResources(); len(failed) != 0 {
					b.Fatalf("failed resources: %v", failed)
				}
				cm, ok := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "late-settings"})
				if !ok || cm.Data["VALUE"] != "late" {
					b.Fatal("late input was not emitted")
				}
				renders += actualRenders.Load()
			}
			b.ReportMetric(float64(renders)/float64(b.N), "renders/op")
		})
	}
}
