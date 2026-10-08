package testutil

import (
	"strings"
	"testing"
)

// WriteGeneratedValuesCluster creates two ordered releases using a generated values ConfigMap.
func WriteGeneratedValuesCluster(t testing.TB, root string) {
	t.Helper()
	WriteFile(t, root, "flux/apps.yaml", dependencyKS("apps", "apps", "")+`  postBuild:
    substituteFrom:
    - kind: ConfigMap
      name: bootstrap-values
`)
	WriteFile(t, root, "apps/kustomization.yaml", `namespace: flux-system
resources:
- a.yaml
configurations:
- references.yaml
configMapGenerator:
- name: generated-values
  files:
  - values.yaml
`)
	WriteFile(t, root, "apps/references.yaml", `nameReference:
- kind: ConfigMap
  version: v1
  fieldSpecs:
  - kind: HelmRelease
    group: helm.toolkit.fluxcd.io
    path: spec/valuesFrom/name
`)
	WriteFile(t, root, "apps/values.yaml", "greeting: hello\nreplicas: 2\n")
	// The parent's substituteFrom must promote the data without admitting its
	// raw HR sibling before the generated nameReference rewrite.
	WriteFile(t, root, "apps/a.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: a, namespace: flux-system}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/app
      sourceRef: {kind: GitRepository, name: shared, namespace: flux-system}
  valuesFrom:
  - kind: ConfigMap
    name: generated-values
---
apiVersion: v1
kind: ConfigMap
metadata: {name: bootstrap-values, namespace: flux-system}
data: {BOOTSTRAP: ready}
`)
	// A standalone dependent must resolve A before A's parent has emitted it.
	WriteFile(t, root, "standalone/b.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: b, namespace: flux-system}
spec:
  interval: 10m
  dependsOn:
  - name: a
  chart:
    spec:
      chart: charts/app
      sourceRef: {kind: GitRepository, name: shared, namespace: flux-system}
  values: {greeting: hello, replicas: 2}
`)
	writeDependencyChart(t, root)
}

// WriteFilteredOrderingCluster keeps required data beside excluded controllers and sources.
func WriteFilteredOrderingCluster(t testing.TB, root, greeting string, brokenSource bool) {
	t.Helper()
	// Excluded resources are marked Ready, so a rejected CEL gate makes
	// the dependsOn prune necessary even when the target is skipped.
	selected := dependencyKS("selected", "apps/selected", "monitor-a")
	selected = strings.Replace(selected, "  - name: monitor-a\n", "  - name: monitor-a\n    readyExpr: 'dep.isHealthy()'\n", 1)
	WriteFile(t, root, "flux/selected.yaml", selected+"  postBuild:\n    substitute:\n      GREETING: "+greeting+"\n")
	for _, name := range []string{"monitor-a", "monitor-b"} {
		WriteFile(t, root, "flux/"+name+".yaml", dependencyKS(name, "apps/"+name, ""))
		WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources:\n- missing.yaml\n- "+map[string]string{"monitor-a": "bundle.yaml", "monitor-b": "hr.yaml"}[name]+"\n")
	}
	chart := "  chartRef: {kind: HelmChart, name: shared-chart}\n"
	if brokenSource {
		chart = "  chartRef: {kind: OCIRepository, name: selected-source}\n"
	}
	WriteFile(t, root, "apps/selected/kustomization.yaml", "resources:\n- hr.yaml\n- values.yaml\n")
	WriteFile(t, root, "apps/selected/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: flux-system}
spec:
  interval: 10m
  dependsOn:
  - name: monitor-a
  - name: monitor-b
`+chart+`  valuesFrom:
  - kind: ConfigMap
    name: selected-values
  values:
    greeting: `+greeting+"\n")
	WriteFile(t, root, "apps/selected/values.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected-values, namespace: flux-system}
data:
  values.yaml: |
    shared: selected
`)
	WriteFile(t, root, "apps/monitor-a/bundle.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: shared-chart, namespace: flux-system}
spec:
  chart: charts/app
  sourceRef: {kind: GitRepository, name: shared}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: excluded-values, namespace: flux-system}
data: {values.yaml: "shared: excluded"}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: excluded-source, namespace: flux-system}
spec:
  url: https://example.invalid/excluded.git
  secretRef: {name: absent}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: selected-source, namespace: flux-system}
spec:
  url: oci://example.invalid/app
  ref: {tag: 0.1.0}
  secretRef: {name: absent}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: monitor-a, namespace: flux-system}
spec:
  interval: 10m
  chartRef: {kind: HelmChart, name: shared-chart}
`)
	WriteFile(t, root, "apps/monitor-b/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: monitor-b, namespace: flux-system}
spec:
  interval: 10m
  chartRef: {kind: HelmChart, name: shared-chart}
`)
	writeDependencyChart(t, root)
}

func dependencyKS(name, path, dependsOn string) string {
	dep := ""
	if dependsOn != "" {
		dep = "  dependsOn:\n  - name: " + dependsOn + "\n"
	}
	return `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ` + name + `, namespace: flux-system}
spec:
  path: ./` + path + "\n" + dep +
		"  sourceRef: {kind: GitRepository, name: shared, namespace: flux-system}\n"
}

func writeDependencyChart(t testing.TB, root string) {
	t.Helper()
	WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	WriteFile(t, root, "charts/app/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-rendered
  namespace: flux-system
data:
  greeting: {{ .Values.greeting | quote }}
  replicas: {{ .Values.replicas | default 1 | quote }}
  shared: {{ .Values.shared | default "" | quote }}
`)
}
