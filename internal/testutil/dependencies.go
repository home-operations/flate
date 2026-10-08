package testutil

import (
	"fmt"
	"testing"
)

// WriteGeneratedValuesCluster creates two ordered releases using a generated values ConfigMap.
func WriteGeneratedValuesCluster(t testing.TB, root string) {
	t.Helper()
	WriteFile(t, root, "flux/apps.yaml", dependencyKS("apps", "apps", ""))
	WriteFile(t, root, "apps/kustomization.yaml", `namespace: flux-system
resources:
- a.yaml
- b.yaml
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
	for _, name := range []string{"a", "b"} {
		dep := ""
		if name == "b" {
			dep = "  dependsOn:\n  - name: a\n"
		}
		WriteFile(t, root, "apps/"+name+".yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %s, namespace: flux-system}
spec:
  interval: 10m
%s  chart:
    spec:
      chart: charts/app
      sourceRef: {kind: GitRepository, name: shared, namespace: flux-system}
  valuesFrom:
  - kind: ConfigMap
    name: generated-values
`, name, dep))
	}
	writeDependencyChart(t, root)
}

// WriteFilteredOrderingCluster keeps required data beside excluded controllers and sources.
func WriteFilteredOrderingCluster(t testing.TB, root, greeting string, brokenSource bool) {
	t.Helper()
	WriteFile(t, root, "flux/selected.yaml", dependencyKS("selected", "apps/selected", "monitor-a"))
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
