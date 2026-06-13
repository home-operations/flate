package orchestrator

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

// TestRender_StaleValuesWarning is the konflate-facing contract: a HelmRelease
// pinning a value no chart template references surfaces as a structured
// Result.Warnings entry (Category WarnStaleValues, attributed to the HR, Detail
// = the unused keys) — not just a log line. A sibling HR on a chart that
// consumes .Values wholesale (toYaml) produces no warning.
func TestRender_StaleValuesWarning(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "hr.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: apps
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/app
      sourceRef:
        kind: GitRepository
        name: flux-system
        namespace: flux-system
  values:
    greeting: hi
    oldUnusedKey: true
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: opaque
  namespace: apps
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/opaque
      sourceRef:
        kind: GitRepository
        name: flux-system
        namespace: flux-system
  values:
    greeting: hi
    oldUnusedKey: true
`)
	// appchart names .Values.greeting → flate flags the unreferenced oldUnusedKey.
	testutil.WriteFile(t, dir, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "charts/app/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, dir, "charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  g: {{ .Values.greeting | quote }}\n")
	// opaquechart dumps .Values wholesale → detection must stay silent.
	testutil.WriteFile(t, dir, "charts/opaque/Chart.yaml", "apiVersion: v2\nname: opaque\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "charts/opaque/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, dir, "charts/opaque/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  v: |\n    {{- toYaml .Values | nindent 4 }}\n")

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(res.Failed) != 0 {
		t.Fatalf("advisory must not fail the render; Failed=%v", res.Failed)
	}

	app := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "app"}
	opaque := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "opaque"}

	var got *manifest.Warning
	for i := range res.Warnings {
		w := res.Warnings[i]
		if w.Resource == opaque {
			t.Errorf("a chart consuming .Values wholesale must not warn; got %+v", w)
		}
		if w.Resource == app && w.Category == manifest.WarnStaleValues {
			got = &res.Warnings[i]
		}
	}
	if got == nil {
		t.Fatalf("missing StaleValues warning for %s; Warnings=%+v", app, res.Warnings)
	}
	if !slices.Equal(got.Detail, []string{"oldUnusedKey"}) {
		t.Errorf("warning Detail = %v, want [oldUnusedKey]", got.Detail)
	}
}
