package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_ScanAndPathWarnings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		sameRoot bool
		category string
	}{
		{name: "empty scan", category: "EmptyScan"},
		{name: "same root", sameRoot: true, category: "PathConfig"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			cfg := Config{Path: root, CacheDir: t.TempDir(), Concurrency: 2}
			if tt.sameRoot {
				cfg.PathOrig = root
			}
			o, err := New(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := o.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
			res, err := o.Render(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var warnings []manifest.Warning
			for _, w := range res.Warnings {
				if w.Category == tt.category {
					warnings = append(warnings, w)
				}
			}
			if len(warnings) != 1 {
				t.Fatalf("warnings = %+v, want one %s", res.Warnings, tt.category)
			}
			assert.Equal(t, warnings[0].Count, 1)
			if tt.sameRoot {
				assert.Diff(t, warnings[0].Detail, []string{root})
			}
		})
	}
}

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

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true})
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

	app := testutil.HelmReleaseID("apps", "app")
	opaque := testutil.HelmReleaseID("apps", "opaque")

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

func TestRender_UnresolvedSubstitutionWarning(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "kubernetes/flux/cluster-apps.yaml", testutil.KustomizationYAML("cluster-apps", "flux-system", `  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: Secret
        name: cluster-secrets
`))
	testutil.WriteFile(t, dir, "kubernetes/flux/other-apps.yaml", testutil.KustomizationYAML("other-apps", "other", `  path: ./kubernetes/apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: Secret
        name: cluster-secrets
`))
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\ndata:\n  k: v\n")

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true, Concurrency: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, o.Stop()) })
	res, err := o.Render(t.Context())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	var warnings []manifest.Warning
	for _, w := range res.Warnings {
		if w.Category == manifest.WarnUnresolvedSubstitution {
			warnings = append(warnings, w)
		}
	}
	want := manifest.Warning{
		Category: manifest.WarnUnresolvedSubstitution,
		Message: `substituteFrom Secret "cluster-secrets" could not be read offline; ` +
			"its variables are unavailable, so their ${VAR} references may remain unsubstituted " +
			"or resolve to defaults or empty values",
		Count: 2,
	}
	assert.Diff(t, warnings, []manifest.Warning{want})
}

// TestRender_BestEffortRescue: a HelmRelease whose required value rendered empty
// because its parent Kustomization couldn't read a substituteFrom Secret offline
// is rendered best-effort — the empty value is filled with a placeholder so the
// chart templates instead of failing, the HR stays OUT of the failed roster, its
// child carries the placeholder, and a per-HR UnresolvedSubstitution warning
// names the filled field. The gate is the unreadable parent secret, so a chart
// that fails for an unrelated reason would NOT be rescued.
func TestRender_BestEffortRescue(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/apps.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substitute: {KEEP: "ok"}
    substituteFrom:
      - kind: Secret
        name: missing
`))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ./hr.yaml\n")
	testutil.WriteFile(t, dir, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: charts/req
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  values:
    greeting: "${MISSING}"
`)
	// The chart hard-requires .Values.greeting — an empty value fails the
	// template (the offline-substitution-empty case the rescue targets).
	testutil.WriteFile(t, dir, "charts/req/Chart.yaml", "apiVersion: v2\nname: req\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "charts/req/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-cm\ndata:\n  g: {{ required \"greeting is required\" .Values.greeting | quote }}\n")

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	demo := testutil.HelmReleaseID("apps", "demo")
	if info, failed := res.Failed[demo]; failed {
		t.Fatalf("demo should be rescued, not failed; got %+v", info)
	}
	sawPlaceholder := false
	for _, doc := range res.Manifests[demo] {
		if manifest.ContainsValuePlaceholder(doc) {
			sawPlaceholder = true
		}
	}
	if !sawPlaceholder {
		t.Errorf("rescued render should carry a placeholder; manifests=%v", res.Manifests[demo])
	}
	warned := false
	for _, w := range res.Warnings {
		if w.Resource == demo && w.Category == manifest.WarnUnresolvedSubstitution && slices.Contains(w.Detail, "greeting") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("missing per-HR best-effort warning naming greeting; Warnings=%+v", res.Warnings)
	}
}

// TestRender_ProducerDeclaredSecretPlaceholder is the konflate-facing contract
// for the cloudflared-secret case: when a Kustomization's postBuild.substituteFrom
// Secret is materialized by an in-repo ExternalSecret that enumerates its keys
// (spec.target.template.data), flate synthesizes a placeholder Secret for those
// keys — so the ${VAR}s it supplies resolve to ..PLACEHOLDER_<key>.. (not the
// empty string) and NO UnresolvedSubstitution advisory fires, because the secret
// is now readable-as-placeholders, exactly like a SOPS-encrypted Secret.
func TestRender_ProducerDeclaredSecretPlaceholder(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/apps.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://example.test/cluster.git
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  postBuild:
    substituteFrom:
      - kind: Secret
        name: cloudflared-secret
`))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nnamespace: flux-system\nresources:\n  - ./externalsecret.yaml\n  - ./cm.yaml\n")
	// The ExternalSecret materializing cloudflared-secret enumerates its output
	// keys via target.template.data (the dataFrom feeds the template's inputs).
	testutil.WriteFile(t, dir, "apps/externalsecret.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: cloudflared}
spec:
  secretStoreRef: {kind: ClusterSecretStore, name: onepassword}
  target:
    name: cloudflared-secret
    template:
      data:
        CLOUDFLARE_TUNNEL_ID: "{{ .id }}"
        identity: "{{ .identity }}"
  dataFrom:
    - extract: {key: cloudflare}
`)
	// A resource consuming the secret-sourced var through postBuild substitution.
	testutil.WriteFile(t, dir, "apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: tunnel\ndata:\n  target: \"${CLOUDFLARE_TUNNEL_ID}.cfargotunnel.com\"\n")

	o, err := New(t.Context(), Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	repo := &manifest.GitRepository{Name: "ssh", Namespace: "flux-system"}
	repo.URL = "ssh://git@example.test/cluster"
	repo.SecretRef = &manifest.LocalObjectReference{Name: "cloudflared-secret"}
	_, err = o.src.Fetchers[manifest.KindGitRepository].Fetch(t.Context(), repo, sourcecache.Policy{})
	if missing, ok := errors.AsType[*source.MissingSecretError](err); !ok || missing.Detail != "missing 'identity' for SSH auth" {
		t.Fatalf("producer-declared credential did not retain placeholder classification: %v", err)
	}

	// The ${CLOUDFLARE_TUNNEL_ID} resolved to its placeholder, not empty/literal.
	want := fmt.Sprintf(manifest.ValuePlaceholderTemplate, "CLOUDFLARE_TUNNEL_ID") + ".cfargotunnel.com"
	var got string
	for _, docs := range res.Manifests {
		for _, d := range docs {
			md, _ := d["metadata"].(map[string]any)
			data, _ := d["data"].(map[string]any)
			if d["kind"] == "ConfigMap" && md["name"] == "tunnel" {
				got, _ = data["target"].(string)
			}
		}
	}
	if got != want {
		t.Errorf("tunnel ConfigMap data.target = %q, want %q", got, want)
	}
	// No unreadable-secret advisory: a producer-declared secret is readable as placeholders.
	for _, w := range res.Warnings {
		if w.Category == manifest.WarnUnresolvedSubstitution && strings.Contains(w.Message, "cloudflared-secret") {
			t.Errorf("a producer-declared secret must not warn as unreadable; got %+v", w)
		}
	}
}
