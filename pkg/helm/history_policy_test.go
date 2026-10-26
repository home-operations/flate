package helm

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestTemplate_HistoryPolicyProjection(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, chartDir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n")
	directory := t.TempDir()
	renderer := NewClient(cachetest.New(t, directory, cache.New))
	hr := newHR()
	assert.NoError(t, json.Unmarshal([]byte(`{"chart":{"spec":{"chart":"mychart","sourceRef":{"kind":"GitRepository","name":"chart-repo","namespace":"flux-system"}}}}`), &hr.HelmReleaseSpec))
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", chartDir)}
	_, receipt, err := renderer.Template(t.Context(), hr, nil, input)
	assert.NoError(t, err)
	child := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "child"}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Refs: []manifest.NamedResource{manifest.BootstrapSourceID}, ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"apps/blue"}}}}
	assert.NoError(t, renderer.RecordRender(t.Context(), hr.Named(), receipt, maps.All(records)))
	indexPath := filepath.Join(directory, "oci", "index.json")
	index, err := os.ReadFile(indexPath)
	assert.NoError(t, err)
	input.Admit = func(string) (bool, error) { t.Fatal("history called Admit"); return false, nil }
	for _, tc := range []struct {
		name, patch string
		complete    bool
	}{
		{"common labels", `{"commonMetadata":{"labels":{"rev":"changed"}}}`, true},
		{"common empty", `{"commonMetadata":{}}`, true},
		{"interval", `{"interval":"5m"}`, true},
		{"kube config", `{"kubeConfig":{"secretRef":{"name":"cluster"}}}`, true},
		{"storage namespace", `{"storageNamespace":"storage"}`, true},
		{"timeout", `{"timeout":"5m"}`, true},
		{"max history", `{"maxHistory":7}`, true},
		{"service account", `{"serviceAccountName":"controller"}`, true},
		{"persistent client", `{"persistentClient":false}`, true},
		{"drift detection", `{"driftDetection":{"mode":"enabled"}}`, true},
		{"rollback", `{"rollback":{"timeout":"5m"}}`, true},
		{"uninstall", `{"uninstall":{"timeout":"5m"}}`, true},
		{"wait strategy", `{"waitStrategy":{"name":"poller"}}`, true},
		{"chart labels", `{"chart":{"metadata":{"labels":{"team":"changed"}}}}`, true},
		{"chart annotations", `{"chart":{"metadata":{"annotations":{"note":"changed"}}}}`, true},
		{"chart interval", `{"chart":{"spec":{"interval":"5m"}}}`, true},
		{"chart reconcile strategy", `{"chart":{"spec":{"reconcileStrategy":"Revision"}}}`, true},
		{"chart verification", `{"chart":{"spec":{"verify":{"provider":"cosign"}}}}`, true},
		{"prepared chart name", `{"chart":{"spec":{"chart":"already-resolved"}}}`, true},
		{"prepared chart version", `{"chart":{"spec":{"version":"2.0.0"}}}`, true},
		{"prepared chart values files", `{"chart":{"spec":{"valuesFiles":["already-resolved.yaml"]}}}`, true},
		{"prepared chart missing values", `{"chart":{"spec":{"ignoreMissingValuesFiles":true}}}`, true},
		{"prepared inline values", `{"values":{"already":"resolved"}}`, true},
		{"install wait", `{"install":{"disableWait":true}}`, true},
		{"install wait jobs", `{"install":{"disableWaitForJobs":true}}`, true},
		{"install timeout", `{"install":{"timeout":"5m"}}`, true},
		{"install remediation", `{"install":{"remediation":{"retries":2}}}`, true},
		{"install strategy", `{"install":{"strategy":{"name":"RetryOnFailure","retryInterval":"2m"}}}`, true},
		{"install ownership", `{"install":{"disableTakeOwnership":true}}`, true},
		{"install namespace", `{"install":{"createNamespace":true}}`, true},
		{"install apply", `{"install":{"serverSideApply":true}}`, true},
		{"prepared install skip crds", `{"install":{"skipCRDs":true}}`, true},
		{"prepared install crds", `{"install":{"crds":"Skip"}}`, true},
		{"prepared install schema", `{"install":{"disableSchemaValidation":true}}`, true},
		{"prepared install openapi", `{"install":{"disableOpenAPIValidation":true}}`, true},
		{"upgrade wait", `{"upgrade":{"disableWait":true}}`, true},
		{"upgrade wait jobs", `{"upgrade":{"disableWaitForJobs":true}}`, true},
		{"upgrade timeout", `{"upgrade":{"timeout":"5m"}}`, true},
		{"upgrade remediation", `{"upgrade":{"remediation":{"retries":2}}}`, true},
		{"upgrade strategy", `{"upgrade":{"strategy":{"name":"RetryOnFailure","retryInterval":"2m"}}}`, true},
		{"upgrade ownership", `{"upgrade":{"disableTakeOwnership":true}}`, true},
		{"upgrade cleanup", `{"upgrade":{"cleanupOnFail":true}}`, true},
		{"upgrade force", `{"upgrade":{"force":true}}`, true},
		{"upgrade preserve values", `{"upgrade":{"preserveValues":true}}`, true},
		{"upgrade chart lifecycle", `{"upgrade":{"chartNameChangeStrategy":"InPlaceUpdate"}}`, true},
		{"upgrade apply", `{"upgrade":{"serverSideApply":"enabled"}}`, true},
		{"prepared upgrade crds", `{"upgrade":{"crds":"Skip"}}`, true},
		{"prepared upgrade schema", `{"upgrade":{"disableSchemaValidation":true}}`, true},
		{"prepared upgrade openapi", `{"upgrade":{"disableOpenAPIValidation":true}}`, true},
		{"test failure", `{"test":{"ignoreFailures":true}}`, true},
		{"test timeout", `{"test":{"timeout":"5m"}}`, true},
		{"test filters", `{"test":{"filters":[{"name":"ignored","exclude":true}]}}`, true},
		{"suspend", `{"suspend":true}`, false},
		{"declared chart kind", `{"chart":{"spec":{"sourceRef":{"kind":"Bucket"}}}}`, false},
		{"declared chart name", `{"chart":{"spec":{"sourceRef":{"name":"other"}}}}`, false},
		{"declared chart namespace", `{"chart":{"spec":{"sourceRef":{"namespace":"other"}}}}`, false},
		{"chart ref", `{"chartRef":{"kind":"HelmChart","name":"other","namespace":"flux-system"}}`, false},
		{"depends on", `{"dependsOn":[{"name":"dependency","readyExpr":"true"}]}`, false},
		{"values from", `{"valuesFrom":[{"kind":"ConfigMap","name":"values","valuesKey":"key","targetPath":"root","optional":true}]}`, false},
		{"health expressions", `{"healthCheckExprs":[{"apiVersion":"v1","kind":"Pod","current":"true"}]}`, false},
		{"post render strategy", `{"postRenderStrategy":"separate"}`, false},
		{"common annotations", `{"commonMetadata":{"annotations":{"policy":"changed"}}}`, false},
		{"release name", `{"releaseName":"different"}`, false},
		{"target namespace", `{"targetNamespace":"different"}`, false},
		{"post renderers", `{"postRenderers":[{"kustomize":{"images":[{"name":"image","newTag":"different"}]}}]}`, false},
		{"install hooks", `{"install":{"disableHooks":true}}`, false},
		{"install replace", `{"install":{"replace":true}}`, false},
		{"upgrade hooks", `{"upgrade":{"disableHooks":true}}`, false},
		{"test enable", `{"test":{"enable":true}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := hr.Clone()
			assert.NoError(t, json.Unmarshal([]byte(tc.patch), &changed.HelmReleaseSpec))
			if manifest.Fingerprint(hr.HelmReleaseSpec) == manifest.Fingerprint(changed.HelmReleaseSpec) {
				t.Fatal("fixture did not change a decoded declaration input")
			}
			previous, complete := renderer.PreviousRender(t.Context(), changed, nil, input)
			assert.Equal(t, complete, tc.complete)
			if complete {
				assert.Diff(t, maps.Collect(previous), records)
			}
			after, err := os.ReadFile(indexPath)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Equal(index, after), true)
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*manifest.HelmRelease, *RenderOptions)
	}{
		{"effective chart", func(h *manifest.HelmRelease, _ *RenderOptions) { h.Chart.Name = "different" }},
		{"effective version", func(h *manifest.HelmRelease, _ *RenderOptions) { h.Chart.Version = "different" }},
		{"effective values files", func(h *manifest.HelmRelease, _ *RenderOptions) { h.ChartValuesFiles = []string{"different.yaml"} }},
		{"effective missing values", func(h *manifest.HelmRelease, _ *RenderOptions) { h.IgnoreMissingValuesFiles = true }},
		{"effective crds", func(h *manifest.HelmRelease, _ *RenderOptions) { h.CRDsPolicy = "Skip" }},
		{"effective schema", func(h *manifest.HelmRelease, _ *RenderOptions) { h.DisableSchemaValidation = true }},
		{"effective openapi", func(h *manifest.HelmRelease, _ *RenderOptions) { h.DisableOpenAPIValidation = true }},
		{"effective values", func(h *manifest.HelmRelease, _ *RenderOptions) { h.Values = map[string]any{"different": true} }},
		{"skip kinds", func(_ *manifest.HelmRelease, o *RenderOptions) { o.SkipKinds = []string{"ConfigMap"} }},
		{"skip crds", func(_ *manifest.HelmRelease, o *RenderOptions) { o.SkipCRDs = true }},
		{"skip tests", func(_ *manifest.HelmRelease, o *RenderOptions) { o.SkipTests = true }},
		{"kube version", func(_ *manifest.HelmRelease, o *RenderOptions) { o.KubeVersion = "1.31.0" }},
		{"api versions", func(_ *manifest.HelmRelease, o *RenderOptions) { o.APIVersions = "custom.io/v1" }},
		{"upgrade", func(_ *manifest.HelmRelease, o *RenderOptions) { o.IsUpgrade = true }},
		{"no hooks", func(_ *manifest.HelmRelease, o *RenderOptions) { o.NoHooks = true }},
		{"show only", func(_ *manifest.HelmRelease, o *RenderOptions) { o.ShowOnly = []string{"different.yaml"} }},
		{"dns", func(_ *manifest.HelmRelease, o *RenderOptions) { o.EnableDNS = true }},
		{"digest policy", func(_ *manifest.HelmRelease, o *RenderOptions) { o.DisableChartDigestTracking = true }},
		{"wipe policy", func(_ *manifest.HelmRelease, o *RenderOptions) { o.WipeSecrets = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed, options := hr.Clone(), input
			tc.change(changed, &options)
			if _, complete := renderer.PreviousRender(t.Context(), changed, changed.Values, options); complete {
				t.Fatal("changed prepared input borrowed accepted history")
			}
		})
	}
	t.Run("metadata acceptance writes nothing", func(t *testing.T) {
		t.Parallel()
		changed := hr.Clone()
		assert.NoError(t, json.Unmarshal([]byte(`{"commonMetadata":{"labels":{"rev":"changed"}}}`), &changed.HelmReleaseSpec))
		_, next, err := renderer.Template(t.Context(), changed, nil, RenderOptions{Sources: input.Sources})
		assert.NoError(t, err)
		assert.NoError(t, renderer.RecordRender(t.Context(), changed.Named(), next, maps.All(records)))
		after, err := os.ReadFile(indexPath)
		assert.NoError(t, err)
		assert.Equal(t, bytes.Equal(index, after), true)
	})
}
