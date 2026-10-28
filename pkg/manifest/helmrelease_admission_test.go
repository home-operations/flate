package manifest

import (
	"errors"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseHelmRelease_ChartTemplateAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, spec, field string
	}{
		{"default kind", "chart: {spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}}", ""},
		{"explicit HelmChart", "chart: {kind: HelmChart, spec: {chart: ./app, sourceRef: {kind: GitRepository, name: charts, namespace: sources}}}", ""},
		{"OCI", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app'}}", ""},
		{"OCI star version", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', version: '*', reconcileStrategy: ChartVersion}}", ""},
		{"OCI null fields", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', chart: null, sourceRef: null, version: null, reconcileStrategy: null, valuesFiles: null, ignoreMissingValuesFiles: null}}", ""},
		{"null kind", "chart: {kind: null, spec: {chart: app, sourceRef: {kind: Bucket, name: charts}, url: null}}", ""},
		{"null chart", "chart: null\nchartRef: {kind: OCIRepository, name: chart}", ""},
		{"null optional fields", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', interval: null, timeout: null, provider: null, layerSelector: {operation: null}, verify: {provider: null}}}", ""},
		{"HelmChart OCI fields", "chart: {spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}, provider: aws, layerSelector: {}, ref: {tag: latest}, serviceAccountName: pull, insecure: true, verify: {provider: notation}}}", ""},
		{"HelmChart empty version", "chart: {spec: {chart: app, version: '', sourceRef: {kind: HelmRepository, name: charts}}}", ""},
		{"no chart", "{}", "chart"},
		{"both charts", "chart: {spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}}\nchartRef: {kind: OCIRepository, name: chart}", "chartRef"},
		{"empty kind", "chart: {kind: '', spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}}", "kind"},
		{"unknown kind", "chart: {kind: Bucket, spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}}", "kind"},
		{"wrong kind type", "chart: {kind: false, spec: {}}", "decode"},
		{"OCI missing URL", "chart: {kind: OCIRepository, spec: {}}", "url"},
		{"OCI empty URL", "chart: {kind: OCIRepository, spec: {url: ''}}", "url"},
		{"OCI URL scheme", "chart: {kind: OCIRepository, spec: {url: 'https://registry.example/app'}}", "url"},
		{"OCI empty chart", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', chart: ''}}", "chart"},
		{"OCI empty source", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', sourceRef: {}}}", "HelmChart-only"},
		{"OCI empty values files", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', valuesFiles: []}}", "valuesFiles"},
		{"OCI false ignore", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', ignoreMissingValuesFiles: false}}", "ignoreMissingValuesFiles"},
		{"OCI empty version", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', version: ''}}", "version"},
		{"OCI concrete version", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', version: '1.0.0'}}", "HelmChart-only"},
		{"OCI empty strategy", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', reconcileStrategy: ''}}", "reconcileStrategy"},
		{"OCI revision strategy", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', reconcileStrategy: Revision}}", "HelmChart-only"},
		{"HelmChart URL presence", "chart: {spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}, url: ''}}", "url"},
		{"HelmChart no chart", "chart: {spec: {sourceRef: {kind: HelmRepository, name: charts}}}", "chart"},
		{"HelmChart empty chart", "chart: {spec: {chart: '', sourceRef: {kind: HelmRepository, name: charts}}}", "chart"},
		{"HelmChart no source", "chart: {spec: {chart: app}}", "sourceRef"},
		{"HelmChart null source", "chart: {spec: {chart: app, sourceRef: null}}", "sourceRef"},
		{"HelmChart no source kind", "chart: {spec: {chart: app, sourceRef: {name: charts}}}", "sourceRef"},
		{"HelmChart bad source kind", "chart: {spec: {chart: app, sourceRef: {kind: OCIRepository, name: charts}}}", "sourceRef"},
		{"HelmChart empty source namespace", "chart: {spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts, namespace: ''}}}", "namespace"},
		{"empty provider", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', provider: ''}}", "provider"},
		{"unknown provider", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', provider: other}}", "provider"},
		{"empty operation", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', layerSelector: {operation: ''}}}", "operation"},
		{"unknown operation", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', layerSelector: {operation: other}}}", "operation"},
		{"timeout hours", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', timeout: 1h}}", "timeout"},
		{"negative interval", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', interval: '-1m'}}", "interval"},
		{"wrong field type", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', valuesFiles: false}}", "decode"},
		{"missing credential name", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', secretRef: {}}}", "secretRef.name"},
		{"empty verification provider", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', verify: {provider: ''}}}", "verify.provider"},
		{"unknown verification provider", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', verify: {provider: other}}}", "verify.provider"},
		{"missing OIDC subject", "chart: {kind: OCIRepository, spec: {url: 'oci://registry.example/app', verify: {matchOIDCIdentity: [{issuer: issuer}]}}}", "matchOIDCIdentity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var spec map[string]any
			assert.NoError(t, yaml.Unmarshal([]byte(tc.spec), &spec))
			doc := map[string]any{"apiVersion": HelmReleaseDomain + "/v2", "kind": KindHelmRelease,
				"metadata": map[string]any{"name": "app", "namespace": "apps"}, "spec": spec}
			before := DeepCopyMap(doc)
			obj, _, _, err := ParseDoc(doc, ParseDocOptions{})
			assert.Diff(t, doc, before)
			if tc.field == "" {
				assert.NoError(t, err)
				if obj == nil {
					t.Fatal("valid declaration was not admitted")
				}
				return
			}
			if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("error = %v, want classified input error identifying %s", err, tc.field)
			}
		})
	}
}
