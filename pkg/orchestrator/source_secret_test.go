package orchestrator

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// secretChartFetcher checks the source's CA Secret and returns a local
// chart, so the test exercises Kustomize and Helm without a registry.
type secretChartFetcher struct {
	store *store.Store
	chart string
}

func (f *secretChartFetcher) Fetch(_ context.Context, obj manifest.BaseManifest, _ sourcecache.Policy) (*store.SourceArtifact, error) {
	repo := obj.(*manifest.OCIRepository)
	secret, ok := f.store.Get[*manifest.Secret](manifest.NamedResource{Kind: manifest.KindSecret, Namespace: repo.Namespace, Name: repo.CertSecretRef.Name})
	if !ok {
		return nil, source.MissingSecretErr(repo.Named(), repo.CertSecretRef.Name, "not found")
	}
	if got, _, _ := secret.Value("ca.crt"); got != "rendered-ca" {
		return nil, fmt.Errorf("source: CA = %q, want rendered-ca", got)
	}
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	return &store.SourceArtifact{
		Kind: manifest.KindOCIRepository, LocalPath: f.chart, Digest: digest, Revision: "0.1.0@" + digest, Key: digest, FS: os.DirFS(f.chart),
	}, nil
}

func TestOrchestrator_SourceSecretRenderedBeforeFetch(t *testing.T) {
	tests := []struct {
		name      string
		secret    string
		kustomize string
		ref       string
		postBuild string
	}{
		{
			name: "plain",
			secret: `metadata:
  name: ca
  namespace: flux-system
stringData:
  ca.crt: rendered-ca
`,
			kustomize: "resources: [secret.yaml]\n",
			ref:       "ca",
		},
		{
			name: "target namespace",
			secret: `metadata:
  name: ca
stringData:
  ca.crt: rendered-ca
`,
			kustomize: "resources: [secret.yaml]\n",
			ref:       "ca",
			postBuild: "  targetNamespace: flux-system\n",
		},
		{
			name: "renamed",
			secret: `metadata:
  name: ca
  namespace: flux-system
stringData:
  ca.crt: rendered-ca
`,
			kustomize: "namePrefix: rendered-\nresources: [secret.yaml]\n",
			ref:       "rendered-ca",
		},
		{
			name: "generated with hash",
			kustomize: `secretGenerator:
  - name: ca
    literals: [ca.crt=rendered-ca]
`,
			ref:       "ca",
			postBuild: "  targetNamespace: flux-system\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			chart := t.TempDir()
			testutil.WriteFile(t, chart, "Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
			testutil.WriteFile(t, chart, "templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: chart-output
`)
			testutil.WriteFile(t, dir, "flux/apps.yaml", ksYAML("apps", "apps", "")+tt.postBuild)
			testutil.WriteFile(t, dir, "apps/kustomization.yaml", `resources:
  - oci.yaml
  - hr.yaml
  - ca
configurations:
  - references.yaml
`)
			testutil.WriteFile(t, dir, "apps/references.yaml", `nameReference:
  - kind: Secret
    fieldSpecs:
      - kind: OCIRepository
        path: spec/certSecretRef/name
`)
			testutil.WriteFile(t, dir, "apps/ca/kustomization.yaml", tt.kustomize)
			if tt.secret != "" {
				testutil.WriteFile(t, dir, "apps/ca/secret.yaml", "apiVersion: v1\nkind: Secret\n"+tt.secret)
			}
			testutil.WriteFile(t, dir, "apps/oci.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: charts
  namespace: flux-system
spec:
  url: oci://example.invalid/charts
  ref:
    tag: 0.1.0
  certSecretRef:
    name: %s
`, tt.ref))
			testutil.WriteFile(t, dir, "apps/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: demo
  namespace: flux-system
spec:
  interval: 5m
  chartRef:
    kind: OCIRepository
    name: charts
`)
			o, err := New(t.Context(), Config{
				Path: dir, CacheDir: t.TempDir(), WipeSecrets: true,
				Concurrency: 4,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			o.WithFetcher(manifest.KindOCIRepository, &secretChartFetcher{store: o.Store(), chart: chart})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			res, err := o.Render(ctx)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			for _, id := range []manifest.NamedResource{
				{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "charts"},
				testutil.HelmReleaseID("flux-system", "demo"),
			} {
				if info, ok := o.Store().GetStatus(id); !ok || info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped {
					t.Errorf("%s status = %+v (present=%t), want Ready without a skip", id, info, ok)
				}
			}
			hrID := testutil.HelmReleaseID("flux-system", "demo")
			if docs := res.Manifests[hrID]; len(docs) != 1 || manifest.DocKind(docs[0]) != manifest.KindConfigMap {
				t.Fatalf("HelmRelease output = %+v, want the chart's ConfigMap", docs)
			}
		})
	}
}

func TestOrchestrator_OCIFetchUsesRenderedCA(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "flux/apps.yaml", ksYAML("apps", "apps", ""))
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [oci.yaml, ca.yaml]\n")
	testutil.WriteFile(t, dir, "apps/ca.yaml", fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: ca
  namespace: flux-system
stringData:
  ca.crt: %q
`, cert))
	testutil.WriteFile(t, dir, "apps/oci.yaml", fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: charts
  namespace: flux-system
spec:
  url: oci://%s/charts
  ref:
    tag: 0.1.0
  certSecretRef:
    name: ca
`, strings.TrimPrefix(server.URL, "https://")))
	o, err := New(t.Context(), Config{
		Path: dir, CacheDir: t.TempDir(), WipeSecrets: true,
		AllowMissingSecrets: true, Concurrency: 4,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = o.Render(ctx)
	// The registry returns 404. Reaching its handler proves the OCI fetcher
	// read the rendered CA Secret and passed TLS verification.
	if requests.Load() == 0 {
		t.Fatalf("OCI fetch never reached the TLS server: %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "charts:0.1.0: not found") {
		t.Fatalf("Render error = %v, want the registry's not-found response", err)
	}
}
