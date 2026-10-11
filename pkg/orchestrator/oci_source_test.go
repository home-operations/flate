package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type generatedOCIFetcher struct {
	root    string
	err     error
	fetched atomic.Bool
}

func (f *generatedOCIFetcher) Fetch(_ context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	if obj.Named().Name == "app" {
		f.fetched.Store(true)
	}
	if f.err != nil {
		return nil, f.err
	}
	return &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: f.root, Digest: "sha256:fixture"}, nil
}

func TestRender_DeferredOCIArtifactRoot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		producer bool
		fetchErr error
	}{
		{name: "missing"},
		{name: "produced", producer: true},
		{name: "fetch_failed", producer: true, fetchErr: fmt.Errorf("%w: fixture fetch failed", manifest.ErrFlux)},
	} {
		for _, workers := range []int{2, 8} {
			t.Run(fmt.Sprintf("%s_workers_%d", tc.name, workers), func(t *testing.T) {
				root, artifact := t.TempDir(), t.TempDir()
				testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  path: ./
  sourceRef: {kind: OCIRepository, name: app}
`)
				if tc.producer {
					testutil.WriteFile(t, root, "flux/rs.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: producer, namespace: flux-system}
spec:
  inputs: [{name: app}]
  resourcesTemplate: |
    apiVersion: source.toolkit.fluxcd.io/v1
    kind: OCIRepository
    metadata: {name: << inputs.name >>, namespace: flux-system}
    spec: {url: oci://fixture.invalid/app, interval: 1h, ref: {tag: latest}}
`)
				}
				testutil.WriteFile(t, artifact, "kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, artifact, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output}\n")
				o, err := New(Config{Path: root + "/flux", RepoRoot: root, CacheDir: t.TempDir(), Concurrency: workers})
				if err != nil {
					t.Fatal(err)
				}
				f := &generatedOCIFetcher{root: artifact, err: tc.fetchErr}
				o.WithFetcher(manifest.KindOCIRepository, f)
				res, err := o.Render(t.Context())
				if (err == nil) != (tc.producer && tc.fetchErr == nil) {
					t.Fatalf("Render error=%v", err)
				}
				id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "consumer"}
				if tc.fetchErr != nil {
					o.ksc.ReconcileNode(t.Context(), id, manifest.DrainCascade)
				}
				status, _ := o.Store().GetStatus(id)
				if tc.fetchErr != nil {
					if !f.fetched.Load() || status.Status != store.StatusFailed || !strings.Contains(status.Message, "fixture fetch failed") || strings.Contains(status.Message, "unknown OCI artifact root") {
						t.Fatalf("fetched=%t status=%+v", f.fetched.Load(), status)
					}
				} else if tc.producer {
					if !f.fetched.Load() || status.Status != store.StatusReady || len(res.Manifests[id]) != 1 {
						t.Fatalf("fetched=%t status=%+v manifests=%v", f.fetched.Load(), status, res.Manifests[id])
					}
					if name, _ := manifest.DocMetadata(res.Manifests[id][0]); name != "artifact-output" {
						t.Fatalf("rendered %q from the working tree instead of the OCI artifact", name)
					}
				} else if f.fetched.Load() || status.Status != store.StatusFailed || !strings.Contains(status.Message, "unknown OCI artifact root") {
					t.Fatalf("fetched=%t status=%+v", f.fetched.Load(), status)
				}
			})
		}
	}
}
