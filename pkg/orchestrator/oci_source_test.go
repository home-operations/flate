package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

type generatedOCIFetcher struct {
	root      string
	err       error
	fetched   atomic.Bool
	wantRetry sourcecache.RetryConfig
}

func (f *generatedOCIFetcher) Fetch(ctx context.Context, obj manifest.BaseManifest, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	if reads == nil {
		return nil, fmt.Errorf("missing captured inputs")
	}
	if policy.Retry != f.wantRetry {
		return nil, fmt.Errorf("unexpected retry: %+v", policy.Retry)
	}
	if obj.Named().Name == "app" {
		f.fetched.Store(true)
	}
	if f.err != nil {
		return nil, f.err
	}
	location, err := gitrepo.Locate(ctx, f.root, f.root)
	if err != nil {
		return nil, err
	}
	input, err := gittree.AdmitDirectory(ctx, location)
	if err != nil {
		return nil, err
	}
	input.Kind, input.Digest = manifest.KindOCIRepository, "sha256:fixture"
	return input, nil
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
		t.Run(tc.name, func(t *testing.T) {
			root, artifact := t.TempDir(), t.TempDir()
			testutil.WriteFile(t, root, "flux/ks.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./payload\n  sourceRef: {kind: OCIRepository, name: app}\n"))
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
			testutil.WriteFile(t, artifact, "payload/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, artifact, "payload/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output}\n")
			retry := sourcecache.RetryConfig{Attempts: 2}
			o, err := New(t.Context(), Config{SourceRetry: retry, Path: root + "/flux", RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			f := &generatedOCIFetcher{root: artifact, err: tc.fetchErr, wantRetry: retry}
			o.WithFetcher(manifest.KindOCIRepository, f)
			res, err := o.Render(t.Context())
			if (err == nil) != (tc.producer && tc.fetchErr == nil) {
				t.Fatalf("Render error=%v", err)
			}
			id := testutil.KustomizationID("flux-system", "consumer")
			if tc.fetchErr != nil {
				o.ksc.ReconcileNode(t.Context(), id, manifest.DrainCascade)
			}
			status, _ := o.Store().GetStatus(id)
			if tc.fetchErr != nil {
				if !f.fetched.Load() || status.Status != store.StatusFailed || !strings.Contains(status.Message, "fixture fetch failed") || strings.Contains(status.Message, "unknown OCI artifact root") {
					t.Fatalf("fetched=%t status=%+v", f.fetched.Load(), status)
				}
			} else if tc.producer {
				if !f.fetched.Load() || status.Status != store.StatusReady || len(outputByParent(res.Manifests)[id]) != 1 {
					t.Fatalf("fetched=%t status=%+v manifests=%v", f.fetched.Load(), status, outputByParent(res.Manifests)[id])
				}
				if name, _ := manifest.DocMetadata(outputByParent(res.Manifests)[id][0]); name != "artifact-output" {
					t.Fatalf("rendered %q from the working tree instead of the OCI artifact", name)
				}
			} else if f.fetched.Load() || status.Status != store.StatusFailed || !strings.Contains(status.Message, "unknown OCI artifact root") || len(status.BlockedBy()) != 0 {
				t.Fatalf("fetched=%t status=%+v", f.fetched.Load(), status)
			}
		})
	}
}
