package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/schedule"
	src "github.com/home-operations/flate/internal/source"

	"github.com/home-operations/flate/internal/assert"

	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gitrepo"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

type generatedOCIFetcher struct {
	root      string
	err       error
	fetched   atomic.Bool
	calls     atomic.Int64
	wantRetry sourcecache.RetryConfig
}

func (f *generatedOCIFetcher) Fetch(
	_ context.Context,
	obj manifest.BaseManifest,
	reads *store.ReadSet,
	policy *sourcecache.Policy,
) (src.PreparedFetch, error) {
	return src.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}

			f.calls.Add(1)
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
		},
	}, nil
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_DeferredOCIArtifactRoot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		producer bool
		fetchErr error
	}{
		{name: "missing"},
		{name: "produced", producer: true},
		{name: "fetch_failed", producer: true, fetchErr: manifest.NewError(manifest.ErrFlux, nil, "fixture fetch failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testRenderDeferredOCIArtifactRootCase(t, &tc)
		},
		)
	}
}

func testRenderDeferredOCIArtifactRootCase(t *testing.T, testCaseInput *struct {
	name string

	producer bool
	fetchErr error
},
) {
	t.Helper()

	root, artifact := t.TempDir(), t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"flux-system",
			"  path: ./payload\n  sourceRef: {kind: OCIRepository, name: app}\n",
		),
	)
	if testCaseInput.producer {
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
	testutil.WriteFile(
		t,
		artifact,
		"payload/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: artifact-output}\n",
	)
	retry := sourcecache.RetryConfig{Attempts: 2}
	o, err := newTestOrchestrator(
		t,
		Tree{Path: root + "/flux", RepoRoot: root},
		Config{SourceRetry: retry, CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	f := &generatedOCIFetcher{root: artifact, err: testCaseInput.fetchErr, wantRetry: retry}
	o.src.Fetchers[manifest.KindOCIRepository] = f
	res, err := o.render(t.Context(), t)
	assert.FatalIf(
		t,
		(err == nil) != (testCaseInput.producer && testCaseInput.fetchErr == nil),
		"Render error=%v",
		err,
	)
	id := testutil.KustomizationID("flux-system", "consumer")
	if testCaseInput.fetchErr != nil {
		o.ksc.ReconcileNode(t.Context(), id, manifest.DrainCascade, schedule.Result{})
	}
	status, _ := o.Store().GetStatus(id)
	switch {
	case testCaseInput.fetchErr != nil:
		if !f.fetched.Load() || status.Status != store.StatusFailed ||
			!strings.Contains(status.Message, "fixture fetch failed") ||
			strings.Contains(status.Message, "unknown OCI artifact root") {
			t.Fatalf("fetched=%t status=%+v", f.fetched.Load(), status)
		}
	case testCaseInput.producer:
		if !f.fetched.Load() || status.Status != store.StatusReady ||
			len(outputByParent(res.Manifests)[id]) != 1 {
			t.Fatalf(
				"fetched=%t status=%+v manifests=%v",
				f.fetched.Load(),
				status,
				outputByParent(res.Manifests)[id],
			)
		}
		{
			metadata := manifest.DocMetadata(outputByParent(res.Manifests)[id][0])
			name := metadata.Name
			assert.FatalIf(
				t,
				name != "artifact-output",
				"rendered %q from the working tree instead of the OCI artifact",
				name,
			)
		}
	default:
		missing := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "flux-system",
			Name:      "app",
		}
		if f.fetched.Load() || status.Status != store.StatusFailed ||
			!slices.Contains(status.BlockedBy(), missing) {
			t.Fatalf("fetched=%t status=%+v", f.fetched.Load(), status)
		}
		assert.FatalIf(
			t,
			len(outputByParent(res.Manifests)[id]) != 0,
			"%v",
			"missing source emitted consumer payload",
		)
	}
}
