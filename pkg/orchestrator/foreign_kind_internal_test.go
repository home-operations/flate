package orchestrator

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestOrchestrator_ForeignBucketKindNotFetched reproduces issue #872: a
// Kustomization renders a seaweed.seaweedfs.com Bucket, whose kind name
// collides with the Flux source.toolkit.fluxcd.io Bucket. Routing source
// nodes on Kind alone handed it to the Flux Bucket fetcher, which rejected
// it ("Bucket fetcher: unexpected payload *manifest.RawObject") and failed
// the whole run. It must reconcile as an ordinary rendered resource: no
// status, no fetch, and still present in the parent's output.
func TestOrchestrator_ForeignBucketKindNotFetched(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"ks.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef:\n    kind: GitRepository\n    name: flux-system\n    namespace: flux-system\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- bucket.yaml\n")
	testutil.WriteFile(t, dir, "apps/bucket.yaml", `apiVersion: seaweed.seaweedfs.com/v1
kind: Bucket
metadata:
  name: greptimedb
  namespace: observability
spec:
  name: greptimedb
  reclaimPolicy: Retain
  clusterRef:
    name: example
    namespace: seaweedfs
`)

	renderedSingleton1, err := RenderTrees(
		context.Background(),
		[]Tree{{Path: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
		Config{},
	)
	assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
	o := renderedSingleton1[0].Orchestrator
	res := renderedSingleton1[0].Result
	assert.FatalIf(t, err != nil, "Render: %v", err)
	assert.ErrorIf(t, len(res.Failed) != 0,
		"foreign Bucket CRD must not fail the run; failed=%+v", res.Failed)

	id := manifest.NamedResource{
		Group:     "seaweed.seaweedfs.com",
		Kind:      manifest.KindBucket,
		Namespace: "observability",
		Name:      "greptimedb",
	}
	{
		info, ok := o.Store().GetStatus(id)
		assert.ErrorIf(t, ok, "foreign Bucket CRD is not a source node, so it must carry no status; got %+v", info)
	}
	{
		art := o.Store().GetArtifact(id, store.ArtifactContent)
		assert.ErrorIf(t, art != nil, "foreign Bucket CRD must not be fetched; artifact=%+v", art)
	}
	obj := o.Store().GetObject(id)
	assert.FatalIf(t, manifest.KindOf(obj).Tracked, "stored object = %T, want *manifest.RawObject", obj)

	// The resource still renders — dropping it would show up in a diff as a
	// deletion of a live resource.
	ksID := testutil.KustomizationID("flux-system", "apps")
	var found bool
	for _, doc := range outputByParent(res.Manifests)[ksID] {
		metadata := manifest.DocMetadata(doc)
		name, ns := metadata.Name, metadata.Namespace
		if manifest.DocKind(doc) == manifest.KindBucket && ns == id.Namespace && name == id.Name {
			found = true
			{
				got := manifest.DocAPIVersion(doc)
				assert.ErrorIf(
					t,
					got != "seaweed.seaweedfs.com/v1",
					"apiVersion = %q, want the foreign group verbatim",
					got,
				)
			}
		}
	}
	assert.ErrorIf(t, !found,
		"foreign Bucket missing from the parent's rendered output: %+v", outputByParent(res.Manifests)[ksID])

	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease, manifest.KindResourceSet} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testOrchestratorForeignBucketKindNotFetchedCase2(
				t,
				testOrchestratorForeignBucketKindNotFetchedCase2Inputs{o: &o, kind: &kind},
			)
		},
		)
	}
}

// TestOrchestrator_FluxBucketStillScheduled is the positive control for the
// #872 fix: a real source.toolkit.fluxcd.io Bucket must still reach the source
// controller. It has no reachable endpoint here, so failing the fetch is the
// proof it was dispatched at all.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestOrchestrator_FluxBucketStillScheduled(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bucket.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata:
  name: minio
  namespace: flux-system
spec:
  interval: 5m
  provider: generic
  bucketName: podinfo
  endpoint: 127.0.0.1:1
  insecure: true
`)

	o, err := newTestOrchestrator(t, Tree{Path: dir}, Config{})
	assert.FatalIf(t, err != nil, "New: %v", err)
	{
		_, err := o.render(context.Background())
		assert.FatalIf(t, err == nil, "%v", "expected the unreachable Flux Bucket's fetch to fail")
	}
	id := manifest.NamedResource{
		Group:     "source.toolkit.fluxcd.io",
		Kind:      manifest.KindBucket,
		Namespace: "flux-system",
		Name:      "minio",
	}
	info, ok := o.Store().GetStatus(id)
	assert.FatalIf(
		t,
		!ok || info.Status != store.StatusFailed,
		"Flux Bucket status = %+v (present=%v), want Failed from the fetch attempt",
		info,
		ok,
	)
}

type testOrchestratorForeignBucketKindNotFetchedCaseInputs struct {
	o    **Orchestrator
	kind *string
}

func testOrchestratorForeignBucketKindNotFetchedCase(
	t *testing.T,
	testCaseInput testOrchestratorForeignBucketKindNotFetchedCaseInputs,
) {
	t.Helper()

	foreign := &manifest.RawObject{
		APIVersion: "example.test/v1",
		Kind:       (*testCaseInput.kind),
		Name:       "apps",
		Namespace:  "flux-system",
	}
	(*testCaseInput.o).store.AddObject(foreign)
	result := (dagDispatcher{(*testCaseInput.o)}).Dispatch(t.Context(), foreign.Named(), manifest.DrainNone)
	{
		_, hasStatus := (*testCaseInput.o).store.GetStatus(foreign.Named())
		assert.FatalIf(
			t,
			hasStatus || result.Assessment != nil,
			"foreign %s entered a Flux controller", (*testCaseInput.kind),
		)
	}
	(*testCaseInput.o).store.UpdateStatus(
		foreign.Named(),
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "foreign status"},
	)
	(dagDispatcher{(*testCaseInput.o)}).Dispatch(t.Context(), foreign.Named(), manifest.DrainNone)
	{
		info, _ := (*testCaseInput.o).store.GetStatus(
			foreign.Named(),
		)
		assert.FatalIf(t, info.Status != store.StatusPending ||
			info.Message != "foreign status", "foreign %s status was rewritten by a Flux controller: %+v", (*testCaseInput.kind), info)
	}
}

type testOrchestratorForeignBucketKindNotFetchedCase2Inputs struct {
	o    **Orchestrator
	kind *string
}

func testOrchestratorForeignBucketKindNotFetchedCase2(
	t *testing.T,
	testCaseInput testOrchestratorForeignBucketKindNotFetchedCase2Inputs,
) {
	t.Helper()

	testOrchestratorForeignBucketKindNotFetchedCase(
		t,
		testOrchestratorForeignBucketKindNotFetchedCaseInputs(testCaseInput),
	)
}
