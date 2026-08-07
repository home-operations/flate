package orchestrator

import (
	"context"
	"testing"

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
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
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

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(res.Failed) != 0 {
		t.Errorf("foreign Bucket CRD must not fail the run; failed=%+v", res.Failed)
	}

	id := manifest.NamedResource{Kind: manifest.KindBucket, Namespace: "observability", Name: "greptimedb"}
	if info, ok := o.Store().GetStatus(id); ok {
		t.Errorf("foreign Bucket CRD is not a source node, so it must carry no status; got %+v", info)
	}
	if art := o.Store().GetArtifact(id); art != nil {
		t.Errorf("foreign Bucket CRD must not be fetched; artifact=%+v", art)
	}
	obj := o.Store().GetObject(id)
	if !manifest.IsRawObject(obj) {
		t.Fatalf("stored object = %T, want *manifest.RawObject", obj)
	}

	// The resource still renders — dropping it would show up in a diff as a
	// deletion of a live resource.
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	var found bool
	for _, doc := range res.Manifests[ksID] {
		name, ns := manifest.DocMetadata(doc)
		if manifest.DocKind(doc) == manifest.KindBucket && ns == id.Namespace && name == id.Name {
			found = true
			if got := manifest.DocAPIVersion(doc); got != "seaweed.seaweedfs.com/v1" {
				t.Errorf("apiVersion = %q, want the foreign group verbatim", got)
			}
		}
	}
	if !found {
		t.Errorf("foreign Bucket missing from the parent's rendered output: %+v", res.Manifests[ksID])
	}
}

// TestOrchestrator_FluxBucketStillScheduled is the positive control for the
// #872 fix: a real source.toolkit.fluxcd.io Bucket must still reach the source
// controller. It has no reachable endpoint here, so failing the fetch is the
// proof it was dispatched at all.
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

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := o.Render(context.Background()); err == nil {
		t.Fatal("expected the unreachable Flux Bucket's fetch to fail")
	}
	id := manifest.NamedResource{Kind: manifest.KindBucket, Namespace: "flux-system", Name: "minio"}
	info, ok := o.Store().GetStatus(id)
	if !ok || info.Status != store.StatusFailed {
		t.Fatalf("Flux Bucket status = %+v (present=%v), want Failed from the fetch attempt", info, ok)
	}
}
