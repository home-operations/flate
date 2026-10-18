package kustomize

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"sigs.k8s.io/yaml"
)

// TestRenderFlux_RespectsCanceledContext asserts the renderer bails
// fast when ctx is already done. The fluxcd/pkg/kustomize Generator
// and SecureBuild don't take ctx themselves, so flate checks at
// coarse boundaries — this guards that those checks fire.
func TestRenderFlux_RespectsCanceledContext(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, t.TempDir()), ApplyIgnore: false, SubPath: ".", Spec: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "k", "namespace": "ns"},
		"spec":       map[string]any{"path": "./"},
	}}, sourcecache.RetryConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRenderFlux_Metadata(t *testing.T) {
	t.Parallel()
	const plain = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: c, namespace: ns}\ndata: {k: v}\n"
	const ownerName = "kustomize.toolkit.fluxcd.io/name"
	const ownerNamespace = "kustomize.toolkit.fluxcd.io/namespace"
	for _, tc := range []struct {
		name, fixture, effectiveNamespace string
		owner, common                     map[string]any
		labels, annotations               map[string]any
	}{
		{
			name:    "common metadata merges and overrides",
			fixture: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  namespace: ns\n  labels: {app: x}\n  annotations: {note: y}\ndata: {k: v}\n",
			owner:   map[string]any{"name": "k", "namespace": "ns"},
			common: map[string]any{
				"labels":      map[string]any{"team": "flate", "app": "override"},
				"annotations": map[string]any{"managed-by": "flate"},
			},
			labels:      map[string]any{"team": "flate", "app": "override", ownerName: "k", ownerNamespace: "ns"},
			annotations: map[string]any{"managed-by": "flate", "note": "y"},
		},
		{
			name: "common metadata overrides owner labels", fixture: plain,
			owner:  map[string]any{"name": "owner-name", "namespace": "owner-ns"},
			common: map[string]any{"labels": map[string]any{ownerName: "user-override", ownerNamespace: "user-ns-override"}},
			labels: map[string]any{ownerName: "user-override", ownerNamespace: "user-ns-override"},
		},
		{
			name: "effective owner namespace", fixture: plain,
			owner: map[string]any{"name": "infra", "namespace": "authored"}, effectiveNamespace: "flux-system",
			labels: map[string]any{ownerName: "infra", ownerNamespace: "flux-system"},
		},
		{
			name:    "owner without namespace preserves existing label",
			fixture: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  labels:\n    kustomize.toolkit.fluxcd.io/namespace: existing\n",
			owner:   map[string]any{"name": "infra"},
			labels:  map[string]any{ownerName: "infra", ownerNamespace: "existing"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": tc.fixture,
			})
			spec := map[string]any{"path": "./"}
			if tc.common != nil {
				spec["commonMetadata"] = tc.common
			}
			doc := ksDoc(spec)
			doc["metadata"] = tc.owner
			if tc.effectiveNamespace != "" {
				obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
				assert.NoError(t, err)
				projected := obj.WithNamespace(tc.effectiveNamespace).(*manifest.Kustomization)
				prepared, err := Prepare(projected, nil, nil)
				assert.NoError(t, err)
				doc = prepared.Contents
			}
			out, err := RenderFlux(t.Context(), newTestTreeCache(t, ""), RenderRequest{
				Source: localSource(t, root), Label: root, SubPath: ".", Spec: doc,
			}, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			var got map[string]any
			assert.NoError(t, yaml.Unmarshal(out, &got))
			md := got["metadata"].(map[string]any)
			labels, _ := md["labels"].(map[string]any)
			annotations, _ := md["annotations"].(map[string]any)
			for key, want := range tc.labels {
				if labels[key] != want {
					t.Errorf("label %s = %v, want %v", key, labels[key], want)
				}
			}
			for key, want := range tc.annotations {
				if annotations[key] != want {
					t.Errorf("annotation %s = %v, want %v", key, annotations[key], want)
				}
			}
		})
	}
}
