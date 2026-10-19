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

	_, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, t.TempDir()), ApplyIgnore: false, SubPath: ".", Spec: parsedKS(t, map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "k", "namespace": "ns"},
		"spec":       map[string]any{"path": "./"},
	})}, sourcecache.RetryConfig{})
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
		editNull                          bool
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
			name:    "null common metadata preserves existing values",
			fixture: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  labels: {app: existing}\n  annotations: {note: existing}\n",
			owner:   map[string]any{"name": "owner", "namespace": "ns"},
			common: map[string]any{
				"labels":      map[string]any{"app": nil, ownerName: nil},
				"annotations": map[string]any{"note": nil},
			},
			labels:      map[string]any{"app": "existing", ownerName: "owner", ownerNamespace: "ns"},
			annotations: map[string]any{"note": "existing"},
		},
		{
			name:    "empty common metadata overwrites existing values",
			fixture: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  labels: {app: existing}\n  annotations: {note: existing}\n",
			owner:   map[string]any{"name": "owner", "namespace": "ns"},
			common: map[string]any{
				"labels":      map[string]any{"app": "", ownerName: ""},
				"annotations": map[string]any{"note": ""},
			},
			labels:      map[string]any{"app": "", ownerName: "", ownerNamespace: "ns"},
			annotations: map[string]any{"note": ""},
		},

		{
			name: "typed values replace null metadata", fixture: plain,
			owner:       map[string]any{"name": "owner", "namespace": "ns"},
			common:      map[string]any{"labels": map[string]any{ownerName: nil}, "annotations": map[string]any{"note": nil}},
			labels:      map[string]any{ownerName: "typed", ownerNamespace: "ns"},
			annotations: map[string]any{"note": "typed"}, editNull: true,
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
			ks := parsedKS(t, doc)
			if tc.editNull {
				ks.CommonMetadata.Labels[ownerName] = "typed"
				ks.CommonMetadata.Annotations["note"] = "typed"
			}
			if tc.effectiveNamespace != "" {
				obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
				assert.NoError(t, err)
				projected := obj.WithNamespace(tc.effectiveNamespace).(*manifest.Kustomization)
				prepared, err := Prepare(projected, nil, nil)
				assert.NoError(t, err)
				ks = prepared
			}
			borrowed := renderDoc(t, ks)
			snapshot := manifest.DeepCopyMap(borrowed)
			cache := newTestTreeCache(t, "")
			req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: ks}
			out, err := RenderFlux(t.Context(), cache, req, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Diff(t, borrowed, snapshot)
			repeated, err := RenderFlux(t.Context(), cache, req, sourcecache.RetryConfig{})
			assert.Equal(t, string(repeated), string(out))
			assert.NoError(t, err)
			assert.Diff(t, borrowed, snapshot)
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
