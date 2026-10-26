package kustomize

import (
	"context"
	"errors"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

// TestRender_RespectsCanceledContext asserts the renderer bails
// fast when ctx is already done. The fluxcd/pkg/kustomize Generator
// and SecureBuild don't take ctx themselves, so flate checks at
// coarse boundaries — this guards that those checks fire.
func TestRender_RespectsCanceledContext(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Render(ctx, cache, RenderRequest{Source: localSource(t, t.TempDir()), ApplyIgnore: false, SubPath: ".", Spec: parsedKS(t, map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "k", "namespace": "ns"},
		"spec":       map[string]any{"path": "./"},
	})}, sourcecache.RetryConfig{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRender_UnstampedMetadata(t *testing.T) {
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
			fixture: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  namespace: ns\n  labels: {app: x}\n  annotations: {note: 'y'}\ndata: {k: v}\n",
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
			name:    "null common metadata overwrites existing values",
			fixture: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  labels: {app: existing}\n  annotations: {note: existing}\n",
			owner:   map[string]any{"name": "owner", "namespace": "ns"},
			common: map[string]any{
				"labels":      map[string]any{"app": nil, ownerName: nil},
				"annotations": map[string]any{"note": nil},
			},
			labels:      map[string]any{"app": "", ownerName: "", ownerNamespace: "ns"},
			annotations: map[string]any{"note": ""},
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
			out, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Diff(t, borrowed, snapshot)
			repeated, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
			assert.Diff(t, repeated, out)
			assert.NoError(t, err)
			assert.Diff(t, borrowed, snapshot)
			got := out[0]
			rawWant, err := manifest.SplitDocs([]byte(tc.fixture))
			assert.NoError(t, err)
			assert.Diff(t, out, rawWant)
			base.Stamp(ks.EmissionPolicy(), out)
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

func TestRender_TypedNulls(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		spec      map[string]any
		wantError string
	}{
		{name: "patches null", spec: map[string]any{"patches": nil}},
		{name: "patch element null", spec: map[string]any{"patches": []any{nil}}, wantError: "must specify one of patch and path"},
		{name: "image element null", spec: map[string]any{"images": []any{nil}}},
		{name: "build metadata element null", spec: map[string]any{"buildMetadata": []any{nil}}},
		{name: "component element null", spec: map[string]any{"components": []any{nil}}, wantError: "new root cannot be empty"},
		{name: "empty scalar overrides", spec: map[string]any{"targetNamespace": "", "namePrefix": "", "nameSuffix": ""}},
		{name: "generator-only fields", spec: map[string]any{
			"patchesStrategicMerge": []any{map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "demo"}, "data": map[string]any{"value": "ignored"}}},
			"patchesJson6902":       []any{map[string]any{"target": map[string]any{"kind": "ConfigMap", "name": "demo"}, "patch": []any{map[string]any{"op": "replace", "path": "/data/value", "value": "ignored"}}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml") + `namespace: existing
namePrefix: pre-
nameSuffix: -post
patches:
- patch: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: demo}
    data: {value: disk}
`,
				"cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo, labels: {app: original}}\ndata: {value: original}\n",
			})
			req := RenderRequest{Source: localSource(t, root), SubPath: ".", Spec: parsedKS(t, ksDoc(tc.spec))}
			out, err := Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{})
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("render error = %v, want %q", err, tc.wantError)
				}
				return
			}
			assert.NoError(t, err)
			repeated, err := Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Diff(t, repeated, out)
			req.Spec = parsedKS(t, ksDoc(nil))
			absent, err := Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Diff(t, out, absent)
			got := out[0]
			md := got["metadata"].(map[string]any)
			assert.Equal(t, md["name"], any("pre-demo-post"))
			assert.Equal(t, md["namespace"], any("existing"))
			assert.Equal(t, got["data"].(map[string]any)["value"], any("disk"))
		})
	}
}

func TestRender_EmittedLists(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input string
		wantError   bool
	}{
		{"generic", "apiVersion: v1\nkind: List\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: a}}, {apiVersion: v1, kind: Secret, metadata: {name: b}}]\n", false},
		{"typed explicit", "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: a}}]\n", false},
		{"typed both absent", "apiVersion: v1\nkind: ConfigMapList\nitems: [{metadata: {name: bare}}]\n", true},
		{"missing kind", "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: partial}}]\n", true},
		{"missing version", "apiVersion: v1\nkind: ConfigMapList\nitems: [{kind: ConfigMap, metadata: {name: partial}}]\n", false},
		{"nested", `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: List
  items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]
- {apiVersion: v1, kind: ConfigMap, metadata: {name: sibling}}
`, false},
		{"empty", "apiVersion: v1\nkind: ConfigMapList\nitems: []\n", false},
		{"neighbors and empty", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: before}\n---\napiVersion: v1\nkind: ConfigMapList\nitems: []\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: after}\n", false},
		{"null items", "apiVersion: v1\nkind: ConfigMapList\nitems: null\n", false},
		{"null member", "apiVersion: v1\nkind: ConfigMapList\nitems: [null]\n", false},
		{"scalar member", "apiVersion: v1\nkind: ConfigMapList\nitems: [scalar]\n", true},
		{"object items", "apiVersion: v1\nkind: ConfigMapList\nitems: {metadata: {name: bad}}\n", true},
		{"scalar items", "apiVersion: v1\nkind: ConfigMapList\nitems: scalar\n", true},
		{"custom kind", "apiVersion: example.test/v1\nkind: Bundle\nmetadata: {name: bundle}\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: child}}]\n", false},
		{"scalar AllowList", "apiVersion: example.test/v1\nkind: AllowList\nmetadata: {name: acl}\nitems: [scalar]\n", true},
		{"ordinary AllowList", "apiVersion: example.test/v1\nkind: AllowList\nmetadata: {name: acl}\nspec: {items: [scalar]}\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("list.yaml"), "list.yaml": tc.input})
			kustomizestate.BuildMutex.Lock()
			rm, oracleErr := kustomizestate.Build(filesys.MakeFsOnDisk(), root)
			var raw []byte
			if oracleErr == nil {
				raw, oracleErr = rm.AsYaml()
			}
			kustomizestate.BuildMutex.Unlock()
			if (oracleErr != nil) != tc.wantError {
				t.Fatalf("complete upstream build error = %v, wantError = %t", oracleErr, tc.wantError)
			}
			out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), SubPath: "."}, sourcecache.RetryConfig{})
			if oracleErr != nil {
				if err == nil || out != nil {
					t.Fatalf("Render = %#v, %v; upstream error = %v", out, err, oracleErr)
				}
				return
			}
			assert.NoError(t, err)
			want, err := manifest.SplitDocs(raw)
			assert.NoError(t, err)
			assert.Diff(t, out, want)
			if tc.name == "nested" {
				var names []string
				for _, doc := range out {
					name, _ := manifest.DocMetadata(doc)
					names = append(names, name)
				}
				assert.Diff(t, names, []string{"sibling", "nested"})
			}
		})
	}
}
