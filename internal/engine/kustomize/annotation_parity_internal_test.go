package kustomize

import (
	"bytes"
	"testing"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"

	"github.com/home-operations/flate/internal/engine/kustomize/krusty"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_AnnotationPatchesMatchFlux(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, metadata string }{
		{name: "without annotations"},
		{name: "with annotations", metadata: "buildMetadata: [originAnnotations]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": tc.metadata + `resources: [media, copy.yaml]
configMapGenerator:
- name: settings
  behavior: merge
  literals: [FROM_ROOT=on]
generatorOptions:
  disableNameSuffixHash: true
patches:
- target:
    kind: ConfigMap
    name: copy
    annotationSelector: '!config.kubernetes.io/origin'
  patch: |-
    - op: add
      path: /data/selector
      value: matched
- target: {kind: ConfigMap, name: copy}
  patch: |-
    - op: copy
      from: /metadata/annotations
      path: /data/copied
`,
				"media/kustomization.yaml": `configMapGenerator:
- name: settings
  literals: [FROM_MEDIA=on]
generatorOptions:
  disableNameSuffixHash: true
`,
				"copy.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: copy
  annotations: {example.com/kept: "true"}
data: {}
`,
			})
			krusty.BuildMutex.Lock()
			upstream, err := fluxkustomize.SecureBuild(root, root, false)
			krusty.BuildMutex.Unlock()
			assert.NoError(t, err)
			data, err := upstream.AsYaml()
			assert.NoError(t, err)
			want, err := manifest.DecodeDocs(bytes.NewReader(data), manifest.RequiredDocuments)
			assert.NoError(t, err)

			cacheDir := t.TempDir()
			renderer := newTestTreeCache(t, cacheDir)
			req := RenderRequest{
				Source: localSource(t, root), Spec: &manifest.Kustomization{Name: "emitter", Path: "."},
				SubPath: ".",
			}
			docs, receipt, err := Render(t.Context(), renderer, req, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Diff(t, docs, want)
			for i := range docs {
				if manifest.DocIdentity(docs[i].Document).Name == "settings" {
					assert.Diff(t, docs[i].Document["data"], map[string]any{"FROM_MEDIA": "on", "FROM_ROOT": "on"})
				}
			}
			shaping := PrepareShape(&req, docs)
			defer shaping.Retire()
			content := shaping.Content()
			owner := req.Spec.Named()
			e := acceptedEmission(t, owner, receipt, content)
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptanceForEmission(owner, e, content)))
			closeTestTreeCache(t, renderer)
			reopened := newTestTreeCache(t, cacheDir)
			previous, _, found := reopened.PreviousRender(t.Context(), req)
			if sourcecache.RenderPersistenceDisabledReason() == "" {
				assert.FatalIf(t, !found, "%s", "accepted render was not read from disk")
				assert.Diff(t, previous, want)
			}
		})
	}
}
