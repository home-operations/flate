package manifest

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestGeneratedObjects_FluxInstance(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct{ version, apiVersion, kind string }{
		{"2", "v1", KindOCIRepository},
		{"2.x", "v1", KindOCIRepository},
		{"2.5.x", "v1beta2", KindOCIRepository},
		{"v2.5.1", "v1beta2", KindOCIRepository},
		{"2.6.x", "v1", KindOCIRepository},
		{">=2.0.0 <3.0.0", "v1", KindOCIRepository},
		{">=2.7.0 <2.7.1", "v1", KindOCIRepository},
		{">2 <4", "v1", KindOCIRepository},
		{">= 2.10.1 < 2.11.0", "v1", KindOCIRepository},
		{">=2.11", "v1", KindOCIRepository},
		{">=2.5.0 <2.6.0", "v1beta2", KindOCIRepository},
		{"2.5.x || 2.8.4", "v1", KindOCIRepository},
		{">=2.6.0 || broken", "", KindOCIRepository},
		{">=2.11.0 <2.12.0 !=2.11.0", "v1", KindOCIRepository},
		{">=2.11.0 <=2.11.0 !=2.11.0", "v1beta2", KindOCIRepository},
		{">=2.6.0 <2.8.0 !=2.6.x", "v1", KindOCIRepository},
		{">=2.6.0-rc.1 <2.6.0-rc.2", "v1beta2", KindOCIRepository},
		{">2.6.0-18446744073709551616 <2.6.0", "v1beta2", KindOCIRepository},
		{"2.5.0 - 2.6.0", "v1", KindOCIRepository},
		{">18446744073709551615.18446744073709551615.18446744073709551615", "v1beta2", KindOCIRepository},
		{"v2.6.0", "v1", KindOCIRepository},
		{"2.8.4@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "v1", KindOCIRepository},
		{"2.5.x", "v1beta2", KindBucket},
		{"2.6.x", "v1", KindBucket},
	} {
		t.Run(tt.kind+"/"+tt.version, func(t *testing.T) {
			t.Parallel()
			url := "oci://example.invalid/repo"
			sourceSpec := map[string]any{"url": url, "ref": map[string]any{"tag": "latest"}}
			if tt.kind == KindBucket {
				url = "objects.example.invalid"
				sourceSpec = map[string]any{"endpoint": url, "bucketName": "latest"}
			}
			sourceSpec["interval"], sourceSpec["provider"] = "45s", "aws"
			sourceSpec["secretRef"] = map[string]any{"name": "credentials"}
			obj := &RawObject{
				APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control",
				Spec: map[string]any{
					"sync": map[string]any{
						"kind":       tt.kind,
						"name":       "custom",
						"url":        url,
						"path":       "./workload",
						"ref":        "latest",
						"interval":   "45s",
						"provider":   "aws",
						"pullSecret": "credentials",
					},
					"distribution": map[string]any{"version": tt.version},
					"cluster":      map[string]any{"multitenant": true},
					"kustomize": map[string]any{"patches": []any{
						map[string]any{"patch": "first"}, map[string]any{"patch": "last"},
					}},
				},
			}
			for _, input := range []BaseManifest{obj, CatalogObject(obj, obj.Named())} {
				generation, err := GeneratedObjects(input)
				if tt.apiVersion == "" {
					assert.Equal(t, errors.Is(err, ErrInput), true)
					assert.Equal(t, len(generation.Objects), 0)
					assert.Equal(t, len(generation.Documents), 0)
					continue
				}
				assert.NoError(t, err)
				assert.Equal(t, len(generation.Objects), 2)
				assert.Equal(t, len(generation.Documents), 2)
				assert.Equal(t, DocAPIVersion(generation.Documents[0].Document), SourceDomain+"/"+tt.apiVersion)
				assert.Equal(t, DocKind(generation.Documents[0].Document), tt.kind)
				assert.Diff(t, generation.Documents[0].Document["spec"], sourceSpec)
				assert.Diff(t, generation.Documents[1].Document["spec"], map[string]any{
					"path": "./workload", "prune": true, "interval": "10m0s",
					"sourceRef":          map[string]any{"kind": tt.kind, "name": "custom"},
					"serviceAccountName": "kustomize-controller",
				})
				assert.Equal(t, len(generation.Patches), 2)
				assert.Equal(t, generation.Patches[0].Patch, "first")
				assert.Equal(t, generation.Patches[1].Patch, "last")
			}
		})
	}
}

func TestGeneratedTargets_PatchedSyncRequiresPreparation(t *testing.T) {
	t.Parallel()
	obj := &RawObject{
		APIVersion: FluxOperatorDomain + "/v1", Kind: "FluxInstance", Name: "flux", Namespace: "control",
		Spec: map[string]any{
			"sync": map[string]any{
				"kind": KindGitRepository, "url": "https://example.invalid/repo",
				"path": ".", "ref": "refs/heads/main",
			},
		},
	}
	targets, err := GeneratedTargets(obj)
	assert.NoError(t, err)
	assert.Equal(t, len(targets), 2)
	assert.Equal(t, ProducerNeedsCandidates(obj), false)
	obj.Spec["kustomize"] = map[string]any{"patches": []any{map[string]any{
		"patch": "- op: replace\n  path: /metadata/name\n  value: replacement",
	}}}
	assert.Equal(t, ProducerNeedsCandidates(obj), true)
	replacement := NamedResource{
		Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "control", Name: "replacement",
	}
	assert.Diff(t, ProducerTargets(obj, replacement), []ProducerTarget{{NamedResource: replacement}})
}
