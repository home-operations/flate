package kustomize

import (
	"maps"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"github.com/fluxcd/pkg/apis/meta"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepareApplication_Kustomization(t *testing.T) {
	t.Parallel()
	spec := &manifest.Kustomization{
		Name: "app", Namespace: "apps", Path: "./app", TargetNamespace: "workloads",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source"},
		DependsOn: []kustomizev1.DependencyReference{{Name: "prerequisite"}},
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"NAME": "effective"}},
		Patches:   []fluxapis.Patch{{Patch: "patch"}},
	}
	application, valid := spec.EntryApplication()
	assert.Equal(t, valid, true)
	prepared, err := PrepareApplication(t.Context(), application, nil)
	assert.NoError(t, err)
	assert.Equal(t, prepared.Spec.Named(), spec.Named())
	assert.Equal(t, len(prepared.Spec.DependsOn), 0)
	assert.Equal(t, prepared.Spec.PostBuild == nil, true)
	assert.Equal(t, prepared.Spec.TargetNamespace, "")
	assert.Equal(t, len(prepared.Spec.Patches), 0)
	assert.Equal(t, len(spec.DependsOn), 1)
	assert.Equal(t, spec.PostBuild.Substitute["NAME"], "effective")
	assert.Equal(t, prepared.Source, spec.SourceID())
	assert.Equal(t, prepared.Path, spec.Path)
}

func TestPrepareApplication_EffectiveProjection(t *testing.T) {
	t.Parallel()
	object := &manifest.RawObject{
		APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Name: "flux", Namespace: "control",
		Spec: map[string]any{
			"distribution": map[string]any{"version": "2.x"},
			"sync": map[string]any{
				"kind":       manifest.KindGitRepository,
				"url":        "https://fixture.invalid/authored",
				"ref":        "refs/heads/main",
				"path":       "./authored",
				"pullSecret": "credentials",
				"provider":   "github",
				"interval":   "2m",
			},
			"kustomize": map[string]any{"patches": []any{
				map[string]any{"target": map[string]any{"kind": manifest.KindGitRepository}, "patch": `
- op: replace
  path: /metadata/name
  value: patched
- op: replace
  path: /spec/url
  value: https://fixture.invalid/effective
- op: replace
  path: /spec/ref/name
  value: refs/heads/unavailable
`},
				map[string]any{"target": map[string]any{"kind": manifest.KindKustomization}, "patch": `
- op: replace
  path: /metadata/name
  value: renamed
- op: replace
  path: /spec/sourceRef/name
  value: patched
- op: replace
  path: /spec/path
  value: ./effective
- op: add
  path: /spec/dependsOn
  value: [{name: prerequisite}]
- op: add
  path: /spec/postBuild
  value: {substitute: {NAME: effective}}
- op: add
  path: /spec/targetNamespace
  value: workloads
`},
				map[string]any{"patch": installationPatch},
			}},
		},
	}
	before := manifest.DeepCopyMap(object.Spec)
	application, valid := object.EntryApplication()
	assert.Equal(t, valid, true)
	prepared, err := PrepareApplication(t.Context(), application, nil)
	assert.NoError(t, err)
	assert.Equal(t, prepared.Source.Name, "patched")
	assert.Equal(t, prepared.Path, "./effective")
	assert.Equal(t, prepared.Spec.Name, "renamed")
	assert.Equal(t, prepared.Spec.Path, prepared.Path)
	assert.Equal(t, prepared.Spec.SourceID(), prepared.Source)
	assert.Equal(t, prepared.Spec.TargetNamespace, "workloads")
	assert.Diff(t, prepared.Spec.DependsOn, []kustomizev1.DependencyReference{{Name: "prerequisite"}})
	assert.Diff(t, prepared.Spec.PostBuild.Substitute, map[string]string{"NAME": "effective"})
	source := prepared.Declaration.(*manifest.GitRepository)
	assert.Equal(t, source.Named(), prepared.Source)
	assert.Equal(t, source.URL, "https://fixture.invalid/effective")
	assert.Equal(t, source.Reference.Name, "refs/heads/unavailable")
	assert.Equal(t, source.Provider, "github")
	assert.Equal(t, source.Interval.Duration, 2*time.Minute)
	assert.Diff(t, source.SecretRef, &meta.LocalObjectReference{Name: "credentials"})
	assert.Equal(t, prepared.Spec.Labels["app.kubernetes.io/managed-by"], "flux-operator")
	assert.Equal(t, source.Annotations["kustomize.toolkit.fluxcd.io/ssa"], "Ignore")
	assert.Equal(t, len(prepared.Patches)+len(prepared.Transformers), 0)
	assert.Equal(t, application.Path, "./authored")
	assert.Equal(t, application.Spec.Name, "control")
	assert.Diff(t, object.Spec, before)
}

func TestPrepareApplication_SourceKinds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind   string
		fields map[string]any
	}{
		{manifest.KindGitRepository, map[string]any{"url": "https://fixture.invalid/source", "ref": map[string]any{"name": "selection"}}},
		{manifest.KindOCIRepository, map[string]any{"url": "https://fixture.invalid/source", "ref": map[string]any{"tag": "selection"}}},
		{manifest.KindBucket, map[string]any{"endpoint": "https://fixture.invalid/source", "bucketName": "selection"}},
	} {
		kind := tc.kind
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			object := &manifest.RawObject{
				APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Namespace: "control",
				Spec: map[string]any{"sync": map[string]any{
					"kind":       kind,
					"url":        "https://fixture.invalid/source",
					"ref":        "selection",
					"path":       "./app",
					"pullSecret": "credentials",
				}},
			}
			application, valid := object.EntryApplication()
			assert.Equal(t, valid, true)
			prepared, err := PrepareApplication(t.Context(), application, nil)
			assert.NoError(t, err)
			assert.Equal(t, prepared.Source.Kind, kind)
			assert.Equal(t, prepared.Source, application.Source)
			assert.Equal(t, prepared.Path, application.Path)
			expectedSpec := map[string]any{
				"interval": "1m0s", "secretRef": map[string]any{"name": "credentials"},
			}
			maps.Copy(expectedSpec, tc.fields)
			expected, err := manifest.ParseDoc(map[string]any{
				"apiVersion": prepared.Source.KindInfo().APIVersion(), "kind": kind,
				"metadata": map[string]any{
					"name": "control", "namespace": "control",
					"annotations": map[string]any{
						"kustomize.toolkit.fluxcd.io/ssa":   "Ignore",
						"kustomize.toolkit.fluxcd.io/prune": "Disabled",
					},
					"labels": map[string]any{
						"app.kubernetes.io/managed-by": "flux-operator",
						"app.kubernetes.io/instance":   "control",
						"app.kubernetes.io/version":    "",
						"app.kubernetes.io/part-of":    "flux",
					},
				},
				"spec": expectedSpec,
			}, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			assert.Equal(t, manifest.Equal(prepared.Declaration, expected.Object), true)
			initial, valid := object.EntryApplication()
			assert.Equal(t, valid, true)
			assert.Equal(t, manifest.Equal(application.Declaration, initial.Declaration), true)
		})
	}
}
