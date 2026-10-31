package kustomize

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/api/resource"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
)

const syncDocuments = `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 1m0s
  url: https://fixture.invalid/cluster
  ref:
    name: refs/heads/main
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 10m0s
  prune: true
  path: ./clusters
  sourceRef:
    kind: GitRepository
    name: flux-system
`

const rootIntervalPatch = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: flux-system
  namespace: flux-system
spec:
  interval: 42m
`

const installationPatch = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: kustomize-controller
  namespace: flux-system
spec:
  replicas: 2
`

func TestPatchDocuments_Targets(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		subset bool
		patch  fluxapis.Patch
		want   string
		fail   bool
	}{
		{name: "present", subset: true, patch: fluxapis.Patch{Patch: rootIntervalPatch}, want: "interval: 42m"},
		{name: "absent subset", subset: true, patch: fluxapis.Patch{Patch: installationPatch}, want: "interval: 10m0s"},
		{name: "absent complete", patch: fluxapis.Patch{Patch: installationPatch}, fail: true},
		{name: "mixed documents", subset: true, patch: fluxapis.Patch{Patch: installationPatch + "---\n" + rootIntervalPatch}, want: "interval: 42m"},
		{name: "selector zero matches", subset: true, patch: fluxapis.Patch{Patch: "- op: replace\n  path: /spec/replicas\n  value: 2", Target: &fluxapis.Selector{Kind: "Deployment"}}, want: "interval: 10m0s"},
		{name: "source selector", subset: true, patch: fluxapis.Patch{Patch: "- op: replace\n  path: /spec/ref/name\n  value: refs/heads/patched", Target: &fluxapis.Selector{Kind: "GitRepository"}}, want: "name: refs/heads/patched"},
		{name: "present invalid operation", subset: true, patch: fluxapis.Patch{Patch: "- op: replace\n  path: /spec/absent/value\n  value: broken", Target: &fluxapis.Selector{Kind: "Kustomization"}}, fail: true},
		{name: "present invalid merge", subset: true, patch: fluxapis.Patch{Patch: rootIntervalPatch + "$patch: invalid\n"}, fail: true},
		{name: "malformed", subset: true, patch: fluxapis.Patch{Patch: "metadata: ["}, fail: true},
		{name: "missing identity", subset: true, patch: fluxapis.Patch{Patch: "spec: {interval: 42m}"}, fail: true},
		{name: "targetless JSON", subset: true, patch: fluxapis.Patch{Patch: "- op: replace\n  path: /spec/interval\n  value: 42m"}, fail: true},
		{name: "empty", subset: true, patch: fluxapis.Patch{}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := PatchDocuments(DocumentsInput{Documents: []byte(syncDocuments), Patches: []fluxapis.Patch{tc.patch}, Subset: tc.subset})
			if tc.fail {
				if err == nil {
					t.Fatalf("expected patch error, got %v; output %s", err, out)
				}
				return
			}
			assert.NoError(t, err)
			if !strings.Contains(string(out), tc.want) {
				t.Fatalf("missing %q in:\n%s", tc.want, out)
			}
		})
	}
}

func TestPatchDocuments_SkippedDocumentLog(t *testing.T) {
	var log bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	out, err := PatchDocuments(DocumentsInput{
		Documents: []byte(syncDocuments), Subset: true,
		Patches: []fluxapis.Patch{{Patch: installationPatch + "---\n" + rootIntervalPatch}},
	})
	assert.NoError(t, err)
	assert.Equal(t, strings.Count(log.String(), "kustomize: skipping patch outside document set"), 1)
	if !strings.Contains(log.String(), "kustomize-controller") || !strings.Contains(string(out), "interval: 42m") {
		t.Fatalf("log %s; output %s", &log, out)
	}
}

func TestPatchDocuments_AmbiguousIdentity(t *testing.T) {
	t.Parallel()
	const documents = `apiVersion: v1
kind: ConfigMap
metadata:
  name: first
  annotations:
    internal.config.kubernetes.io/previousNames: original
    internal.config.kubernetes.io/previousNamespaces: default
    internal.config.kubernetes.io/previousKinds: ConfigMap
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: second
  annotations:
    internal.config.kubernetes.io/previousNames: original
    internal.config.kubernetes.io/previousNamespaces: default
    internal.config.kubernetes.io/previousKinds: ConfigMap
`
	patches := []fluxapis.Patch{{Patch: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\ndata: {key: patched}\n"}}
	_, err := PatchDocuments(DocumentsInput{Documents: []byte(documents), Patches: patches, Subset: true})
	if err == nil {
		t.Fatalf("expected ambiguous patch error, got %v", err)
	}
	_, upstreamErr := upstreamDocumentBuild(t, documents, patches, nil)
	if upstreamErr == nil {
		t.Fatal("upstream accepted the ambiguous patch")
	}
}

func TestPatchDocuments_RenameOldNameParity(t *testing.T) {
	t.Parallel()
	patches := renamePatches("flux-system")
	out, err := PatchDocuments(DocumentsInput{Documents: []byte(syncDocuments), Patches: patches, Subset: true})
	assert.NoError(t, err)
	want, err := upstreamDocumentBuild(t, syncDocuments, patches, nil)
	assert.NoError(t, err)
	assert.Equal(t, string(out), string(want))
	root := documentByKind(t, out, "Kustomization")
	assert.Equal(t, root.GetName(), "renamed")
	assert.Equal(t, documentSpec(t, root)["interval"], any("42m"))
}

func TestPatchDocuments_RenameNewNameKnownDifference(t *testing.T) {
	t.Parallel()
	patches := renamePatches("renamed")
	out, err := PatchDocuments(DocumentsInput{Documents: []byte(syncDocuments), Patches: patches, Subset: true})
	assert.NoError(t, err)
	upstream, err := upstreamDocumentBuild(t, syncDocuments, patches, nil)
	assert.NoError(t, err)
	root := documentByKind(t, out, "Kustomization")
	control := documentByKind(t, upstream, "Kustomization")
	assert.Equal(t, root.GetName(), "renamed")
	assert.Equal(t, control.GetName(), "renamed")
	assert.Equal(t, documentSpec(t, root)["interval"], any("10m0s"))
	assert.Equal(t, documentSpec(t, control)["interval"], any("42m"))
}

func TestPatchDocuments_OperatorComposition(t *testing.T) {
	t.Parallel()
	object := &manifest.RawObject{
		APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Name: "flux", Namespace: "flux-system",
		Spec: map[string]any{
			"distribution": map[string]any{"version": "2.x"},
			"sync":         map[string]any{"kind": "GitRepository", "url": "https://fixture.invalid/cluster", "ref": "refs/heads/main", "path": "./clusters"},
			"kustomize": map[string]any{"patches": []any{
				map[string]any{"target": map[string]any{"labelSelector": "app.kubernetes.io/managed-by=flux-operator"}, "patch": "- op: add\n  path: /spec/selectorApplied\n  value: true"},
				map[string]any{"target": map[string]any{"annotationSelector": "kustomize.toolkit.fluxcd.io/ssa=Ignore"}, "patch": "- op: add\n  path: /spec/annotationApplied\n  value: true"},
				map[string]any{"target": map[string]any{"kind": ".*"}, "patch": `apiVersion: v1
kind: all
metadata:
  name: all
  labels:
    app.kubernetes.io/managed-by: user
    custom: retained
  annotations:
    kustomize.toolkit.fluxcd.io/ssa: user
    custom: retained
`},
				map[string]any{"target": map[string]any{"kind": "GitRepository"}, "patch": "- op: replace\n  path: /spec/url\n  value: https://fixture.invalid/patched"},
			}},
		},
	}
	before := manifest.DeepCopyMap(object.Spec)
	projection, err := object.SyncProjection()
	assert.NoError(t, err)
	out, err := PatchDocuments(DocumentsInput{
		Documents: projection.Documents, Patches: projection.Patches, Namespace: projection.Namespace,
		Transformers: projection.Transformers, Subset: true,
	})
	assert.NoError(t, err)
	again, err := PatchDocuments(DocumentsInput{
		Documents: projection.Documents, Patches: projection.Patches, Namespace: projection.Namespace,
		Transformers: projection.Transformers, Subset: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, string(again), string(out))
	assert.Diff(t, object.Spec, before)

	const annotations = `apiVersion: builtin
kind: AnnotationsTransformer
metadata: {name: annotations}
annotations:
  kustomize.toolkit.fluxcd.io/ssa: Ignore
  kustomize.toolkit.fluxcd.io/prune: Disabled
fieldSpecs:
- path: metadata/annotations
  create: true
`
	const labels = `apiVersion: builtin
kind: LabelTransformer
metadata: {name: labels}
labels:
  app.kubernetes.io/managed-by: flux-operator
  app.kubernetes.io/instance: flux-system
  app.kubernetes.io/version: 2.x
  app.kubernetes.io/part-of: flux
fieldSpecs:
- path: metadata/labels
  create: true
`
	control, err := upstreamDocumentBuild(t, syncDocuments, projection.Patches, [][]byte{[]byte(annotations), []byte(labels)})
	assert.NoError(t, err)
	assert.Equal(t, string(out), string(control))
	for _, kind := range []string{"GitRepository", "Kustomization"} {
		doc := documentByKind(t, out, kind)
		assert.Diff(t, doc.GetLabels(), map[string]string{
			"app.kubernetes.io/managed-by": "flux-operator", "app.kubernetes.io/instance": "flux-system",
			"app.kubernetes.io/version": "2.x", "app.kubernetes.io/part-of": "flux", "custom": "retained",
		})
		assert.Diff(t, doc.GetAnnotations(), map[string]string{
			"kustomize.toolkit.fluxcd.io/ssa": "Ignore", "kustomize.toolkit.fluxcd.io/prune": "Disabled", "custom": "retained",
		})
		spec := documentSpec(t, doc)
		if spec["selectorApplied"] != nil || spec["annotationApplied"] != nil {
			t.Fatalf("selector observed operator metadata before external transformers: %v", spec)
		}
	}
	source := documentByKind(t, out, "GitRepository")
	assert.Equal(t, documentSpec(t, source)["url"], any("https://fixture.invalid/patched"))
}

func renamePatches(name string) []fluxapis.Patch {
	return []fluxapis.Patch{
		{Patch: "- op: replace\n  path: /metadata/name\n  value: renamed", Target: &fluxapis.Selector{Kind: "Kustomization"}},
		{Patch: strings.Replace(rootIntervalPatch, "name: flux-system", "name: "+name, 1)},
	}
}

func upstreamDocumentBuild(t *testing.T, documents string, patches []fluxapis.Patch, transformers [][]byte) ([]byte, error) {
	t.Helper()
	fs := filesys.MakeFsInMemory()
	assert.NoError(t, fs.WriteFile("documents.yaml", []byte(documents)))
	cfg := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion, Kind: kustypes.KustomizationKind,
		Namespace: "flux-system", Resources: []string{"documents.yaml"},
	}
	for _, patch := range patches {
		cfg.Patches = append(cfg.Patches, kustypes.Patch{Patch: patch.Patch, Target: AdaptSelector(patch.Target)})
	}
	for i, transformer := range transformers {
		name := []string{"annotations.yaml", "labels.yaml"}[i]
		cfg.Transformers = append(cfg.Transformers, name)
		assert.NoError(t, fs.WriteFile(name, transformer))
	}
	raw, err := json.Marshal(cfg)
	assert.NoError(t, err)
	assert.NoError(t, fs.WriteFile("kustomization.yaml", raw))
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	rm, err := krusty.MakeKustomizer(&krusty.Options{
		LoadRestrictions: kustypes.LoadRestrictionsNone, PluginConfig: kustypes.DisabledPluginConfig(),
	}).Run(fs, ".")
	if err != nil {
		return nil, err
	}
	return rm.AsYaml()
}

func documentByKind(t *testing.T, raw []byte, kind string) *resource.Resource {
	t.Helper()
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	rf := provider.NewDefaultDepProvider().GetResourceFactory()
	rm, err := resmap.NewFactory(rf).NewResMapFromBytes(raw)
	assert.NoError(t, err)
	for _, resource := range rm.Resources() {
		if resource.GetKind() == kind {
			return resource
		}
	}
	t.Fatalf("missing kind %s in %s", kind, raw)
	return nil
}

func documentSpec(t *testing.T, resource *resource.Resource) map[string]any {
	t.Helper()
	doc, err := resource.Map()
	assert.NoError(t, err)
	return doc["spec"].(map[string]any)
}
