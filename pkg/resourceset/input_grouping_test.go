package resourceset_test

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
)

func TestRender_GroupDedup(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name:      "group",
		Namespace: "default",
		Inputs:    []fluxopv1.ResourceSetInput{{}},
		Resources: []*apix.JSON{
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"shared"}}`),
			jsonTmpl(t, `{"apiVersion":"example.test/v1","kind":"Widget"}`),
			jsonTmpl(t, `{"apiVersion":"other.test/v1","kind":"Widget"}`),
			jsonTmpl(t, `{"apiVersion":"v1","metadata":{"name":"kindless"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared","namespace":"a"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared","namespace":"b"}}`),
			jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"other"}}`),
			jsonTmpl(t, `{"data":{"value":"zero"}}`),
			jsonTmpl(t, `{}`),
		},
		ResourcesTemplate: `apiVersion: apps/v1beta2
kind: Deployment
metadata: {name: shared}
---
apiVersion: other.example/v1
kind: Deployment
metadata: {name: shared}
---
apiVersion: example.test/v1
kind: Deployment
metadata: {}
---
apiVersion: example.test/v1
metadata: {name: incomplete}`,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	assert.NoError(t, err)
	assert.FatalIf(t, len(docs) != 10, "docs = %d, want 10", len(docs))
	assert.Diff(
		t,
		[]string{manifest.DocAPIVersion(docs[0].Document), manifest.DocAPIVersion(docs[len(docs)-1].Document)},
		[]string{"apps/v1", "other.example/v1"},
	)
	malformed := map[string]any{
		"apiVersion": "example.io/v1/extra",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "shared", "namespace": "default"},
	}
	assert.Equal(t, manifest.DocIdentity(malformed).Group, "example.io")
	assert.Equal(
		t,
		resourceset.DedupKey(malformed),
		manifest.NamedResource{Kind: "Deployment", Namespace: "default", Name: "shared"},
	)
}

func TestRender_NoInputJSONBypassesExclusionAndDedup(t *testing.T) {
	t.Parallel()
	raw := jsonTmpl(
		t,
		`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"static","annotations":{"fluxcd.controlplane.io/reconcile":"disabled"}}}`,
	)
	rs := &manifest.ResourceSet{
		Name:      "static",
		Namespace: "default",
		Resources: []*apix.JSON{raw, raw},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: static}
---
apiVersion: v1
kind: ConfigMap
metadata:
 name: excluded
 annotations: {fluxcd.controlplane.io/reconcile: disabled}`,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	assert.NoError(t, err)
	assert.FatalIf(t, len(docs) != 2, "docs = %d, want 2 JSON duplicates", len(docs))
	for _, doc := range docs {
		assert.FatalIf(
			t,
			doc.Document["metadata"].(map[string]any)["annotations"].(map[string]any)[fluxopv1.ReconcileAnnotation] != fluxopv1.DisabledValue,
			"wrong resource: %v",
			doc,
		)
	}
}
