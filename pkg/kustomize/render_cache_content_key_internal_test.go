package kustomize

import (
	"maps"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderKey_ContentInputs(t *testing.T) {
	t.Parallel()
	req := RenderRequest{
		Source:  &store.SourceArtifact{Key: "family", Content: "generation"},
		SubPath: ".",
		Spec:    demoSpec,
	}
	key := renderKey(&req, "/source", nil)
	for _, tc := range []struct {
		name   string
		change func(*RenderRequest)
		root   string
		ignore any
		spec   map[string]any
		typed  map[string]any
		same   bool
	}{
		{name: "substitution", same: true, change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "typed"}}
		}},
		{name: "substitution strategy", same: true, change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways}
		}},
		{name: "substitution references", same: true, change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{
				SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "provider"}},
			}
		}},
		{name: "spec", change: func(r *RenderRequest) { r.Spec.NamePrefix = "other-" }},
		{name: "owner name", change: func(r *RenderRequest) { r.Spec.Name = "other" }},
		{name: "owner namespace", change: func(r *RenderRequest) { r.Spec.Namespace = "other" }},
		{name: "explicit empty namespace", same: true, spec: map[string]any{"targetNamespace": ""}},
		{name: "explicit empty prefix", same: true, spec: map[string]any{"namePrefix": ""}},
		{name: "explicit empty suffix", same: true, spec: map[string]any{"nameSuffix": ""}},
		{name: "null fields", same: true, spec: map[string]any{"components": nil}},
		{name: "null whole fields", same: true, spec: map[string]any{"patches": nil, "images": nil, "components": nil, "buildMetadata": nil, "commonMetadata": nil, "targetNamespace": nil, "namePrefix": nil, "nameSuffix": nil}},
		{name: "empty lists", same: true, spec: map[string]any{"patches": []any{}, "images": []any{}, "components": []any{}, "buildMetadata": []any{}}},
		{name: "null image elements", spec: map[string]any{"images": []any{nil}}, typed: map[string]any{"images": []any{map[string]any{"name": ""}}}},
		{name: "null component elements", spec: map[string]any{"components": []any{nil}}, typed: map[string]any{"components": []any{""}}},
		{name: "null build metadata elements", spec: map[string]any{"buildMetadata": []any{nil}}, typed: map[string]any{"buildMetadata": []any{""}}},
		{name: "null patch elements", spec: map[string]any{"patches": []any{nil}}, typed: map[string]any{"patches": []any{map[string]any{"patch": ""}}}},
		{name: "null common labels", spec: map[string]any{"commonMetadata": map[string]any{"labels": map[string]any{"app": nil}}}, typed: map[string]any{"commonMetadata": map[string]any{"labels": map[string]any{"app": ""}}}},
		{name: "null common annotations", spec: map[string]any{"commonMetadata": map[string]any{"annotations": map[string]any{"note": nil}}}, typed: map[string]any{"commonMetadata": map[string]any{"annotations": map[string]any{"note": ""}}}},
		{name: "strategic merge patches", same: true, spec: map[string]any{"patchesStrategicMerge": []any{map[string]any{"metadata": map[string]any{"name": "app"}}}}},
		{name: "JSON 6902 patches", same: true, spec: map[string]any{"patchesJson6902": []any{map[string]any{"patch": []any{map[string]any{"op": "remove", "path": "/data/old"}}}}}},
		{name: "root", root: "/other"},
		{name: "subpath", change: func(r *RenderRequest) { r.SubPath = "app" }},
		{name: "ignore selection", change: func(r *RenderRequest) { r.ApplyIgnore = true }},
		{name: "ignore bytes", ignore: []byte("*.yaml")},
		{name: "acquisition", change: func(r *RenderRequest) { r.Source.Key = "other-family" }},
		{name: "revision", same: true, change: func(r *RenderRequest) { r.Source.Revision = "revision" }},
		{name: "generation", change: func(r *RenderRequest) { r.Source.Content = "next" }},
		{name: "origin", same: true, change: func(r *RenderRequest) {
			r.Source.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
		}},
		{name: "digest", change: func(r *RenderRequest) { r.Source.Digest = "digest" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderKeyContentInputsCase(t, &testRenderKeyContentInputsCaseInputs{req: &req, key: &key, tc: &tc})
		},
		)
	}
	for _, field := range []string{"targetNamespace", "namePrefix", "nameSuffix"} {
		t.Run("typed override/"+field, func(t *testing.T) {
			t.Parallel()
			testRenderKeyContentInputsCase2(t, &testRenderKeyContentInputsCase2Inputs{req: &req, field: &field})
		},
		)
	}

	stamped := req
	stamped.Spec = req.Spec.Clone()
	stamped.Spec.Labels = map[string]string{"owner": "parent"}
	stamped.Spec.Annotations = map[string]string{"stamp": "changed"}
	{
		got := renderKey(&stamped, "/source", nil)
		assert.FatalIf(t, got != key, "%s", "emission metadata changed the render key")
	}
	{
		got := renderKey(&req, "/source", nil)
		assert.FatalIf(t, got != key, "%s", "unchanged render input changed the key")
	}
}

type testRenderKeyContentInputsCaseInputs struct {
	req *RenderRequest
	key *string
	tc  *struct {
		name   string
		change func(*RenderRequest)
		root   string
		ignore any
		spec   map[string]any
		typed  map[string]any
		same   bool
	}
}

func testRenderKeyContentInputsCase(t *testing.T, testCaseInput *testRenderKeyContentInputsCaseInputs) {
	t.Helper()

	changed := (*testCaseInput.req)
	changed.Spec = testCaseInput.req.Spec.Clone()
	source := *testCaseInput.req.Source
	changed.Source = &source
	if testCaseInput.tc.spec != nil {
		doc := renderDoc(t, changed.Spec)
		maps.Copy(doc["spec"].(map[string]any), testCaseInput.tc.spec)
		changed.Spec = parsedKS(t, doc)
	}
	if testCaseInput.tc.change != nil {
		testCaseInput.tc.change(&changed)
	}
	root := "/source"
	if testCaseInput.tc.root != "" {
		root = testCaseInput.tc.root
	}
	got := renderKey(&changed, root, testCaseInput.tc.ignore)
	if got == "" || (got == (*testCaseInput.key)) != testCaseInput.tc.same {
		t.Fatalf("render key equality = %t, want %t", got == (*testCaseInput.key), testCaseInput.tc.same)
	}
	if testCaseInput.tc.typed != nil {
		typed := (*testCaseInput.req)
		doc := renderDoc(t, testCaseInput.req.Spec)
		maps.Copy(doc["spec"].(map[string]any), testCaseInput.tc.typed)
		typed.Spec = parsedKS(t, doc)
		{
			assert.FatalIf(
				t,
				got != renderKey(&typed, root, testCaseInput.tc.ignore),
				"%s",
				"null input differs from its typed zero value",
			)
		}
	}
}

type testRenderKeyContentInputsCase2Inputs struct {
	req   *RenderRequest
	field *string
}

func testRenderKeyContentInputsCase2(t *testing.T, testCaseInput *testRenderKeyContentInputsCase2Inputs) {
	t.Helper()

	changed := (*testCaseInput.req)
	changed.Spec = testCaseInput.req.Spec.Clone()
	doc := renderDoc(t, changed.Spec)
	doc["spec"].(map[string]any)[(*testCaseInput.field)] = nil
	changed.Spec = parsedKS(t, doc)
	plain := (*testCaseInput.req)
	plain.Spec = testCaseInput.req.Spec.Clone()
	switch *testCaseInput.field {
	case "targetNamespace":
		changed.Spec.TargetNamespace, plain.Spec.TargetNamespace = "apps", "apps"
	case "namePrefix":
		changed.Spec.NamePrefix, plain.Spec.NamePrefix = "pre-", "pre-"
	case "nameSuffix":
		changed.Spec.NameSuffix, plain.Spec.NameSuffix = "-post", "-post"
	}
	{
		assert.FatalIf(
			t,
			renderKey(&changed, "/source", nil) != renderKey(&plain, "/source", nil),
			"%s",
			"authored null changed typed content",
		)
	}
}
