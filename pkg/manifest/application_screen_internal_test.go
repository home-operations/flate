package manifest

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestProjectProducer_EntryPrecondition(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		kind     string
		spec     map[string]any
		content  [][]byte
		complete bool
		possible bool
	}{
		{name: "ResourceSet envelope alone", kind: KindResourceSet, complete: true},
		{name: "ResourceSet unknown inputs", kind: KindResourceSet, possible: true},
		{
			name: "ResourceSet literal group", kind: KindResourceSet, complete: true,
			spec: map[string]any{"resourcesTemplate": "apiVersion: kustomize.toolkit.fluxcd.io/v9\n"}, possible: true,
		},
		{
			name: "ResourceSet templated group", kind: KindResourceSet, complete: true,
			spec: map[string]any{
				"inputs":            []any{map[string]any{"group": "kustomize"}},
				"resourcesTemplate": "apiVersion: << inputs.group >>.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec: {path: ./entry}\n",
			}, possible: true,
		},
		{
			name: "HelmRelease templated group", kind: KindHelmRelease, complete: true,
			content: [][]byte{[]byte(`apiVersion: {{ printf "%s.%s" "fluxcd" "controlplane.io/v1" }}
kind: FluxInstance
metadata: {name: owner, namespace: apps}
spec: {sync: {kind: GitRepository, url: https://fixture.invalid/repo, ref: main, path: ./entry}}
`)},
			possible: true,
		},
		{
			name: "ResourceSet resolved input", kind: KindResourceSet, complete: true,
			content: [][]byte{[]byte(`{"api":"fluxcd.controlplane.io"}`)}, possible: true,
		},
		{
			name: "ResourceSet inline input", kind: KindResourceSet, complete: true,
			spec: map[string]any{"inputs": []any{map[string]any{"api": FluxKustomizeDomain}}}, possible: true,
		},
		{
			name: "HelmRelease nested continuation", kind: KindHelmRelease, complete: true,
			content: [][]byte{[]byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n")}, possible: true,
		},
		{
			name: "ResourceSet nested continuation", kind: KindResourceSet, complete: true,
			spec: map[string]any{"resourcesTemplate": "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n"}, possible: true,
		},
		{
			name: "HelmRelease nested ResourceSet", kind: KindHelmRelease, complete: true,
			content: [][]byte{[]byte("apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\n")}, possible: true,
		},
		{
			name: "HelmRelease unrelated workload", kind: KindHelmRelease, complete: true,
			content: [][]byte{[]byte("apiVersion: apps/v1\nkind: Deployment\n")},
		},
		{name: "HelmRelease negative", kind: KindHelmRelease, complete: true},
		{name: "HelmRelease unknown chart", kind: KindHelmRelease, possible: true},
		{
			name: "HelmRelease resolved value", kind: KindHelmRelease, complete: true,
			content: [][]byte{[]byte(FluxOperatorDomain)}, possible: true,
		},
		{
			name: "HelmRelease declaration value", kind: KindHelmRelease, complete: true,
			spec: map[string]any{"values": map[string]any{"api": FluxKustomizeDomain}}, possible: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			group := FluxOperatorDomain
			if tc.kind == KindHelmRelease {
				group = HelmReleaseDomain
			}
			version := "/v1"
			if tc.kind == KindHelmRelease {
				version = "/v2"
			}
			spec := map[string]any{}
			maps.Copy(spec, tc.spec)
			if tc.kind == KindHelmRelease {
				spec["chartRef"] = map[string]any{"kind": KindOCIRepository, "name": "chart"}
			}
			parsed, err := ParseDoc(map[string]any{
				"apiVersion": group + version, "kind": tc.kind,
				"metadata": map[string]any{"name": "producer", "namespace": "apps"}, "spec": spec,
			}, ParseDocOptions{})
			assert.NoError(t, err)
			object := parsed.Object
			content := slices.Values(tc.content)
			if !tc.complete {
				content = nil
			}
			_, possible := ProjectProducer(object, content)
			assert.Equal(t, possible, tc.possible)
		})
	}
}

func TestDecodeDocs_ApplicationProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, document string
	}{
		{"Kustomization", `apiVersion: "kustomize.toolkit.fluxcd.io/v\x31"
kind: Kustomization
metadata: {name: owner, namespace: apps}
spec:
  path: ./clusters/test
  sourceRef: {apiVersion: source.toolkit.fluxcd.io/v1, kind: GitRepository, name: content, namespace: sources}
  dependsOn: [{name: prerequisite}]
  postBuild: {substitute: {ignored: !!int invalid}}
`},
		{"FluxInstance", `apiVersion: "fluxcd.controlplane.io/v\x31"
kind: FluxInstance
metadata: {name: flux, namespace: apps}
spec:
  sync: {kind: GitRepository, url: https://fixture.invalid/repo, ref: refs/heads/main, path: ./clusters/test}
  distribution: {version: !!int invalid}
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, err := DecodeDocs(strings.NewReader(tc.document), ApplicationDocuments)
			assert.NoError(t, err)
			assert.NoError(t, DocumentFailure(docs))
			assert.Equal(t, len(docs), 1)
			parsed, err := ParseDoc(docs[0].Document, ParseDocOptions{})
			assert.NoError(t, err)
			object := parsed.Object
			application, present := ApplicationFor(object)
			assert.Equal(t, present, true)
			assert.Equal(t, application.Path, "./clusters/test")
			if tc.name == KindKustomization {
				assert.Equal(t, application.Source.Namespace, "sources")
				assert.Equal(t, object.(*Kustomization).DependsOn[0].Name, "prerequisite")
			}
		})
	}
}

func TestDecodeDocs_ApplicationProjectionRetainsLists(t *testing.T) {
	t.Parallel()
	input := `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: List
  items:
  - apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: one, namespace: apps}
    spec:
      path: ./one
      sourceRef: {kind: GitRepository, name: one, namespace: sources}
      dependsOn: [{name: prerequisite}]
  - apiVersion: kustomize.toolkit.fluxcd.io/v1
    kind: Kustomization
    metadata: {name: two, namespace: apps}
    spec: {path: ./two, sourceRef: {kind: GitRepository, name: two}}
- apiVersion: fluxcd.controlplane.io/v1
  kind: FluxInstance
  metadata: {name: flux, namespace: apps}
  spec:
    sync: {kind: GitRepository, url: https://fixture.invalid/repo, ref: refs/heads/main, path: ./three}
---
apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: absent, namespace: apps}
`
	docs, err := DecodeDocs(strings.NewReader(input), ApplicationDocuments)
	assert.NoError(t, err)
	assert.NoError(t, DocumentFailure(docs))
	assert.Equal(t, len(docs), 2)
	outer := docs[0].Document["items"].([]any)
	assert.Equal(t, len(outer), 2)
	nested := outer[0].(map[string]any)["items"].([]any)
	assert.Equal(t, len(nested), 2)
	for i, name := range []string{"one", "two"} {
		parsed, parseErr := ParseDoc(nested[i].(map[string]any), ParseDocOptions{})
		assert.NoError(t, parseErr)
		object := parsed.Object.(*Kustomization)
		assert.Equal(t, object.Path, "./"+name)
		assert.Equal(t, object.SourceRef.Name, name)
		assert.Equal(t, object.SourceRef.Namespace, []string{"sources", ""}[i])
		assert.Equal(t, len(object.DependsOn), []int{1, 0}[i])
	}
	parsed, err := ParseDoc(outer[1].(map[string]any), ParseDocOptions{})
	assert.NoError(t, err)
	application, present := ApplicationFor(parsed.Object)
	assert.Equal(t, present, true)
	assert.Equal(t, application.Path, "./three")
	_, hasSpec := docs[1].Document["spec"]
	assert.Equal(t, hasSpec, false)
}
