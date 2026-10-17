package kustomize_test

import (
	"path/filepath"
	"strings"
	"testing"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/openapi"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/testutil"
)

const buildResources = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
spec:
  template:
    spec:
      containers:
      - name: main
        image: nginx:1
      - name: sidecar
        image: busybox:1
---
apiVersion: schema.example/v1
kind: Widget
metadata:
  name: app
spec:
  items:
  - name: first
    value: old
  - name: second
    value: keep
`

const buildConfig = `resources: [resources.yaml]
patches:
- patch: |-
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: app
    spec:
      template:
        spec:
          containers:
          - name: main
            image: nginx:2
- patch: |-
    apiVersion: schema.example/v1
    kind: Widget
    metadata:
      name: app
    spec:
      items:
      - name: first
        value: new
`

const buildSchema = `{"definitions":{"schema.example.v1.Widget":{
 "type":"object",
 "x-kubernetes-group-version-kind":[{"group":"schema.example","version":"v1","kind":"Widget"}],
 "properties":{"spec":{"type":"object","properties":{"items":{
  "type":"array","x-kubernetes-patch-strategy":"merge","x-kubernetes-patch-merge-key":"name",
  "items":{"type":"object","properties":{"name":{"type":"string"},"value":{"type":"string"}}}
 }}}}
}}}`

func buildFixture(t *testing.T) (filesys.FileSystem, string) {
	t.Helper()
	root := t.TempDir()
	configs := map[string]string{
		"plain":          buildConfig,
		"custom":         buildConfig + "openapi:\n  path: schema.json\n",
		"nested":         "resources: [../custom]\n",
		"default":        buildConfig + "openapi:\n  version: " + openapi.DefaultOpenAPI + "\n",
		"unsupported":    buildConfig + "openapi:\n  version: not-built-in\n",
		"unreadable":     buildConfig + "openapi:\n  path: missing.json\n",
		"conflict":       buildConfig + "openapi:\n  path: schema.json\n  version: " + openapi.DefaultOpenAPI + "\n",
		"ordinary-error": "resources: [missing.yaml]\n",
		"resource-error": "resources: [missing.yaml]\nopenapi:\n  path: schema.json\n",
		"parent-file":    "resources: [../shared.yaml]\n",
		"plugin":         "resources: [../shared.yaml]\ntransformers: [transformer.yaml]\n",
	}
	for dir, config := range configs {
		testutil.WriteFile(t, root, dir+"/kustomization.yaml", config)
		testutil.WriteFile(t, root, dir+"/resources.yaml", buildResources)
		testutil.WriteFile(t, root, dir+"/schema.json", buildSchema)
	}
	testutil.WriteFile(t, root, "shared.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n")
	testutil.WriteFile(t, root, "plugin/transformer.yaml", "apiVersion: schema.example/v1\nkind: ExternalTransformer\nmetadata: {name: external}\n")
	return filesys.MakeFsOnDisk(), root
}

func TestBuild_BuiltinRetention(t *testing.T) {
	fs, root := buildFixture(t)
	kustomize.BuildMutex.Lock()
	defer kustomize.BuildMutex.Unlock()
	openapi.ResetOpenAPI()
	defer openapi.ResetOpenAPI()

	first, err := kustomize.Build(fs, filepath.Join(root, "plain"))
	assert.NoError(t, err)
	want, err := first.AsYaml()
	assert.NoError(t, err)
	meta := yaml.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	definition := openapi.SchemaForResourceType(meta).Schema
	for _, dir := range []string{"plain", "ordinary-error"} {
		t.Run(dir, func(t *testing.T) {
			rm, err := kustomize.Build(fs, filepath.Join(root, dir))
			if dir == "plain" {
				assert.NoError(t, err)
				got, err := rm.AsYaml()
				assert.NoError(t, err)
				assert.Diff(t, string(got), string(want))
			} else if err == nil {
				t.Fatal("expected missing-resource error")
			}
			assert.Equal(t, openapi.SchemaForResourceType(meta).Schema, definition)
		})
	}

	openapi.ResetOpenAPI()
	custom, err := kustomize.Build(fs, filepath.Join(root, "custom"))
	assert.NoError(t, err)
	got, err := custom.AsYaml()
	assert.NoError(t, err)
	for _, retained := range []string{"name: sidecar", "name: second"} {
		if !strings.Contains(string(got), retained) {
			t.Errorf("cold additive custom build lost %q:\n%s", retained, got)
		}
	}
}

func TestBuild_FluxParity(t *testing.T) {
	fs, root := buildFixture(t)
	cases := []struct {
		name    string
		failure string
	}{
		{"plain", ""},
		{"custom", ""},
		{"nested", ""},
		{"default", ""},
		{"unsupported", "not built in"},
		{"unreadable", "missing.json"},
		{"conflict", "cannot use both"},
		{"resource-error", "missing.yaml"},
		{"parent-file", ""},
		{"plugin", "external plugins disabled"},
	}
	kustomize.BuildMutex.Lock()
	defer kustomize.BuildMutex.Unlock()
	openapi.ResetOpenAPI()
	defer openapi.ResetOpenAPI()
	type result struct{ output, failure string }
	expected := make(map[string]result, len(cases))
	for _, tc := range cases {
		rm, err := fluxkustomize.Build(fs, filepath.Join(root, tc.name))
		if tc.failure != "" {
			if err == nil || !strings.Contains(err.Error(), tc.failure) {
				t.Fatalf("%s: expected Flux error containing %q, got %v", tc.name, tc.failure, err)
			}
			expected[tc.name] = result{failure: err.Error()}
			continue
		}
		assert.NoError(t, err)
		out, err := rm.AsYaml()
		assert.NoError(t, err)
		expected[tc.name] = result{output: string(out)}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			openapi.ResetOpenAPI()
			for _, dir := range []string{"plain", tc.name, "plain"} {
				rm, err := kustomize.Build(fs, filepath.Join(root, dir))
				want := expected[dir]
				if want.failure != "" {
					if err == nil {
						t.Fatalf("%s: expected error %q", dir, want.failure)
					}
					assert.Equal(t, err.Error(), want.failure)
					if !strings.Contains(err.Error(), tc.failure) {
						t.Errorf("%s: expected error containing %q, got %v", dir, tc.failure, err)
					}
				} else {
					assert.NoError(t, err)
					out, err := rm.AsYaml()
					assert.NoError(t, err)
					assert.Diff(t, string(out), want.output)
				}
			}
		})
	}
}

func TestBuild_PanicRecovery(t *testing.T) {
	fs, root := buildFixture(t)
	testutil.WriteFile(t, root, "malformed/kustomization.yaml", buildConfig+"openapi:\n  path: schema.json\n")
	testutil.WriteFile(t, root, "malformed/resources.yaml", buildResources)
	testutil.WriteFile(t, root, "malformed/schema.json", "{invalid")
	testutil.WriteFile(t, root, "invalid-object/kustomization.yaml", "namespace: test\nnamePrefix: test\nresources: [resource.yaml]\n")
	testutil.WriteFile(t, root, "invalid-object/resource.yaml", "apiVersion: invalid[value,kind:synthetic]\nkind: invalid[value,kind:synthetic]\nmetadata: {name: app}\n")
	kustomize.BuildMutex.Lock()
	defer kustomize.BuildMutex.Unlock()
	openapi.ResetOpenAPI()
	defer openapi.ResetOpenAPI()
	expected := make(map[string]string)
	for _, dir := range []string{"malformed", "invalid-object"} {
		_, err := fluxkustomize.Build(fs, filepath.Join(root, dir))
		if err == nil || !strings.HasPrefix(err.Error(), "recovered from kustomize build panic:") {
			t.Fatalf("%s: expected real Flux recovered panic, got %v", dir, err)
		}
		expected[dir] = err.Error()
	}
	for _, dir := range []string{"malformed", "invalid-object"} {
		t.Run(dir, func(t *testing.T) {
			openapi.ResetOpenAPI()
			definitions := openapi.Schema()
			_, err := kustomize.Build(fs, filepath.Join(root, dir))
			if err == nil {
				t.Fatal("expected recovered panic")
			}
			assert.Equal(t, err.Error(), expected[dir])
			assert.Equal(t, openapi.GetSchemaVersion(), openapi.DefaultOpenAPI)
			assert.Equal(t, len(definitions.Definitions), 0)
			_, err = kustomize.Build(fs, filepath.Join(root, "plain"))
			assert.NoError(t, err)
			namespaced, found := openapi.IsNamespaceScoped(yaml.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"})
			assert.Equal(t, found, true)
			assert.Equal(t, namespaced, true)
		})
	}
}
