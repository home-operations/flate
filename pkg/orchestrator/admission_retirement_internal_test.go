package orchestrator

import (
	"path/filepath"
	"runtime"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

func TestRenderInvocation_AdmissionReclaimsRetiredCaptures(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"construction", "admission"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testRenderInvocationAdmissionReclaimsRetiredCaptures(t, phase)
		})
	}
}

func testRenderInvocationAdmissionReclaimsRetiredCaptures(t *testing.T, phase string) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./payload\n"))
	testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"payload/output.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: retained}\n",
	)
	o, err := newTestOrchestrator(t, Tree{
		Path: filepath.Join(root, "flux"), RepoRoot: root,
		ExternalChanges: change.NewSet([]string{"flux/owner.yaml"}),
	}, Config{Concurrency: 2, CacheDir: t.TempDir(), Selection: selection.Request{Changed: true}})
	assert.NoError(t, err)
	assert.NoError(t, o.prepareBootstrap(t.Context()))
	assert.Equal(t, o.preparation != nil, true)
	captured := weak.Make(o.preparation)
	transient := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "transient"}
	declaration := func() weak.Pointer[manifest.ConfigMap] {
		object := &manifest.ConfigMap{Name: transient.Name, Namespace: transient.Namespace}
		o.store.AddObject(object)
		return weak.Make(object)
	}()
	invocation := renderInvocation{results: []Rendered{{Orchestrator: o}}}
	if phase == "construction" {
		invocation.prepareSession(t.Context(), 0)
		assert.NoError(t, invocation.results[0].Err)
		o.store.DeleteObject(transient)
		invocation.prepareSession(t.Context(), 0)
		assert.NoError(t, invocation.results[0].Err)
		runtime.GC()
		assert.Equal(t, declaration.Value() == nil, true)
	}
	invocation.installAdmission(t.Context())
	assert.NoError(t, invocation.results[0].Err)
	o.store.DeleteObject(transient)
	runtime.GC()
	assert.Equal(t, declaration.Value() == nil, true)
	assert.Equal(t, captured.Value() == nil, true)
	assert.Equal(t, o.admissionPreparation == nil, true)
	result, err := o.collectResult(t.Context(), o.run(t.Context()))
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	owner := testutil.KustomizationID("apps", "owner")
	docs := outputByParent(result.Manifests)[owner]
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0]["data"].(map[string]any)["value"], "retained")
}
