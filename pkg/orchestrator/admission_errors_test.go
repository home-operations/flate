package orchestrator

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

type admissionErrorFS struct {
	fs.FS
	armed atomic.Bool
	cause error
}

func (f *admissionErrorFS) Open(name string) (fs.File, error) {
	if name == "flux/input.yaml" && f.armed.Load() {
		return nil, f.cause
	}
	return f.FS.Open(name)
}

func TestRender_AdmissionErrorPrecedesOptionalPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind     string
		helm, optional bool
		allowMissing   bool
	}{
		{name: "helm_optional", kind: manifest.KindConfigMap, helm: true, optional: true},
		{name: "helm_allow_missing", kind: manifest.KindSecret, helm: true, allowMissing: true},
		{name: "substitute_optional", kind: manifest.KindConfigMap, optional: true},
		{name: "substitute_secret", kind: manifest.KindSecret, allowMissing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			consumer := testutil.KustomizationID("apps", "consumer")
			declaration := testutil.KustomizationYAML("consumer", "apps", "  path: ./payload\n")
			if tc.helm {
				consumer = testutil.HelmReleaseID("apps", "consumer")
				declaration = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: consumer, namespace: apps}\nspec:\n  interval: 10m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: charts}}}\n"
			}
			testutil.WriteFile(t, root, "flux/consumer.yaml", declaration)
			testutil.WriteFile(t, root, "flux/charts.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: charts, namespace: apps}\nspec: {url: https://fixture.invalid/repo}\n")
			field := "data"
			if tc.kind == manifest.KindSecret {
				field = "stringData"
			}
			testutil.WriteFile(t, root, "flux/input.yaml", "apiVersion: v1\nkind: "+tc.kind+"\nmetadata: {name: input, namespace: apps}\n"+field+": {values.yaml: '{}'}\n")
			testutil.WriteFile(t, root, "payload/kustomization.yaml", "resources: []\n")
			cause := errors.New("fixture exact hydration failed")
			inputFS := &admissionErrorFS{FS: os.DirFS(root), cause: cause}
			input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: inputFS}
			var missingKinds []string
			if tc.allowMissing {
				missingKinds = []string{manifest.KindSecret}
			}
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root, Input: input, SelfURLs: []string{"https://fixture.invalid/repo"}}, Config{Concurrency: 2, AllowMissingKinds: missingKinds, Selection: manifest.Selection{Roots: []manifest.NamedResource{consumer}}})
			assert.NoError(t, err)
			// Exact hydration requires a frozen census without prepared entry payloads.
			assert.NoError(t, o.discover(t.Context()))
			assert.NoError(t, o.loader.FinalizeIndex(t.Context(), root, true))
			o.completeBootstrap()
			assert.NoError(t, o.buildChangeFilter(t.Context()))
			assert.NoError(t, o.installAdmission(t.Context()))
			dependency := manifest.NamedResource{Kind: tc.kind, Namespace: "apps", Name: "input"}
			assert.Equal(t, o.store.GetObject(dependency) == nil, true)
			var object manifest.BaseManifest
			controller := o.ksc.Controller
			if tc.helm {
				hr := o.store.GetDeclaration(consumer).(*manifest.HelmRelease).Clone()
				hr.ValuesFrom = []manifest.ValuesReference{{Kind: tc.kind, Name: "input", Optional: tc.optional}}
				object, controller = hr, o.hrc.Controller
			} else {
				ks := o.store.GetDeclaration(consumer).(*manifest.Kustomization).Clone()
				ks.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: tc.kind, Name: "input", Optional: tc.optional}}}
				object = ks
			}
			o.store.AddObject(object)
			inputFS.armed.Store(true)
			var execution base.Execution
			execution.Init(controller, consumer)
			_, err = execution.DataInputs(t.Context(), object, values.NewStoreProvider(&execution.Reads, false))
			assert.Equal(t, errors.Is(err, cause), true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			execution.Close()
			result, err := o.collectResult(o.run(t.Context()))
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			status, _ := o.store.GetStatus(consumer)
			assert.Equal(t, status.Status, store.StatusFailed)
			assert.Equal(t, strings.Contains(status.Message, cause.Error()), true)
			assert.Equal(t, status.IsDependencyFailure(), false)
			assert.Equal(t, o.store.GetObject(dependency) == nil, true)
			assert.Equal(t, o.store.GetArtifact(consumer, store.ArtifactContent) == nil, true)
			assert.Equal(t, len(outputByParent(result.Manifests)[consumer]), 0)
		})
	}
}
