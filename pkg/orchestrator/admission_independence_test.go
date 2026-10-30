package orchestrator

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type admissionPeerFS struct {
	fs.FS
	armed            atomic.Bool
	once             sync.Once
	entered, release chan struct{}
	failure          error
}

func (f *admissionPeerFS) Open(name string) (fs.File, error) {
	if name == "peer/cm.yaml" && f.armed.Load() {
		f.once.Do(func() {
			close(f.entered)
			<-f.release
		})
		if f.failure != nil {
			return nil, f.failure
		}
	}
	return f.FS.Open(name)
}

func TestRenderTrees_IndependentAdmissionAfterPreparation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		peer              manifest.TreeSide
		failure, headOnly bool
	}{
		{name: "held_base", peer: manifest.TreeBase},
		{name: "held_head", peer: manifest.TreeHead},
		{name: "failed_base", peer: manifest.TreeBase, failure: true},
		{name: "failed_head", peer: manifest.TreeHead, failure: true},
		{name: "head_only", peer: manifest.TreeBase, headOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				roots := [2]string{t.TempDir(), t.TempDir()}
				for side, root := range roots {
					testutil.WriteFile(t, root, "flux/set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: selected, namespace: apps}
spec:
  inputs: [{name: values}]
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: values, namespace: apps}
    data: {values.yaml: 'value: `+[]string{"before", "after"}[side]+`'}
`)
					testutil.WriteFile(t, root, "flux/release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: waiting, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n  valuesFrom: [{kind: ConfigMap, name: values}]\n")
					testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
					testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settled}\ndata: {value: '{{ .Values.value }}'}\n")
				}
				testutil.WriteFile(t, roots[tc.peer], "flux/parent.yaml", testutil.KustomizationYAML("peer", "apps", "  path: ./peer\n"))
				testutil.WriteFile(t, roots[tc.peer], "peer/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, roots[tc.peer], "peer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: peer-output}\n")
				peer := &admissionPeerFS{FS: os.DirFS(roots[tc.peer]), entered: make(chan struct{}), release: make(chan struct{})}
				if tc.failure {
					peer.failure = manifest.NewError(manifest.ErrInput, nil, "peer proof failed")
				}
				var released sync.Once
				release := func() { released.Do(func() { close(peer.release) }) }
				defer release()
				if tc.failure {
					release()
				}
				selected := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "selected"}
				waiting := testutil.HelmReleaseID("apps", "waiting")
				var mu sync.Mutex
				ready := map[manifest.NamedResource]bool{}
				settled := make(chan struct{})
				cfg := Config{Concurrency: 2, HeadOnly: tc.headOnly, StatusListener: func(tree manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
					id := subject.ID
					if tree.Side == tc.peer && id == testutil.KustomizationID("apps", "peer") && info.Message == "resolving dependencies" {
						peer.armed.Store(true)
					}
					if tree.Side == tc.peer || id != selected && id != waiting || info.Outcome() != store.OutcomePassed {
						return
					}
					mu.Lock()
					if !ready[id] {
						ready[id] = true
						if len(ready) == 2 {
							close(settled)
						}
					}
					mu.Unlock()
				}}
				inputs := [2]fs.FS{os.DirFS(roots[0]), os.DirFS(roots[1])}
				inputs[tc.peer] = peer
				local := manifest.TreeHead - tc.peer
				var rendered []Rendered
				var renderErr error
				done := make(chan struct{})
				go func() {
					defer close(done)
					changes := change.NewSet([]string{"flux/set.yaml", "flux/parent.yaml"})
					rendered, renderErr = RenderTrees(t.Context(), []Tree{
						{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Path: filepath.Join(roots[0], "flux"), RepoRoot: roots[0], Input: &store.SourceArtifact{LocalPath: roots[0], LocalRoot: roots[0], FS: inputs[0]}, ExternalChanges: changes},
						{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: filepath.Join(roots[1], "flux"), RepoRoot: roots[1], Input: &store.SourceArtifact{LocalPath: roots[1], LocalRoot: roots[1], FS: inputs[1]}, ExternalChanges: changes},
					}, cfg)
				}()
				if !tc.headOnly {
					<-peer.entered
				}
				synctest.Wait()
				localSettled := false
				select {
				case <-settled:
					localSettled = true
				default:
				}
				assert.Equal(t, t.Context().Err() == nil, true)
				release()
				<-done
				assert.Equal(t, localSettled, true)
				assert.Equal(t, renderErr != nil, tc.failure)
				if len(rendered) != 2 {
					t.Fatalf("rendered %d sides: %v", len(rendered), renderErr)
				}
				assert.NoError(t, rendered[local].Err)
				documents := outputByParent(rendered[local].Result.Manifests)[waiting]
				if len(documents) != 1 {
					t.Fatalf("local output=%v, want one document", documents)
				}
				assert.Diff(t, documents[0]["data"], map[string]any{"value": []string{"before", "after"}[local]})
			})
		})
	}
}
