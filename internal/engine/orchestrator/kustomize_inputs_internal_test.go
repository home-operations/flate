package orchestrator

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestKustomizeInputPair_CatalogAndIncomingAdmission(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"previous", "next"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			owners, changes := pairedCatalogReaders(t, changed)
			before, head := owners[0], owners[1]
			id := testutil.KustomizationID("apps", "reader")
			record, found := head.index.Lookup(id)
			occurrence, _ := head.loader.Occurrence(id)
			assert.Equal(t, found, true)
			assert.Equal(t, occurrence.Located, true)
			assert.Equal(t, record.Declaration, manifest.BaseManifest(nil))
			assert.Equal(t, head.loader.AdmissionCandidate(id).Obj, manifest.BaseManifest(nil))
			assert.Equal(t, head.store.GetObject(id), manifest.BaseManifest(nil))

			head.bindKustomizeInputs(t.Context(), before, changes)
			previous := head.kustomizeInputs.indexed[id].object
			reader, ok := previous.(*manifest.Kustomization)
			assert.FatalIf(t, !ok, "catalog did not supply a Kustomization for %v", id)
			assert.Equal(t, reader.Path, "previous")
			capture := head.kustomizeInputs.prepared[id]
			assert.FatalIf(t, capture == nil, "catalog did not supply captured paths for %v", id)
			assert.Diff(t, capture.Files, []string{
				"inputs/previous.yaml", "previous/kustomization.yaml",
			})

			successor := reader.Clone()
			successor.Path = "next"
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			head.store.AddObject(parent)
			controller := base.New(head.store, nil)
			controller.Configure(base.Options{Session: head.selection, Loader: head.loader, Index: head.index})
			assert.NoError(t, controllertest.Emit(t.Context(), controller, parent.Named(), &store.Emission{
				Complete: true, Children: []store.Child{{Obj: successor}},
			}))
			assert.Equal(t, head.selection.Snapshot().Changed(id), changed == "next")
			assert.Equal(t, head.store.GetObject(id), manifest.BaseManifest(successor))
		})
	}
}

func pairedCatalogReaders(t *testing.T, changed string) ([2]*Orchestrator, *change.Set) {
	t.Helper()
	generator := &manifest.RawObject{
		APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
		Kind:       manifest.KindArtifactGenerator, Name: "generator", Namespace: "apps",
	}
	artifact := generator.ExternalArtifact(metav1.ObjectMeta{Name: "generated", Namespace: "apps"})
	var owners [2]*Orchestrator
	var pair change.SourcePair
	for i, path := range []string{"base", "previous"} {
		root := t.TempDir()
		st := store.New()
		l := loader.New(st)
		selected, err := selection.New(st, l, selection.Request{Changed: true})
		assert.NoError(t, err)
		l.SourceRoot, l.Options.Session = root, selected
		testutil.WriteFile(t, root, "flux/reader.yaml", testutil.KustomizationYAML(
			"reader", "apps", "  path: "+path+"\n  sourceRef: {kind: ExternalArtifact, name: generated}\n",
		))
		_, err = l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		st.AddObject(generator)
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{FS: fstest.MapFS{}, Content: "bootstrap"})
		files := fstest.MapFS{}
		for _, pkg := range []string{"base", "previous", "next"} {
			files[pkg+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [../inputs/" + pkg + ".yaml]")}
			files["inputs/"+pkg+".yaml"] = &fstest.MapFile{Data: []byte("same")}
		}
		if i == 1 {
			files["inputs/"+changed+".yaml"] = &fstest.MapFile{Data: []byte("changed")}
		}
		st.RecordEmitted(generator.Named(), &store.Emission{Complete: true, Children: []store.Child{{Obj: artifact}}},
			map[manifest.NamedResource]*store.SourceArtifact{
				artifact.Named(): {FS: &catalogReaderFiles{MapFS: files}, Key: "apps/generated"},
			},
		)
		side := manifest.TreeSide(i)
		owners[i] = &Orchestrator{store: st, loader: l, index: l.Index, selection: selected, cfg: treeConfig{
			ID: manifest.TreeID{Cluster: ".", Side: side},
		}}
		pair.Stores[i], pair.Indexes[i] = st, l.Index
	}
	changes, err := change.Qualify(t.Context(), nil, pair, manifest.WorkScope{})
	assert.NoError(t, err)
	for _, owner := range owners {
		owner.loader.Options.Changes = changes
	}
	return owners, changes
}

func TestInvocation_QualifyReadersReturnsCancellation(t *testing.T) {
	t.Parallel()
	id := manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}
	invocation := Invocation{results: []Rendered{{ID: id, Orchestrator: &Orchestrator{cfg: treeConfig{ID: id}}}}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, invocation.qualifyReaders(ctx, 0), context.Canceled)
}

type catalogReaderFiles struct {
	fstest.MapFS

	cancel context.CancelFunc
}

func (f *catalogReaderFiles) ReadFile(name string) ([]byte, error) {
	if name == "base/kustomization.yaml" && f.cancel != nil {
		f.cancel()
	}
	return f.MapFS.ReadFile(name)
}

func TestOrchestrator_BindDeclaredReadersReturnsCancellation(t *testing.T) {
	t.Parallel()
	owners, changes := pairedCatalogReaders(t, "next")
	artifact := (&manifest.ExternalArtifact{Name: "generated", Namespace: "apps"}).Named()
	_, snapshots, _ := changes.ForSource(artifact)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	snapshots[manifest.TreeBase].FS.(*catalogReaderFiles).cancel = cancel

	err := owners[1].bindDeclaredReaders(ctx, owners[0], changes)
	assert.Equal(t, owners[0].kustomizeInputs != nil, true)
	assert.Equal(t, err, context.Canceled)
}
