package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDAGDispatch_SelectionPromotionWakesPublishedConsumer(t *testing.T) {
	t.Parallel()
	for _, mode := range []struct {
		kind            string
		source, upfront bool
	}{
		{kind: manifest.KindKustomization}, {kind: manifest.KindHelmRelease},
		{kind: manifest.KindKustomization, source: true}, {kind: manifest.KindKustomization, source: true, upfront: true},
	} {
		kind := mode.kind
		name := kind
		if mode.source {
			name += fmt.Sprintf("/source_replacement/upfront_%t", mode.upfront)
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var previous []byte
			for range 2 {
				synctest.Test(t, func(t *testing.T) {
					s := store.New()
					tasks := task.NewBounded(2)
					parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
					emitter := &manifest.Kustomization{Name: "emitter", Namespace: "apps"}
					target := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"value": "promoted"}}
					var consumer manifest.BaseManifest
					if kind == manifest.KindKustomization {
						consumer = &manifest.Kustomization{
							Name: "consumer", Namespace: "apps", Path: "./consumer",
							PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: target.Name, Optional: true}}},
						}
					} else {
						consumer = &manifest.HelmRelease{
							Name: "consumer", Namespace: "apps",
							ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: target.Name}},
						}
					}
					root := t.TempDir()
					source := &manifest.GitRepository{Name: "selected", Namespace: "apps", URL: "git://fixture.invalid/before"}
					artifact := &store.SourceArtifact{LocalPath: filepath.Join(root, ".source"), LocalRoot: root, InputAlias: true, FS: fstest.MapFS{"consumer/config.yaml": {Data: []byte("before")}}}
					replacement := *source
					replacement.URL = "git://fixture.invalid/after"
					replacedArtifact := *artifact
					replacedArtifact.FS = fstest.MapFS{"consumer/config.yaml": {Data: []byte("after")}}
					if mode.source {
						consumer.(*manifest.Kustomization).SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: source.Named().Kind, Name: source.Name}
						s.AddObject(source)
						s.SetArtifact(source.Named(), artifact)
						s.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
					}
					s.AddObjects([]manifest.BaseManifest{parent, emitter})
					s.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: consumer}}}, nil)
					s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"})
					index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						emitter.Named(): {File: "emitter.yaml"},
					}}
					changes := change.NewSet([]string{"emitter.yaml"})
					if mode.source {
						peer := store.New()
						peer.AddObject(source)
						peer.SetArtifact(source.Named(), artifact)
						s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
						peer.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
						index.Records[source.Named()] = manifest.DiscoveryRecord{File: "old-source.yaml"}
						if mode.upfront {
							s.AddObject(&replacement)
							s.SetArtifact(source.Named(), &replacedArtifact)
							index.Records[source.Named()] = manifest.DiscoveryRecord{File: "source.yaml"}
						}
						var err error
						changes, err = change.Qualify(t.Context(), change.NewSet([]string{"emitter.yaml", "source.yaml"}), peer, s)
						assert.NoError(t, err)
					}
					s.SetDiscoveryIndex(index)
					filter := change.NewFilter(changes, index, s, root)
					c := base.New(s, tasks)
					c.Configure(base.Options{Filter: filter, Index: index})
					release := make(chan struct{})
					var dispatches, renders atomic.Int64
					dispatch := fixtureDispatcher{store: s, dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
						execution := controllertest.NewExecution(c, id)
						if id == emitter.Named() {
							tasks.YieldSlot(func() { <-release })
							execution.Dispatch(ctx, level, execution, func(execution *base.Execution, _ context.Context, _ *manifest.Kustomization, _ manifest.DrainLevel) error {
								_, _, next := execution.Reads.GetEmission(id)
								defer next.Finish()
								next.Children = []store.Child{{Obj: target}}
								if mode.source {
									s.SetArtifact(source.Named(), &replacedArtifact)
									next.Children = append(next.Children, store.Child{Obj: &replacement, Productions: []manifest.Production{{Source: manifest.BootstrapSourceID, File: "source.yaml", Path: filepath.Join(root, "source.yaml")}}})
								}
								next.Complete = true
								execution.Emit(id, &next, nil)
								return nil
							})
							return schedule.Result{Assessment: execution}
						}
						if mode.source && id == source.Named() {
							s.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
						}
						if id != consumer.Named() {
							execution.Reads.GetObject(id)
							execution.Reads.Accept()
							return schedule.Result{Assessment: execution}
						}
						dispatches.Add(1)
						execution.Dispatch(ctx, level, execution, func(execution *base.Execution, ctx context.Context, obj manifest.BaseManifest, level manifest.DrainLevel) error {
							if err := execution.Require(ctx, []manifest.DependencyRef{{NamedResource: target.Named()}}, level); err != nil {
								return err
							}
							input := execution.Reads.GetObject(target.Named()).(*manifest.ConfigMap)
							_, _, next := execution.Reads.GetEmission(id)
							defer next.Finish()
							s.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "rendering"})
							next.Manifests = []map[string]any{{"value": input.Data["value"]}}
							next.Complete = true
							renders.Add(1)
							execution.Emit(obj.Named(), &next, nil)
							return nil
						})
						return schedule.Result{Assessment: execution}
					}}

					sched := schedule.New(tasks, dispatch)
					o := &Orchestrator{store: s, src: &sourcectrl.Controller{}}
					defer o.watchDAG(sched)()
					sched.Seed([]schedule.NodeID{emitter.Named(), consumer.Named()})
					done := make(chan error, 1)
					go func() { done <- sched.Run(t.Context()) }()
					synctest.Wait()
					initial, _ := s.GetStatus(consumer.Named())
					initialDispatches, initialRenders := dispatches.Load(), renders.Load()
					initiallySelected := filter.ShouldReconcile(consumer.Named())
					close(release)
					if err := <-done; err != nil {
						t.Fatal(err)
					}
					if !mode.upfront {
						assert.Equal(t, initial.Status, store.StatusReady)
						assert.Equal(t, initial.Message, "unchanged")
						assert.Equal(t, initialDispatches, int64(1))
						assert.Equal(t, initialRenders, int64(0))
						assert.Equal(t, initiallySelected, false)
					} else {
						assert.Equal(t, initiallySelected, true)
					}
					assert.Equal(t, filter.ShouldReconcile(consumer.Named()), true)
					assert.Equal(t, tasks.Failures(), 0)
					if renders.Load() == 0 {
						t.Fatal("selected published consumer did not render after its input arrived")
					}
					status, _ := s.GetStatus(parent.Named())
					assert.Equal(t, status.Status, store.StatusPending)
					status, _ = s.GetStatus(consumer.Named())
					assert.Equal(t, status.Status, store.StatusReady)
					output, ok := s.GetArtifact(consumer.Named()).(*store.Emission)
					if !ok {
						t.Fatal("promoted consumer has no rendered emission")
					}
					outputBytes, err := json.Marshal(output.Manifests)
					assert.NoError(t, err)
					assert.Equal(t, string(outputBytes), `[{"value":"promoted"}]`)
					assert.Diff(t, output.Manifests, []map[string]any{{"value": "promoted"}})
					if mode.source {
						want := []string{source.Named().String(), parent.Named().String(), emitter.Named().String(), target.Named().String(), consumer.Named().String()}
						slices.Sort(want)
						assert.Diff(t, filter.KeepNames(), want)
						t.Logf("upfront=%t exact final identities=%v bytes=%q", mode.upfront, filter.KeepNames(), outputBytes)
						assert.Equal(t, filter.ShouldReconcile(source.Named()), true)
						assert.Equal(t, s.GetObject(source.Named()).(*manifest.GitRepository).URL, replacement.URL)
					}
					if previous != nil {
						assert.Equal(t, string(outputBytes), string(previous))
					}
					previous = bytes.Clone(outputBytes)
				})
			}
		})
	}
}
