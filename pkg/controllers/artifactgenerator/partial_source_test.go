package artifactgenerator

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcecontroller "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sources "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestArtifactGenerator_PartialSourceConvergence(t *testing.T) {
	var first, second []map[string]any
	t.Run("first", func(t *testing.T) { first = partialSourcePublication(t) })
	t.Run("second", func(t *testing.T) { second = partialSourcePublication(t) })
	assert.Diff(t, second, first)
}

func partialSourcePublication(t *testing.T) []map[string]any {
	t.Helper()
	var output []map[string]any
	// A real krusty build must not wait on BuildMutex held outside its synctest bubble.
	synctest.Test(t, func(t *testing.T) {
		const unrelated = 40
		st, bodies := store.New(), task.NewBounded(2)
		owner, err := cache.New(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		}()
		outputs := make([]any, 0, unrelated+1)
		for i := range unrelated + 1 {
			name := "input"
			if i != 0 {
				name = fmt.Sprintf("unrelated-%02d", i)
			}
			outputs = append(outputs, map[string]any{"name": name, "copy": []any{map[string]any{"from": "@repo/app/", "to": "@artifact/app/"}}})
		}
		generator, generatorDoc := generatorForTest(t, outputs)
		st.AddObject(generator, generatorDoc)
		consumerDoc := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "consumer", "namespace": "apps"}, "spec": map[string]any{"interval": "1m", "path": "./app", "sourceRef": map[string]any{"kind": manifest.KindExternalArtifact, "name": "input"}}}
		consumer, authoritative, _, err := manifest.ParseDoc(consumerDoc, manifest.ParseDocOptions{})
		if err != nil {
			t.Fatal(err)
		}
		st.AddObject(consumer, authoritative)
		repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
		st.SetArtifact(repo, &store.SourceArtifact{Key: "stable", Content: "stable", FS: fstest.MapFS{
			"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
			"app/cm.yaml":            {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: stable}\n")},
		}})
		gen := New(st, bodies, owner)
		gen.StartLifecycle()
		trees := kustomize.NewTreeCache(owner, nil, bodies)
		defer trees.Close()
		ks := kustomization.New(st, bodies, trees, false)
		ks.StartLifecycle()
		src := sourcecontroller.New(st, bodies)
		src.Fetchers[manifest.KindExternalArtifact] = sources.Wrap(manifest.KindExternalArtifact, &external.Fetcher{})
		src.StartLifecycle()
		var work generatorWork
		d := &convergenceDispatcher{generator: gen, id: generator.Named(), consumer: ks, source: src, work: &work}
		s := schedule.New(bodies, d)
		committed := make(chan bool, 1)
		advance, captured, resume := make(chan struct{}), make(chan struct{}, 1), make(chan struct{})
		var captures, publications atomic.Int64
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) }, false)()
		for _, event := range []store.EventKind{store.EventStatusUpdated, store.EventArtifactUpdated} {
			defer st.AddListener(event, func(manifest.NamedResource, any) { s.OnPublication() }, false)()
		}
		defer st.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, value any) {
			if id != generator.Named() {
				return
			}
			complete := value.(*store.Emission).Complete
			committed <- complete
			if !complete && publications.Add(1) > 1 {
				bodies.YieldSlot(func() {
					select {
					case <-advance:
					case <-ctx.Done():
					}
				})
			}
		}, false)()
		defer st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, value any) {
			if id != consumer.Named() || value.(store.StatusInfo).Message != "resolving source artifact" {
				return
			}
			inventory, _ := st.GetArtifact(generator.Named(), store.ArtifactContent).(*store.Emission)
			if inventory == nil || inventory.Complete {
				return
			}
			captures.Add(1)
			captured <- struct{}{}
			bodies.YieldSlot(func() {
				select {
				case <-resume:
				case <-ctx.Done():
				}
			})
		}, false)()
		s.Seed([]schedule.NodeID{generator.Named(), consumer.Named()})
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		defer func() {
			cancel()
			bodies.BlockTillDone()
		}()
		firstComplete := <-committed
		if firstComplete || <-committed {
			t.Fatal("generator completed before its first partial publication")
		}
		input := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "input"}
		var runErr error
		for publication := 2; publication <= unrelated+1; publication++ {
			synctest.Wait()
			select {
			case runErr = <-done:
				t.Fatalf("convergent generator exhausted consumer budget: captures=%d unrelated=%d error=%v", captures.Load(), publication-1, runErr)
			default:
			}
			paused := false
			select {
			case <-captured:
				paused = true
			default:
				status, _ := st.GetStatus(consumer.Named())
				if status.Status != store.StatusReady {
					cancel()
					runErr = <-done
					t.Fatalf("consumer neither captured nor accepted: captures=%d publications=%d status=%+v error=%v", captures.Load(), publications.Load(), status, runErr)
				}
			}
			before := st.GetArtifact(input, store.ArtifactContent)
			advance <- struct{}{}
			complete := <-committed
			if before != st.GetArtifact(input, store.ArtifactContent) {
				t.Fatalf("unrelated publication %d changed the consumed source", publication)
			}
			if paused {
				resume <- struct{}{}
			}
			if complete {
				break
			}
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		status, _ := st.GetStatus(consumer.Named())
		inventory, ok := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
		if !ok || !inventory.Complete || len(inventory.Manifests) != 1 || inventory.Manifests[0]["data"].(map[string]any)["value"] != "stable" || status.Status != store.StatusReady || !s.Results()[consumer.Named()].Assessment.Valid() {
			t.Fatalf("unaccepted stable consumer: status=%+v inventory=%+v", status, inventory)
		}
		assert.Equal(t, captures.Load(), int64(1))
		output = inventory.Manifests
	})
	return output
}
