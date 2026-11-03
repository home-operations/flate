package artifactgenerator

import (
	"archive/tar"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil/controllertest"
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

func consumedPublication(t *testing.T, publication, change string) []byte {
	t.Helper()
	var documents []byte
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		st, bodies := store.New(), task.NewBounded(2)
		owner, err := cache.New(ctx, "")
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
		defer func() {
			{
				{
					localErr := owner.Close()
					assert.ErrorIf(t, localErr != nil, "%v", localErr)
				}
			}
		}()
		acquire := func(value string) *store.SourceArtifact {
			t.Helper()
			input, descriptor, id, innerLocalErr := owner.Acquire(
				ctx,
				cache.ArchiveKey{Policy: "fixed", Content: value},
				cache.Policy{MaxAge: cache.Forever},
				func(_ context.Context, target cache.ProducerInput) (cache.ProducerResult, error) {
					archive := tar.NewWriter(target.Writer)
					files := fstest.MapFS{
						"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
						"app/cm.yaml": {
							Data: []byte(
								"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-config, namespace: apps}\ndata: {value: " + value + "}\n",
							),
						},
					}
					if innerInnerLocalErr := archive.AddFS(files); innerInnerLocalErr != nil {
						return cache.ProducerResult{}, innerInnerLocalErr
					}
					return cache.ProducerResult{}, archive.Close()
				},
			)
			{
				assert.FatalIf(t, innerLocalErr != nil, "%v", innerLocalErr)
			}
			return &store.SourceArtifact{
				FS:       input,
				Key:      id,
				Content:  cache.AcquisitionIdentity(input),
				Digest:   descriptor.Digest.String(),
				Revision: value + "-revision",
				Metadata: map[string]string{"org.opencontainers.image.revision": value + "-origin"},
			}
		}
		s1, s2 := acquire("S1"), acquire("S2")
		if change == "selected-origin-presence" || change == "selected-origin-fallback" {
			delete(s1.Metadata, "org.opencontainers.image.revision")
		}
		if change != "selected-union" {
			clone := *s1
			clone.Metadata = maps.Clone(s1.Metadata)
			s2 = &clone
			if change == "ignored-artifact-metadata" {
				s2.Metadata["ignored"] = "new"
			}
			if change == "unselected-revision" || change == "selected-revision" ||
				change == "origin-without-revision" ||
				change == "selected-origin-fallback" {
				s2.Revision = "next-revision"
			}
			if change == "unselected-origin" {
				s2.Metadata["org.opencontainers.image.revision"] = "next-origin"
			}
			if change == "selected-origin-presence" {
				s2.Metadata["org.opencontainers.image.revision"] = ""
			}
		}
		obj, doc := generatorForTest(
			t,
			[]any{map[string]any{"name": "output", "revision": "@repo", "originRevision": "@repo", "copy": []any{
				map[string]any{"from": "@repo/app/kustomization.yaml", "to": "@artifact/app/kustomization.yaml"},
				map[string]any{"from": "@repo/app/cm.yaml", "to": "@artifact/app/cm.yaml"},
			}}},
		)
		if strings.HasPrefix(change, "unselected-") || change == "origin-without-revision" ||
			change == "selected-origin-fallback" {
			output := doc["spec"].(map[string]any)["artifacts"].([]any)[0].(map[string]any)
			delete(output, "revision")
			if strings.HasPrefix(change, "unselected-") {
				delete(output, "originRevision")
			}
			parsed, authoritative, _, innerInnerInnerLocalErr := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			{
				assert.FatalIf(t, innerInnerInnerLocalErr != nil, "%v", innerInnerInnerLocalErr)
			}
			obj, doc = parsed.(*manifest.RawObject), authoritative
		}
		repo := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "apps",
			Name:      "repo",
		}
		if publication == "emitted" {
			repo.Kind = manifest.KindExternalArtifact
			doc["spec"].(map[string]any)["sources"].([]any)[0].(map[string]any)["kind"] = repo.Kind
			parsed, authoritative, _, innerInnerInnerInnerLocalErr := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			{
				assert.FatalIf(t, innerInnerInnerInnerLocalErr != nil, "%v", innerInnerInnerInnerLocalErr)
			}
			obj, doc = parsed.(*manifest.RawObject), authoritative
		}
		st.AddObject(obj, doc)
		rebound := repo
		rebound.Name = "other"
		if change == "selected-binding" {
			st.SetArtifact(rebound, s1)
		}
		consumerDoc := map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1",
			"kind":       manifest.KindKustomization,
			"metadata":   map[string]any{"name": "consumer", "namespace": "apps"},
			"spec": map[string]any{
				"interval":  "1m",
				"path":      "./app",
				"sourceRef": map[string]any{"kind": manifest.KindExternalArtifact, "name": "output"},
			},
		}
		consumer, authoritative, _, err := manifest.ParseDoc(consumerDoc, manifest.ParseDocOptions{})
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
		st.AddObject(consumer, authoritative)
		driver := &manifest.Kustomization{Name: "driver", Namespace: "apps"}
		st.AddObject(driver)
		generator := New(st, bodies, owner)
		generator.StartLifecycle()
		trees := kustomize.NewTreeCache(owner, nil, bodies)
		defer trees.Close()
		ks := kustomization.New(st, bodies, trees, false)
		ks.StartLifecycle()
		src := sourcecontroller.New(st, bodies)
		src.Fetchers[manifest.KindExternalArtifact] = sources.Wrap(manifest.KindExternalArtifact, &external.Fetcher{})
		src.StartLifecycle()
		publish := func(input *store.SourceArtifact) { st.SetArtifact(repo, input) }
		if publication == "emitted" {
			supplier := &manifest.ResourceSet{Name: "supplier", Namespace: "apps"}
			st.AddObject(supplier)
			producer := controllertest.NewExecution(generator.Controller, supplier.Named())
			defer producer.Close()
			publish = func(input *store.SourceArtifact) {
				_, _, next := producer.Reads.GetEmission(supplier.Named())
				defer next.Finish()
				childDoc := map[string]any{
					"apiVersion": manifest.SourceDomain + "/v1",
					"kind":       repo.Kind,
					"metadata":   map[string]any{"name": repo.Name, "namespace": repo.Namespace},
				}
				children, docs := testutil.ParseChildren(
					generator,
					supplier,
					[]map[string]any{childDoc},
					manifest.ParseDocOptions{},
				)
				next.Children, next.Manifests, next.Complete = children, testutil.DocumentOccurrences(docs), true
				{
					{
						innerInnerInnerInnerInnerLocalErr := producer.Emit(
							t.Context(),
							supplier.Named(),
							&next,
							map[manifest.NamedResource]*store.SourceArtifact{repo: input},
						)
						assert.ErrorIf(
							t,
							innerInnerInnerInnerInnerLocalErr != nil,
							"%v",
							innerInnerInnerInnerInnerLocalErr,
						)
					}
				}
				producer.Reads.Accept()
			}
			publish(s1)
			inventory, ok := st.GetArtifact(supplier.Named(), store.ArtifactContent).(*store.Emission)
			{
				assert.FatalIf(
					t,
					!ok || !inventory.Complete || st.EmissionOwner(repo) != supplier.Named(),
					"%v",
					"S1 supplying emission is not complete and visible",
				)
			}
			st.UpdateStatus(supplier.Named(), store.StatusInfo{Status: store.StatusReady})
		} else {
			st.AddObject(&manifest.GitRepository{Name: repo.Name, Namespace: repo.Namespace})
			publish(s1)
		}
		{
			assert.FatalIf(
				t,
				st.GetArtifact(repo, store.ArtifactContent) != s1,
				"%v",
				"S1 source is not visible before scheduler start",
			)
		}
		firstGenerator, firstConsumer, startPublication := make(chan struct{}), make(chan struct{}), make(chan struct{})
		committed, release, settled, finishDriver := make(
			chan struct{},
		), make(
			chan struct{},
		), make(
			chan struct{},
		), make(
			chan struct{},
		)
		driverDone := make(chan struct{})
		var genSignaled, consumerSignaled, successorSignaled, paused atomic.Bool
		var work generatorWork
		tasks := &convergenceTasks{Service: bodies, generator: obj.Named(), work: &work}
		tasks.completed = func(name string) {
			if name == "schedule/"+driver.Named().String() {
				close(driverDone)
			}
			if name == "schedule/"+obj.Named().String() || name == "schedule/settle/"+obj.Named().String() {
				status, _ := st.GetStatus(obj.Named())
				if status.Status == store.StatusReady && genSignaled.CompareAndSwap(false, true) {
					close(firstGenerator)
				}
			}
			if name == "schedule/"+consumer.Named().String() || name == "schedule/settle/"+consumer.Named().String() {
				inventory, ok := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
				if ok && inventory.Complete && len(inventory.Manifests) == 1 {
					value := inventory.Manifests[0].Document["data"].(map[string]any)["value"]
					if value == "S1" && consumerSignaled.CompareAndSwap(false, true) {
						close(firstConsumer)
					}
					if value == "S2" && successorSignaled.CompareAndSwap(false, true) {
						close(settled)
					}
				}
			}
		}
		wait := func(ctx context.Context, ch <-chan struct{}) bool {
			var received bool
			bodies.YieldSlot(func() {
				select {
				case <-ch:
					received = true
				case <-ctx.Done():
				}
			})
			return received
		}
		d := &convergenceDispatcher{
			generator: generator,
			id:        obj.Named(),
			consumer:  ks,
			source:    src,
			driver:    driver.Named(),
			work:      &work,
		}
		d.drive = func(ctx context.Context) {
			if !wait(ctx, firstGenerator) || !wait(ctx, firstConsumer) || !wait(ctx, startPublication) {
				return
			}
			bodies.YieldSlot(func() {
				if change == "ignored-declaration-metadata" || change == "selected-uid" ||
					change == "selected-binding" {
					updated := manifest.DeepCopyMap(doc)
					updated["metadata"].(map[string]any)["annotations"] = map[string]any{"ignored": "new"}
					if change == "selected-uid" {
						updated["metadata"].(map[string]any)["uid"] = "next"
					}
					selected := manifest.BaseManifest(obj)
					if change == "selected-binding" {
						updated["spec"].(map[string]any)["sources"].([]any)[0].(map[string]any)["name"] = rebound.Name
						parsed, authoritative, _, innerInnerInnerInnerInnerInnerLocalErr := manifest.ParseDoc(
							updated,
							manifest.ParseDocOptions{},
						)
						{
							assert.FatalIf(
								t,
								innerInnerInnerInnerInnerInnerLocalErr != nil,
								"%v",
								innerInnerInnerInnerInnerInnerLocalErr,
							)
						}
						selected, updated = parsed, authoritative
					}
					st.AddObject(selected, updated)
				} else {
					publish(s2)
				}
			})
			if change == "selected-union" {
				wait(ctx, settled)
			}
			wait(ctx, finishDriver)
		}
		s := schedule.New(tasks, d)
		pause := func(id manifest.NamedResource, _ any) {
			if change == "selected-union" && id == repo && st.GetArtifact(repo, store.ArtifactContent) == s2 &&
				paused.CompareAndSwap(false, true) {
				close(committed)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
		}
		for _, event := range []store.EventKind{store.EventObjectAdded, store.EventArtifactUpdated} {
			defer st.AddListener(event, pause, false)()
		}
		defer st.AddListener(
			store.EventObjectAdded,
			func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) },
			false,
		)()
		for _, event := range []store.EventKind{store.EventArtifactUpdated, store.EventStatusUpdated} {
			defer st.AddListener(event, func(manifest.NamedResource, any) { s.OnPublication() }, false)()
		}
		s.Seed([]schedule.NodeID{obj.Named(), consumer.Named(), driver.Named()})
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		defer func() {
			cancel()
			bodies.BlockTillDone()
		}()
		synctest.Wait()
		select {
		case <-firstGenerator:
		default:
			status, _ := st.GetStatus(obj.Named())
			t.Fatalf("initial generator did not settle: %+v", status)
		}
		select {
		case <-firstConsumer:
		default:
			status, _ := st.GetStatus(consumer.Named())
			t.Fatalf(
				"initial consumer did not settle: status=%+v inventory=%+v generator=%+v",
				status,
				st.GetArtifact(consumer.Named(), store.ArtifactContent),
				st.GetArtifact(obj.Named(), store.ArtifactContent),
			)
		}
		before := work.snapshot()
		if work.active.Load() != 0 || before.Admitted != 1 || before.Dispatched != 1 || before.Settled != 0 ||
			before.Acquires != 1 {
			t.Fatalf(
				"S1 boundary has outstanding or unexpected generator work: active=%d work=%+v",
				work.active.Load(),
				before,
			)
		}
		beforeSweep := d.sweeps.Load()
		close(startPublication)
		if change == "selected-union" {
			synctest.Wait()
			select {
			case <-committed:
			default:
				t.Fatal("S2 publication did not reach its paused notification")
			}
			{
				assert.FatalIf(
					t,
					st.GetArtifact(repo, store.ArtifactContent) != s2,
					"%v",
					"publication listener ran before S2 commit/unlock",
				)
			}
			inventory := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
			{
				assert.FatalIf(
					t,
					inventory.Manifests[0].Document["data"].(map[string]any)["value"] != "S1",
					"%v",
					"paused notification did not permit the last completed S1 inventory",
				)
			}
			select {
			case <-driverDone:
				t.Fatal("driver finished before notification release")
			default:
			}
			close(release)
			synctest.Wait()
			select {
			case <-settled:
			default:
				t.Fatalf(
					"consumer did not settle to S2 after release: generator work=%+v inventory=%+v",
					work.snapshot(),
					st.GetArtifact(consumer.Named(), store.ArtifactContent),
				)
			}
		}
		synctest.Wait()
		// An explicit publication requests an acknowledged real Sweep even when the descriptor deduplicates.
		s.OnPublication()
		synctest.Wait()
		{
			assert.FatalIf(t, d.sweeps.Load() <= beforeSweep, "%v", "no acknowledged Sweep after publication")
		}
		want := before
		if strings.HasPrefix(change, "selected-") {
			want.Admitted++
			want.Slots++
			want.Dispatched++
			want.Acquires++
		}
		atSweep := work.snapshot()
		{
			{
				diff := cmp.Diff(want, atSweep)
				assert.ErrorIf(t, diff != "", "%s work after acknowledged Sweep (-want +got):\n%s", change, diff)
			}
		}
		{
			assert.ErrorIf(
				t,
				work.active.Load() != 0,
				"%v",
				"generator closure remains in flight after acknowledged Sweep",
			)
		}
		close(finishDriver)
		{
			{
				innerInnerInnerInnerInnerInnerInnerLocalErr := <-done
				assert.FatalIf(
					t,
					innerInnerInnerInnerInnerInnerInnerLocalErr != nil,
					"%v",
					innerInnerInnerInnerInnerInnerInnerLocalErr,
				)
			}
		}
		bodies.BlockTillDone()
		if bodies.Failures() != 0 || work.active.Load() != 0 || len(slices.Collect(st.Sweep())) != 0 {
			t.Fatalf("undrained boundary: task failures=%d", bodies.Failures())
		}
		after := work.snapshot()
		{
			{
				diff := cmp.Diff(atSweep, after)
				assert.ErrorIf(
					t,
					diff != "",
					"%s delayed generator work after final drain (-want +got):\n%s",
					change,
					diff,
				)
			}
		}
		// No outstanding task, dispatch or Settle structurally excludes slot reacquisition and other Acquire paths.
		value := "S1"
		if change == "selected-union" {
			value = "S2"
		}
		artifact := st.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "output"}, store.ArtifactContent).(*store.SourceArtifact)
		if strings.HasPrefix(change, "unselected-") {
			{
				assert.FatalIf(
					t,
					artifact.Revision != "latest@"+artifact.Digest || len(artifact.Metadata) != 0,
					"content-only output consumed metadata: %+v",
					artifact,
				)
			}
		} else {
			expected := s1
			if strings.HasPrefix(change, "selected-") {
				expected = s2
			}
			origin := expected.Metadata["org.opencontainers.image.revision"]
			if origin == "" {
				origin = expected.Revision
			}
			revision := expected.Revision
			if change == "origin-without-revision" || change == "selected-origin-fallback" {
				revision = "latest@" + artifact.Digest
			}
			{
				assert.FatalIf(
					t,
					artifact.Revision != revision || artifact.Metadata["org.opencontainers.image.revision"] != origin,
					"final selected metadata = %+v",
					artifact,
				)
			}
		}
		if change == "selected-uid" {
			_, doc, emission := st.GetEmission(
				manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindExternalArtifact,
					Namespace: "apps",
					Name:      "output",
				},
			)
			defer emission.Finish()
			{
				assert.FatalIf(
					t,
					doc["metadata"].(map[string]any)["labels"].(map[string]any)["source.extensions.fluxcd.io/generator"] != "next",
					"%v",
					"successor retained the old UID",
				)
			}
		}
		inventory := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
		{
			assert.FatalIf(t, !inventory.Complete || len(inventory.Manifests) != 1 ||
				inventory.Manifests[0].Document["data"].(map[string]any)["value"] != value, "final consumer inventory = %+v", inventory)
		}
		documents, err = json.Marshal(inventory.Manifests)
		{
			assert.FatalIf(t, err != nil, "%v", err)
		}
	})
	return documents
}
