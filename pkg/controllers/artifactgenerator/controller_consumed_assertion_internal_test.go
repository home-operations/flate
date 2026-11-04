package artifactgenerator

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func (f *consumedFixture) assertPublication(t *testing.T) {
	t.Helper()
	change, st, repo, s2, consumer, work := f.change, f.st, f.repo, f.s2, f.consumer, &f.work
	committed, release, driverDone, settled := f.committed, f.release, f.driverDone, f.settled
	if change == "selected-union" {
		synctest.Wait()
		select {
		case <-committed:
		default:
			t.Fatal("S2 publication did not reach its paused notification")
		}
		assert.FatalIf(
			t,
			st.GetArtifact(repo, store.ArtifactContent) != s2,
			"%v",
			"publication listener ran before S2 commit/unlock",
		)
		inventory := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(
			t,
			inventory.Manifests[0].Document["data"].(map[string]any)["value"] != "S1",
			"%v",
			"paused notification did not permit the last completed S1 inventory",
		)
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
}

func (f *consumedFixture) finalDocuments(t *testing.T, generatorKeys map[manifest.WorkPhase]map[string]bool) []byte {
	t.Helper()
	st, consumer, change, s1, s2 := f.st, f.consumer, f.change, f.s1, f.s2
	// No outstanding task, dispatch or Settle structurally excludes slot reacquisition and other Acquire paths.
	value := "S1"
	if change == "selected-union" {
		value = "S2"
	}
	artifact := st.GetArtifact(
		manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Namespace: "apps",
			Name:      "output",
		},
		store.ArtifactContent,
	).(*store.SourceArtifact)
	assert.Equal(t, generatorKeys[manifest.WorkEvaluation][artifact.Key], true)
	if strings.HasPrefix(change, "unselected-") {
		assert.FatalIf(
			t,
			artifact.Revision != "latest@"+artifact.Digest || len(artifact.Metadata) != 0,
			"content-only output consumed metadata: %+v",
			artifact,
		)
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
		assert.FatalIf(
			t,
			artifact.Revision != revision || artifact.Metadata["org.opencontainers.image.revision"] != origin,
			"final selected metadata = %+v",
			artifact,
		)
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
		assert.FatalIf(
			t,
			doc["metadata"].(map[string]any)["labels"].(map[string]any)["source.extensions.fluxcd.io/generator"] != "next",
			"%v",
			"successor retained the old UID",
		)
	}
	inventory := st.GetArtifact(consumer.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !inventory.Complete || len(inventory.Manifests) != 1 ||
		inventory.Manifests[0].Document["data"].(map[string]any)["value"] != value, "final consumer inventory = %+v", inventory)
	documents, err := json.Marshal(inventory.Manifests)
	assert.FatalIf(t, err != nil, "%v", err)
	return documents
}

func (f *consumedFixture) run(ctx context.Context, t *testing.T, cancel context.CancelFunc) []byte {
	t.Helper()
	st, bodies, obj, consumer, driver := f.st, f.bodies, f.obj, f.consumer, f.driver
	change, repo, s2, paused, work := f.change, f.repo, f.s2, &f.paused, &f.work
	f.observeWork()
	tasks := f.tasks
	committed, release, finishDriver := f.committed, f.release, f.finishDriver
	d := &convergenceDispatcher{
		generator: f.generator,
		id:        f.obj.Named(),
		consumer:  f.ks,
		source:    f.src,
		driver:    f.driver.Named(),
		work:      &f.work,
	}
	d.drive = func(ctx context.Context) { f.drive(ctx, t) }
	s := schedule.New(tasks, d)
	pause := func(id manifest.NamedResource, _ any) {
		if change == "selected-union" && id == repo && st.GetArtifact(
			repo,
			store.ArtifactContent,
		) == s2 && paused.CompareAndSwap(
			false,
			true,
		) {
			close(committed)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	defer st.AddListener(store.EventObjectAdded, pause, false)()
	defer st.AddListener(store.EventArtifactUpdated, pause, false)()
	defer st.AddListener(
		store.EventObjectAdded,
		func(id manifest.NamedResource, _ any) { s.OnArrival(id, id.KindInfo().Runnable) },
		false,
	)()
	onPublication := func(manifest.NamedResource, any) { s.OnPublication() }
	defer st.AddListener(store.EventArtifactUpdated, onPublication, false)()
	defer st.AddListener(store.EventStatusUpdated, onPublication, false)()
	s.Seed([]schedule.NodeID{obj.Named(), consumer.Named(), driver.Named()})
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	defer func() {
		cancel()
		bodies.BlockTillDone()
	}()
	before := f.assertInitial(t)
	beforeSweep := d.sweeps.Load()
	close(f.startPublication)
	f.assertPublication(t)
	synctest.Wait()
	// An explicit publication requests an acknowledged real Sweep even when the descriptor deduplicates.
	s.OnPublication()
	synctest.Wait()
	assert.FatalIf(t, d.sweeps.Load() <= beforeSweep, "%v", "no acknowledged Sweep after publication")
	want := before
	if strings.HasPrefix(change, "selected-") {
		want.Admitted++
		want.Slots++
		want.Dispatched++
		want.Acquires++
	}
	atSweep := work.snapshot()
	{
		diff := cmp.Diff(want, atSweep)
		assert.ErrorIf(t, diff != "", "%s work after acknowledged Sweep (-want +got):\n%s", change, diff)
	}
	assert.ErrorIf(t, work.active.Load() != 0, "%v", "generator closure remains in flight after acknowledged Sweep")
	close(finishDriver)
	{
		err := <-done
		assert.FatalIf(t, err != nil, "%v", err)
	}
	bodies.BlockTillDone()
	assert.FatalIf(
		t,
		bodies.Failures() != 0 || work.active.Load() != 0 || len(slices.Collect(st.Sweep())) != 0,
		"undrained boundary: task failures=%d",
		bodies.Failures(),
	)
	after := work.snapshot()
	{
		diff := cmp.Diff(atSweep, after)
		assert.ErrorIf(t, diff != "", "%s delayed generator work after final drain (-want +got):\n%s", change, diff)
	}
	generatorKeys := f.assertRecordedWork(t)
	return f.finalDocuments(t, generatorKeys)
}

func (f *consumedFixture) assertInitial(t *testing.T) workSnapshot {
	t.Helper()
	st, obj, consumer, work := f.st, f.obj, f.consumer, &f.work
	firstGenerator, firstConsumer := f.firstGenerator, f.firstConsumer
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
	assert.FatalIf(
		t,
		work.active.Load() != 0 || before.Admitted != 1 || before.Dispatched != 1 || before.Settled != 0 ||
			before.Acquires != 1,
		"S1 boundary has outstanding or unexpected generator work: active=%d work=%+v",
		work.active.Load(),
		before,
	)
	return before
}

func (f *consumedFixture) assertRecordedWork(t *testing.T) map[manifest.WorkPhase]map[string]bool {
	t.Helper()
	st, obj, consumer, change := f.st, f.obj, f.consumer, f.change
	generatorID := manifest.NamedResource{
		Group: "source.extensions.fluxcd.io",

		Kind: "ArtifactGenerator",

		Namespace: "apps",

		Name: "generator",
	}
	consumerID := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "consumer",
	}
	outputID := manifest.NamedResource{
		Group: manifest.SourceDomain,

		Kind: manifest.KindExternalArtifact,

		Namespace: "apps",

		Name: "output",
	}
	assert.Equal(t, obj.Named(), generatorID)
	assert.Equal(t, consumer.Named(), consumerID)
	physical, cacheCalls, service := uint64(1), uint64(1), uint64(0)
	if strings.HasPrefix(change, "selected-") {
		physical, cacheCalls = 2, 2
	}
	if change == "selected-uid" {
		physical, service = 1, 1
	}
	gotWork := map[manifest.NamedResource]map[manifest.WorkPhase]manifest.WorkCounts{}
	generatorKeys := make(map[manifest.WorkPhase]map[string]bool)

	records := st.WorkSnapshot()
	for i := range records {
		record := &records[i]
		assert.Equal(t, record.Key.Tree, manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead})
		switch record.Key.Phase {
		case manifest.WorkAcquisition, manifest.WorkEvaluation, manifest.WorkKustomize, manifest.WorkHelm,
			manifest.WorkPostRender, manifest.WorkSubstitution, manifest.WorkCache:
			id := record.Key.Owner
			if gotWork[id] == nil {
				gotWork[id] = make(map[manifest.WorkPhase]manifest.WorkCounts)
			}
			counts := gotWork[id][record.Key.Phase]
			counts.Add(&record.Counts)
			gotWork[id][record.Key.Phase] = counts
			assert.Equal(t, record.Key.Input != "", true)
			if id == generatorID {
				assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
				if generatorKeys[record.Key.Phase] == nil {
					generatorKeys[record.Key.Phase] = map[string]bool{}
				}
				generatorKeys[record.Key.Phase][record.Key.Input] = true
			} else {
				assert.Equal(t, record.Key.Source, outputID)
			}

		case manifest.WorkAdmission,
			manifest.WorkPreparation,
			manifest.WorkValidation,
			manifest.WorkIndexDecode,
			manifest.WorkChartDecode:
		}
	}
	phaseCounts1 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts1[manifest.WorkAcquisition] = manifest.WorkCounts{
		Attempts:          physical,
		Completed:         physical,
		LocalAcquisitions: physical,
	}
	phaseCounts1[manifest.WorkEvaluation] = manifest.WorkCounts{Attempts: physical, Completed: physical}
	phaseCounts1[manifest.WorkCache] = manifest.WorkCounts{
		Attempts:     cacheCalls,
		Completed:    cacheCalls,
		CacheService: service,
	}
	phaseCounts2 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts2[manifest.WorkKustomize] = manifest.WorkCounts{Attempts: physical, Completed: physical}
	phaseCounts2[manifest.WorkCache] = manifest.WorkCounts{
		Attempts:     cacheCalls,
		Completed:    cacheCalls,
		CacheService: service,
	}
	assert.Diff(t, gotWork, map[manifest.NamedResource]map[manifest.WorkPhase]manifest.WorkCounts{
		generatorID: phaseCounts1,
		consumerID:  phaseCounts2,
	})
	assert.Equal(t, len(generatorKeys[manifest.WorkEvaluation]), int(physical))
	assert.Diff(t, generatorKeys[manifest.WorkAcquisition], generatorKeys[manifest.WorkEvaluation])
	assert.Diff(t, generatorKeys[manifest.WorkCache], generatorKeys[manifest.WorkEvaluation])
	return generatorKeys
}
