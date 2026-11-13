package cache

import (
	"context"
	"errors"
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCompose_ImmutableInputsCoalesce(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		owner := cachetest.New(t, "", New)
		files := fstest.MapFS{"file": {Data: []byte("shared"), Mode: 0o640}}
		entered, release := make(chan struct{}), make(chan struct{})
		var reads atomic.Int32
		input := compositionReadFS{FS: files, read: func(name string) ([]byte, error) {
			if reads.Add(1) == 1 {
				close(entered)
			}
			<-release
			return fs.ReadFile(files, name)
		}}
		request := Composition{
			Content: "shared composition", Inputs: []CompositionInput{{FS: input, Content: "immutable"}},
			Base:   CompositionInput{FS: fstest.MapFS{}, Content: "immutable base"},
			Copies: []Copy{{From: "file", To: "out"}}, Policy: "fixed",
		}
		var work [2]manifest.WorkRecorder
		var keys [2]string
		var outputs [2]fs.FS
		done := make(chan error, 2)
		acquire := func(side int) {
			accounting := manifest.AcquisitionWork{Scope: work[side].Scope()}
			var err error
			outputs[side], _, keys[side], err = owner.Compose(
				t.Context(),
				request,
				&Policy{MaxAge: Forever, Work: &accounting},
			)
			done <- err
		}
		for side := range work {
			work[side].Key = manifest.WorkKey{
				Owner: manifest.NamedResource{Kind: "Test", Name: []string{"producer", "waiter"}[side]},
			}
		}
		go acquire(0)
		<-entered
		go acquire(1)
		synctest.Wait()
		close(release)
		assert.NoError(t, <-done)
		assert.NoError(t, <-done)
		assert.Equal(t, reads.Load(), int32(1))
		assert.Equal(t, keys[0], keys[1])
		assert.Equal(t, keys[0] != "", true)
		for side := range work {
			assert.Diff(
				t,
				compositionFiles(t, outputs[side]),
				map[string]compositionFile{"out": {Data: "shared", Mode: 0o600}},
			)
			records := work[side].Scope().Snapshot(0)
			for _, record := range records {
				assert.Equal(t, record.Key.Owner, work[side].Key.Owner)
				assert.Equal(t, record.Key.Input, keys[side])
			}
			assert.Equal(t, phaseWork(records, manifest.WorkPreparation), manifest.WorkCounts{RawKeyPrepares: 1})
			if side == 0 {
				assert.Equal(
					t,
					phaseWork(records, manifest.WorkAcquisition),
					manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1},
				)
				assert.Equal(
					t,
					phaseWork(records, manifest.WorkEvaluation),
					manifest.WorkCounts{Attempts: 1, Completed: 1},
				)
			} else {
				assert.Equal(t, phaseWork(records, manifest.WorkAcquisition), manifest.WorkCounts{})
				assert.Equal(t, phaseWork(records, manifest.WorkEvaluation), manifest.WorkCounts{})
				assert.Equal(t, phaseWork(records, manifest.WorkCache).CacheService, uint64(1))
			}
		}
	})
}

func TestCompose_MutableInputsRefresh(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	files := fstest.MapFS{"plain": {Data: []byte("aaa"), Mode: 0o640}, "archive.tgz": {}}
	var recorder manifest.WorkRecorder
	work := manifest.AcquisitionWork{Scope: recorder.Scope()}
	policy := Policy{MaxAge: Forever, Work: &work}
	selections := 0
	request := Composition{
		Content: "unknown generation", Inputs: []CompositionInput{{FS: files}},
		Copies: []Copy{
			{From: "plain", To: "plain"},
			{From: "archive.tgz", To: "out", Mode: ExtractArchive, ExtractContent: func(name, destination string) any {
				selections++
				return []string{"unknown archive generation", name, destination}
			}},
		}, Policy: "fixed",
	}
	var previousKey, previousDigest string
	var previousSize int
	for _, text := range []string{"aaa", "bbb"} {
		files["plain"].Data = []byte(text)
		files["archive.tgz"].Data = compressedForTest(t, fstest.MapFS{"member": {Data: []byte(text), Mode: 0o750}})
		if previousSize != 0 {
			assert.Equal(t, len(files["archive.tgz"].Data), previousSize)
		}
		previousSize = len(files["archive.tgz"].Data)
		before := compositionFiles(t, files)
		output, descriptor, key, err := owner.Compose(t.Context(), request, &policy)
		assert.NoError(t, err)
		assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{
			"plain": {
				Data: text,
				Mode: 0o600,
			},
			"out":        {Mode: fs.ModeDir | 0o750},
			"out/member": {Data: text, Mode: 0o700},
		})
		assert.Diff(t, compositionFiles(t, files), before)
		assert.Equal(t, policy.MaxAge, Forever)
		assert.Equal(t, policy.Work, &work)
		if previousKey != "" {
			assert.Equal(t, key, previousKey)
			assert.Equal(t, descriptor.Annotations["flate.tree-digest"] == previousDigest, false)
		}
		previousKey, previousDigest = key, descriptor.Annotations["flate.tree-digest"]
	}
	assert.Equal(t, selections, 2)
	records := recorder.Scope().Snapshot(0)
	assert.Equal(
		t,
		phaseWork(records, manifest.WorkAcquisition),
		manifest.WorkCounts{Attempts: 4, Completed: 4, LocalAcquisitions: 2},
	)
	assert.Equal(t, phaseWork(records, manifest.WorkEvaluation), manifest.WorkCounts{Attempts: 2, Completed: 2})
}

func TestCompose_ExtractionWorkAndPolicy(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	files := fstest.MapFS{
		"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"member": {Data: []byte("archive"), Mode: 0o750}})},
	}
	recorder := manifest.WorkRecorder{Key: manifest.WorkKey{Owner: manifest.NamedResource{Kind: "Test", Name: "owner"}}}
	var historyRecorder manifest.WorkRecorder
	work := manifest.AcquisitionWork{Scope: recorder.Scope()}
	history := manifest.AcquisitionWork{Scope: historyRecorder.Scope()}
	admissions, releases := 0, 0
	timeout := time.Hour
	policy := Policy{
		MaxAge: Forever, Work: &work, HistoryWork: &history, Machine: true,
		Retry: RetryConfig{Attempts: 3}, Timeout: &timeout,
		Admit: func(context.Context) (func(), error) {
			admissions++
			if admissions != 1 {
				return nil, errors.New("nested acquisition reentered outer admission")
			}
			return func() { releases++ }, nil
		},
	}
	nestedFacts := struct{ Generation, Path, Destination string }{"generation", "archive.tgz", "out"}
	request := Composition{
		Content: "outer", Inputs: []CompositionInput{{FS: files, Content: "generation"}},
		Copies: []Copy{{
			From: "archive.tgz", To: "out", Mode: ExtractArchive,
			ExtractContent: func(name, destination string) any {
				assert.Equal(t, name, "archive.tgz")
				assert.Equal(t, destination, "out")
				return nestedFacts
			},
		}}, Policy: "fixed",
	}
	output, _, outerKey, err := owner.Compose(t.Context(), request, &policy)
	assert.NoError(t, err)
	assert.Diff(t, compositionFiles(t, output), map[string]compositionFile{
		"out": {Mode: fs.ModeDir | 0o750}, "out/member": {Data: "archive", Mode: 0o700},
	})
	e := composition{request: request}
	plan, err := e.prepareCopy(&request.Copies[0])
	assert.NoError(t, err)
	_, _, nestedKey, err := owner.Acquire(
		t.Context(),
		plan.extractionKey("archive.tgz"),
		&Policy{MaxAge: Forever},
		unexpectedComposition,
	)
	assert.NoError(t, err)
	assert.Equal(t, nestedKey == outerKey, false)
	assert.Equal(t, admissions, 1)
	assert.Equal(t, releases, 1)
	assert.Equal(t, len(historyRecorder.Scope().Snapshot(0)), 0)
	assert.Equal(t, policy.Work, &work)
	assert.Equal(t, policy.HistoryWork, &history)
	assert.Equal(t, policy.Timeout, &timeout)
	assert.Equal(t, policy.MaxAge, Forever)
	assert.Equal(t, policy.Retry.Attempts, 3)
	assert.Equal(t, policy.Machine, true)
	byKey := map[string]manifest.WorkSummary{}
	for _, record := range recorder.Scope().Snapshot(0) {
		assert.Equal(t, record.Key.Owner, recorder.Key.Owner)
		byKey[record.Key.Input] = append(byKey[record.Key.Input], record)
	}
	assert.Equal(t, len(byKey), 2)
	for _, key := range []string{outerKey, nestedKey} {
		records := byKey[key]
		assert.Equal(t, phaseWork(records, manifest.WorkPreparation), manifest.WorkCounts{RawKeyPrepares: 1})
		acquisition := manifest.WorkCounts{Attempts: 1, Completed: 1}
		evaluation := manifest.WorkCounts{}
		if key == outerKey {
			acquisition.LocalAcquisitions = 1
			evaluation = manifest.WorkCounts{Attempts: 1, Completed: 1}
		}
		assert.Equal(t, phaseWork(records, manifest.WorkAcquisition), acquisition)
		assert.Equal(t, phaseWork(records, manifest.WorkEvaluation), evaluation)
		assert.Equal(t, phaseWork(records, manifest.WorkCache), manifest.WorkCounts{Attempts: 1, Completed: 1})
	}
}
