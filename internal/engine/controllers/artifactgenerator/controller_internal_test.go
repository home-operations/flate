package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/schedule"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func generatorForTest(t *testing.T, artifacts []any) generatorFixture {
	t.Helper()
	doc := map[string]any{
		"apiVersion": "source.extensions.fluxcd.io/v1beta1",
		"kind":       "ArtifactGenerator",
		"metadata":   map[string]any{"name": "generator", "namespace": "apps", "uid": "authored"},
		"spec": map[string]any{
			"sources":   []any{map[string]any{"alias": "repo", "kind": "GitRepository", "name": "repo"}},
			"artifacts": artifacts,
		},
	}
	parseDocResult, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, authoritative := parseDocResult.Object, parseDocResult.Document
	assert.FatalIf(t, err != nil, "%v", err)
	return generatorFixture{object: obj.(*manifest.RawObject), document: authoritative}
}

func runForTest(t *testing.T, c *Controller, id manifest.NamedResource) {
	t.Helper()
	c.Tasks.Go(
		t.Context(),
		"generator",
		func(ctx context.Context) {
			t.Cleanup(c.ReconcileNode(ctx, id, manifest.DrainNone, schedule.Result{}).Close)
		},
	)
	c.Tasks.BlockTillDone()

	assert.FatalIf(t, c.Tasks.Failures() != 0, "%v", "reconcile task panicked")
}

func TestArtifactGenerator_CopyValidationPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, pattern, from, to, strategy, want string
		sourceReady                             bool
	}{
		{
			"missing source", "@repo/absent/{App}", "@repo/file", "@artifact/file", "",
			"generator spec: invalid exclude pattern '['", false,
		},
		{
			"ready source with no matches", "@repo/absent/{App}", "@repo/file", "@artifact/file", "Overwrite",
			"generator spec: invalid exclude pattern '['", true,
		},
		{
			"source before destination", "@repo/absent/{App}", "@repo", "file", "Append",
			"generator spec: invalid copy source '@repo': source format must be '@alias/pattern'", true,
		},
		{
			"destination before strategy", "@repo/absent/{App}", "@repo/file", "file", "Append",
			"generator spec: invalid copy destination 'file': destination must start with '@artifact/'", true,
		},
		{
			"strategy before exclude", "@repo/absent/{App}", "@repo/file", "@artifact/file", "Append",
			`generator spec: unsupported strategy "Append"`, true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := cachetest.New(t, "", cache.New)
			c := New(st, task.NewBounded(2), owner)
			fixture := generatorForTest(t, []any{map[string]any{
				"name": "output",
				"copy": []any{map[string]any{
					"from": tc.from, "to": tc.to, "strategy": tc.strategy, "exclude": []string{"["},
				}},
			}})
			doc := fixture.document
			doc["spec"].(map[string]any)["pathPattern"] = tc.pattern
			parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			st.AddObject(parsed.Object, parsed.Document)
			if tc.sourceReady {
				repo := manifest.NamedResource{
					Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo",
				}
				st.SetArtifact(repo, &store.SourceArtifact{FS: fstest.MapFS{"apps/Prod/file": {}}, Content: "source"})
			}
			id := parsed.Object.Named()
			ex := &execution{Controller: c}
			ex.run.Init(c.Controller, id)
			defer ex.run.Close()
			var reconcileErr error
			c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
				ex.run.Dispatch(ctx, manifest.DrainNone, ex,
					func(ex *execution, ctx context.Context, obj *manifest.RawObject, level manifest.DrainLevel) error {
						reconcileErr = ex.reconcile(ctx, obj, level)
						return reconcileErr
					})
			})
			c.Tasks.BlockTillDone()
			assert.Equal(t, c.Tasks.Failures(), 0)
			assert.ErrorIf(t, !errors.Is(reconcileErr, manifest.ErrInput), "reconcile error category: %v", reconcileErr)
			status, _ := st.GetStatus(id)
			assert.Equal(t, status.Status, store.StatusFailed)
			assert.Equal(t, status.Message, tc.want)
		})
	}
}

func TestArtifactGenerator_AllStrategies(t *testing.T) {
	t.Parallel()
	compressed := strategyArchive(t)
	for _, tc := range []struct {
		name       string
		operations []any
		path, want string
		skipOutput bool
	}{
		{"directory", []any{map[string]any{"from": "@repo/app/", "to": "@artifact/app/", "exclude": []string{"app"}}}, "app/app/file.txt", "file", false},
		{
			"merge",
			[]any{
				map[string]any{"from": "@repo/base.yaml", "to": "@artifact/values.yaml"},
				map[string]any{"from": "@repo/overlay.yaml", "to": "@artifact/values.yaml", "strategy": "Merge"},
			},
			"values.yaml",
			"a: 1\nb: 3\n",
			false,
		},
		{
			"extract",
			[]any{map[string]any{"from": "@repo/archive.TGZ", "to": "@artifact/out/", "strategy": "Extract"}},
			"out/inside.txt",
			"inside",
			false,
		},
		{
			"optional_misses_and_match",
			[]any{
				map[string]any{"from": "@repo/absent", "to": "@artifact/first", "optional": true},
				map[string]any{"from": "@repo/absent/**", "to": "@artifact/second", "optional": true},
				map[string]any{"from": "@repo/app/*.txt", "to": "@artifact/third", "exclude": []string{"*.txt"}, "optional": true},
				map[string]any{"from": "@repo/app/*.txt", "to": "@artifact/archive", "strategy": "Extract"},
				map[string]any{"from": "@repo/app/file.txt", "to": "@artifact/selected", "optional": true},
			},
			"selected", "file", false,
		},
		{"skip_output", []any{map[string]any{"from": "@repo/app/", "to": "@artifact/app/"}}, "app/app/file.txt", "file", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner, err := cache.New(t.Context(), "")
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() {
				{
					closeErr := owner.Close()
					assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
				}
			})
			c := New(st, task.NewBounded(2), owner)
			if tc.skipOutput {
				c.SkipKinds = []string{manifest.KindExternalArtifact}
			}

			generatorForTestResult := generatorForTest(
				t,
				[]any{map[string]any{"name": "output", "copy": tc.operations}},
			)
			obj, doc := generatorForTestResult.object, generatorForTestResult.document
			st.AddObject(obj, doc)
			repo := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "repo",
			}
			st.SetArtifact(
				repo,
				&store.SourceArtifact{
					Kind:    repo.Kind,
					Key:     "input",
					Content: "input",
					Digest:  "sha256:input",
					FS: fstest.MapFS{
						"app/file.txt": {
							Data: []byte("file"),
							Mode: 0o644,
						}, "base.yaml": {
							Data: []byte("a: 1\nb: 2\n"),
						}, "overlay.yaml": {
							Data: []byte("b: 3\n"),
						}, "archive.TGZ": {
							Data: compressed.Bytes(),
						},
					},
				},
			)
			runForTest(t, c, obj.Named())
			status, _ := st.GetStatus(obj.Named())
			assert.FatalIf(t, status.Status != store.StatusReady, "generator status = %+v", status)
			target := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindExternalArtifact,
				Namespace: "apps",
				Name:      "output",
			}
			artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok || artifact.FS == nil || artifact.Key == "" || !strings.HasPrefix(
				artifact.Digest,
				"sha256:",
			) || artifact.Kind != manifest.KindExternalArtifact || artifact.Revision != "latest@"+artifact.Digest, "generated artifact = %+v", artifact)
			data, err := fs.ReadFile(artifact.FS, tc.path)
			assert.FatalIf(t, err != nil || string(data) != tc.want, "output %s = %q, %v", tc.path, data, err)
			accepted, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
			assert.FatalIf(
				t,
				!ok || !accepted.Complete || accepted.Fingerprint == "" || len(accepted.Children) != 1,
				"accepted inventory = %+v",
				accepted,
			)
			assert.Equal(t, accepted.Children[0].Obj.Named(), target)
			assert.Equal(t, manifest.DocIdentity(accepted.Children[0].Doc), target)
			assert.Equal(t, st.GetObject(target) != nil, true)
			if tc.skipOutput {
				assert.Equal(t, len(accepted.Manifests), 0)
			} else {
				assert.Equal(t, len(accepted.Manifests), 1)
			}
			runForTest(t, c, obj.Named())
			assert.FatalIf(
				t,
				st.GetArtifact(target, store.ArtifactContent) != artifact,
				"%v",
				"complete replay changed accepted bytes",
			)
		})
	}
}

func TestArtifactGenerator_PartialFailure(t *testing.T) {
	t.Parallel()
	t.Run("late_invalid_inputs", testArtifactGeneratorLateInputs)
	t.Run("invalid_exclude", testArtifactGeneratorInvalidExclude)
	c, obj, input, failedInput := partialGeneratorFixture(t)
	st := c.Store
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	runForTest(t, c, obj.Named())
	phaseCounts1 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts1[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 4, Completed: 4}
	phaseCounts1[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 4}
	phaseCounts1[manifest.WorkAcquisition] = manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1}
	phaseCounts1[manifest.WorkEvaluation] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	phaseCounts1[manifest.WorkCache] = manifest.WorkCounts{Attempts: 3, Completed: 3, CacheService: 2}
	retry := assertGeneratorWork(t, st, obj.Named(), phaseCounts1)
	assert.Equal(t, slices.ContainsFunc(retry, func(record manifest.WorkRecord) bool {
		return record.Key.Phase == manifest.WorkEvaluation &&
			record.Key.Input == failedInput && record.Counts.Completed == 1
	}), true)
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(t, status.Status != store.StatusReady, "retry status = %+v", status)
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "c",
	}
	artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok, "%v", "same-input retry did not execute the failed output")
	assert.Equal(t, artifact.Key, target.NamespacedName())
	{
		data, err := fs.ReadFile(artifact.FS, "file")
		assert.FatalIf(t, err != nil || string(data) != "c", "retried c = %q, %v", data, err)
	}
	accepted := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		!accepted.Complete || len(accepted.Children) != 3 || accepted.Fingerprint == "",
		"retried inventory = %+v",
		accepted,
	)
	nextInput := maps.Clone(input)
	delete(nextInput, "b")
	st.SetArtifact(repo, &store.SourceArtifact{Key: "second", Content: "second", Digest: "second", FS: nextInput})
	runForTest(t, c, obj.Named())
	phaseCounts2 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts2[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 4, Completed: 4}
	phaseCounts2[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 3}
	phaseCounts2[manifest.WorkAcquisition] = manifest.WorkCounts{
		Attempts:          2,
		Completed:         1,
		Failures:          1,
		LocalAcquisitions: 2,
	}
	phaseCounts2[manifest.WorkEvaluation] = manifest.WorkCounts{Attempts: 2, Completed: 1, Failures: 1}
	phaseCounts2[manifest.WorkCache] = manifest.WorkCounts{Attempts: 2, Completed: 1, Failures: 1}
	assertGeneratorWork(t, st, obj.Named(), phaseCounts2)
	status, _ = st.GetStatus(obj.Named())
	assert.FatalIf(t, status.Status != store.StatusFailed, "successor failure status = %+v", status)
	{
		current := st.GetArtifact(target, store.ArtifactContent)
		assert.FatalIf(t, current != artifact, "%v", "failure withdrew an unvisited accepted output")
	}
	{
		data, err := fs.ReadFile(artifact.FS, "file")
		assert.FatalIf(t, err != nil || string(data) != "c", "retained c = %q, %v", data, err)
	}
}

func testArtifactGeneratorInvalidExclude(t *testing.T) {
	t.Helper()
	t.Parallel()
	for _, tc := range []struct {
		name  string
		input fstest.MapFS
	}{
		{name: "zero_matches", input: fstest.MapFS{"elsewhere/file": {}}},
		{name: "one_match", input: fstest.MapFS{"apps/prod/file": {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			c := New(st, task.NewBounded(2), cachetest.New(t, "", cache.New))
			fixture := generatorForTest(t, []any{
				map[string]any{"name": "first-{app}", "copy": []any{
					map[string]any{"from": "@repo/apps/{app}/file", "to": "@artifact/file"},
				}},
				map[string]any{"name": "second-{app}", "copy": []any{
					map[string]any{"from": "@repo/apps/{app}/file", "to": "@artifact/file", "exclude": []any{"["}},
				}},
			})
			fixture.document["spec"].(map[string]any)["pathPattern"] = "@repo/apps/{app}"
			parsed, err := manifest.ParseDoc(fixture.document, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			id := parsed.Object.Named()
			st.AddObject(parsed.Object, parsed.Document)
			repo := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo",
			}
			st.SetArtifact(repo, &store.SourceArtifact{Content: "input", FS: tc.input})
			runForTest(t, c, id)
			status, _ := st.GetStatus(id)
			assert.Equal(t, status.Status, store.StatusFailed)
			assert.Equal(t, status.Message, "generator spec: invalid exclude pattern '['")
			assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
			assert.Equal(t, st.GetArtifact(managedArtifact("first-prod", "apps"), store.ArtifactContent) == nil, true)
		})
	}
}

func TestArtifactGenerator_Disabled(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks, nil)

	generatorForTestResult2 := generatorForTest(t, nil)
	doc := generatorForTestResult2.document
	doc["spec"].(map[string]any)["pathPattern"] = 12
	doc["metadata"].(map[string]any)["annotations"] = map[string]any{
		"source.extensions.fluxcd.io/reconcile": "DiSaBlEd",
	}
	parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	obj := parsed.Object.(*manifest.RawObject)
	st.AddObject(obj, doc)
	var emissions atomic.Int64
	unsubscribe := st.AddListener(
		store.EventArtifactUpdated,
		func(manifest.NamedResource, any) { emissions.Add(1) },
		false,
	)
	t.Cleanup(unsubscribe)
	runForTest(t, c, obj.Named())
	status, _ := st.GetStatus(obj.Named())

	assert.FatalIf(
		t,
		status.Status != store.StatusReady || st.GetArtifact(obj.Named(), store.ArtifactContent) != nil,
		"disabled result = %+v",
		status,
	)

	assert.FatalIf(t, emissions.Load() != 0, "%v", "disabled generator published an artifact")

	{
		decoded, err := obj.ArtifactGeneratorSpec()
		if err == nil {
			_, err = validateSpec(&decoded, "apps")
		}
		assert.FatalIf(t, err == nil, "%v", "disabled fixture should be invalid when enabled")
	}
}

type observedFS struct {
	fs.FS

	opens    atomic.Int64
	failPath string
	failed   atomic.Bool
	onOpen   func(string)
}

func (f *observedFS) Open(name string) (fs.File, error) {
	if f.onOpen != nil {
		f.onOpen(name)
	}
	if name == f.failPath && f.failed.CompareAndSwap(false, true) {
		return nil, fs.ErrPermission
	}
	if name == "file" || strings.HasSuffix(name, "/file") {
		f.opens.Add(1)
	}
	return f.FS.Open(name)
}

func TestArtifactGenerator_Metadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []metadataCase{
		{"selected_origin", "@repo", "@repo", map[string]string{"org.opencontainers.image.revision": "origin"}, "origin", true},
		{"origin_revision_fallback", "@repo", "@repo", nil, "revision", true},
		{"empty_origin_fallback", "@repo", "@repo", map[string]string{"org.opencontainers.image.revision": ""}, "revision", true},
		{"absent_origin_alias", "", "@absent", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &metadataFixture{}
			f.prepare(t, &tc)
			if tc.origin == "@absent" {
				runForTest(t, f.c, f.obj.Named())
				status, _ := f.c.Store.GetStatus(f.obj.Named())
				assert.Equal(t, status.Status, store.StatusFailed)
				assert.Equal(
					t,
					strings.Contains(status.String(), "origin revision source alias 'absent' not found"),
					true,
				)
				assert.Equal(t, f.c.Store.GetObject(f.target), nil)
				return
			}
			artifact := f.assertMetadata(t, &tc)
			assertReplayResult := f.assertReplay(t, &tc, artifact)
			updatedDoc, updatedMetadata := assertReplayResult.document, assertReplayResult.metadata
			f.assertEnabled(t, updatedDoc, updatedMetadata)
		})
	}
}

func TestArtifactGenerator_RequiredUnusedAlias(t *testing.T) {
	t.Parallel()
	st := store.New()
	c := New(st, task.NewBounded(2), nil)

	generatorForTestResult3 := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name": "output",
				"copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}},
			},
		},
	)
	doc := generatorForTestResult3.document
	spec := doc["spec"].(map[string]any)
	spec["sources"] = append(
		spec["sources"].([]any),
		map[string]any{"alias": "unused", "name": "unused", "kind": "GitRepository"},
	)
	parseDocResult2, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, doc := parseDocResult2.Object, parseDocResult2.Document
	assert.FatalIf(t, err != nil, "%v", err)
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:     "input",
			Content: "input",
			Digest:  "digest",
			FS:      fstest.MapFS{"file": {Data: []byte("bytes")}},
		},
	)
	repo.Name = "unused"
	st.UpdateStatus(repo, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "unavailable unused alias"})
	runForTest(t, c, obj.Named())
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(
		t,
		status.Status != store.StatusFailed || st.GetArtifact(obj.Named(), store.ArtifactContent) != nil,
		"unused alias result = %+v",
		status,
	)
}

func TestArtifactGenerator_InputAvailability(t *testing.T) {
	t.Parallel()
	for _, tc := range []inputAvailabilityCase{
		{"missing", nil, "", "", store.StatusPending, ""},
		{"key_without_fs", &store.SourceArtifact{Key: "key", LocalPath: "unused"}, store.StatusReady, "", store.StatusFailed, ""},
		{"contentless", &store.SourceArtifact{}, store.StatusReady, "", store.StatusFailed, ""},
		{"skipped", nil, store.StatusReady, store.ReasonSkipped, store.StatusReady, store.ReasonSkipped},
		{
			"readable_before_ready",
			&store.SourceArtifact{Key: "key", Content: "key", Digest: "digest", FS: fstest.MapFS{"file": {Data: []byte("bytes")}}},
			store.StatusFailed,
			"",
			store.StatusReady,
			"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testInputAvailability(t, &tc)
		})
	}
}

type inputAvailabilityCase struct {
	name         string
	artifact     *store.SourceArtifact
	sourceStatus store.Status
	sourceReason store.StatusReason
	want         store.Status
	wantReason   store.StatusReason
}

func strategyArchive(t *testing.T) *bytes.Buffer {
	t.Helper()
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	archive := tar.NewWriter(gz)
	{
		err := archive.WriteHeader(&tar.Header{Name: "inside.txt", Mode: 0o755, Size: 6})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		_, err := archive.Write([]byte("inside"))
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := archive.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := gz.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	return &compressed
}

func partialGeneratorFixture(t *testing.T) (*Controller, *manifest.RawObject, fstest.MapFS, string) {
	t.Helper()
	st := store.New()
	st.SetWorkTree(manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, (&manifest.WorkRecorder{}).Scope())
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			closeErr := owner.Close()
			assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	outputs := make([]any, 0, 3)
	for _, name := range []string{"a", "b", "c"} {
		outputs = append(
			outputs,
			map[string]any{
				"name": name,
				"copy": []any{map[string]any{"from": "@repo/" + name, "to": "@artifact/file", "optional": name == "c"}},
			},
		)
	}
	generatorForTestResult4 := generatorForTest(t, outputs)
	obj, doc := generatorForTestResult4.object, generatorForTestResult4.document
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	input := fstest.MapFS{"a": {Data: []byte("a")}, "b": {Data: []byte("b")}, "c": {Data: []byte("c")}}
	st.SetArtifact(
		repo,
		&store.SourceArtifact{
			Key:     "first",
			Content: "first",
			Digest:  "first",
			FS:      &observedFS{FS: input, failPath: "c"},
		},
	)
	runForTest(t, c, obj.Named())
	phaseCounts3 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts3[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 3, Completed: 3}
	phaseCounts3[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 4}
	phaseCounts3[manifest.WorkAcquisition] = manifest.WorkCounts{
		Attempts:          3,
		Completed:         2,
		Failures:          1,
		LocalAcquisitions: 3,
	}
	phaseCounts3[manifest.WorkEvaluation] = manifest.WorkCounts{Attempts: 3, Completed: 2, Failures: 1}
	phaseCounts3[manifest.WorkCache] = manifest.WorkCounts{Attempts: 3, Completed: 2, Failures: 1}
	partialWork := assertGeneratorWork(t, st, obj.Named(), phaseCounts3)
	var failedInput string
	for i := range partialWork {
		record := &partialWork[i]
		if record.Key.Phase == manifest.WorkEvaluation && record.Counts.Failures != 0 {
			failedInput = record.Key.Input
		}
	}
	assert.Equal(t, failedInput != "", true)
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(t, status.Status != store.StatusFailed, "partial status = %+v", status)
	for _, name := range []string{"a", "b"} {
		artifact, ok := st.GetArtifact(
			manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: name},
			store.ArtifactContent,
		).(*store.SourceArtifact)
		assert.FatalIf(t, !ok || artifact.FS == nil, "accepted %s unavailable", name)
		{
			data, err := fs.ReadFile(artifact.FS, "file")
			assert.FatalIf(t, err != nil || string(data) != name, "accepted %s = %q, %v", name, data, err)
		}
	}
	accepted := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, accepted.Complete || accepted.Fingerprint != "", "%v", "partial attempt became replayable")
	return c, obj, input, failedInput
}

func testInputAvailability(t *testing.T, tc *inputAvailabilityCase) {
	t.Helper()
	st := store.New()
	st.SetWorkTree(manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, (&manifest.WorkRecorder{}).Scope())
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			closeErr := owner.Close()
			assert.ErrorIf(t, closeErr != nil, "%v", closeErr)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	generatorForTestResult5 := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name":           "output",
				"revision":       "@repo",
				"originRevision": "@repo",
				"copy":           []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}},
			},
		},
	)
	obj, doc := generatorForTestResult5.object, generatorForTestResult5.document
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	if tc.artifact != nil {
		st.SetArtifact(repo, tc.artifact)
	}
	if tc.sourceStatus != "" {
		st.UpdateStatus(
			repo,
			store.StatusInfo{Status: tc.sourceStatus, Reason: tc.sourceReason, Message: "source condition"},
		)
	}
	runForTest(t, c, obj.Named())
	phaseCounts4 := make(map[manifest.WorkPhase]manifest.WorkCounts)
	phaseCounts4[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	wantWork := phaseCounts4
	if tc.name == "readable_before_ready" {
		wantWork[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 2, Completed: 2}
		wantWork[manifest.WorkPreparation] = manifest.WorkCounts{RawKeyPrepares: 2}
		wantWork[manifest.WorkAcquisition] = manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1}
		wantWork[manifest.WorkEvaluation] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		wantWork[manifest.WorkCache] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	}
	assertGeneratorWork(t, st, obj.Named(), wantWork)
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(
		t,
		status.Status != tc.want || tc.wantReason != "" && status.Reason != tc.wantReason,
		"input availability = %+v; want %s/%s",
		status,
		tc.want,
		tc.wantReason,
	)
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "output",
	}
	assert.FatalIf(
		t,
		(st.GetArtifact(target, store.ArtifactContent) != nil) != (tc.name == "readable_before_ready"),
		"input %s output = %+v",
		tc.name,
		st.GetArtifact(target, store.ArtifactContent),
	)
	if tc.name == "missing" {
		c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
			c.ReconcileNode(ctx, obj.Named(), manifest.DrainCascade, schedule.Result{})
		})
		c.Tasks.BlockTillDone()
		status, _ = st.GetStatus(obj.Named())
		assert.FatalIf(t, status.Status != store.StatusFailed, "missing input after drain = %+v", status)
		phaseCounts5 := make(map[manifest.WorkPhase]manifest.WorkCounts)
		phaseCounts5[manifest.WorkAdmission] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		assertGeneratorWork(t, st, obj.Named(), phaseCounts5)
	}
}

func assertGeneratorWork(
	t *testing.T,
	st *store.Store,
	id manifest.NamedResource,
	want map[manifest.WorkPhase]manifest.WorkCounts,
) manifest.WorkSummary {
	t.Helper()

	summary := st.WorkSnapshot()
	if len(summary) != 0 {
		tree := summary[0].Key.Tree
		st.SetWorkTree(tree, (&manifest.WorkRecorder{Key: manifest.WorkKey{Tree: tree}}).Scope())
	}
	counts := make(map[manifest.WorkPhase]manifest.WorkCounts)
	for i := range summary {
		record := &summary[i]
		assert.Equal(t, record.Key.Tree, manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead})
		source := manifest.BootstrapSourceID
		if record.Key.Phase != manifest.WorkAdmission {
			source = manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "repo",
			}
		}
		assert.Equal(t, record.Key.Source, source)
		if record.Key.Phase != manifest.WorkAdmission {
			assert.Equal(t, record.Key.Owner, id)
		}
		switch record.Key.Phase {
		case manifest.WorkAcquisition, manifest.WorkCache, manifest.WorkEvaluation:
			assert.Equal(t, record.Key.Input != "", true)

		case manifest.WorkAdmission,
			manifest.WorkPreparation,
			manifest.WorkKustomize,
			manifest.WorkHelm,
			manifest.WorkPostRender,
			manifest.WorkSubstitution,
			manifest.WorkValidation,
			manifest.WorkIndexDecode,
			manifest.WorkChartDecode:
		}
		sum := counts[record.Key.Phase]
		sum.Add(&record.Counts)
		counts[record.Key.Phase] = sum
	}
	assert.Diff(t, counts, want)
	return summary
}

type generatorFixture struct {
	object   *manifest.RawObject
	document map[string]any
}
