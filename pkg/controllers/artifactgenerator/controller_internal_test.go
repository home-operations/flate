package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io/fs"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type generatorFixture struct {
	object   *manifest.RawObject
	document map[string]any
}

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
	parsed2, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, authoritative := parsed2.Object, parsed2.Document
	assert.FatalIf(t, err != nil, "%v", err)
	return generatorFixture{object: obj.(*manifest.RawObject), document: authoritative}
}

func runForTest(t *testing.T, c *Controller, id manifest.NamedResource) {
	t.Helper()
	c.Tasks.Go(
		t.Context(),
		"generator",
		func(ctx context.Context) { t.Cleanup(c.ReconcileNode(ctx, id, manifest.DrainNone).Close) },
	)
	c.Tasks.BlockTillDone()
	assert.FatalIf(t, c.Tasks.Failures() != 0, "%v", "reconcile task panicked")
}

func TestArtifactGenerator_AllStrategies(t *testing.T) {
	t.Parallel()
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
	for _, tc := range []struct {
		name       string
		operations []any
		path, want string
	}{
		{"directory", []any{map[string]any{"from": "@repo/app/", "to": "@artifact/app/"}}, "app/app/file.txt", "file"},
		{"merge", []any{map[string]any{"from": "@repo/base.yaml", "to": "@artifact/values.yaml"}, map[string]any{"from": "@repo/overlay.yaml", "to": "@artifact/values.yaml", "strategy": "Merge"}}, "values.yaml", "a: 1\nb: 3\n"},
		{"extract", []any{map[string]any{"from": "@repo/archive.TGZ", "to": "@artifact/out/", "strategy": "Extract"}}, "out/inside.txt", "inside"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(2)
			owner, err := cache.New(t.Context(), "")
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() {
				{
					localErr := owner.Close()
					assert.ErrorIf(t, localErr != nil, "%v", localErr)
				}
			})
			c := New(st, tasks, owner)

			generatorForTestResult1 := generatorForTest(
				t,
				[]any{map[string]any{"name": "output", "copy": tc.operations}},
			)
			obj, doc := generatorForTestResult1.object, generatorForTestResult1.document
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
						},
						"base.yaml":    {Data: []byte("a: 1\nb: 2\n")},
						"overlay.yaml": {Data: []byte("b: 3\n")},
						"archive.TGZ":  {Data: compressed.Bytes()},
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
			assert.FatalIf(
				t,
				!ok || artifact.FS == nil || artifact.Key == "" || !strings.HasPrefix(artifact.Digest, "sha256:") ||
					artifact.Kind != manifest.KindExternalArtifact ||
					artifact.Revision != "latest@"+artifact.Digest,
				"generated artifact = %+v",
				artifact,
			)
			data, err := fs.ReadFile(artifact.FS, tc.path)
			assert.FatalIf(t, err != nil || string(data) != tc.want, "output %s = %q, %v", tc.path, data, err)
			accepted, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
			assert.FatalIf(
				t,
				!ok || !accepted.Complete || accepted.Fingerprint == "" || len(accepted.Children) != 1,
				"accepted inventory = %+v",
				accepted,
			)
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
	st := store.New()
	tasks := task.NewBounded(2)
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			err := owner.Close()
			assert.ErrorIf(t, err != nil, "%v", err)
		}
	})
	c := New(st, tasks, owner)

	outputs := make([]any, 0, 3)
	for _, name := range []string{"a", "b", "c"} {
		outputs = append(
			outputs,
			map[string]any{
				"name": name,
				"copy": []any{map[string]any{"from": "@repo/" + name, "to": "@artifact/file"}},
			},
		)
	}
	generatorForTestResult2 := generatorForTest(t, outputs)
	obj, doc := generatorForTestResult2.object, generatorForTestResult2.document
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
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(t, status.Status != store.StatusFailed, "partial status = %+v", status)
	for _, name := range []string{"a", "b"} {
		artifact, ok := st.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: name}, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok || artifact.FS == nil, "accepted %s unavailable", name)
		{
			data, err := fs.ReadFile(artifact.FS, "file")
			assert.FatalIf(t, err != nil || string(data) != name, "accepted %s = %q, %v", name, data, err)
		}
	}
	accepted := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, accepted.Complete || accepted.Fingerprint != "", "%v", "partial attempt became replayable")
	runForTest(t, c, obj.Named())
	status, _ = st.GetStatus(obj.Named())
	assert.FatalIf(t, status.Status != store.StatusReady, "retry status = %+v", status)
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "c",
	}
	artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok, "%v", "same-input retry did not execute the failed output")
	{
		data, err := fs.ReadFile(artifact.FS, "file")
		assert.FatalIf(t, err != nil || string(data) != "c", "retried c = %q, %v", data, err)
	}
	accepted = st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
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

func TestArtifactGenerator_Disabled(t *testing.T) {
	t.Parallel()
	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks, nil)

	generatorForTestResult3 := generatorForTest(t, nil)
	obj, doc := generatorForTestResult3.object, generatorForTestResult3.document
	doc["metadata"].(map[string]any)["annotations"] = map[string]any{
		"source.extensions.fluxcd.io/reconcile": "DiSaBlEd",
	}
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
		_, err := decodeSpec(obj.Spec)
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
	for _, tc := range []struct {
		name, revision, origin string
		sourceMetadata         map[string]string
		wantOrigin             string
		uid                    bool
	}{
		{"selected_origin", "@repo", "@repo", map[string]string{"org.opencontainers.image.revision": "origin"}, "origin", true},
		{"origin_revision_fallback", "@repo", "@repo", nil, "revision", true},
		{"empty_origin_fallback", "@repo", "@repo", map[string]string{"org.opencontainers.image.revision": ""}, "revision", true},
		{"absent_origin_alias", "", "@absent", nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testArtifactGeneratorMetadataCase(t, &tc)
		},
		)
	}
}

func TestArtifactGenerator_RequiredUnusedAlias(t *testing.T) {
	t.Parallel()
	st := store.New()
	c := New(st, task.NewBounded(2), nil)

	generatorForTestResult4 := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name": "output",
				"copy": []any{map[string]any{"from": "@repo/file", "to": "@artifact/file"}},
			},
		},
	)
	_, doc := generatorForTestResult4.object, generatorForTestResult4.document
	spec := doc["spec"].(map[string]any)
	spec["sources"] = append(
		spec["sources"].([]any),
		map[string]any{"alias": "unused", "name": "unused", "kind": "GitRepository"},
	)
	parsed3, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	obj, doc := parsed3.Object, parsed3.Document
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
	for _, tc := range []struct {
		name         string
		artifact     *store.SourceArtifact
		sourceStatus store.Status
		sourceReason store.StatusReason
		want         store.Status
		wantReason   store.StatusReason
	}{
		{"missing", nil, "", "", store.StatusPending, ""},
		{"key_without_fs", &store.SourceArtifact{Key: "key", LocalPath: "unused"}, store.StatusReady, "", store.StatusFailed, ""},
		{"contentless", &store.SourceArtifact{}, store.StatusReady, "", store.StatusFailed, ""},
		{"skipped", nil, store.StatusReady, store.ReasonSkipped, store.StatusReady, store.ReasonSkipped},
		{"readable_before_ready", &store.SourceArtifact{Key: "key", Content: "key", Digest: "digest", FS: fstest.MapFS{"file": {Data: []byte("bytes")}}}, store.StatusFailed, "", store.StatusReady, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testArtifactGeneratorInputAvailabilityCase(t, &tc)
		},
		)
	}
}

func testArtifactGeneratorMetadataCase(t *testing.T, testCaseInput *struct {
	name string

	revision       string
	origin         string
	sourceMetadata map[string]string
	wantOrigin     string
	uid            bool
},
) {
	t.Helper()

	st := store.New()
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			innerLocalErr := owner.Close()
			assert.ErrorIf(t, innerLocalErr != nil, "%v", innerLocalErr)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	generatorForTestResult5 := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name":           "{App}",
				"revision":       testCaseInput.revision,
				"originRevision": testCaseInput.origin,
				"copy": []any{
					map[string]any{"from": "@repo/apps/{App}/file", "to": "@artifact/file"},
				},
			},
		},
	)
	_, doc := generatorForTestResult5.object, generatorForTestResult5.document
	spec := doc["spec"].(map[string]any)
	spec["pathPattern"] = "@repo/apps/{App}"
	spec["commonMetadata"] = map[string]any{
		"labels": map[string]any{
			"App":                                   "common",
			"app.kubernetes.io/managed-by":          "common",
			"source.extensions.fluxcd.io/generator": "common",
			"kept":                                  "common",
		},
		"annotations": map[string]any{"note": "kept"},
	}
	if !testCaseInput.uid {
		delete(doc["metadata"].(map[string]any), "uid")
	}
	parsed4, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	parsed, doc := parsed4.Object, parsed4.Document
	assert.FatalIf(t, err != nil, "%v", err)
	obj := parsed.(*manifest.RawObject)
	st.AddObject(obj, doc)
	input := &observedFS{FS: fstest.MapFS{"apps/Prod/file": {Data: []byte("bytes")}}}
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	selected := &store.SourceArtifact{
		Key:      "input",
		Content:  "input",
		Digest:   "digest",
		Revision: "revision",
		FS:       input,
		Metadata: testCaseInput.sourceMetadata,
	}
	st.SetArtifact(repo, selected)
	runForTest(t, c, obj.Named())
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "prod",
	}
	artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
	if !ok {
		status, _ := st.GetStatus(obj.Named())
		t.Fatalf("missing generated source: %+v", status)
	}
	wantRevision := "latest@" + artifact.Digest
	if testCaseInput.revision != "" {
		wantRevision = "revision"
	}
	assert.FatalIf(t, artifact.Revision != wantRevision ||
		artifact.Metadata["org.opencontainers.image.revision"] != testCaseInput.wantOrigin, "metadata = %+v; revision=%q origin=%q", artifact, wantRevision, testCaseInput.wantOrigin)
	if testCaseInput.origin == "@absent" && len(artifact.Metadata) != 0 {
		t.Fatalf("absent origin alias added metadata: %+v", artifact.Metadata)
	}
	emission, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "%v", "missing complete generator inventory")
	metadata := emission.Children[0].Doc["metadata"].(map[string]any)
	uid := ""
	if testCaseInput.uid {
		uid = "authored"
	}
	want := map[string]any{
		"App":                                   "prod",
		"app.kubernetes.io/managed-by":          "source-watcher",
		"source.extensions.fluxcd.io/generator": uid,
		"kept":                                  "common",
	}
	{
		diff := cmp.Diff(want, metadata["labels"])
		assert.FatalIf(t, diff != "", "%v", diff)
	}
	{
		diff := cmp.Diff(map[string]any{"note": "kept"}, metadata["annotations"])
		assert.FatalIf(t, diff != "", "%v", diff)
	}
	for _, key := range []string{"uid", "resourceVersion", "creationTimestamp"} {
		{
			_, present := metadata[key]
			assert.FatalIf(t, present, "invented %s", key)
		}
	}
	sourceRef := emission.Children[0].Doc["spec"].(map[string]any)["sourceRef"]
	{
		diff := cmp.Diff(
			map[string]any{
				"apiVersion": obj.APIVersion,
				"kind":       obj.Kind,
				"name":       obj.Name,
				"namespace":  obj.Namespace,
			},
			sourceRef,
		)
		assert.FatalIf(t, diff != "", "%v", diff)
	}
	opens := input.opens.Load()
	updated := *selected
	updated.Metadata = maps.Clone(selected.Metadata)
	if updated.Metadata == nil {
		updated.Metadata = map[string]string{}
	}
	updated.Metadata["unrelated"] = "changed"
	st.SetArtifact(repo, &updated)
	runForTest(t, c, obj.Named())
	assert.FatalIf(
		t,
		st.GetArtifact(target, store.ArtifactContent) != artifact || input.opens.Load() != opens,
		"%v",
		"unrelated metadata prevented replay",
	)
	updatedDoc := maps.Clone(doc)
	updatedMetadata := maps.Clone(doc["metadata"].(map[string]any))
	updatedMetadata["uid"] = "next"
	updatedDoc["metadata"] = updatedMetadata
	st.AddObject(obj, updatedDoc)
	runForTest(t, c, obj.Named())
	emission, ok = st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "%v", "UID refresh left no authoritative generator inventory")
	labels := emission.Children[0].Doc["metadata"].(map[string]any)["labels"].(map[string]any)
	assert.FatalIf(
		t,
		labels["source.extensions.fluxcd.io/generator"] != "next" || input.opens.Load() != opens,
		"%v",
		"cache-hit metadata did not refresh without input work",
	)
	if testCaseInput.origin == "@repo" {
		next := updated
		next.Metadata = maps.Clone(updated.Metadata)
		next.Metadata["org.opencontainers.image.revision"] = "next-origin"
		st.SetArtifact(repo, &next)
		runForTest(t, c, obj.Named())
		fresh := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, fresh.Revision != "revision" ||
			fresh.Metadata["org.opencontainers.image.revision"] != "next-origin", "origin-only metadata stayed stale: %+v", fresh)
		last := next
		last.Revision = "next-revision"
		st.SetArtifact(repo, &last)
		runForTest(t, c, obj.Named())
		fresh = st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, fresh.Revision != "next-revision" ||
			fresh.Metadata["org.opencontainers.image.revision"] != "next-origin", "selected metadata stayed stale: %+v", fresh)
	}
	disabledDoc := maps.Clone(updatedDoc)
	disabledMetadata := maps.Clone(updatedMetadata)
	disabledMetadata["annotations"] = map[string]any{"source.extensions.fluxcd.io/reconcile": "disabled"}
	disabledDoc["metadata"] = disabledMetadata
	st.AddObject(obj, disabledDoc)
	beforeDisabled := st.GetArtifact(target, store.ArtifactContent)
	beforeDisabledOpens := input.opens.Load()
	var emissions atomic.Int64
	unsubscribe := st.AddListener(
		store.EventArtifactUpdated,
		func(manifest.NamedResource, any) { emissions.Add(1) },
		false,
	)
	runForTest(t, c, obj.Named())
	unsubscribe()
	assert.FatalIf(
		t,
		emissions.Load() != 0 || st.GetArtifact(target, store.ArtifactContent) != beforeDisabled,
		"%v",
		"disable annotation arrival changed accepted outputs",
	)
	st.AddObject(obj, updatedDoc)
	runForTest(t, c, obj.Named())
	enabled := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, input.opens.Load() != beforeDisabledOpens ||
		enabled.Children[0].Doc["metadata"].(map[string]any)["labels"].(map[string]any)["source.extensions.fluxcd.io/generator"] != "next", "%v", "re-enabling did not retain cached content and current UID")
}

func testArtifactGeneratorInputAvailabilityCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		artifact     *store.SourceArtifact
		sourceStatus store.Status
		sourceReason store.StatusReason
		want         store.
				Status
		wantReason store.StatusReason
	},
) {
	t.Helper()

	st := store.New()
	owner, err := cache.New(t.Context(), "")
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		{
			err := owner.Close()
			assert.ErrorIf(t, err != nil, "%v", err)
		}
	})
	c := New(st, task.NewBounded(2), owner)

	generatorForTestResult6 := generatorForTest(
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
	obj, doc := generatorForTestResult6.object, generatorForTestResult6.document
	st.AddObject(obj, doc)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	if testCaseInput.artifact != nil {
		st.SetArtifact(repo, testCaseInput.artifact)
	}
	if testCaseInput.sourceStatus != "" {
		st.UpdateStatus(
			repo,
			store.StatusInfo{
				Status:  testCaseInput.sourceStatus,
				Reason:  testCaseInput.sourceReason,
				Message: "source condition",
			},
		)
	}
	runForTest(t, c, obj.Named())
	status, _ := st.GetStatus(obj.Named())
	assert.FatalIf(t, status.Status != testCaseInput.want || testCaseInput.
		wantReason != "" && status.Reason != testCaseInput.wantReason, "input availability = %+v; want %s/%s",
		status, testCaseInput.
			want, testCaseInput.
			wantReason,
	)
	target := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "output",
	}
	if (st.GetArtifact(target, store.ArtifactContent) != nil) != (testCaseInput.name == "readable_before_ready") {
		t.Fatalf("input %s output = %+v", testCaseInput.name, st.GetArtifact(target, store.ArtifactContent))
	}
	if testCaseInput.name == "missing" {
		c.Tasks.Go(t.Context(), "generator", func(ctx context.Context) {
			c.ReconcileNode(ctx, obj.Named(), manifest.DrainCascade)
		})
		c.Tasks.BlockTillDone()
		status, _ = st.GetStatus(obj.Named())
		assert.FatalIf(t, status.Status != store.StatusFailed, "missing input after drain = %+v", status)
	}
}
