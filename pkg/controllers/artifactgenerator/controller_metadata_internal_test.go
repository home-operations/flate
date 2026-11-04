package artifactgenerator

import (
	"io/fs"
	"maps"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type metadataReplay struct {
	document map[string]any
	metadata map[string]any
}

type metadataCase struct {
	name, revision, origin string
	sourceMetadata         map[string]string
	wantOrigin             string
	uid                    bool
}

type metadataFixture struct {
	c            *Controller
	obj          *manifest.RawObject
	doc          map[string]any
	input        *observedFS
	selected     *store.SourceArtifact
	repo, target manifest.NamedResource
}

func (f *metadataFixture) assertReplay(t *testing.T, tc *metadataCase, artifact *store.SourceArtifact) metadataReplay {
	t.Helper()
	c, obj, doc, input, selected := f.c, f.obj, f.doc, f.input, f.selected
	st, repo, target := c.Store, f.repo, f.target
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
	emission, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "%v", "UID refresh left no authoritative generator inventory")
	labels := emission.Children[0].Doc["metadata"].(map[string]any)["labels"].(map[string]any)
	assert.FatalIf(
		t,
		labels["source.extensions.fluxcd.io/generator"] != "next" || input.opens.Load() != opens,
		"%v",
		"cache-hit metadata did not refresh without input work",
	)
	if tc.origin == "@repo" {
		next := updated
		next.Metadata = maps.Clone(updated.Metadata)
		next.Metadata["org.opencontainers.image.revision"] = "next-origin"
		st.SetArtifact(repo, &next)
		runForTest(t, c, obj.Named())
		fresh := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(
			t,
			fresh.Revision != "revision" || fresh.Metadata["org.opencontainers.image.revision"] != "next-origin",
			"origin-only metadata stayed stale: %+v",
			fresh,
		)
		last := next
		last.Revision = "next-revision"
		st.SetArtifact(repo, &last)
		runForTest(t, c, obj.Named())
		fresh = st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(
			t,
			fresh.Revision != "next-revision" || fresh.Metadata["org.opencontainers.image.revision"] != "next-origin",
			"selected metadata stayed stale: %+v",
			fresh,
		)
	}
	return metadataReplay{document: updatedDoc, metadata: updatedMetadata}
}

func (f *metadataFixture) assertEnabled(t *testing.T, updatedDoc, updatedMetadata map[string]any) {
	t.Helper()
	c, obj, input := f.c, f.obj, f.input
	st, target := c.Store, f.target
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
	labels := enabled.Children[0].Doc["metadata"].(map[string]any)["labels"].(map[string]any)
	assert.FatalIf(
		t,
		input.opens.Load() != beforeDisabledOpens || labels["source.extensions.fluxcd.io/generator"] != "next",
		"%v",
		"re-enabling did not retain cached content and current UID",
	)
}

func (f *metadataFixture) prepare(t *testing.T, tc *metadataCase) {
	t.Helper()
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

	generatorForTestResult := generatorForTest(
		t,
		[]any{
			map[string]any{
				"name":           "{App}",
				"revision":       tc.revision,
				"originRevision": tc.origin,
				"copy":           []any{map[string]any{"from": "@repo/apps/{App}/file", "to": "@artifact/file"}},
			},
		},
	)
	doc := generatorForTestResult.document
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
	if !tc.uid {
		delete(doc["metadata"].(map[string]any), "uid")
	}
	parseDocResult, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	parsed, doc := parseDocResult.Object, parseDocResult.Document
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
		Metadata: tc.sourceMetadata,
	}
	st.SetArtifact(repo, selected)
	runForTest(t, c, obj.Named())
	f.c, f.obj, f.doc = c, obj, doc
	f.input, f.selected, f.repo = input, selected, repo
}

func checkCapturedMetadata(t *testing.T, st *store.Store, target manifest.NamedResource, want string) {
	t.Helper()
	artifact, ok := st.GetArtifact(target, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok, "missing %s output", want)
	data, err := fs.ReadFile(artifact.FS, "file")
	assert.FatalIf(t, err != nil || string(
		data,
	) != want || artifact.Revision != want+"-revision" || artifact.Metadata["org.opencontainers.image.revision"] != want+"-origin", "captured %s = %q, %+v, %v", want, data, artifact, err)
}

func (f *metadataFixture) assertMetadata(t *testing.T, tc *metadataCase) *store.SourceArtifact {
	t.Helper()
	st, obj := f.c.Store, f.obj
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
	if tc.revision != "" {
		wantRevision = "revision"
	}
	assert.FatalIf(
		t,
		artifact.Revision != wantRevision || artifact.Metadata["org.opencontainers.image.revision"] != tc.wantOrigin,
		"metadata = %+v; revision=%q origin=%q",
		artifact,
		wantRevision,
		tc.wantOrigin,
	)
	assert.FatalIf(
		t,
		tc.origin == "@absent" && len(artifact.Metadata) != 0,
		"absent origin alias added metadata: %+v",
		artifact.Metadata,
	)
	emission, ok := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "%v", "missing complete generator inventory")
	metadata := emission.Children[0].Doc["metadata"].(map[string]any)
	uid := ""
	if tc.uid {
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
	f.target = target
	return artifact
}
