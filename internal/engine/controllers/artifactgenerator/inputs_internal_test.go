package artifactgenerator

import (
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEvaluate_ExportedInputs(t *testing.T) {
	t.Parallel()
	const atLimit = "# note\n"
	for _, tc := range []struct {
		name, selector, payload, cause string
		want                           map[string]any
	}{
		{name: "absent"},
		{name: "empty", selector: "@repo/values"},
		{name: "null", selector: "@repo/values", payload: "null\n"},
		{name: "empty_mapping", selector: "@repo/values", payload: "{}\n", want: map[string]any{}},
		{name: "at_byte_limit", selector: "@repo/tmp/../values", payload: atLimit + strings.Repeat(" ", maxInputsFileSize-len(atLimit))},
		{name: "over_byte_limit", selector: "@repo/values", payload: atLimit + strings.Repeat(" ", maxInputsFileSize), cause: "file exceeds the maximum size of 1048576 bytes"},
		{name: "absolute", selector: "@repo//values", cause: "must stay within the source root"},
		{name: "escape", selector: "@repo/stage/../../values", cause: "must stay within the source root"},
		{name: "missing", selector: "@repo/no-file", cause: `failed to read inputs file "@repo/no-file"`},
		{name: "parse", selector: "@repo/values", payload: "name: [", cause: `failed to parse inputs file "@repo/values"`},
		{name: "sequence", selector: "@repo/values", payload: "- value", cause: `failed to parse inputs file "@repo/values"`},
		{name: "artifact", selector: "@artifact/values", want: map[string]any{"from": "artifact"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := evaluation{inputs: map[string]fs.FS{"repo": fstest.MapFS{"values": {Data: []byte(tc.payload)}}}}
			got, err := e.exportedInputs(
				t.Context(),
				tc.selector,
				fstest.MapFS{"values": {Data: []byte("from: artifact\n")}},
			)
			if tc.cause != "" {
				assert.FatalIf(
					t,
					err == nil || !strings.Contains(err.Error(), tc.cause),
					"exported inputs error = %v",
					err,
				)
				return
			}
			assert.NoError(t, err)
			assert.Diff(t, got, tc.want)
		})
	}

	t.Run("read_error", func(t *testing.T) {
		t.Parallel()
		repo := inputsReadErrorFS{MapFS: fstest.MapFS{"values.yaml": {Data: []byte("from: repo\n")}}}
		e := evaluation{inputs: map[string]fs.FS{"repo": repo}}
		got, err := e.exportedInputs(t.Context(), "@repo/values.yaml", nil)
		assert.ErrorIf(t, got != nil, "exported inputs = %v, want nil", got)
		assert.FatalIf(t, !errors.Is(err, io.ErrUnexpectedEOF), "exported inputs error = %v, want unexpected EOF", err)
		assert.ErrorIf(t, !strings.Contains(err.Error(), `"@repo/values.yaml"`), "exported inputs error = %v", err)
	})
}

type inputsReadErrorFS struct{ fstest.MapFS }

func (f inputsReadErrorFS) Open(name string) (fs.File, error) {
	file, err := f.MapFS.Open(name)
	if err != nil {
		return nil, err
	}

	return inputsReadErrorFile{File: file}, nil
}

type inputsReadErrorFile struct{ fs.File }

func (f inputsReadErrorFile) Read(data []byte) (int, error) {
	n, _ := f.File.Read(data)
	return n, io.ErrUnexpectedEOF
}

func testArtifactGeneratorExportedInputs(t *testing.T) {
	t.Helper()
	t.Parallel()
	st := store.New()
	c := New(st, task.NewBounded(2), cachetest.New(t, "", cache.New))
	fixture := generatorForTest(t, []any{
		map[string]any{
			"name": "{App}", "namespace": "{Tenant}", "inputsFrom": "@repo/inputs/{Tenant}.yaml",
			"copy": []any{map[string]any{"from": "@repo/apps/{Tenant}/{App}/file", "to": "@artifact/file"}},
		},
	})
	fixture.document["spec"].(map[string]any)["pathPattern"] = "@repo/apps/{Tenant}/{App}"
	parsed, err := manifest.ParseDoc(fixture.document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	st.AddObject(parsed.Object, parsed.Document)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	first := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "one",
		Name:      "api",
	}
	second := first
	second.Namespace = "two"
	supplier := func(content, values string) {
		t.Helper()
		st.SetArtifact(repo, &store.SourceArtifact{Content: content, FS: fstest.MapFS{
			"apps/One/API/file": {Data: []byte("one")}, "apps/Two/API/file": {Data: []byte("two")},
			"inputs/One.yaml": {Data: []byte(values)}, "inputs/Two.yaml": {Data: []byte("from: two")},
		}})
		runForTest(t, c, parsed.Object.Named())
		status, _ := st.GetStatus(parsed.Object.Named())
		assert.Equal(t, status.Status, store.StatusReady)
	}
	verify := func(target manifest.NamedResource, want any) {
		t.Helper()
		obj := st.GetObject(target).(*manifest.ExternalArtifact)
		doc, err := obj.RenderDocument()
		assert.NoError(t, err)
		assert.Diff(t, doc["status"], want)
		assert.Equal(t, obj.SourceRef.Namespace, "apps")
		assert.Equal(t, manifest.DocIdentity(doc), target)
	}
	supplier("first", "from: one\nlist: [true, {key: value}]")
	verify(
		first,
		map[string]any{
			"exportedInputs": map[string]any{"from": "one", "list": []any{true, map[string]any{"key": "value"}}},
		},
	)
	two := map[string]any{"exportedInputs": map[string]any{"from": "two"}}
	verify(second, two)
	asset := st.GetArtifact(first, store.ArtifactContent).(*store.SourceArtifact)
	emission := st.GetArtifact(parsed.Object.Named(), store.ArtifactContent).(*store.Emission)
	runForTest(t, c, parsed.Object.Named())
	replay := st.GetArtifact(parsed.Object.Named(), store.ArtifactContent).(*store.Emission)
	assert.Equal(t, replay.Fingerprint, emission.Fingerprint)
	assert.Equal(t, replay.Children[0].Obj == emission.Children[0].Obj, true)
	assert.Diff(t, replay.Children[0].Doc, emission.Children[0].Doc)
	assert.Equal(t, st.GetArtifact(first, store.ArtifactContent) == asset, true)

	supplier("next", "from: updated")
	verify(first, map[string]any{"exportedInputs": map[string]any{"from": "updated"}})
	updated := st.GetArtifact(first, store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, updated.Digest, asset.Digest)
	supplier("empty", "{}")
	verify(first, nil)
	verify(second, two)
	cleared := st.GetArtifact(first, store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, cleared.Digest, asset.Digest)
}

func testArtifactGeneratorLateInputs(t *testing.T) {
	t.Helper()
	t.Parallel()
	st := store.New()
	c := New(st, task.NewBounded(2), cachetest.New(t, "", cache.New))
	fixture := generatorForTest(t, []any{
		map[string]any{
			"name":       "one",
			"inputsFrom": "@artifact/cloudbuild.yaml",
			"copy": []any{
				map[string]any{"from": "@repo/good.yaml", "to": "@artifact/values.yaml"},
				map[string]any{"from": "@repo/staged.yaml", "to": "@artifact/cloudbuild.yaml"},
			},
		},
		map[string]any{
			"name":       "two",
			"inputsFrom": "@repo/broken.yaml",
			"copy":       []any{map[string]any{"from": "@repo/good.yaml", "to": "@artifact/values.yaml"}},
		},
	})
	st.AddObject(fixture.object, fixture.document)
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	one := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindExternalArtifact,
		Namespace: "apps",
		Name:      "one",
	}
	two := one
	two.Name = "two"
	var firstDigest string
	for _, region := range []string{"east", "west"} {
		st.SetArtifact(repo, &store.SourceArtifact{Content: region, FS: fstest.MapFS{
			"good.yaml": {Data: []byte("from: artifact\n")}, "broken.yaml": {Data: []byte("from: [\n")},
			"staged.yaml": {Data: []byte("region: " + region + "\n")},
		}})
		runForTest(t, c, fixture.object.Named())
		status, _ := st.GetStatus(fixture.object.Named())
		assert.Equal(t, status.Status, store.StatusFailed)
		assert.Equal(t, strings.Contains(status.String(), `failed to parse inputs file "@repo/broken.yaml"`), true)
		assert.Equal(t, st.GetObject(two), nil)
		assert.Equal(t, st.GetArtifact(two, store.ArtifactContent), nil)
		payload, ok := st.GetArtifact(one, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, !ok, "the earlier output was not published")
		_, err := fs.Stat(payload.FS, "cloudbuild.yaml")
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		data, err := fs.ReadFile(payload.FS, "values.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "from: artifact\n")
		if firstDigest == "" {
			firstDigest = payload.Digest
		}
		assert.Equal(t, payload.Digest, firstDigest)
		emitted := st.GetArtifact(fixture.object.Named(), store.ArtifactContent).(*store.Emission)
		assert.Equal(t, emitted.Complete, false)
		assert.Equal(t, manifest.DocIdentity(emitted.Children[0].Doc), one)
		assert.Diff(
			t,
			emitted.Children[0].Doc["status"],
			map[string]any{"exportedInputs": map[string]any{"region": region}},
		)
	}
}
