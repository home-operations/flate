package loader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"weak"

	kustomizetypes "sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestRestoreFrameFiles_IndependentLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	l := cachedFixtureLoader(t, root, fstest.MapFS{}, nil)
	defer l.Close()
	state := &inputCache{
		input:   l.documents.artifacts[root],
		digests: map[string]string{"first.yaml": "first", "second.yaml": "second"},
		types:   map[string]fs.FileMode{"first.yaml": 0, "second.yaml": 0},
		files:   map[inputCoordinate]inputEvidence{},
	}
	backing := func() weak.Pointer[cachedInput] {
		frame := inputFrame{Files: []cachedInput{
			{Name: "first.yaml", Digest: "first", Objects: []cachedOccurrence{{Ordinal: 3}}},
			{Name: "second.yaml", Digest: "second", Objects: []cachedOccurrence{{Ordinal: 7}}},
		}}
		pointer := weak.Make(&frame.Files[0])
		assert.NoError(t, l.restoreFrameFiles(state, &frame))
		return pointer
	}()
	runtime.GC()
	assert.Equal(t, backing.Value() == nil, true)
	first := l.documents.cache[filepath.Join(root, "first.yaml")].deferred
	second := l.documents.cache[filepath.Join(root, "second.yaml")].deferred
	assert.Equal(t, first.Objects[0].Ordinal, 3)
	assert.Equal(t, second.Objects[0].Ordinal, 7)
	runtime.KeepAlive(l)
}

func TestInputFrame_LazyPayload(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("declaration"), 8192)
	frame := inputFrame{Files: []cachedInput{{Name: "object.yaml", Objects: []cachedOccurrence{{
		Ordinal: 3, payload: &inputPayload{data: payload},
	}}}}}
	visit := cachedVisit{Context: cachedGraphContext{Root: "."}, File: "object.yaml"}
	frame.Walks = []cachedWalk{{Visits: []cachedVisit{visit}}, {Visits: []cachedVisit{visit}}}
	encoded := encodeInputFrame(t, &frame)
	for _, complete := range []bool{false, true} {
		t.Run(strconv.FormatBool(complete), func(t *testing.T) {
			t.Parallel()
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			data := encoded
			if !complete {
				data = data[:len(data)-len(payload)]
			}
			generation, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: "frame"}, &cache.Policy{},
				func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
					_, writeErr := out.Write(data)
					return cache.ProducerResult{}, writeErr
				})
			assert.NoError(t, err)
			restored, err := readInputFrame(t.Context(), generation)
			assert.NoError(t, err)
			assert.Equal(t, restored.Files[0].Objects[0].Ordinal, 3)
			assert.Equal(t, len(restored.Visits), 1)
			assert.Equal(t, len(restored.Walks[0].Visits), 0)
			assert.Equal(t, &restored.Walks[0].dictionary[0] == &restored.Walks[1].dictionary[0], true)
			lazy := inputPayload{
				frame:  restored.frame,
				offset: restored.Files[0].Objects[0].Offset,
				size:   restored.Files[0].Objects[0].Size,
			}
			actual, err := lazy.read(t.Context())
			if complete {
				assert.NoError(t, err)
				assert.Equal(t, bytes.Equal(actual, payload), true)
				restored.Files[0].Objects[0].payload = &lazy
				first, second := encodeInputFrame(t, restored), encodeInputFrame(t, restored)
				assert.Equal(t, bytes.Equal(first, encoded), true)
				assert.Equal(t, bytes.Equal(first, second), true)
			} else {
				assert.Equal(t, err != nil, true)
				assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			}
		})
	}
}

func TestInputFrame_RejectsInvalidIndex(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, index string
	}{
		{name: "negative-visit", index: `{"Walks":[{"Visits":[-1]}]}`},
		{name: "missing-visit", index: `{"Walks":[{"Visits":[0]}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var data bytes.Buffer
			frame, err := cache.NewFrame("loader-inputs", json.RawMessage(tc.index), nil)
			assert.NoError(t, err)
			publication, err := frame.PreparePublication(t.Context(), cache.FramePublicationInput{})
			assert.NoError(t, err)
			_, err = publication.WriteTo(&data)
			assert.NoError(t, err)
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			generation, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: "frame"}, &cache.Policy{},
				func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
					_, writeErr := out.Write(data.Bytes())
					return cache.ProducerResult{}, writeErr
				})
			assert.NoError(t, err)
			_, err = readInputFrame(t.Context(), generation)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		})
	}
}

func encodeInputFrame(t *testing.T, frame *inputFrame) []byte {
	t.Helper()
	content, err := frame.prepare(t.Context())
	assert.NoError(t, err)
	publication, err := content.PreparePublication(t.Context(), cache.FramePublicationInput{})
	assert.NoError(t, err)
	var encoded bytes.Buffer
	_, err = publication.WriteTo(&encoded)
	assert.NoError(t, err)
	return encoded.Bytes()
}

func TestFramePublication_UnavailableContent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	if !owner.CanPersistRenders() {
		t.Fatal("input cache witness requires a clean stamped test binary")
	}
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
		"object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
	}
	first := cachedFixtureLoader(t, root, files, owner)
	assert.NoError(t, first.PersistInputs(t.Context()))
	lookup := first.inputCaches[root].lookup
	previous, previousDescriptor, _, err := owner.Blob(t.Context(), lookup, &cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	previousBytes, err := fs.ReadFile(previous, cache.FileName(previous))
	assert.NoError(t, err)
	first.Close()

	files["kustomization.yaml"] = &fstest.MapFile{
		Data: []byte("resources: [object.yaml]\ncommonAnnotations:\n  large: " + strings.Repeat("x", 16<<20) + "\n"),
	}
	l := cachedFixtureLoader(t, root, files, owner)
	defer l.Close()
	cold := cachedFixtureLoader(t, root, files, nil)
	defer cold.Close()
	assert.Diff(t, l.Index, cold.Index)
	state := l.inputCaches[root]
	priorFiles, priorLoads, priorWalks := maps.Clone(state.files), maps.Clone(state.loads), maps.Clone(state.walks)
	frame, err := l.cacheSourceInputs(state)
	assert.NoError(t, err)
	assert.FatalIf(t, frame == nil, "changed builder supplied no authored facts")
	content, err := frame.prepare(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, content == nil, true)
	assert.NoError(t, l.PersistInputs(t.Context()))
	assert.Equal(t, maps.Equal(state.files, priorFiles), true)
	assert.Equal(t, maps.Equal(state.loads, priorLoads), true)
	assert.Equal(t, maps.Equal(state.walks, priorWalks), true)
	assert.Diff(t, l.Index, cold.Index)
	occurrence, known := l.Occurrence(manifest.NamedResource{Kind: "ConfigMap", Name: "object"})
	assert.Equal(t, known, true)
	child, err := l.OccurrenceObject(t.Context(), &occurrence, nil)
	assert.NoError(t, err)
	assert.Diff(t, child.Doc, map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "object"},
	})

	retained, retainedDescriptor, _, err := owner.Blob(t.Context(), lookup, &cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	assert.Equal(t, retainedDescriptor.Digest, previousDescriptor.Digest)
	retainedBytes, err := fs.ReadFile(retained, cache.FileName(retained))
	assert.NoError(t, err)
	assert.Equal(t, bytes.Equal(retainedBytes, previousBytes), true)
}

func TestInputFrame_RejectsInvalidCoordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		corrupt func(*inputFrame)
	}{
		{"file-name", func(f *inputFrame) { f.Files[0].Name = "../object.yaml" }},
		{"file-root", func(f *inputFrame) { f.Files[0].Name = "." }},
		{"file-digest", func(f *inputFrame) { f.Files[0].Digest = "" }},
		{"builder-name", func(f *inputFrame) {
			f.Files[0].Builder = "other.yaml"
			f.Files[0].Body = &kustomizetypes.Kustomization{}
		}},
		{"builder-body", func(f *inputFrame) { f.Files[0].Builder = "object.yaml" }},
		{"load-path", func(f *inputFrame) { f.Loads[0].Path = "../outside" }},
		{"load-ignore", func(f *inputFrame) { f.Loads[0].Ignore = "../ignore" }},
		{"load-file", func(f *inputFrame) { f.Loads[0].Files = []cachedAdmission{{File: "../object.yaml"}} }},
		{"load-kind", func(f *inputFrame) {
			f.Loads[0].Files = []cachedAdmission{{File: "object.yaml", Kind: admissionWarning}}
		}},
		{"load-declaration", func(f *inputFrame) {
			f.Loads[0].Files = []cachedAdmission{{File: "object.yaml", Declared: true, Coordinate: "../object.yaml"}}
		}},
		{"load-directory", func(f *inputFrame) { f.Loads[0].Directories = []cachedDirectory{{Path: "../outside"}} }},
		{"load-domain", func(f *inputFrame) { f.Loads[0].Domains = []string{"../outside"} }},
		{"load-input", func(f *inputFrame) { f.Loads[0].Inputs = []string{"../outside"} }},
		{"load-type", func(f *inputFrame) { f.Loads[0].Types = []string{"../outside"} }},
		{"load-input-order", func(f *inputFrame) { f.Loads[0].Inputs = []string{"b", "a"} }},
		{"load-type-order", func(f *inputFrame) { f.Loads[0].Types = []string{"b", "a"} }},
		{"walk-root", func(f *inputFrame) { f.Walks[0].Context.Root = "../outside" }},
		{"walk-input", func(f *inputFrame) { f.Walks[0].Inputs = []string{"../outside"} }},
		{"walk-listing", func(f *inputFrame) { f.Walks[0].Listed = []string{"../outside"} }},
		{"walk-input-order", func(f *inputFrame) { f.Walks[0].Inputs = []string{"b", "a"} }},
		{"walk-listing-order", func(f *inputFrame) { f.Walks[0].Listed = []string{"b", "a"} }},
		{"visit-root", func(f *inputFrame) {
			f.Walks[0].Visits = []cachedVisit{{Context: cachedGraphContext{Root: "../outside"}, File: "object.yaml"}}
		}},
		{"visit-file", func(f *inputFrame) {
			f.Walks[0].Visits = []cachedVisit{{Context: cachedGraphContext{Root: "."}, File: "../object.yaml"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			l := cachedFixtureLoader(t, root, fstest.MapFS{}, nil)
			defer l.Close()
			state := &inputCache{
				input: l.documents.artifacts[root], digests: map[string]string{"object.yaml": "digest"},
				types: map[string]fs.FileMode{"object.yaml": 0}, files: map[inputCoordinate]inputEvidence{},
				loads: map[loadContext]manifest.DocumentRole{}, walks: map[physicalContext]struct{}{},
			}
			frame := inputFrame{
				Files: []cachedInput{{Name: "object.yaml", Digest: "digest"}},
				Loads: []cachedLoad{{Path: "."}}, Walks: []cachedWalk{{Context: cachedGraphContext{Root: "."}}},
			}
			tc.corrupt(&frame)
			err := l.restoreFrameFiles(state, &frame)
			if err == nil {
				err = l.restoreFrameTraversal(state, &frame)
			}
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		})
	}
}
