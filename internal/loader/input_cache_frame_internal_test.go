package loader

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"testing/fstest"
	"weak"

	kustomizetypes "sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/pkg/manifest"
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
	frame := inputFrame{Schema: 1, Files: []cachedInput{{Name: "object.yaml", Objects: []cachedOccurrence{{
		Ordinal: 3, payload: &inputPayload{data: payload},
	}}}}}
	visit := cachedVisit{Context: cachedGraphContext{Root: "."}, File: "object.yaml"}
	frame.Walks = []cachedWalk{{Visits: []cachedVisit{visit}}, {Visits: []cachedVisit{visit}}}
	var encoded bytes.Buffer
	assert.NoError(t, frame.write(&encoded))
	size := binary.BigEndian.Uint64(encoded.Bytes()[:8])
	for _, complete := range []bool{false, true} {
		t.Run(strconv.FormatBool(complete), func(t *testing.T) {
			t.Parallel()
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			data := encoded.Bytes()
			if !complete {
				data = data[:8+size]
			}
			generation, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: "frame"}, &cache.Policy{},
				func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
					_, writeErr := out.Write(data)
					return cache.ProducerResult{}, writeErr
				})
			assert.NoError(t, err)
			restored, err := readInputFrame(generation)
			assert.NoError(t, err)
			assert.Equal(t, restored.Files[0].Objects[0].Ordinal, 3)
			assert.Equal(t, len(restored.Visits), 1)
			assert.Equal(t, len(restored.Walks[0].Visits), 0)
			assert.Equal(t, &restored.Walks[0].dictionary[0] == &restored.Walks[1].dictionary[0], true)
			lazy := inputPayload{
				archive: restored.archive,
				offset:  restored.Files[0].Objects[0].Offset,
				size:    restored.Files[0].Objects[0].Size,
			}
			actual, err := lazy.read()
			if complete {
				assert.NoError(t, err)
				assert.Equal(t, bytes.Equal(actual, payload), true)
				restored.Files[0].Objects[0].payload = &lazy
				var first, second bytes.Buffer
				assert.NoError(t, restored.write(&first))
				assert.NoError(t, restored.write(&second))
				assert.Equal(t, bytes.Equal(first.Bytes(), encoded.Bytes()), true)
				assert.Equal(t, bytes.Equal(first.Bytes(), second.Bytes()), true)
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
		header      uint64
	}{
		{name: "schema", index: `{"Schema":2}`},
		{name: "json", index: `{"Schema":1,`},
		{name: "negative-visit", index: `{"Schema":1,"Walks":[{"Visits":[-1]}]}`},
		{name: "missing-visit", index: `{"Schema":1,"Walks":[{"Visits":[0]}]}`},
		{name: "index-length", index: `{"Schema":1}`, header: 1000},
		{name: "index-overflow", index: `{"Schema":1}`, header: 1 << 63},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var data bytes.Buffer
			size := uint64(len(tc.index))
			if tc.header != 0 {
				size = tc.header
			}
			assert.NoError(t, binary.Write(&data, binary.BigEndian, size))
			data.WriteString(tc.index)
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			generation, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: "frame"}, &cache.Policy{},
				func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
					_, writeErr := out.Write(data.Bytes())
					return cache.ProducerResult{}, writeErr
				})
			assert.NoError(t, err)
			_, err = readInputFrame(generation)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			if tc.name == "json" {
				_, syntax := errors.AsType[*json.SyntaxError](err)
				assert.Equal(t, syntax, true)
			}
		})
	}
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
