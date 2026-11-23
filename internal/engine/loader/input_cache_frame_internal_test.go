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
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"weak"

	"oras.land/oras-go/v2/errdef"
	kustomizetypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/engine/kustomize/krusty"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestInputCache_RejectsFrameWithoutRecipe(t *testing.T) {
	t.Parallel()
	if reason := cache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skipf("render persistence disabled: %s", reason)
	}
	t.Log("render persistence enabled")
	const builder = "apps/kustomization.yaml"
	root, backing := t.TempDir(), t.TempDir()
	files := fstest.MapFS{
		builder: {Data: []byte(`resources: [leaf/object.yaml]
transformers: [labels.yaml]
patches:
- target: {kind: ConfigMap, name: original}
  patch: '[{"op":"replace","path":"/metadata/name","value":"patched"}]'
`)},
		"apps/labels.yaml": {Data: []byte(`apiVersion: builtin
kind: LabelTransformer
metadata: {name: labels}
labels: {from: transformer}
fieldSpecs: [{path: metadata/labels, create: true}]
`)},
		"apps/leaf/object.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n")},
	}
	fresh := cachedFixtureLoader(t, root, files, nil)
	defer fresh.Close()
	expected := patchedInputResult(t, fresh, files)
	assert.Equal(t, expected, `apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    from: transformer
  name: patched
`)

	owner, err := cache.New(t.Context(), backing)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, owner.Close()) })
	seed := cachedFixtureLoader(t, root, files, owner)
	state := seed.inputCaches[root]
	publishInputFrameWithoutRecipe(t, seed, builder)
	seed.Close()

	origin, url := state.input.OriginPath(root), state.input.URL
	current, _ := cache.InputFrameIdentity(origin, url, nil)
	policy := &cache.Policy{MaxAge: cache.Forever}
	_, _, _, err = owner.Blob(t.Context(), current, policy, nil)
	assert.ErrorIf(t, !errors.Is(err, errdef.ErrNotFound), "input cache accepted the version 4 key: %v", err)

	coordinate := inputCoordinate{name: builder, builder: true}
	rebuilt := cachedFixtureLoader(t, root, files, owner)
	_, restored := rebuilt.inputCaches[root].files[coordinate]
	assert.Equal(t, restored, false)
	assert.Equal(t, patchedInputResult(t, rebuilt, files), expected)
	assert.NoError(t, rebuilt.PersistInputs(t.Context()))
	_, _, _, err = owner.Blob(t.Context(), current, policy, nil)
	assert.NoError(t, err)
	rebuilt.Close()
	assert.NoError(t, owner.Close())

	reopened, err := cache.New(t.Context(), backing)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
	warm := cachedFixtureLoader(t, root, files, reopened)
	defer warm.Close()
	_, restored = warm.inputCaches[root].files[coordinate]
	assert.Equal(t, restored, true)
	assert.Equal(t, patchedInputResult(t, warm, files), expected)
}

func publishInputFrameWithoutRecipe(t *testing.T, l *Loader, name string) {
	t.Helper()
	owner, state := l.Options.Cache, l.inputCaches[l.SourceRoot]
	origin, url := state.input.OriginPath(state.input.LocalPath), state.input.URL
	previous := manifest.Fingerprint(struct {
		Version             int
		Domain, Origin, URL string
	}{Version: 4, Domain: "loader-inputs", Origin: origin, URL: url})
	frame := inputFrame{Files: []cachedInput{{
		Name: name, Builder: filepath.Base(name), Digest: state.digests[name],
		Body: l.documents.cache[filepath.Dir(filepath.Join(l.SourceRoot, name))].body,
	}}}
	assert.NoError(t, state.certify(t.Context(), &frame))
	content, err := frame.prepare(t.Context())
	assert.NoError(t, err)
	_, identity := cache.InputFrameIdentity(origin, url, content)
	binding := inputFrameContent{Origin: origin, URL: url, Content: identity}
	publication, err := content.PreparePublication(t.Context(), cache.FramePublicationInput{
		Owner: manifest.BootstrapSourceID, Content: binding, Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	})
	assert.NoError(t, err)
	assert.NoError(t, cache.RecordInventory(t.Context(), owner, cache.AcceptedInventory[*cache.FramePublication]{
		Key: previous, Acceptance: publication.Identity(), Content: publication,
	}))
	generation, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: previous, Render: cache.RenderAccepted},
		&cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	var wire struct {
		Files []map[string]json.RawMessage
	}
	stored, err := cache.OpenFrame(t.Context(), "loader-inputs", generation, &wire)
	assert.NoError(t, err)
	assert.FatalIf(t, len(wire.Files) != 1, "version 4 frame files: %v", wire.Files)
	_, hasRecipe := wire.Files[0]["Recipe"]
	assert.Equal(t, hasRecipe, false)
	_, complete := stored.History(manifest.BootstrapSourceID, binding)
	assert.Equal(t, complete, true)
}

func patchedInputResult(t *testing.T, l *Loader, files fstest.MapFS) string {
	t.Helper()
	input := l.documents.artifacts[l.SourceRoot]
	renderFS := filesys.MakeFsInMemory()
	for name, file := range files {
		data, err := fs.ReadFile(input.FS, name)
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(file.Data))
		target := filepath.Join(l.SourceRoot, name)
		assert.NoError(t, renderFS.MkdirAll(filepath.Dir(target)))
		assert.NoError(t, renderFS.WriteFile(target, data))
	}
	const recipe = recipeTransformers | recipeOpaquePluginInputs
	app := filepath.Join(l.SourceRoot, "apps")
	assert.Equal(t, l.documents.read(app).recipe, recipe)
	packages, err := l.EnclosingPackages(t.Context(), input, "apps/leaf",
		manifest.NamedResource{Kind: "ConfigMap", Name: "patched"})
	assert.NoError(t, err)
	assert.FatalIf(t, !slices.Equal(packages, []string{"apps"}), "enclosing packages: %v", packages)
	occurrences, err := l.Occurrences(t.Context(), app, "")
	assert.NoError(t, err)
	assert.FatalIf(t, len(occurrences) != 1, "authored occurrences: %v", occurrences)
	assert.Equal(t, occurrences[0].RequiresBuild, true)

	krusty.BuildMutex.Lock()
	result, err := krusty.Build(renderFS, filepath.Join(l.SourceRoot, packages[0]), false)
	krusty.BuildMutex.Unlock()
	assert.NoError(t, err)
	data, err := result.AsYaml()
	assert.NoError(t, err)
	return string(data)
}

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
	frame := inputFrame{Files: []cachedInput{{
		Name: "object.yaml", Projection: &cachedProjection{},
		Objects: []cachedOccurrence{{Ordinal: 3, payload: &inputPayload{data: payload}}},
	}}}
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
		{name: "negative-declaration-block", index: `{"DeclarationBytes":-1}`},
		{name: "missing-declaration-block", index: `{"DeclarationBytes":1}`},
		{name: "negative-declaration-offset", index: `{"Files":[{"DeclarationOffset":-1,"DeclarationSize":1}]}`},
		{name: "negative-declaration-size", index: `{"Files":[{"DeclarationSize":-1}]}`},
		{name: "declaration-past-block", index: `{"Files":[{"DeclarationOffset":1,"DeclarationSize":1}]}`},
		{name: "declaration-exceeds-block", index: `{"Files":[{"DeclarationSize":1}]}`},
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
	persistent := cache.RenderPersistenceDisabledReason() == ""
	assert.Equal(t, owner.CanPersistRenders(), persistent)
	files := fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [object.yaml]\n")},
		"object.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object}\n")},
	}
	first := cachedFixtureLoader(t, root, files, owner)
	assert.NoError(t, first.PersistInputs(t.Context()))
	input := first.documents.artifacts[root]
	lookup, _ := cache.InputFrameIdentity(input.OriginPath(root), input.URL, nil)
	previous, previousDescriptor, _, err := owner.Blob(t.Context(), lookup, &cache.Policy{MaxAge: cache.Forever}, nil)
	var previousBytes []byte
	if persistent {
		assert.NoError(t, err)
		previousBytes, err = fs.ReadFile(previous, cache.FileName(previous))
		assert.NoError(t, err)
	} else {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, previous == nil, true)
	}
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
	if persistent {
		priorFiles, priorLoads, priorWalks := maps.Clone(state.files), maps.Clone(state.loads), maps.Clone(state.walks)
		frame, frameErr := l.cacheSourceInputs(state)
		assert.NoError(t, frameErr)
		assert.FatalIf(t, frame == nil, "changed builder supplied no authored facts")
		content, prepareErr := frame.prepare(t.Context())
		assert.NoError(t, prepareErr)
		assert.Equal(t, content == nil, true)
		assert.NoError(t, l.PersistInputs(t.Context()))
		assert.Equal(t, maps.Equal(state.files, priorFiles), true)
		assert.Equal(t, maps.Equal(state.loads, priorLoads), true)
		assert.Equal(t, maps.Equal(state.walks, priorWalks), true)
	} else {
		assert.Equal(t, state == nil, true)
		assert.NoError(t, l.PersistInputs(t.Context()))
	}
	assert.Diff(t, l.Index, cold.Index)
	occurrence, known := l.Occurrence(manifest.NamedResource{Kind: "ConfigMap", Name: "object"})
	assert.Equal(t, known, true)
	child, err := l.OccurrenceObject(t.Context(), &occurrence, nil)
	assert.NoError(t, err)
	assert.Diff(t, child.Doc, map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "object"},
	})

	retained, retainedDescriptor, _, err := owner.Blob(t.Context(), lookup, &cache.Policy{MaxAge: cache.Forever}, nil)
	if persistent {
		assert.NoError(t, err)
		assert.Equal(t, retainedDescriptor.Digest, previousDescriptor.Digest)
		retainedBytes, readErr := fs.ReadFile(retained, cache.FileName(retained))
		assert.NoError(t, readErr)
		assert.Equal(t, bytes.Equal(retainedBytes, previousBytes), true)
	} else {
		assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
		assert.Equal(t, retained == nil, true)
		assert.Equal(t, retainedDescriptor.Digest, previousDescriptor.Digest)
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

func TestInputCache_FileDeclarations(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		"",
		"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: z}\n---\napiVersion: example.io/v1\nkind: Widget\nmetadata: {name: a}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '${NAME}'}\n",
	} {
		t.Run(strconv.Itoa(len(body)), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			reader := readerForTest(root)
			reader.compact = true
			reader.views[root] = fstest.MapFS{"input.yaml": {Data: []byte(body)}}
			file := filepath.Join(root, "input.yaml")
			_, err := reader.censusFile(t.Context(), file, manifest.ParseDocOptions{})
			assert.NoError(t, err)
			doc := reader.cache[file]
			saved, known, err := cacheInput("input.yaml", "digest", &doc)
			assert.NoError(t, err)
			assert.Equal(t, known, true)
			assert.Equal(t, saved.declarations == &doc.declarations.payload, true)
			frame := inputFrame{Files: []cachedInput{saved, {
				Name: "empty.yaml", declarations: &inputPayload{
					observation: &fileObservation{Present: true, Complete: true},
				},
			}}}
			data := encodeInputFrame(t, &frame)
			owner, err := cache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			generation, _, _, err := owner.Blob(t.Context(), cache.BlobKey{Content: "declarations"}, &cache.Policy{},
				func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
					_, writeErr := out.Write(data)
					return cache.ProducerResult{}, writeErr
				})
			assert.NoError(t, err)
			restoredFrame, err := readInputFrame(t.Context(), generation)
			assert.NoError(t, err)
			restored := restoredFrame.Files[0]
			var encoded cachedFileDeclarations
			assert.NoError(t, json.Unmarshal(restored.declarations.data, &encoded))
			assert.Equal(t, slices.IsSortedFunc(encoded.Objects, func(a, b cachedDeclaration) int {
				return a.Identity.Compare(b.Identity)
			}), true)
			var empty cachedFileDeclarations
			assert.NoError(t, json.Unmarshal(restoredFrame.Files[1].declarations.data, &empty))
			assert.Equal(t, empty.Present && empty.Complete, true)
			assert.Equal(t, len(empty.Objects), 0)
			reader.cache[file] = document{deferred: &restored}
			reader.restoreCachedFile(file)
			pending := reader.cache[file]
			assert.Equal(t, pending.declarations == nil, true)
			assert.Equal(t, pending.declarationsData != nil, true)
			assert.NoError(t, pending.restoreDeclarations())
			assert.Equal(t, pending.declarationsData == nil, true)
			reader.cache[file] = pending
			observation := reader.cache[file].declarations
			assert.Diff(t, observation.FileDeclarations, doc.declarations.FileDeclarations)
			reader.restoreCachedFile(file)
			assert.Equal(t, reader.cache[file].declarations == observation, true)
			assert.NoError(t, pending.restoreDeclarations())
			assert.Equal(t, pending.declarations == observation, true)
		})
	}
}
