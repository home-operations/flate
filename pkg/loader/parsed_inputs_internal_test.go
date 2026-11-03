package loader

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestParsedInputs_ContentAndAdmissionIsolation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"left/kustomization.yaml":   {Data: []byte("namespace: left\nnamePrefix: left-\nresources: [../shared]\n")},
		"right/kustomization.yaml":  {Data: []byte("namespace: right\nnamePrefix: right-\nresources: [../shared]\n")},
		"shared/kustomization.yaml": {Data: []byte("resources: [objects.yaml]\n")},
		"shared/objects.yaml": {
			Data: []byte(
				"plain: skipped\n---\napiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner}\nspec: {path: ./target}\n---\napiVersion: example.test/v2\nkind: Widget\nmetadata: {name: raw}\n---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: invalid}\nspec: {interval: []}\n",
			),
		},
	}
	reads := map[string]*atomic.Int64{}
	for name := range files {
		reads[name] = &atomic.Int64{}
	}
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: parsedReadFS{FS: files, reads: reads}},
	)
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: view}
	newLoader := func(inputs *ParsedInputs) *Loader {
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		l.SourceRoot, l.Options.ParsedInputs = root, inputs
		return l
	}
	planner := newLoader(nil)
	for _, side := range []string{"left", "right"} {
		_, err := planner.Load(t.Context(), filepath.Join(root, side))
		assert.NoError(t, err)
	}
	assert.Equal(t, planner.facts, (*ParsedInputs)(nil))
	assert.NoError(t, planner.FinalizeIndex(t.Context(), root, false))
	assert.Equal(t, len(planner.Index.Visibility), 0)
	inputs := planner.ParsedInputs()
	file := filepath.Join(root, "shared/objects.yaml")
	snapshot := inputs.sources[root].documents[file]
	ordinals := make([]int, 0, len(snapshot.objects))
	versions := make([]string, 0, len(snapshot.objects))
	for _, object := range snapshot.objects {
		ordinals = append(ordinals, object.outcome.Ordinal)
		versions = append(versions, object.version)
	}
	assert.Diff(t, ordinals, []int{1, 2, 3})
	assert.Diff(t, versions, []string{"kustomize.toolkit.fluxcd.io/v1", "example.test/v2", "helm.toolkit.fluxcd.io/v2"})
	if !errors.Is(snapshot.objects[2].outcome.Err, manifest.ErrInput) || snapshot.objects[2].Obj != nil {
		t.Fatalf("failed declaration was not retained: %+v", snapshot.objects[2])
	}
	before := map[string]int64{}
	for name, count := range reads {
		before[name] = count.Load()
	}
	planner.documents.cache[file].objects[0].Doc["metadata"].(map[string]any)["name"] = "planner-mutation"
	t.Cleanup(func() {
		for name, count := range reads {
			assert.Equal(t, count.Load(), before[name])
		}
		assert.Equal(t, snapshot.objects[0].Doc["metadata"].(map[string]any)["name"], any("owner"))
	})
	t.Run("independent entries", func(t *testing.T) {
		t.Parallel()
		testParsedInputsContentAndAdmissionIsolationIndependentEntries(
			t,
			&testParsedInputsContentAndAdmissionIsolationIndependentEntriesInputs{
				root:      &root,
				newLoader: &newLoader,
				inputs:    &inputs,
				file:      &file,
				snapshot:  &snapshot,
			},
		)
	},
	)
}

func TestParsedInputs_SourceAndParsePolicy(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: secret}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n"
	files := fstest.MapFS{
		"secret.yaml":     {Data: []byte(secret)},
		"broken.yaml":     {Data: []byte("null\n---\nkey: first\nkey: duplicate\n")},
		"empty.yaml":      {Data: []byte{}},
		"unreadable.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: inaccessible}\n")},
	}
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: unreadableInputFS{FS: files, denied: "unreadable.yaml"}},
	)
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: view}
	load := func(input *store.SourceArtifact, inputs *ParsedInputs, wipe bool) *Loader {
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		l.Options.WipeSecrets, l.Options.ParsedInputs = wipe, inputs
		_, loadErr := l.Load(t.Context(), root)
		assert.NoError(t, loadErr)
		return l
	}
	planner := load(input, nil, true)
	inputs := planner.ParsedInputs()
	file := filepath.Join(root, "unreadable.yaml")
	snapshot := inputs.sources[root].documents[file].objects
	{
		assert.FatalIf(
			t,
			len(snapshot) != 1 || !errors.Is(snapshot[0].outcome.Err, fs.ErrPermission) || snapshot[0].Obj != nil ||
				snapshot[0].Doc != nil ||
				snapshot[0].outcome.Document != nil,
			"snapshot lost open failure: %+v",
			snapshot,
		)
	}
	planner.documents.cache[file].objects[0].outcome.Err = errors.New("planner mutation")
	for _, wipe := range []bool{false, true} {
		l := load(input, inputs, wipe)
		obj := l.documents.cache[filepath.Join(root, "secret.yaml")].objects[0].Obj.(*manifest.Secret)
		assert.Equal(t, obj.HasPlaceholder(), wipe)
		failed := l.documents.cache[filepath.Join(root, "broken.yaml")].objects
		{
			assert.FatalIf(
				t,
				len(failed) != 1 || failed[0].outcome.Ordinal != 1 || !errors.Is(failed[0].outcome.Err, manifest.ErrInput) ||
					failed[0].Obj != nil,
				"snapshot lost failed read: %+v",
				failed,
			)
		}
		openFailure := l.documents.cache[file].objects
		{
			assert.FatalIf(t, len(openFailure) != 1 || !errors.Is(openFailure[0].outcome.Err, fs.ErrPermission) ||
				openFailure[0].outcome.Ordinal != 0 ||
				openFailure[0].Obj != nil ||
				openFailure[0].Doc != nil ||
				!errors.Is(
					snapshot[0].outcome.Err,
					fs.ErrPermission,
				), "restoration lost immutable open failure: %+v", openFailure)
		}
		{
			assert.FatalIf(
				t,
				wipe && !errors.Is(openFailure[0].outcome.Err, snapshot[0].outcome.Err),
				"%s",
				"open failure was not reused from parsed inputs",
			)
		}
		{
			empty := l.documents.cache[filepath.Join(root, "empty.yaml")].objects
			assert.FatalIf(t, empty == nil || len(empty) != 0, "snapshot lost completed empty read: %+v", empty)
		}
	}
	other, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
			"secret.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: Secret\nmetadata: {name: authoritative}\nstringData: {KEY: value}\n",
				),
			},
		}},
	)
	assert.NoError(t, err)
	l := load(&store.SourceArtifact{LocalPath: root, FS: other}, inputs, true)
	assert.Equal(t, l.documents.cache[filepath.Join(root, "secret.yaml")].objects[0].Obj.Named().Name, "authoritative")
	{
		failures := retainedErrorsForTest(l)
		assert.FatalIf(t, len(failures) != 0, "replaced source retained obsolete failures: %v", failures)
	}
}

type parsedReadFS struct {
	fs.FS

	reads       map[string]*atomic.Int64
	directories *atomic.Int64
}

func (f parsedReadFS) Open(name string) (fs.File, error) {
	if count := f.reads[name]; count != nil {
		count.Add(1)
	}
	return f.FS.Open(name)
}

func (f parsedReadFS) Stat(name string) (fs.FileInfo, error) { return fs.Stat(f.FS, name) }

func (f parsedReadFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.directories != nil {
		f.directories.Add(1)
	}
	return fs.ReadDir(f.FS, name)
}

func TestParsedInputs_CompactPhysicalFacts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"left/kustomization.yaml":   {Data: []byte("namespace: left\nnamePrefix: left-\nresources: [../shared]\n")},
		"right/kustomization.yaml":  {Data: []byte("namespace: right\nnamePrefix: right-\nresources: [../shared]\n")},
		"shared/kustomization.yaml": {Data: []byte("resources: [objects.yaml]\n")},
		"shared/objects.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\ndata: {KEY: value}\n"),
		},
	}
	reads := map[string]*atomic.Int64{}
	for name := range files {
		reads[name] = &atomic.Int64{}
	}
	var directories atomic.Int64
	input := &store.SourceArtifact{
		LocalPath: root,
		FS:        parsedReadFS{FS: files, reads: reads, directories: &directories},
	}
	newLoader := func(input *store.SourceArtifact, parsed *ParsedInputs) *Loader {
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.ParsedInputs = parsed
		return l
	}
	planner := newLoader(input, nil)
	_, err := planner.Load(t.Context(), root)
	assert.NoError(t, err)
	for _, side := range []string{"left", "right"} {
		_, occurrencesErr := planner.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, occurrencesErr)
	}
	parsed := planner.ParsedInputs()
	before := map[string]int64{}
	for name, count := range reads {
		before[name] = count.Load()
	}
	directoryCount := directories.Load()
	for _, side := range []string{"left", "right"} {
		l := newLoader(input, parsed)
		_, loadErr2 := l.Load(t.Context(), root)
		assert.NoError(t, loadErr2)
		occurrences, loadErr2 := l.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, loadErr2)
		assert.Equal(t, len(occurrences), 1)
		assert.Equal(t, occurrences[0].Identity.Namespace, side)
		assert.Equal(t, occurrences[0].Identity.Name, side+"-data")
		assert.Equal(t, len(l.Store.Objects()), 0)
		l.Index.Records = nil
	}
	assert.Equal(t, directories.Load(), directoryCount)
	for name, count := range reads {
		assert.Equal(t, count.Load(), before[name])
	}
	assert.Equal(t, len(planner.Index.Records), 1)

	other := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"only.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other}\n")},
	}}
	l := newLoader(other, parsed)
	_, err = l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.Equal(t, len(l.Index.Records), 1)
	{
		_, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "other"})
		assert.FatalIf(t, !ok, "%s", "borrowed facts from another source view")
	}

	visibilityRoot := t.TempDir()
	data := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\n")
	original := &store.SourceArtifact{
		LocalPath: visibilityRoot,
		FS:        fstest.MapFS{"left/a.yaml": {Data: data}, "right/b.yaml": {Data: data}},
	}
	physical := newLoader(original, nil)
	for _, dir := range []string{"left", "right"} {
		_, loadErr3 := physical.Load(t.Context(), filepath.Join(visibilityRoot, dir))
		assert.NoError(t, loadErr3)
	}
	assert.NoError(t, physical.FinalizeIndex(t.Context(), visibilityRoot, false))
	observations := slices.Clone(physical.Index.Visibility)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	assert.Equal(t, len(observations), 2)
	for i, dir := range []string{"left", "right"} {
		assert.Equal(t, observations[i].Complete, true)
		assert.Equal(t, observations[i].Root, dir)
		assert.Equal(t, len(observations[i].Inputs[id]), 1)
	}
	partial := newLoader(original, physical.ParsedInputs())
	_, err = partial.Load(t.Context(), filepath.Join(visibilityRoot, "left"))
	assert.NoError(t, err)
	assert.NoError(t, partial.FinalizeIndex(t.Context(), visibilityRoot, false))
	assert.Diff(t, partial.Index.Visibility, observations[:1])

	canceled := newLoader(original, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = canceled.Load(ctx, filepath.Join(visibilityRoot, "left"))
	{
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "canceled load: %v", err)
	}
	assert.NoError(t, canceled.FinalizeIndex(t.Context(), visibilityRoot, false))
	assert.Equal(t, len(canceled.Index.Visibility), 1)
	assert.Equal(t, canceled.Index.Visibility[0].Complete, false)
	incomplete := canceled.ParsedInputs()
	canceledVisibility := slices.Clone(canceled.Index.Visibility)
	recovered := newLoader(original, incomplete)
	_, err = recovered.Load(t.Context(), filepath.Join(visibilityRoot, "left"))
	assert.NoError(t, err)
	assert.Equal(t, len(recovered.Index.Records), 1)
	_, found := recovered.Index.Lookup(id)
	assert.Equal(t, found, true)
	assert.NoError(t, recovered.FinalizeIndex(t.Context(), visibilityRoot, false))
	assert.Diff(t, recovered.Index.Visibility, observations[:1])
	canceledFacts := incomplete.sources[visibilityRoot].loads[loadContext{path: filepath.Join(visibilityRoot, "left")}]
	assert.Equal(t, canceledFacts.complete, false)
	assert.Equal(t, canceledFacts.observed, true)
	assert.Equal(t, len(canceledFacts.files), 0)
	assert.Equal(t, len(canceledFacts.directories), 0)
	assert.Diff(t, canceled.Index.Visibility, canceledVisibility)

	replayed := newLoader(original, physical.ParsedInputs())
	for _, dir := range []string{"left", "right"} {
		_, loadErr4 := replayed.Load(t.Context(), filepath.Join(visibilityRoot, dir))
		assert.NoError(t, loadErr4)
	}
	assert.NoError(t, replayed.FinalizeIndex(t.Context(), visibilityRoot, false))
	assert.Diff(t, replayed.Index.Visibility, observations)
	replayed.Store.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{
			LocalPath: visibilityRoot,
			FS:        fstest.MapFS{".krmignore": {Data: []byte("*.yaml\n")}, "hidden.yaml": {Data: data}},
		},
	)
	assert.NoError(t, replayed.FinalizeIndex(t.Context(), visibilityRoot, false))
	assert.Equal(t, len(replayed.Index.Visibility), 0)
	_, err = replayed.Load(t.Context(), visibilityRoot)
	assert.NoError(t, err)
	assert.NoError(t, replayed.FinalizeIndex(t.Context(), visibilityRoot, true))
	assert.Diff(
		t,
		replayed.Index.Visibility,
		[]manifest.InputVisibility{{Source: manifest.BootstrapSourceID, Root: ".", Complete: true}},
	)
	assert.Diff(t, physical.Index.Visibility, observations)
	assert.Diff(t, observations[0].Inputs[id], []string{"left/a.yaml"})
	assert.Diff(t, observations[1].Inputs[id], []string{"right/b.yaml"})
}

type testParsedInputsContentAndAdmissionIsolationIndependentEntriesInputs struct {
	root      *string
	newLoader *func(inputs *ParsedInputs) *Loader
	inputs    **ParsedInputs
	file      *string
	snapshot  *parsedDocument
}

func testParsedInputsContentAndAdmissionIsolationIndependentEntries(
	t *testing.T,
	testCaseInput *testParsedInputsContentAndAdmissionIsolationIndependentEntriesInputs,
) {
	t.Helper()

	for _, side := range []string{"left", "right"} {
		t.Run(side, func(t *testing.T) {
			t.Parallel()
			l := (*testCaseInput.newLoader)((*testCaseInput.inputs))
			path := filepath.Join((*testCaseInput.root), side)
			n, err := l.Load(t.Context(), path)
			assert.NoError(t, err)
			assert.Equal(t, n, 1)
			occurrences, err := l.Occurrences(t.Context(), path, "")
			assert.NoError(t, err)
			assert.Equal(t, len(occurrences), 2)
			for _, occurrence := range occurrences {
				assert.Equal(t, occurrence.Identity.Namespace, side)
				assert.Equal(t, occurrence.Identity.Name, side+"-"+occurrence.Authored.Name)
			}
			doc := l.documents.cache[(*testCaseInput.file)].objects[0].Doc
			assert.Equal(t, doc["metadata"].(map[string]any)["name"], any("owner"))
			doc["metadata"].(map[string]any)["name"] = side
			assert.Equal(t, len(l.Index.Records), 1)
			failure := l.documents.cache[(*testCaseInput.file)].objects[2]
			{
				assert.FatalIf(
					t,
					failure.outcome.Ordinal != 3 || !errors.Is(failure.outcome.Err, testCaseInput.snapshot.objects[2].outcome.Err) ||
						failure.Obj != nil,
					"restoration lost failed declaration: %+v",
					failure,
				)
			}
		})
	}
}
