package loader

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
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
	file := filepath.Join(root, "shared", "objects.yaml")
	snapshot := inputs.sources[root].documents[file]
	ordinals := make([]int, 0, len(snapshot.objects))
	versions := make([]string, 0, len(snapshot.objects))
	for _, object := range snapshot.objects {
		ordinals = append(ordinals, object.outcome.Ordinal)
		versions = append(versions, object.version)
	}
	assert.Diff(t, ordinals, []int{1, 2, 3})
	assert.Diff(t, versions, []string{"kustomize.toolkit.fluxcd.io/v1", "example.test/v2", "helm.toolkit.fluxcd.io/v2"})
	assert.FatalIf(t, !errors.Is(snapshot.objects[2].outcome.Err, manifest.ErrInput) || snapshot.objects[2].Obj != nil,
		"failed declaration was not retained: %+v", snapshot.objects[2])

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
	load := func(input *store.SourceArtifact, inputs *ParsedInputs) *Loader {
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		l.Options.ParsedInputs = inputs
		_, err = l.Load(t.Context(), root)
		assert.NoError(t, err)
		return l
	}
	planner := load(input, nil)
	inputs := planner.ParsedInputs()
	file := filepath.Join(root, "unreadable.yaml")
	snapshot := inputs.sources[root].documents[file].objects
	assert.FatalIf(
		t,
		len(snapshot) != 1 || !errors.Is(snapshot[0].outcome.Err, fs.ErrPermission) || snapshot[0].Obj != nil ||
			snapshot[0].Doc != nil ||
			snapshot[0].outcome.Document != nil,
		"snapshot lost open failure: %+v",
		snapshot,
	)

	planner.documents.cache[file].objects[0].outcome.Err = errors.New("planner mutation")
	l := load(input, inputs)
	obj := l.documents.cache[filepath.Join(root, "secret.yaml")].objects[0].Obj.(*manifest.Secret)
	assert.Equal(t, obj.HasKeys(), false)
	failed := l.documents.cache[filepath.Join(root, "broken.yaml")].objects
	assert.FatalIf(
		t,
		len(failed) != 1 || failed[0].outcome.Ordinal != 1 || !errors.Is(failed[0].outcome.Err, manifest.ErrInput) ||
			failed[0].Obj != nil,
		"snapshot lost failed read: %+v",
		failed,
	)

	openFailure := l.documents.cache[file].objects
	assert.FatalIf(t, len(openFailure) != 1 || !errors.Is(openFailure[0].outcome.Err, fs.ErrPermission) ||
		openFailure[0].outcome.Ordinal != 0 ||
		openFailure[0].Obj != nil ||
		openFailure[0].Doc != nil ||
		!errors.Is(snapshot[0].outcome.Err, fs.ErrPermission),
		"restoration lost immutable open failure: %+v", openFailure)
	assert.FatalIf(
		t,
		!errors.Is(openFailure[0].outcome.Err, snapshot[0].outcome.Err),
		"%s",
		"open failure was not reused from parsed inputs",
	)

	if empty := l.documents.cache[filepath.Join(root, "empty.yaml")].objects; empty == nil || len(empty) != 0 {
		t.Fatalf("snapshot lost completed empty read: %+v", empty)
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
	l = load(&store.SourceArtifact{LocalPath: root, FS: other}, inputs)
	assert.Equal(t, l.documents.cache[filepath.Join(root, "secret.yaml")].objects[0].Obj.Named().Name, "authoritative")
	if failures := retainedErrorsForTest(l); len(failures) != 0 {
		t.Fatalf("replaced source retained obsolete failures: %v", failures)
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
	t.Run("declaration fingerprints", func(t *testing.T) {
		t.Parallel()
		testParsedInputsCompactPhysicalFactsCase1(t)
	})
	root := t.TempDir()
	files := fstest.MapFS{
		"left/kustomization.yaml":   {Data: []byte("namespace: left\nnamePrefix: left-\nresources: [../shared]\n")},
		"right/kustomization.yaml":  {Data: []byte("namespace: right\nnamePrefix: right-\nresources: [../shared]\n")},
		"shared/kustomization.yaml": {Data: []byte("resources: [objects.yaml]\n")},
		"shared/objects.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data}\ndata: {KEY: value, HIDDEN: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
			),
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
	newLoader := func(input *store.SourceArtifact, parsed *ParsedInputs, name string) *Loader {
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		l.AdmitDeclarations(
			[]store.Declaration{
				{
					Obj: &manifest.RawObject{
						Kind:       "Widget",
						APIVersion: "example.io/v1",
						Name:       name,
						Namespace:  "apps",
					},
					Origin: manifest.SuppliedDeclaration,
				},
			},
		)
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.ParsedInputs = parsed
		return l
	}
	planner := newLoader(input, nil, "planner")
	_, err := planner.Load(t.Context(), root)
	assert.NoError(t, err)
	for _, side := range []string{"left", "right"} {
		_, err = planner.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, err)
	}
	parsed := planner.ParsedInputs()
	compact := planner.documents.cache[filepath.Join(root, "shared", "objects.yaml")].objects
	assert.Equal(t, len(compact), 1)
	assert.Equal(t, compact[0].Doc == nil && compact[0].outcome.Document == nil, true)
	assert.Equal(t, len(compact[0].Warnings), 1)
	assert.Diff(t, compact[0].Warnings[0].Detail, []string{"HIDDEN"})
	assert.Equal(t, len(planner.Store.Warnings()), 0)
	before := map[string]int64{}
	for name, count := range reads {
		before[name] = count.Load()
	}
	directoryCount := directories.Load()
	for _, side := range []string{"left", "right"} {
		l := newLoader(input, parsed, side)
		_, err = l.Load(t.Context(), root)
		assert.NoError(t, err)
		occurrences, errInner := l.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, errInner)
		assert.Equal(t, len(occurrences), 1)
		assert.Equal(t, occurrences[0].Identity.Namespace, side)
		assert.Equal(t, occurrences[0].Identity.Name, side+"-data")
		id := manifest.NamedResource{Group: "example.io", Kind: "Widget", Name: side, Namespace: "apps"}
		record, found := l.Index.Lookup(id)
		assert.Equal(t, found, true)
		assert.Equal(t, record.Origin, manifest.SuppliedDeclaration)
		assert.Equal(t, record.Path, "")
		assert.Equal(t, l.Store.Policy(id).Root, false)
		_, leaked := l.Index.Lookup(
			manifest.NamedResource{Group: "example.io", Kind: "Widget", Name: "planner", Namespace: "apps"},
		)
		assert.Equal(t, leaked, false)
		physical, found := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data"})
		assert.Equal(t, found, true)
		assert.Equal(t, physical.Declaration == nil, true)
		assert.Equal(t, physical.Path, filepath.Join(root, "shared", "objects.yaml"))
		assert.Equal(t, physical.Authored, manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data"})
		assert.Equal(t, physical.Origin, manifest.RepositoryDeclaration)
		assert.Equal(
			t,
			l.Store.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data"}) == nil,
			true,
		)
		assert.Equal(t, len(l.Store.Objects()), 1)
		l.Index.Records = nil
	}
	assert.Equal(t, directories.Load(), directoryCount)
	for name, count := range reads {
		assert.Equal(t, count.Load(), before[name])
	}
	assert.Equal(t, len(planner.Index.Records), 2)

	for _, side := range []string{"left", "right"} {
		l := newLoader(input, parsed, side)
		_, err = l.Load(t.Context(), root)
		assert.NoError(t, err)
		occurrences, errInner := l.Occurrences(t.Context(), filepath.Join(root, side), "")
		assert.NoError(t, errInner)
		child, errInner := l.exactObject(t.Context(), new(occurrences[0]), nil)
		assert.NoError(t, errInner)
		assert.Equal(t, len(child.Warnings), 1)
		assert.Equal(t, child.Warnings[0].Resource, occurrences[0].Identity)
		assert.Diff(t, child.Warnings[0].Detail, []string{"HIDDEN"})
		result1, errInner := child.Obj.(manifest.Data).Value("HIDDEN")
		value, present := result1.Value, result1.Found
		assert.NoError(t, errInner)
		assert.Equal(t, present, false)
		assert.Equal(t, value, "")
		result2, errInner := child.Obj.(manifest.Data).Value("KEY")
		value, present = result2.Value, result2.Found
		assert.NoError(t, errInner)
		assert.Equal(t, present, true)
		assert.Equal(t, value, "value")
		assert.Equal(t, len(l.Store.Warnings()), 0)
	}

	other := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"only.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other}\n")},
	}}
	l := newLoader(other, parsed, "other")
	_, err = l.Load(t.Context(), root)
	assert.NoError(t, err)
	assert.Equal(t, len(l.Index.Records), 2)
	if _, ok := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "other"}); !ok {
		t.Fatal("borrowed facts from another source view")
	}
}

func testParsedInputsCompactPhysicalFactsCase1(t *testing.T) {
	t.Helper()

	for _, conflict := range []bool{false, true} {
		t.Run(strconv.FormatBool(conflict), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: data, namespace: apps, labels: {kustomize.toolkit.fluxcd.io/name: owner}}\ndata: {KEY: 'ENC[AES256_GCM,data:original,iv:iv,tag:tag,type:str]'}\n"
			documents, err := manifest.DecodeDocs(strings.NewReader(body), manifest.IncidentalDocuments)
			assert.NoError(t, err)
			want := manifest.Fingerprint(manifest.WithoutOwnershipLabels(documents[0].Document))
			if conflict {
				body += "---\n" + strings.ReplaceAll(body, "data:original", "data:different") + "---\n" + body
			}
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{"objects.yaml": {Data: []byte(body)}}},
			)
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection.Changed = true
			_, err = l.Load(t.Context(), root)
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "data", Namespace: "apps"}
			record, found := l.Index.Lookup(id)
			assert.Equal(t, found, true)
			if conflict {
				want = ""
			}
			assert.Equal(t, record.Declared, want)
			assert.NoError(
				t,
				(&documentReader{compact: true}).prepareStream(
					strings.NewReader(body),
					manifest.ParseDocOptions{},
					func(object parsedObject) {
						assert.NoError(t, object.outcome.Err)
						_, present := object.Doc["data"].(map[string]any)["KEY"]
						assert.Equal(t, present, false)
						assert.FatalIf(t, object.declared == "" ||
							object.declared == manifest.Fingerprint(
								manifest.WithoutOwnershipLabels(object.Doc),
							), "%s", "preparation lost the original declaration fingerprint")
					},
				),
			)
		})
	}
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
			for i := range occurrences {
				occurrence := occurrences[i]
				assert.Equal(t, occurrence.Identity.Namespace, side)
				assert.Equal(t, occurrence.Identity.Name, side+"-"+occurrence.Authored.Name)
			}
			doc := l.documents.cache[(*testCaseInput.file)].objects[0].Doc
			assert.Equal(t, doc["metadata"].(map[string]any)["name"], any("owner"))
			doc["metadata"].(map[string]any)["name"] = side
			assert.Equal(t, len(l.Index.Records), 1)
			failure := l.documents.cache[(*testCaseInput.file)].objects[2]

			assert.FatalIf(
				t,
				failure.outcome.Ordinal != 3 || !errors.Is(failure.outcome.Err, testCaseInput.snapshot.objects[2].outcome.Err) ||
					failure.Obj != nil,
				"restoration lost failed declaration: %+v",
				failure,
			)
		})
	}
}
