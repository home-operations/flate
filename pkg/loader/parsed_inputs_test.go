package loader

import (
	"io/fs"
	"path/filepath"
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
		"shared/objects.yaml":       {Data: []byte("plain: skipped\n---\napiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner}\nspec: {path: ./target}\n---\napiVersion: example.test/v2\nkind: Widget\nmetadata: {name: raw}\n")},
	}
	reads := map[string]*atomic.Int64{}
	for name := range files {
		reads[name] = &atomic.Int64{}
	}
	view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: parsedReadFS{FS: files, reads: reads}})
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
	inputs := planner.ParsedInputs()
	file := filepath.Join(root, "shared/objects.yaml")
	snapshot := inputs.sources[root].documents[file]
	var ordinals []int
	var versions []string
	for _, object := range snapshot.objects {
		ordinals = append(ordinals, object.ordinal)
		versions = append(versions, object.version)
	}
	assert.Diff(t, ordinals, []int{1, 2})
	assert.Diff(t, versions, []string{"kustomize.toolkit.fluxcd.io/v1", "example.test/v2"})
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
		for _, side := range []string{"left", "right"} {
			t.Run(side, func(t *testing.T) {
				t.Parallel()
				l := newLoader(inputs)
				path := filepath.Join(root, side)
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
				doc := l.documents.cache[file].objects[0].Doc
				assert.Equal(t, doc["metadata"].(map[string]any)["name"], any("owner"))
				doc["metadata"].(map[string]any)["name"] = side
				assert.Equal(t, len(l.Index.Records), 1)
			})
		}
	})
}

func TestParsedInputs_SourceAndParsePolicy(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: secret}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n"
	files := fstest.MapFS{"secret.yaml": {Data: []byte(secret)}}
	view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: files})
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: view}
	load := func(input *store.SourceArtifact, inputs *ParsedInputs, wipe bool) *Loader {
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID, input)
		l := New(st)
		l.Options.WipeSecrets, l.Options.ParsedInputs = wipe, inputs
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
		return l
	}
	planner := load(input, nil, true)
	inputs := planner.ParsedInputs()
	for _, wipe := range []bool{false, true} {
		l := load(input, inputs, wipe)
		obj := l.documents.cache[filepath.Join(root, "secret.yaml")].objects[0].Obj.(*manifest.Secret)
		assert.Equal(t, obj.HasPlaceholder(), wipe)
	}
	other, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
		"secret.yaml": {Data: []byte("apiVersion: v1\nkind: Secret\nmetadata: {name: authoritative}\nstringData: {KEY: value}\n")},
	}})
	assert.NoError(t, err)
	l := load(&store.SourceArtifact{LocalPath: root, FS: other}, inputs, true)
	assert.Equal(t, l.documents.cache[filepath.Join(root, "secret.yaml")].objects[0].Obj.Named().Name, "authoritative")
}

type parsedReadFS struct {
	fs.FS
	reads map[string]*atomic.Int64
}

func (f parsedReadFS) Open(name string) (fs.File, error) {
	if count := f.reads[name]; count != nil {
		count.Add(1)
	}
	return f.FS.Open(name)
}

func (f parsedReadFS) Stat(name string) (fs.FileInfo, error) { return fs.Stat(f.FS, name) }

func (f parsedReadFS) ReadDir(name string) ([]fs.DirEntry, error) { return fs.ReadDir(f.FS, name) }
