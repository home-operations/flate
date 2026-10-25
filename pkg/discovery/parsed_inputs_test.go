package discovery

import (
	"io/fs"
	"path/filepath"
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

func TestPlan_ParsedInputHandoff(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{}
	reads := map[string]*atomic.Int64{}
	for _, side := range []string{"left", "right"} {
		files[side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [owner.yaml]\n")}
		files[side+"/owner.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec:\n  path: ./" + side + "/leaf\n  sourceRef: {kind: GitRepository, name: cluster}\n")}
		files[side+"/leaf/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
		files[side+"/leaf/cm.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {side: " + side + "}\n")}
		reads[side+"/owner.yaml"] = &atomic.Int64{}
		reads[side+"/leaf/cm.yaml"] = &atomic.Int64{}
	}
	view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: entryReadFS{FS: files, reads: reads}})
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, FS: view}
	entries, err := Plan(t.Context(), Config{Input: input, Path: root, WipeSecrets: true})
	assert.NoError(t, err)
	if len(entries) != 2 || len(entries[0].prepared.aliasViews) == 0 {
		t.Fatalf("expected two prepared alias-backed entries: %+v", entries)
	}
	before := map[string]int64{}
	for name, count := range reads {
		before[name] = count.Load()
	}
	t.Cleanup(func() {
		for name, count := range reads {
			if count.Load() != before[name] {
				t.Errorf("%s content reads = %d, want %d", name, count.Load(), before[name])
			}
		}
	})
	t.Run("native admissions", func(t *testing.T) {
		t.Parallel()
		for _, entry := range entries {
			t.Run(string(entry.ID), func(t *testing.T) {
				t.Parallel()
				st := store.New()
				d := newDiscoverer(Config{Input: input, Entry: &entry, Path: entry.Path, Store: st, WipeSecrets: true})
				if d.loader.Options.ParsedInputs == nil {
					t.Fatal("matching input did not borrow its parsed evidence")
				}
				_, err := d.resolveEntrySources(t.Context(), false)
				assert.NoError(t, err)
				assert.NoError(t, d.loadManifests(t.Context(), root, false))
				owner := st.GetObject(manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "owner"}).(*manifest.Kustomization)
				side := strings.Split(owner.Path, "/")[1]
				assert.Equal(t, entry.Path, filepath.Join(root, side))
				for _, record := range d.loader.Index.Records {
					if record.File != "" && !strings.Contains(record.File, side+"/") {
						t.Fatalf("entry admitted sibling evidence: %+v", record)
					}
				}
				for key, view := range entry.prepared.aliasViews {
					assert.Equal(t, d.aliasViews[key], view)
					delete(d.aliasViews, key)
					if entry.prepared.aliasViews[key] == nil {
						t.Fatal("native admission mutated the prepared alias map")
					}
				}
			})
		}
	})
}

func TestRun_PreparedInputAdmission(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"owner.yaml":  {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: planned, namespace: apps}\nspec: {path: ./leaf}\n")},
		"secret.yaml": {Data: []byte("apiVersion: v1\nkind: Secret\nmetadata: {name: secret, namespace: apps}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n")},
	}
	view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: files})
	assert.NoError(t, err)
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: view}
	entries, err := Plan(t.Context(), Config{Input: input, WipeSecrets: true})
	assert.NoError(t, err)
	for _, tc := range []struct {
		name     string
		input    *store.SourceArtifact
		wipe     bool
		wantName string
	}{
		{name: "matching admission", input: input, wipe: true, wantName: "planned"},
		{name: "parse policy", input: input, wantName: "planned"},
		{name: "different source", wantName: "authoritative", wipe: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.input == nil {
				other, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					"owner.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: authoritative, namespace: apps}\nspec: {path: ./other}\n")},
				}})
				assert.NoError(t, err)
				tc.input = &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: other}
			}
			st := store.New()
			result, err := Run(t.Context(), Config{Input: tc.input, Entry: &entries[0], Store: st, Trees: testTrees(t), WipeSecrets: tc.wipe})
			assert.NoError(t, err)
			id := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: tc.wantName}
			if _, ok := result.Index.Lookup(id); !ok || st.GetObject(id) == nil {
				t.Fatalf("authoritative input lost: %+v", result.Index.Records)
			}
			if tc.name != "different source" {
				secret := st.GetObject(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "secret"}).(*manifest.Secret)
				assert.Equal(t, secret.HasPlaceholder(), tc.wipe)
			}
		})
	}
}

type entryReadFS struct {
	fs.FS
	reads map[string]*atomic.Int64
}

func (f entryReadFS) Open(name string) (fs.File, error) {
	if count := f.reads[name]; count != nil {
		count.Add(1)
	}
	return f.FS.Open(name)
}

func (f entryReadFS) Stat(name string) (fs.FileInfo, error) { return fs.Stat(f.FS, name) }

func (f entryReadFS) ReadDir(name string) ([]fs.DirEntry, error) { return fs.ReadDir(f.FS, name) }
