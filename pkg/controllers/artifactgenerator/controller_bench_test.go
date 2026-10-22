package artifactgenerator

import (
	"fmt"
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type workloadFS struct {
	fs.FS
	opens atomic.Int64
	bytes atomic.Int64
}

func (f *workloadFS) Open(name string) (fs.File, error) {
	if name == "unselected" {
		return nil, fmt.Errorf("unselected source body opened")
	}
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	f.opens.Add(1)
	return &workloadFile{File: file, bytes: &f.bytes}, nil
}

func (f *workloadFS) Stat(name string) (fs.FileInfo, error) { return fs.Stat(f.FS, name) }

type workloadFile struct {
	fs.File
	bytes *atomic.Int64
}

func (f *workloadFile) Read(data []byte) (int, error) {
	n, err := f.File.Read(data)
	f.bytes.Add(int64(n))
	return n, err
}

func generatedWorkload(tb testing.TB, count int) (*manifest.RawObject, map[string]any, *workloadFS) {
	tb.Helper()
	artifacts := make([]any, count)
	for i := range artifacts {
		artifacts[i] = map[string]any{"name": fmt.Sprintf("output-%04d", i), "copy": []any{map[string]any{"from": "@repo/input", "to": "@artifact/input"}}}
	}
	doc := map[string]any{"apiVersion": "source.extensions.fluxcd.io/v1beta1", "kind": "ArtifactGenerator", "metadata": map[string]any{"name": "workload", "namespace": "apps"}, "spec": map[string]any{"sources": []any{map[string]any{"alias": "repo", "kind": "GitRepository", "name": "repo"}}, "artifacts": artifacts}}
	obj, doc, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	input := &workloadFS{FS: fstest.MapFS{"input": {Data: []byte("selected\n")}, "unselected": {Data: make([]byte, 1<<20)}}}
	return obj.(*manifest.RawObject), doc, input
}

func reconcileWorkload(tb testing.TB, owner *cache.Cache, child store.Child, input *workloadFS) *store.Store {
	tb.Helper()
	st := store.New()
	st.AddObject(child.Obj, child.Doc)
	repo := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "repo"}
	st.SetArtifact(repo, &store.SourceArtifact{Kind: repo.Kind, FS: input, Key: "input", Digest: "sha256:input"})
	c := New(st, task.NewBounded(2), owner)
	run := &execution{Controller: c}
	run.run.Init(c.Controller, child.Obj.Named())
	defer run.run.Close()
	if err := run.reconcile(tb.Context(), child.Obj.(*manifest.RawObject), manifest.DrainNone); err != nil {
		tb.Fatal(err)
	}
	return st
}

func BenchmarkArtifactGenerator_Outputs(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		for _, backing := range []string{"memory", "persistent"} {
			for _, warm := range []bool{false, true} {
				b.Run(fmt.Sprintf("%d/%s/warm=%t", count, backing, warm), func(b *testing.B) {
					obj, doc, input := generatedWorkload(b, count)
					root := ""
					if backing == "persistent" {
						root = b.TempDir()
					}
					owner, err := cache.New(b.Context(), root)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() {
						if err := owner.Close(); err != nil {
							b.Error(err)
						}
					})
					if warm {
						reconcileWorkload(b, owner, store.Child{Obj: obj, Doc: doc}, input)
					}
					input.opens.Store(0)
					input.bytes.Store(0)
					for b.Loop() {
						if warm {
							reconcileWorkload(b, owner, store.Child{Obj: obj, Doc: doc}, input)
							continue
						}
						coldRoot := ""
						if backing == "persistent" {
							coldRoot = b.TempDir()
						}
						fresh, err := cache.New(b.Context(), coldRoot)
						if err != nil {
							b.Fatal(err)
						}
						reconcileWorkload(b, fresh, store.Child{Obj: obj, Doc: doc}, input)
						if err := fresh.Close(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(input.opens.Load())/float64(b.N), "input-opens/op")
					b.ReportMetric(float64(input.bytes.Load())/float64(b.N), "input-bytes/op")
				})
			}
		}
	}
}

var _ fs.StatFS = (*workloadFS)(nil)
