package helm_test

import (
	"context"
	"io/fs"
	"os"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// TestSourceResolver_NoAddCallsNeeded verifies helm.Client renders a HR
// whose chart lives in a GitRepository on disk, without any AddRepo /
// AddLocalSource push-calls — only the Sources wired in at
// construction. This locks the iter-12 contract: the helm.Client reads
// source CRs through the canonical Store, eliminating the duplicate
// push-registries.
func TestSourceResolver_NoAddCallsNeeded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: test
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "greeting: hi\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-cm
data:
  greeting: {{ .Values.greeting }}
`)

	// Build a Store containing the GitRepository + its on-disk artifact.
	st := store.New()
	gr := &manifest.GitRepository{
		Name: "chart-repo", Namespace: "flux-system",
		URL: "file://" + dir,
	}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})

	owner := cachetest.New(t, "", cache.New)
	cli := helm.NewClient(owner)
	resolver := helm.NewSources(st)
	chart := &manifest.HelmChartSource{Name: "chart-repo", Namespace: "flux-system"}
	st.AddObject(chart)
	for _, tc := range []struct {
		name string
		id   manifest.NamedResource
		want *manifest.HelmChartSource
	}{
		{"complete", chart.Named(), chart},
		{"group", manifest.NamedResource{Group: "foreign.example", Kind: manifest.KindHelmChart, Namespace: chart.Namespace, Name: chart.Name}, nil},
		{"kind", gr.Named(), nil},
		{"namespace", manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "other", Name: chart.Name}, nil},
		{"name", manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: chart.Namespace, Name: "other"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolver.HelmChart(tc.id); got != tc.want {
				t.Fatalf("HelmChart(%+v) = %p, want %p", tc.id, got, tc.want)
			}
		})
	}
	foreign := gr.Named()
	foreign.Group = "example.test"
	foreignArtifact := &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: "foreign"}
	st.SetArtifact(foreign, foreignArtifact)
	if resolver.LocalSourceArtifact(foreign) != foreignArtifact || resolver.LocalSourceArtifact(gr.Named()).URL != gr.URL {
		t.Fatal("source resolver lost canonical artifact identity")
	}
	cliSources := resolver

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "chart-repo", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
	}

	out, err := cli.Template(context.Background(), hr, map[string]any{"greeting": "hello"}, helm.RenderOptions{Options: helm.Options{}, Sources: cliSources})
	if err != nil {
		t.Fatalf("Template via resolver: %v", err)
	}
	if len(out) != 1 || out[0]["metadata"].(map[string]any)["name"] != "demo-cm" {
		t.Errorf("expected rendered ConfigMap; got: %s", out)
	}
	if len(out) != 1 || out[0]["data"].(map[string]any)["greeting"] != "hello" {
		t.Errorf("expected values applied; got: %s", out)
	}
}

func TestSourceResolver_OverlappingCapturedInputs(t *testing.T) {
	t.Parallel()
	st := store.New()
	client := helm.NewClient(cachetest.New(t, "", cache.New))
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var reads [2]store.ReadSet
	var releases [2]*manifest.HelmRelease
	var outputs [2][]map[string]any
	var failures [2]error
	for i, name := range []string{"first", "second"} {
		root := t.TempDir()
		testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: "+name+"\nversion: 1.0.0\n")
		testutil.WriteFile(t, root, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Chart.Name }}\ndata:\n  greeting: {{ .Values.greeting }}\n")
		testutil.WriteFile(t, root, "config.yaml", "greeting: "+name+"\n")
		repository := &manifest.GitRepository{Name: name, Namespace: "apps"}
		st.AddObject(repository)
		st.SetArtifact(repository.Named(), &store.SourceArtifact{FS: &capturedChartFS{FS: os.DirFS(root), entered: entered, release: release}})
		releases[i] = &manifest.HelmRelease{Name: name, Namespace: "apps", Chart: manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: name}}, ChartValuesFiles: []string{"config.yaml"}}
		reads[i].Init(st, releases[i].Named(), nil)
	}
	defer func() {
		for i := range reads {
			reads[i].Retire()
		}
		for range st.Sweep() {
		}
	}()
	var workers sync.WaitGroup
	for i := range reads {
		workers.Go(func() {
			outputs[i], failures[i] = client.Template(t.Context(), releases[i], nil, helm.RenderOptions{Sources: helm.NewSources(&reads[i])})
		})
	}
	<-entered
	<-entered
	replacement := t.TempDir()
	testutil.WriteFile(t, replacement, "mychart/Chart.yaml", "apiVersion: v2\nname: replacement\nversion: 1.0.0\n")
	testutil.WriteFile(t, replacement, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Chart.Name }}\ndata:\n  greeting: {{ .Values.greeting }}\n")
	testutil.WriteFile(t, replacement, "config.yaml", "greeting: replacement\n")
	st.SetArtifact(releases[0].ChartSource(), &store.SourceArtifact{FS: os.DirFS(replacement)})
	close(release)
	workers.Wait()
	for i, name := range []string{"first", "second"} {
		if failures[i] != nil || len(outputs[i]) != 1 || outputs[i][0]["metadata"].(map[string]any)["name"] != name || outputs[i][0]["data"].(map[string]any)["greeting"] != name {
			t.Fatalf("operation %s: err=%v output=%s", name, failures[i], outputs[i])
		}
	}
	if reads[0].Valid() || !reads[1].Valid() {
		t.Fatal("source replacement did not invalidate exactly its captured operation")
	}
	var successor store.ReadSet
	successor.Init(st, releases[0].Named(), nil)
	defer successor.Retire()
	output, err := client.Template(t.Context(), releases[0], nil, helm.RenderOptions{Sources: helm.NewSources(&successor)})
	if err != nil || len(output) != 1 || output[0]["metadata"].(map[string]any)["name"] != "replacement" || output[0]["data"].(map[string]any)["greeting"] != "replacement" || !successor.Valid() {
		t.Fatalf("successor: err=%v output=%s", err, output)
	}
}

type capturedChartFS struct {
	fs.FS
	entered chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (f *capturedChartFS) Open(name string) (fs.File, error) {
	if name == "mychart/Chart.yaml" {
		f.once.Do(func() { f.entered <- struct{}{}; <-f.release })
	}
	return f.FS.Open(name)
}
