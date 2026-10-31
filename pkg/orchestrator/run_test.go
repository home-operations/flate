package orchestrator

import (
	"context"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"io/fs"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestOrchestratorExistence_PromotionDoesNotAdmitControllerSiblings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
data: {value: selected}
`+"---\n"+
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: raw-hr, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
`+"---\n"+
		testutil.KustomizationYAML("raw-ks", "apps", "  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n")+"---\n"+
		`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: raw-rs, namespace: apps}
spec: {}
`+"---\n"+
		`apiVersion: seaweed.seaweedfs.com/v1
kind: Bucket
metadata: {name: foreign-source, namespace: apps}
spec: {}
`)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	idx := loader.New(store.New())
	idx.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml"), Productions: []manifest.Production{{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}}}
	st := store.New()
	idx.Store = st
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{
		store: idx.Store, index: idx.Index, cfg: treeConfig{Config: Config{WipeSecrets: true}},
		ksc: &kustomization.Controller{Controller: base.New(st, nil)},
	}}
	if !e.Promote(t.Context(), nil, id) || st.GetObject(id) == nil {
		t.Fatal("selected data must be admitted")
	}
	for _, excluded := range []manifest.NamedResource{
		testutil.HelmReleaseID("apps", "raw-hr"),
		testutil.KustomizationID("apps", "raw-ks"),
		{Group: "fluxcd.controlplane.io", Kind: manifest.KindResourceSet, Namespace: "apps", Name: "raw-rs"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindBucket, Namespace: "apps", Name: "foreign-source"},
	} {
		if st.GetObject(excluded) != nil {
			t.Errorf("runtime promotion admitted %s", excluded)
		}
	}
}

func TestOrchestratorExistence_PromotionPreservesSelectedContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: authored}
data: {values.yaml: "greeting: selected"}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: excluded, namespace: apps}
data: {value: excluded}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: chart, namespace: apps}
spec: {url: https://example.test/chart.git}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: excluded-source, namespace: apps}
spec: {url: https://example.test/excluded.git}
`)
	st := store.New()
	hr := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}}, HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{Chart: "chart", Version: "", SourceRef: &helmv2.CrossNamespaceObjectReference{Kind: manifest.KindGitRepository, Name: "chart", Namespace: "apps", APIVersion: manifest.SourceDomain + "/v1"}}}, ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "selected"}}}}
	st.AddObject(hr)
	selected := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
	excluded := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "excluded"}
	chart := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "chart"}
	excludedSource := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "excluded-source"}
	files := map[manifest.NamedResource]string{hr.Named(): "consumer.yaml", selected: "bundle.yaml", excluded: "bundle.yaml", chart: "bundle.yaml", excludedSource: "bundle.yaml"}
	filter := change.NewFilter(change.NewSet([]string{"consumer.yaml"}), indexForTest(files), st, "")
	idx := loader.New(store.New())
	for id := range files {
		record := manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}
		if id == selected {
			record.Authored.Namespace = "authored"
		}
		record.Productions = []manifest.Production{{Authored: record.Authored, Path: record.Path}}
		idx.Index.Records[id] = record
	}
	idx.Store = st
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(manifest.BootstrapSourceID, input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Filter: filter, Index: idx.Index})
	e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{
		store: idx.Store, index: idx.Index, cfg: treeConfig{Config: Config{WipeSecrets: true}}, filter: filter,
		ksc: &kustomization.Controller{Controller: controller},
	}}
	size := len(filter.KeepNames())
	if !e.Promote(t.Context(), nil, selected) || st.GetObject(selected) == nil || st.GetObject(chart) == nil {
		t.Fatal("selected values and required chart source must be admitted")
	}
	if got := st.GetObject(selected).(*manifest.ConfigMap).Data["values.yaml"]; got != "greeting: selected" {
		t.Errorf("selected values = %v", got)
	}
	for _, id := range []manifest.NamedResource{excluded, excludedSource} {
		if e.Promote(t.Context(), nil, id) || st.GetObject(id) != nil || filter.ShouldReconcile(id) {
			t.Errorf("excluded sibling %s must remain absent", id)
		}
	}
	if len(filter.KeepNames()) != size {
		t.Error("ordering promotion must not expand keep")
	}
	st.AddObject(&manifest.ConfigMap{Name: "excluded", Namespace: "apps"})
	if e.Promote(t.Context(), nil, excluded) {
		t.Error("availability must not bypass admission")
	}
}

func TestOrchestratorExistence_ExistingDataUsesNewSourceEvidence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := "settings.yaml"
	testutil.WriteFile(t, dir, file, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: selected}\n")
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	data := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"value": "selected"}}
	st.AddObject(source)
	st.AddObject(data)
	id := data.Named()
	index := indexForTest(map[manifest.NamedResource]string{id: file})
	record := index.Records[id]
	record.Path = filepath.Join(dir, file)
	record.Productions = []manifest.Production{{Authored: id, Source: source.Named(), Path: record.Path, File: file}}
	index.Records[id] = record
	filter := change.NewFilter(change.NewSet([]string{file}), index, st, dir)
	if filter.ShouldReconcile(id) {
		t.Fatal("unavailable source evidence must not admit existing data")
	}
	input, _, err := baseline.AdmitDirectory(t.Context(), dir, dir)
	assert.NoError(t, err)
	st.SetArtifact(source.Named(), input)
	controller := base.New(st, nil)
	controller.Configure(base.Options{Filter: filter, Index: index})
	e := &orchestratorExistence{DiscoveryIndex: index, o: &Orchestrator{
		store: st, index: index, filter: filter,
		ksc: &kustomization.Controller{Controller: controller},
	}}
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Close()
	if !e.Promote(t.Context(), &reads, id) || !filter.ShouldReconcile(id) {
		t.Fatal("new source evidence must admit the existing changed data")
	}
	assert.Equal(t, st.GetObject(id).(*manifest.ConfigMap).Data["value"], "selected")
}

func TestOrchestratorExistence_FileIndexNamespaceConvention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, recorded, requested string
		raw, want                 bool
	}{
		{"exact", "apps", "apps", true, true},
		{"inherited namespace", "", "apps", true, true},
		{"explicit other namespace", "other", "apps", true, false},
		{"bare query cannot match explicit", "apps", "", true, false},
		{"generated object", "apps", "apps", false, false},
		{"bare generated object", "", "apps", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idx := loader.New(store.New())
			id := testutil.HelmReleaseID(tc.recorded, "target")
			if tc.raw {
				idx.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: "target.yaml"}
			} else {
				idx.Index.Records[id] = manifest.DiscoveryRecord{File: "target.yaml"}
			}
			e := &orchestratorExistence{DiscoveryIndex: idx.Index, o: &Orchestrator{store: idx.Store, index: idx.Index}}
			id.Namespace = tc.requested
			if got := e.IsFileIndexed(id); got != tc.want {
				t.Errorf("IsFileIndexed(%s) = %v, want %v", id, got, tc.want)
			}
		})
	}
}

func TestOrchestratorExistence_TrackedBootstrapPromotion(t *testing.T) {
	t.Parallel()
	for _, authority := range []string{"root declaration", "owner inventory"} {
		t.Run(authority, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n")
			o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(dir, "entry"), RepoRoot: dir}, Config{Concurrency: 2})
			assert.NoError(t, err)
			defer o.stop()
			fetcher := &advisorySourceFetcher{root: dir}
			o.src.Fetchers[manifest.KindGitRepository] = fetcher
			assert.NoError(t, o.bootstrap(t.Context()))
			o.configureControllers()
			o.src.StartLifecycle()
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "self"}
			root := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "./entry", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name}}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "elsewhere"}
			if authority == "root declaration" {
				o.store.AddObject(root)
			} else {
				o.store.AddObject(owner)
				o.ksc.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: root}}, Complete: true}, nil)
			}
			o.admission.AddApplication(o.store, root)
			var reads store.ReadSet
			reads.Init(o.store, root.Named(), nil)
			defer reads.Close()
			existence := &orchestratorExistence{DiscoveryIndex: o.index, o: o}
			assert.Equal(t, existence.Promote(t.Context(), &reads, source), true)
			assert.Equal(t, o.store.GetObject(source) != nil, true)
			assert.Equal(t, o.store.GetArtifact(source, store.ArtifactContent), store.Artifact(nil))
			if status, ok := o.store.GetStatus(source); ok && status.Status == store.StatusReady {
				t.Fatal("promotion published Ready without a source execution")
			}
			var accepted *base.Execution
			reconcile := func() {
				o.tasks.Go(t.Context(), "source", func(ctx context.Context) { accepted = o.src.ReconcileNode(ctx, source, manifest.DrainNone) })
				o.tasks.BlockTillDone()
			}
			reconcile()
			defer func() { accepted.Close() }()
			artifact := o.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, artifact.InputAlias, true)
			assert.Equal(t, artifact.Reads.Valid(), true)
			assert.Equal(t, artifact.Reads == &reads, false)
			assert.Equal(t, fetcher.calls.Load(), int64(0))
			previous := accepted
			reconcile()
			previous.Retire()
			assert.Equal(t, o.store.GetArtifact(source, store.ArtifactContent), store.Artifact(artifact))
			if authority == "root declaration" {
				next := root.Clone()
				next.Path = "elsewhere"
				o.store.AddObject(next)
			} else {
				o.ksc.Emit(owner.Named(), &store.Emission{Complete: true}, nil)
			}
			assert.Equal(t, artifact.Reads.Valid(), false)
			if changed := slices.Collect(o.store.Sweep()); !slices.Contains(changed, source) {
				t.Fatalf("promoted source was not rescheduled: %v", changed)
			}
			previous = accepted
			reconcile()
			previous.Retire()
			assert.Equal(t, accepted.Valid(), true)
			current := o.store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			if current == artifact || current.Reads == artifact.Reads {
				t.Fatal("invalid promotion authority bypassed fresh resolution")
			}
		})
	}
}

func TestOrchestratorExistence_PromotionAuthorityReplacement(t *testing.T) {
	t.Parallel()
	for _, authority := range []string{"root declaration", "owner inventory"} {
		t.Run(authority, func(t *testing.T) {
			t.Parallel()
			const dir = "/virtual/promotion-capture"
			input := &promotionCaptureFS{FS: fstest.MapFS{"entry/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n")}}, entered: make(chan struct{}), release: make(chan struct{})}
			unblock := sync.OnceFunc(func() { close(input.release) })
			defer unblock()
			o, err := newTestOrchestrator(t, Tree{Path: dir + "/entry", RepoRoot: dir, Input: &store.SourceArtifact{LocalPath: dir, FS: input}}, Config{Concurrency: 2})
			assert.NoError(t, err)
			defer o.stop()
			assert.NoError(t, o.bootstrap(t.Context()))
			o.configureControllers()
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "self"}
			root := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "./entry", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name}}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "elsewhere"}
			if authority == "root declaration" {
				o.store.AddObject(root)
			} else {
				o.store.AddObject(owner)
				o.ksc.Emit(owner.Named(), &store.Emission{Children: []store.Child{{Obj: root}}, Complete: true}, nil)
			}
			o.admission.AddApplication(o.store, root)
			var reads store.ReadSet
			reads.Init(o.store, root.Named(), nil)
			defer reads.Close()
			existence := &orchestratorExistence{DiscoveryIndex: o.index, o: o}
			input.enabled.Store(true)
			done := make(chan bool, 1)
			o.tasks.Go(t.Context(), "promote", func(ctx context.Context) { done <- existence.Promote(ctx, &reads, source) })
			select {
			case <-input.entered:
			case <-done:
				t.Fatal("promotion did not reach the source view")
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			if authority == "root declaration" {
				next := root.Clone()
				next.Path = "elsewhere"
				o.store.AddObject(next)
			} else {
				o.ksc.Emit(owner.Named(), &store.Emission{Complete: true}, nil)
			}
			unblock()
			assert.Equal(t, <-done, false)
			o.tasks.BlockTillDone()
			assert.Equal(t, reads.Valid(), false)
			assert.Equal(t, o.store.GetObject(source), manifest.BaseManifest(nil))
			assert.Equal(t, o.store.GetArtifact(source, store.ArtifactContent), store.Artifact(nil))
		})
	}
}

type promotionCaptureFS struct {
	fs.FS
	enabled          atomic.Bool
	once             sync.Once
	entered, release chan struct{}
}

func (f *promotionCaptureFS) Open(name string) (fs.File, error) {
	if f.enabled.Load() {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return f.FS.Open(name)
}
