package loader

import (
	"iter"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ProjectedReaderNamespace(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	input := &manifest.ConfigMap{Name: "settings", Namespace: "apps"}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: input.Name}}}}
	st.AddObject(consumer)
	owner := (&manifest.Kustomization{Name: "owner", Namespace: "apps"}).Named()
	l := New(st)
	l.SourceRoot = root
	l.Options.Changes = change.NewSet([]string{"consumer.yaml"})
	l.Options.Selection = manifest.Selection{Changed: true}
	l.Options.Filter = change.NewFilter()
	l.Index.Records[consumer.Named()] = manifest.DiscoveryRecord{File: "consumer.yaml"}
	l.Index.Records[input.Named()] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID, Path: filepath.Join(root, "shared.yaml")}}}
	l.Index.Readers = map[string][]manifest.FileReader{}
	l.candidates = map[manifest.NamedResource]store.Child{}
	for _, namespace := range []string{"apps", "other"} {
		reader := &manifest.Kustomization{Name: namespace, Namespace: "flux-system", TargetNamespace: namespace}
		l.candidates[reader.Named()] = store.Child{Obj: reader}
		l.Index.Records[reader.Named()] = manifest.DiscoveryRecord{Parent: (&manifest.ResourceSet{Name: "templates", Namespace: "flux-system"}).Named()}
		l.Index.Readers["shared.yaml"] = append(l.Index.Readers["shared.yaml"], manifest.FileReader{Reader: reader.Named(), Source: manifest.BootstrapSourceID})
	}
	proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, input.Named()), true)
	for _, namespace := range []string{"apps", "other"} {
		reader := (&manifest.Kustomization{Name: namespace, Namespace: "flux-system"}).Named()
		assert.Equal(t, slices.Contains(proposal.Needed, reader), namespace == "apps")
	}
}

type countedProducer struct {
	*manifest.ResourceSet
	classifications atomic.Int64
}

func (p *countedProducer) SourcePath() (manifest.NamedResource, string, bool) {
	p.classifications.Add(1)
	return p.ResourceSet.SourcePath()
}

func TestAdmission_MaterializerClassificationReused(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer := &countedProducer{ResourceSet: &manifest.ResourceSet{Name: "producer", Namespace: "apps"}}
	st.AddObject(producer)
	l := New(st)
	l.Options.Filter = change.NewFilter()
	l.Index.Records[producer.Named()] = manifest.DiscoveryRecord{Builder: "generated"}
	l.candidates = map[manifest.NamedResource]store.Child{producer.Named(): {Obj: producer}}
	l.freezeConstruction()
	c := admissionClosure{loader: l, objects: st}
	assert.Equal(t, len(c.materializerCandidates(false)[0]), 1)
	updated := *producer.ResourceSet
	updated.Suspend = true
	st.AddObject(&updated)
	start := make(chan struct{})
	var workers sync.WaitGroup
	read := func(ordering bool) {
		<-start
		c := admissionClosure{loader: l, objects: st}
		candidates := c.materializerCandidates(ordering)
		assert.Equal(t, len(candidates[0]), 1)
		if ordering {
			assert.Equal(t, len(candidates[1]), 1)
			assert.Equal(t, candidates[1][0].facts.catalog.(*manifest.ResourceSet).Suspend, true)
		}
	}
	workers.Go(func() { read(false) })
	workers.Go(func() { read(true) })
	close(start)
	workers.Wait()
	assert.Equal(t, producer.classifications.Load(), int64(1))
}

func TestAdmission_MaterializerInventoryAuthority(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unknown", "complete", "incomplete", "stale", "history", "positive_history", "unknown_history", "prepared", "prepared_foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"consumer.yaml": {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: apps}\nspec:\n  path: ./payload\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: missing}]}\n")},
				"producer.yaml": {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: producer, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: demo, sourceRef: {kind: GitRepository, name: charts}}}\n")},
			}})
			input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			files := input.FS.(fstest.MapFS)
			files["demo/Chart.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v2\nname: demo\nversion: 0.1.0\n")}
			files["demo/templates/cm.yaml"] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: missing, namespace: apps}\n")}
			charts := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
			st.AddObject(charts)
			st.SetArtifact(charts.Named(), input)
			producer := &manifest.HelmRelease{Name: "producer", Namespace: "apps"}
			st.AddObject(producer)
			l := New(st)
			l.SourceRoot = root
			l.Options.Filter = change.NewFilter()
			consumer := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
			l.Options.Selection = manifest.Selection{Roots: []manifest.NamedResource{consumer}}
			loadPath := root
			if mode == "prepared" || mode == "prepared_foreign" {
				input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
				input.FS.(fstest.MapFS)["entry/consumer.yaml"] = input.FS.(fstest.MapFS)["consumer.yaml"]
				loadPath = filepath.Join(root, "entry")
			}
			_, err := l.Load(t.Context(), loadPath)
			assert.NoError(t, err)
			if mode == "prepared" || mode == "prepared_foreign" {
				input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
				reader := documentReader{}
				reader.setSources(map[string]*store.SourceArtifact{root: input})
				docs, err := reader.parseFile(t.Context(), filepath.Join(root, "consumer.yaml"), manifest.ParseDocOptions{})
				assert.NoError(t, err)
				producers, err := reader.parseFile(t.Context(), filepath.Join(root, "producer.yaml"), manifest.ParseDocOptions{})
				assert.NoError(t, err)
				docs = append(docs, producers...)
				source, builtRoot := manifest.BootstrapSourceID, root
				if mode == "prepared_foreign" {
					source = manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "foreign"}
					builtRoot = t.TempDir()
					input := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
					st.SetArtifact(source, &store.SourceArtifact{LocalPath: builtRoot, FS: input.FS})
				}
				l.RegisterBuilt(BuiltInput{Root: builtRoot, Source: source, Owner: manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Name: "entry"}}, docs)
			}
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			if mode == "complete" || mode == "stale" {
				_, _, next := st.GetEmission(producer.Named())
				next.Complete = true
				st.RecordEmitted(producer.Named(), &next, nil)
				next.Finish()
				st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			}
			if mode == "incomplete" || mode == "stale" {
				if mode == "stale" {
					fresh := producer.Clone()
					fresh.Values = map[string]any{"changed": true}
					st.AddObject(fresh)
				}
				_, _, next := st.GetEmission(producer.Named())
				st.RecordEmitted(producer.Named(), &next, nil)
				next.Finish()
				if mode == "stale" {
					st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "failed successor"})
				}
			}
			history := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{}
			if mode == "history" || mode == "positive_history" || mode == "unknown_history" {
				var records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				if mode != "unknown_history" {
					records = func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}
				}
				history[producer.Named()] = struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{Records: records, Complete: mode != "positive_history"}
			}
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, history)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, producer.Named()), mode != "complete" && mode != "history" && mode != "prepared_foreign")
			assert.Equal(t, slices.Contains(proposal.Selected, producer.Named()), false)
			assert.Equal(t, slices.Contains(proposal.Dirty, producer.Named()), false)
		})
	}
}
