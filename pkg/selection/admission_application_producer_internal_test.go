package selection

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ApplicationProducerEvidence(t *testing.T) {
	t.Parallel()
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprintf("compact=%t", compact), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
			st.AddObject(source)
			owner := &manifest.Kustomization{Name: "factory", Namespace: "apps", Path: "application"}
			st.AddObject(owner)
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "flux", manifest.NamedResource{}).
				Named()
			application := manifest.NamedResource{
				Group:     manifest.FluxOperatorDomain,
				Kind:      "FluxInstance",
				Name:      "flux",
				Namespace: "apps",
			}
			facts := &solverFacts{
				store:   st,
				context: SourceContext{Root: root, Frozen: compact},
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{entry: {
						Productions: []manifest.Production{{Owner: owner.Named(), Declaration: application}},
					}},
				},
			}
			closure := solverClosure(t, st, facts)
			closure.context = facts.context
			closure.materializers(materializerDemand{
				target: source.Named(), source: root, application: filepath.Join(root, "flux"), required: true,
			}, manifest.NamedResource{})
			assert.Equal(t, closure.role(owner.Named()).needed, true)
		})
	}
	for _, namespace := range []string{"apps", ""} {
		t.Run("resourceset/namespace="+namespace, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				root := t.TempDir()
				st := store.New()
				input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
				st.SetArtifact(manifest.BootstrapSourceID, input)
				source := &manifest.GitRepository{Name: "self", Namespace: "apps"}
				st.AddObject(source)
				st.SetArtifact(source.Named(), input)
				producer := &manifest.ResourceSet{Name: "factory", Namespace: namespace}
				producer.ResourcesTemplate = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux}
spec:
  sync: {name: self, kind: GitRepository, url: https://fixture.invalid/repo, ref: main, path: ./flux}
`
				owner := producer.WithNamespace("apps").Named()
				projection, _ := manifest.ProjectProducer(producer, nil)
				occurrence := applicationOccurrence(producer, root)
				occurrence.Identity = owner
				occurrence.TemplateObjects, occurrence.TemplateInputs = projection.Objects, projection.Inputs
				occurrence.Targets = append(occurrence.Targets, projection.Targets...)
				facts := &solverFacts{store: st, context: SourceContext{Root: root}, index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{owner: {Authored: producer.Named()}},
				}, occurrences: map[manifest.NamedResource]OccurrenceFacts{owner: occurrence}}
				closure := solverClosure(t, st, facts)
				closure.context = facts.context
				pending := closure.materializers(materializerDemand{
					target: source.Named(), source: root, application: filepath.Join(root, "flux"), required: true,
				}, manifest.NamedResource{})
				assert.Equal(t, pending, true)
				assert.Equal(t, closure.role(owner).needed, true)
				owner.Namespace = ""
				assert.Equal(t, closure.role(owner).needed, false)
			}
		})
	}
}
