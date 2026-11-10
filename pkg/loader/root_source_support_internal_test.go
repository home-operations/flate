package loader

import (
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestRootSupport_SuppliedDeclarationRetainsRepositoryAuthority(t *testing.T) {
	t.Parallel()
	st := store.New()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	source := (&manifest.GitRepository{Namespace: "sources", Name: "repository"}).Named()
	input := &store.SourceArtifact{LocalPath: t.TempDir(), Content: "initial", FS: fstest.MapFS{}}
	st.SetArtifact(source, input)
	st.AddDeclaration(store.Declaration{
		Obj: manifest.NewConfigMap(id, map[string]any{"VALUE": "supplied"}), Origin: manifest.SuppliedDeclaration,
	})
	record := manifest.DiscoveryRecord{
		Path: filepath.Join(input.LocalPath, "settings.yaml"), Authored: id,
		Productions: []manifest.Production{{Source: source, Authored: id}},
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: record}}
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	evidence := selection.RootEvidence{
		Identity: id,
		Record:   selection.RootRecordFrom(&record),
		Occurrence: selection.OccurrenceFacts{
			Declaration: selection.OccurrenceID{Source: input.LocalPath, File: "settings.yaml"},
		},
		Scope: selection.RootScopeFacts{NamespaceEligible: true, Candidate: true},
	}
	rootSupport(&evidence, &reads, index, &manifest.DiscoveryIndex{}, t.TempDir())
	assert.Equal(t, evidence.Supplied, true)
	assert.Equal(t, evidence.Sources[0].Source, source)
	assert.Equal(t, evidence.Sources[0].Available, true)
	assert.Equal(t, reads.Valid(), true)
	replacement := *input
	replacement.Content = "replacement"
	st.SetArtifact(source, &replacement)
	assert.Equal(t, reads.Valid(), false)
}

func TestCatalog_ApplicationScopeAfterClose(t *testing.T) {
	t.Parallel()
	for _, retained := range []bool{false, true} {
		name := "uncached"
		if retained {
			name = "retained initial scope"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			l, source, _, _, child := admissionMembershipFixture(t)
			defer l.Close()
			if retained {
				previous := l.roots.get(child)
				previous.initialScope = new(previous.scope)
				previous.initialOccurrence = new(occurrenceView(previous.currentOccurrence()))
			} else {
				delete(l.roots.identities, child)
			}
			var before store.ReadSet
			before.Init(l.Store, manifest.NamedResource{}, nil)
			defer before.Retire()
			evidence := l.RootEvidence(child, &before, true)
			assert.Equal(t, evidence.Scope.Application, true)
			assert.Equal(t, evidence.Scope.ApplicationSource, source.Named())
			assert.Equal(t, evidence.Scope.ApplicationPath, "./payload")
			assert.Equal(t, evidence.Scope.Initial, true)
			if retained {
				assert.Equal(t, evidence.InitialScope != nil, true)
				assert.Equal(t, evidence.InitialOccurrence != nil, true)
			}
			catalog := l.DetachCatalog()
			defer catalog.Close()
			var detached store.ReadSet
			detached.Init(l.Store, manifest.NamedResource{}, nil)
			defer detached.Retire()
			assert.Equal(t,
				cmp.Diff(
					evidence, catalog.RootEvidence(child, &detached, true),
					cmp.Exporter(func(reflect.Type) bool { return true }),
				),
				"",
			)
			l.Close()

			var results [2]selection.RootEvidence
			var valid [2]bool
			var workers sync.WaitGroup
			start := make(chan struct{})
			for i := range results {
				workers.Go(func() {
					<-start
					var reads store.ReadSet
					reads.Init(l.Store, manifest.NamedResource{}, nil)
					defer reads.Retire()
					results[i] = catalog.RootEvidence(child, &reads, true)
					valid[i] = reads.Valid()
				})
			}
			close(start)
			workers.Wait()
			for i := range results {
				assert.Equal(t, valid[i], true)
				assert.Equal(t,
					cmp.Diff(
						evidence, results[i],
						cmp.Exporter(func(reflect.Type) bool { return true }),
					),
					"",
				)
				assert.Equal(t,
					cmp.Diff(
						selection.Root(evidence), selection.Root(results[i]),
						cmp.Exporter(func(reflect.Type) bool { return true }),
					),
					"",
				)
			}
		})
	}
}
