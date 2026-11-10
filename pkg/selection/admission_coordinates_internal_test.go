package selection

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SourceCoordinates(t *testing.T) {
	t.Parallel()
	for _, origin := range []string{"/workspace/selected", "/workspace", "/mounted"} {
		t.Run(origin, func(t *testing.T) {
			t.Parallel()
			root := "/workspace/selected"
			if origin == "/mounted" {
				root = "/mounted/selected"
			}
			file := "input.txt"
			if origin != root {
				file = "selected/input.txt"
			}
			var sources [2]*store.SourceArtifact
			for side := range sources {
				sources[side] = &store.SourceArtifact{
					LocalPath: "/mounted", LocalRoot: origin, InputAlias: true,
					FS: fstest.MapFS{file: {Data: fmt.Appendf(nil, "%d", side)}},
				}
			}
			changes, err := change.Detect(t.Context(), sources[0], sources[1], change.DetectConfig{})
			assert.NoError(t, err)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, sources[1])
			toIndex, err := filepath.Rel(root, origin)
			assert.NoError(t, err)
			facts := &solverFacts{
				sources: map[manifest.NamedResource]SourceFacts{
					manifest.BootstrapSourceID: {
						Source:  manifest.BootstrapSourceID,
						Context: SourceContext{Root: root},
						Delta:   changes,
						Input:   sources[1],
						Origin:  origin,
						ToIndex: toIndex,
						Files:   []SourceFile{{Path: file, Indexed: "input.txt"}},
						Paired:  true,
					},
				},
			}
			c := solverClosure(t, st, facts)
			c.context.Root = root
			coordinates2 := c.coordinates(
				new(manifest.Production{
					Source: manifest.BootstrapSourceID,
					File:   file,
					Path:   filepath.Join(sources[1].LocalPath, file),
				}),
			)
			relative, indexed := coordinates2.relative, coordinates2.indexed
			assert.Equal(t, relative, file)
			assert.Equal(t, indexed, "input.txt")
			assert.Equal(t, c.fileChanged(manifest.BootstrapSourceID, "input.txt"), true)
			assert.Diff(t, slices.Collect(c.sourceFiles(manifest.BootstrapSourceID)), []string{"input.txt"})
		})
	}
}

func TestAdmission_DeclarationMountSource(t *testing.T) {
	t.Parallel()
	for _, aliasMounted := range []bool{false, true} {
		t.Run(strconv.FormatBool(aliasMounted), func(t *testing.T) {
			t.Parallel()
			root := "/workspace"
			bootstrap := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}}
			alias := &store.SourceArtifact{LocalPath: "/alias", LocalRoot: root, InputAlias: true, FS: fstest.MapFS{}}
			source := (&manifest.GitRepository{Name: "self", Namespace: "flux-system"}).Named()
			id := (&manifest.Kustomization{Name: "cluster", Namespace: "flux-system"}).Named()
			mounted, want := bootstrap, manifest.BootstrapSourceID
			if aliasMounted {
				mounted, want = alias, source
			}

			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			st.SetArtifact(source, alias)
			facts := &solverFacts{
				store: st,
				sources: map[manifest.NamedResource]SourceFacts{
					source: {Source: source, Input: alias},
				},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{
					id: {
						Identity: id, Authored: id,
						Declaration: OccurrenceID{Source: mounted.LocalPath, File: "cluster.yaml"},
						Mounted:     mounted, CatalogKnown: true,
					},
				},
			}

			c := solverClosure(t, st, facts)
			c.states[id] = admissionState{productionsKnown: true, productions: []manifest.Production{{Source: source}}}
			assert.Equal(t, c.source(id), source)

			var reads store.ReadSet
			reads.Init(st, id, nil)
			defer reads.Retire()
			c.objects, c.proposal.reads = &reads, &reads

			input, support := c.hydrationSource(id, mounted.LocalPath)
			assert.Equal(t, input == mounted, true)
			assert.Equal(t, support, want)
			assert.Equal(t, reads.Valid(), true)

			st.SetArtifact(source, &store.SourceArtifact{LocalPath: "/replacement", FS: fstest.MapFS{}})
			assert.Equal(t, reads.Valid(), !aliasMounted)
		})
	}
}

func TestAdmission_DeclarationMountLifetime(t *testing.T) {
	t.Parallel()
	source := (&manifest.GitRepository{Name: "self", Namespace: "flux-system"}).Named()
	id := (&manifest.Kustomization{Name: "cluster", Namespace: "flux-system"}).Named()
	for _, tc := range []struct {
		name         string
		aliasMounted bool
		committed    bool
		absent       bool
		stale        bool
		replaced     manifest.NamedResource
		wantReads    int
	}{
		{name: "bootstrap replacement", replaced: manifest.BootstrapSourceID, wantReads: 1},
		{name: "alias survives bootstrap replacement", aliasMounted: true, replaced: manifest.BootstrapSourceID, wantReads: 1},
		{name: "absent mount", absent: true, replaced: source},
		{name: "stale bootstrap", stale: true, replaced: manifest.BootstrapSourceID, wantReads: 1},
		{name: "stale alias", aliasMounted: true, stale: true, replaced: source, wantReads: 1},
		{name: "committed mount", aliasMounted: true, committed: true, replaced: source, wantReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bootstrap := &store.SourceArtifact{LocalPath: "/workspace", FS: fstest.MapFS{}}
			alias := &store.SourceArtifact{LocalPath: "/alias", InputAlias: !tc.committed, FS: fstest.MapFS{}}
			mounted, want := bootstrap, manifest.BootstrapSourceID
			if tc.aliasMounted {
				mounted, want = alias, source
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, bootstrap)
			st.SetArtifact(source, alias)
			facts := &solverFacts{
				sources: map[manifest.NamedResource]SourceFacts{
					manifest.BootstrapSourceID: {Source: manifest.BootstrapSourceID, Input: bootstrap},
					source:                     {Source: source, Input: alias},
				},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{
					id: {
						Identity: id, Authored: id,
						Declaration: OccurrenceID{Source: mounted.LocalPath, File: "cluster.yaml"},
						Mounted:     mounted, CatalogKnown: true,
					},
				},
			}
			if tc.absent {
				occurrence := facts.occurrences[id]
				occurrence.Mounted = nil
				facts.occurrences[id] = occurrence
			}
			c := solverClosure(t, st, facts)
			c.states[id] = admissionState{productionsKnown: true, productions: []manifest.Production{{Source: source}}}
			var reads store.ReadSet
			reads.Init(st, id, nil)
			defer reads.Retire()
			tracked := &admissionArtifactReads{admissionReader: &reads, bySource: map[manifest.NamedResource]int{}}
			c.objects, c.proposal.reads = tracked, &reads
			replace, done := make(chan struct{}), make(chan struct{})
			go func() {
				<-replace
				st.SetArtifact(tc.replaced, &store.SourceArtifact{LocalPath: "/replacement", FS: fstest.MapFS{}})
				close(done)
			}()
			if tc.stale {
				close(replace)
				<-done
			}
			input, support := c.hydrationSource(id, mounted.LocalPath)
			if tc.absent || tc.stale {
				assert.Equal(t, input == nil, true)
				assert.Equal(t, support, manifest.NamedResource{})
			} else {
				assert.Equal(t, input == mounted, true)
				assert.Equal(t, support, want)
			}
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, len(tracked.bySource), tc.wantReads)
			_, captured := tracked.bySource[want]
			assert.Equal(t, captured, !tc.absent)
			if !tc.stale {
				close(replace)
				<-done
				assert.Equal(t, reads.Valid(), tc.absent || tc.replaced != want)
			}
		})
	}
}
