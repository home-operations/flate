package selection

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
					LocalPath: "/mounted", LocalRoot: origin, Provider: store.ProviderAlias,
					FS: fstest.MapFS{file: {Data: fmt.Appendf(nil, "%d", side)}},
				}
			}
			changes, err := change.Detect(t.Context(), sources[0], sources[1], change.DetectConfig{})
			assert.NoError(t, err)
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, sources[1])
			facts := &solverFacts{
				sources: map[manifest.NamedResource]SourceFacts{
					manifest.BootstrapSourceID: {
						Source:  manifest.BootstrapSourceID,
						Context: SourceContext{Root: root},
						Delta:   changes,
						Input:   sources[1],
						Origin:  origin,
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
			inverse, known := sources[1].SourcePath(root, indexed)
			assert.Equal(t, known, true)
			assert.Equal(t, inverse, file)
			assert.Equal(
				t,
				c.affectedCoordinates(file, sources[1], true),
				affectedLocation{base: file, prefix: indexed, known: true},
			)
			assert.Equal(
				t,
				c.affectedCoordinates(file, sources[1], false),
				affectedLocation{base: indexed, prefix: indexed, known: true},
			)
			assert.Equal(t, c.comparisonIndexedFile(file, sources[1], true), indexed)
			assert.Equal(t, c.fileChanged(manifest.BootstrapSourceID, "input.txt"), true)
			evidence := facts.sources[manifest.BootstrapSourceID]
			assert.Equal(t, c.comparisonInputLocal(&evidence, changes, indexed), origin == root)
			assert.Diff(t, slices.Collect(c.sourceFiles(manifest.BootstrapSourceID)), []string{"input.txt"})
		})
	}
	t.Run("failed_production_mapping", func(t *testing.T) {
		t.Parallel()
		source := manifest.BootstrapSourceID
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
		input := &store.SourceArtifact{LocalPath: "/mounted", LocalRoot: "relative", Provider: store.ProviderAlias}
		facts := &solverFacts{sources: map[manifest.NamedResource]SourceFacts{
			source: {
				Source: source,
				Input:  input,
				Delta:  detectChanges(t, []string{"input.yaml"}, []string{"input.yaml"}),
				Paired: true,
			},
		}}
		c := solverClosure(t, store.New(), facts)
		c.context.Root = "/workspace"
		c.states[id] = admissionState{productionsKnown: true, productions: []manifest.Production{
			{Source: source, File: "input.yaml", Path: "/mounted/input.yaml"},
		}}
		assert.Equal(t, c.affectedProductions(id, &manifest.DiscoveryRecord{}).fileOwned, false)
	})
	t.Run("failed_unbound_input_mapping", func(t *testing.T) {
		t.Parallel()
		source := manifest.BootstrapSourceID
		dependency := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
		consumer := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "consumer"}
		facts := &solverFacts{index: manifest.DiscoveryIndex{
			Claims: map[string][]manifest.NamedResource{"unrelated/": {consumer}},
		}}
		c := solverClosure(t, store.New(), facts)
		c.context.Root = "/workspace"
		input := &store.SourceArtifact{LocalPath: "/mounted", LocalRoot: "relative", Provider: store.ProviderAlias}
		changed := c.comparisonRecordChanged(
			dependency,
			change.InputChange{File: "input.yaml", Unbound: true},
			&comparisonInput{
				consumer: consumer, dependency: dependency, source: source,
				input: input, paired: true, delta: detectChanges(t, []string{"input.yaml"}, []string{"input.yaml"}),
			},
		)
		assert.Equal(t, changed, false)
	})
}

func TestAdmission_SourceCoordinateClaims(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, origin, file, indexed, prefix string
		pinned, failed                      bool
	}{
		{name: "nested_alias", origin: "/workspace", file: "selected/app/input.txt", indexed: "app/input.txt", prefix: "app"},
		{name: "pinned", origin: "/workspace", file: "app/input.txt", indexed: "../../mounted/app/input.txt", prefix: "../../mounted/app", pinned: true},
		{name: "external", origin: "/outside", file: "app/input.txt", indexed: "../../outside/app/input.txt", prefix: "../../outside/app"},
		{name: "failed", origin: "relative", file: "app/input.txt", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := &store.SourceArtifact{
				LocalPath: "/mounted",
				LocalRoot: tc.origin,
				Provider:  map[bool]store.SourceProvider{true: store.ProviderAlias}[!tc.pinned],
			}
			if tc.pinned {
				input.Content = "pinned"
			}
			source := manifest.BootstrapSourceID
			owner := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "apps", Name: "reader"}
			delta := detectChanges(t, []string{tc.file}, []string{tc.file})
			evidence := SourceFacts{
				Source: source, Input: input, Delta: delta, Paired: true,
				Files: []SourceFile{{Path: tc.file, Indexed: tc.indexed}},
			}
			facts := &solverFacts{
				sources: map[manifest.NamedResource]SourceFacts{source: evidence},
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner: {
						ContentRoots: map[manifest.NamedResource][]string{
							source: {filepath.ToSlash(filepath.Dir(tc.file))},
						},
					},
				}},
			}
			c := solverClosure(t, store.New(), facts)
			c.context.Root = "/workspace/selected"
			assert.Equal(
				t,
				c.coordinates(
					&manifest.Production{Source: source, File: tc.file, Path: filepath.Join(input.LocalPath, tc.file)},
				),
				productionCoordinates{relative: tc.file, indexed: tc.indexed},
			)
			assert.Equal(
				t,
				c.changedProduction(
					manifest.Production{
						Source: source,
						Owner:  owner,
						File:   tc.file,
						Path:   filepath.Join(input.LocalPath, tc.file),
					},
				),
				!tc.failed,
			)
			var wantFiles []string
			if !tc.failed {
				wantFiles = append(wantFiles, tc.indexed)
			}
			assert.Diff(t, slices.Collect(c.sourceFiles(source)), wantFiles)
			assert.Equal(t, c.comparisonIndexedFile(tc.file, input, true), tc.indexed)
			pairedLocation, unpairedLocation := affectedLocation{}, affectedLocation{}
			if !tc.failed {
				pairedLocation = affectedLocation{
					base: filepath.ToSlash(filepath.Dir(tc.file)), prefix: tc.prefix, known: true,
				}
				unpairedLocation = affectedLocation{base: tc.prefix, prefix: tc.prefix, known: true}
			}
			assert.Equal(t, c.affectedCoordinates(filepath.ToSlash(filepath.Dir(tc.file)), input, true), pairedLocation)
			assert.Equal(
				t,
				c.affectedCoordinates(filepath.ToSlash(filepath.Dir(tc.file)), input, false),
				unpairedLocation,
			)
			assert.Equal(t, c.comparisonInputLocal(&evidence, delta, "app/input.txt"), false)
			reader := &manifest.FileReader{Reader: owner, Source: source}
			file := "app/input.txt"
			if !tc.failed {
				file = tc.indexed
			}
			contentChanged := func(manifest.FileReader) bool { return true }
			assert.Equal(t, c.completeChangedClaim(&completeChangedClaimState{
				reader: reader, file: &file, delta: &delta, input: &input, contentChanged: &contentChanged,
			}), !tc.failed)
			if !tc.failed {
				assert.Equal(t, file, tc.file)
			}
		})
	}
	t.Run("failed_file_evidence", func(t *testing.T) {
		t.Parallel()
		source := manifest.BootstrapSourceID
		facts := &solverFacts{sources: map[manifest.NamedResource]SourceFacts{
			source: {
				Input:  &store.SourceArtifact{LocalPath: "/workspace"},
				Paired: true,
				Delta: detectChanges(
					t,
					[]string{"input.txt"},
					[]string{"input.txt"},
				),
				Files: []SourceFile{{Path: "input.txt"}},
			},
		}}
		c := solverClosure(t, store.New(), facts)
		c.context.Root = "/workspace"
		assert.Equal(t, len(slices.Collect(c.sourceFiles(source))), 0)
		assert.Equal(
			t,
			c.affectedCoordinates(".", facts.sources[source].Input, true),
			affectedLocation{base: ".", known: true},
		)
		assert.Equal(
			t,
			c.affectedSourceRoots(
				manifest.NamedResource{},
				source,
				&SourceReadPaths{Roots: []string{"."}},
				&manifest.DiscoveryRecord{},
			),
			affectedResult{settled: true},
		)
	})
}

func TestAdmission_FileCoordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, root, file string
		delta            *change.Set
		input            *store.SourceArtifact
		paired, want     bool
	}{
		{name: "nil", file: "file"},
		{name: "unpaired", delta: detectChanges(t, []string{"file"}, []string{"file"}), input: &store.SourceArtifact{LocalPath: "relative"}, root: "/work", file: "file", want: true},
		{name: "paired_without_input", delta: detectChanges(t, []string{"file"}, []string{"file"}), root: "/work", paired: true, file: "file", want: true},
		{name: "normalized", delta: detectChanges(t, []string{"file"}, []string{"file"}), input: &store.SourceArtifact{LocalPath: "/work"}, root: "/work", paired: true, file: "dir/../file", want: true},
		{name: "origin", delta: detectChanges(t, []string{"nested/file"}, []string{"nested/file"}), input: &store.SourceArtifact{LocalPath: "/work"}, root: "/work/nested", paired: true, file: "file", want: true},
		{name: "failed_coordinates", delta: detectChanges(t, []string{"file"}, []string{"file"}), input: &store.SourceArtifact{LocalPath: "relative"}, root: "/work", paired: true, file: "file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			facts := &solverFacts{sources: map[manifest.NamedResource]SourceFacts{
				manifest.BootstrapSourceID: {Delta: tc.delta, Input: tc.input, Paired: tc.paired},
			}}
			c := solverClosure(t, store.New(), facts)
			c.context.Root = tc.root
			assert.Equal(t, c.fileChanged(manifest.BootstrapSourceID, tc.file), tc.want)
		})
	}
}

func TestAdmission_DeclarationMountSource(t *testing.T) {
	t.Parallel()
	for _, aliasMounted := range []bool{false, true} {
		t.Run(strconv.FormatBool(aliasMounted), func(t *testing.T) {
			t.Parallel()
			root := "/workspace"
			bootstrap := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			alias := &store.SourceArtifact{
				LocalPath: "/alias",
				LocalRoot: root,
				Provider:  store.ProviderAlias,
				FS:        fstest.MapFS{},
			}
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
			bootstrap := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: "/workspace",
				FS:        fstest.MapFS{},
			}
			alias := &store.SourceArtifact{
				LocalPath: "/alias",
				Provider:  map[bool]store.SourceProvider{true: store.ProviderAlias}[!tc.committed],
				FS:        fstest.MapFS{},
			}
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
