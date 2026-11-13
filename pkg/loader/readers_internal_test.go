package loader

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReaderFinalizer_ConfinedSubtrees(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input, want string
	}{
		{name: "file", input: "inputs/file", want: "inputs/file"},
		{name: "directory", input: "inputs", want: "inputs"},
		{name: "source root", input: ".", want: "."},
		{name: "missing suffix", input: "inputs/absent/deep", want: "inputs/absent/deep"},
		{name: "missing prefix", input: "absent/deep", want: "absent/deep"},
		{name: "symlink file", input: "link/file", want: "inputs/file"},
		{name: "symlink missing suffix", input: "link/absent/deep", want: "inputs/absent/deep"},
		{name: "dangling confined symlink", input: "dangling/deep", want: "inputs/absent/deep"},
		{name: "escaped path", input: "../outside"},
		{name: "absolute path", input: "/inputs"},
		{name: "remote path", input: "https://fixture.invalid/inputs"},
		{name: "escaped symlink", input: "escape/file"},
		{name: "absolute symlink", input: "absolute/file"},
		{name: "unavailable source", input: "inputs"},
		{name: "unreadable source", input: "inputs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			sourceRoot := filepath.Join(root, "source")
			testutil.WriteFile(t, sourceRoot, "inputs/file", "input")
			testutil.WriteFile(t, root, "outside/file", "unrelated")
			assert.NoError(t, os.Symlink("inputs", filepath.Join(sourceRoot, "link")))
			assert.NoError(t, os.Symlink("inputs/absent", filepath.Join(sourceRoot, "dangling")))
			assert.NoError(t, os.Symlink("../outside", filepath.Join(sourceRoot, "escape")))
			assert.NoError(t, os.Symlink("/inputs", filepath.Join(sourceRoot, "absolute")))
			st := store.New()
			admitForTest(st, root)
			source := (&manifest.GitRepository{Name: "source", Namespace: "apps"}).Named()
			switch tc.name {
			case "unavailable source":
				source.Kind = manifest.KindExternalArtifact
			case "unreadable source":
				st.SetArtifact(source, &store.SourceArtifact{LocalPath: sourceRoot, LocalRoot: root})
			default:
				st.SetArtifact(source, &store.SourceArtifact{
					LocalPath: sourceRoot, LocalRoot: root, Content: "captured", FS: os.DirFS(sourceRoot),
				})
			}
			l := New(st)
			l.Index.Readers = map[string][]manifest.FileReader{}
			rf := readerFinalizer{loader: l, repoRoot: root, sources: map[manifest.NamedResource]graphVisit{}}
			consumer := testutil.KustomizationID("apps", "reader")
			ref := manifest.Reference{NamedResource: source, Files: []string{tc.input}, Subtree: true}
			assert.NoError(t, rf.observe(t.Context(), &ref, consumer, true))
			want := map[string][]manifest.FileReader{}
			if tc.want != "" {
				want[filepath.ToSlash(filepath.Join("source", tc.want))] = []manifest.FileReader{
					{Reader: consumer, Source: source, Materialize: true},
				}
			}
			assert.Diff(t, l.Index.Readers, want)
		})
	}
}

func TestFilter_CopiedInputsPairedSides(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, from, pattern string
		files               [2]map[string]string
		selected            bool
	}{
		{name: "literal edit", from: "@repo/payload/one/cm.yaml", files: [2]map[string]string{
			{"payload/one/cm.yaml": "before"}, {"payload/one/cm.yaml": "after"},
		}, selected: true},
		{name: "literal creation", from: "@repo/payload/one/cm.yaml", files: [2]map[string]string{
			nil, {"payload/one/cm.yaml": "after"},
		}, selected: true},
		{name: "literal deletion", from: "@repo/payload/one/cm.yaml", files: [2]map[string]string{
			{"payload/one/cm.yaml": "before"}, nil,
		}, selected: true},
		{name: "root edit", from: "@repo/", files: [2]map[string]string{
			{"payload/one/cm.yaml": "before"}, {"payload/one/cm.yaml": "after"},
		}, selected: true},
		{name: "root creation", from: "@repo/", files: [2]map[string]string{
			nil, {"payload/one/cm.yaml": "after"},
		}, selected: true},
		{name: "root deletion", from: "@repo/", files: [2]map[string]string{
			{"payload/one/cm.yaml": "before"}, nil,
		}, selected: true},
		{name: "rename", from: "@repo/payload/one/**", files: [2]map[string]string{
			{"payload/one/old.yaml": "same"}, {"payload/one/new.yaml": "same"},
		}, selected: true},
		{name: "glob prefix creation", from: "@repo/payload/one/**", files: [2]map[string]string{
			nil, {"payload/one/cm.yaml": "after"},
		}, selected: true},
		{name: "glob prefix deletion", from: "@repo/payload/one/**", files: [2]map[string]string{
			{"payload/one/cm.yaml": "before"}, nil,
		}, selected: true},
		{name: "pattern directory creation", pattern: "@repo/apps/{app}", files: [2]map[string]string{
			nil, {"apps/one/cm.yaml": "after"},
		}, selected: true},
		{name: "pattern directory deletion", pattern: "@repo/apps/{app}", files: [2]map[string]string{
			{"apps/one/cm.yaml": "before"}, nil,
		}, selected: true},
		{name: "unrelated destination", from: "@repo/payload/one/**", files: [2]map[string]string{
			{"renamed/cm.yaml": "before"}, {"renamed/cm.yaml": "after"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := &manifest.GitRepository{Name: "input", Namespace: "effective"}
			raw := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator",
				Spec: map[string]any{
					"sources": []any{
						map[string]any{"alias": "repo", "kind": manifest.KindGitRepository, "name": "input"},
					},
					"pathPattern": tc.pattern,
					"artifacts": []any{map[string]any{
						"name": "bundle", "copy": []any{map[string]any{"from": tc.from, "to": "@artifact/renamed"}},
					}},
				},
			}
			id := raw.WithNamespace("effective").Named()
			var stores [2]*store.Store
			var indexes [2]*manifest.DiscoveryIndex
			var roots [2]string
			for side := range stores {
				root := t.TempDir()
				roots[side] = root
				st := store.New()
				stores[side] = st
				admitForTest(st, root)
				st.AddObject(source)
				files := fstest.MapFS{}
				for name, data := range tc.files[side] {
					files[name] = &fstest.MapFile{Data: []byte(data)}
				}
				mount := fmt.Sprintf(".inputs-%d", side)
				st.SetArtifact(source.Named(), &store.SourceArtifact{
					LocalPath: filepath.Join(root, mount), LocalRoot: root,
					Content: strconv.Itoa(side), FS: files,
				})
				l := New(st)
				l.Index.Readers = map[string][]manifest.FileReader{}
				l.Index.Records[id] = manifest.DiscoveryRecord{
					File: "generator.yaml",
					Refs: []manifest.NamedResource{source.Named()},
				}
				rf := readerFinalizer{
					loader: l, repoRoot: root, sources: map[manifest.NamedResource]graphVisit{},
					visited: map[manifest.NamedResource]struct{}{},
				}
				occurrence := compactOccurrence(raw, manifest.TemplateProjection{})
				occurrence.Identity = id
				assert.Equal(t, cmp.Diff(
					slices.Collect(manifest.References(raw.WithNamespace("effective"), manifest.ReferenceRelations)),
					slices.Collect(occurrence.references()),
					cmp.AllowUnexported(manifest.Reference{}),
				), "")
				if side == 0 {
					l.documents.compact = true
					l.locations = map[manifest.NamedResource]authoredOccurrence{id: occurrence}
				} else {
					st.AddObject(raw.WithNamespace("effective"))
				}
				assert.NoError(t, rf.collect(t.Context(), id, id, false))
				indexes[side] = l.Index
				for file := range tc.files[side] {
					readers := slices.Collect(l.Index.ReadersOfFile(mount + "/" + file))
					assert.Equal(
						t,
						slices.Contains(readers, manifest.FileReader{Reader: id, Source: source.Named()}),
						tc.selected,
					)
					assert.Equal(
						t,
						len(slices.Collect(l.Index.ReadersOfFile(fmt.Sprintf(".inputs-%d/%s", 1-side, file)))),
						0,
					)
				}
			}
			changes, err := change.Qualify(t.Context(), nil, stores, manifest.WorkScope{})
			assert.NoError(t, err)
			changes, err = changes.WithReaders(
				t.Context(),
				indexes,
				[2]map[manifest.NamedResource]inputprojection.Projection{},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Equal(t, changes.Len(), 0)
			for side, st := range stores {
				filter := newAdmission(
					t,
					admissionConfig{changes: changes, index: indexes[side], inventory: st, root: roots[side]},
				)
				assert.Equal(t, filter.Reconcile(id), tc.selected)
			}
		})
	}
}

func TestFilter_UnavailableArtifactSource(t *testing.T) {
	t.Parallel()
	for _, declared := range []bool{false, true} {
		t.Run(strconv.FormatBool(declared), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			source := &manifest.ExternalArtifact{Name: "bundle", Namespace: "apps"}
			consumer := &manifest.Kustomization{
				Name:      "consumer",
				Namespace: "apps",
				Path:      "./renamed-artifact",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindExternalArtifact,
					Name: source.Name,
				},
			}
			st := store.New()
			admitForTest(st, root)
			st.AddObject(consumer)
			git := &manifest.GitRepository{Name: "input", Namespace: "apps"}
			st.AddObject(git)
			bootstrap, _ := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			st.SetArtifact(git.Named(), bootstrap)
			generator := &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator",
				Name: "generator", Namespace: "apps",
				Spec: map[string]any{
					"sources": []any{
						map[string]any{"alias": "git", "kind": manifest.KindGitRepository, "name": git.Name},
					},
					"artifacts": []any{
						map[string]any{
							"name": source.Name,
							"copy": []any{
								map[string]any{"from": "@git/payload/one/**", "to": "@artifact/renamed-artifact"},
							},
						},
					},
				},
			}
			st.AddObject(generator)
			if declared {
				st.AddObject(source)
			}
			testutil.WriteFile(t, root, "renamed-artifact/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "renamed-artifact/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n")
			l := loaderFixture(st, map[manifest.NamedResource]string{
				consumer.Named(): "flux/consumer.yaml", generator.Named(): "flux/generator.yaml",
			})
			l.SourceRoot = root
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			assert.Equal(t, len(l.Claims(root)), 0)
			assert.Equal(t, len(l.Index.Claims), 0)
			assert.Equal(t, len(slices.Collect(l.Index.ReadersOfFile("renamed-artifact/cm.yaml"))), 0)
			changes := change.NewSet([]string{"renamed-artifact/cm.yaml"})
			l.Options.Changes = changes
			facts := l.SourceFacts(source.Named(), nil)
			assert.Equal(t, facts.External, true)
			assert.Equal(t, facts.Delta.Len(), 0)
			filter := newAdmission(t, admissionConfig{changes: changes, index: l.Index, inventory: st, root: root})
			assert.Equal(t, filter.Reconcile(consumer.Named()), false)
			assert.Equal(t, filter.Output(consumer.Named()), false)
			assert.Equal(t, filter.Reconcile(generator.Named()), false)
		})
	}
}
