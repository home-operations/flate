package loader

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestInputCache_ContextualSelectorReplay(t *testing.T) {
	t.Parallel()
	for _, direction := range []string{"forward", "reverse"} {
		t.Run(direction, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			persistent := cache.RenderPersistenceDisabledReason() == ""
			assert.Equal(t, owner.CanPersistRenders(), persistent)
			before, after := contextualSelectorFiles(), contextualSelectorFiles()
			before["providers/provider.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: provider, namespace: apps, labels: {role: y}, annotations: {revision: before}}
spec: {type: Static}
`)}
			after["providers/provider.yaml"] = &fstest.MapFile{Data: []byte(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: provider, namespace: apps, labels: {role: y}, annotations: {revision: after}}
spec: {type: Static}
`)}
			if direction == "reverse" {
				before, after = after, before
			}
			first := cachedFixtureLoader(t, root, before, owner)
			assert.NoError(t, first.PersistInputs(t.Context()))
			first.Close()
			reader := manifest.NamedResource{
				Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "reader",
			}
			load := func(cached bool) *Loader {
				view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
					gittree.ViewOptions{Mode: gittree.Direct, Current: after})
				assert.NoError(t, err)
				st := store.New()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
				l := New(st)
				t.Cleanup(l.Close)
				l.SourceRoot = root
				if cached {
					l.Options.Cache = owner
				}
				l.Options.Changes = change.NewSet([]string{"providers/provider.yaml"})
				configureSelection(t, l, selection.Request{Changed: true})
				l.documents.compact = true
				l.mountSources()
				assert.NoError(t, l.restoreCachedInputs(t.Context()))
				file := filepath.Join(root, "readers", "set.yaml")
				if cached {
					doc, known := l.documents.cache[file]
					assert.Equal(t, known, persistent)
					assert.Equal(t, doc.deferred != nil, persistent)
					assert.Equal(t, len(doc.objects), 0)
					objects, censusErr := l.documents.censusFile(t.Context(), file, manifest.ParseDocOptions{})
					assert.NoError(t, censusErr)
					assert.Equal(t, len(objects), 1)
					assert.Equal(t, objects[0].fact.Authored, reader)
					assert.Equal(t, objects[0].fact.facts.structural == nil, true)
					assert.Equal(t, objects[0].fact.facts.refs[0].Selector.MatchExpressions[0].Key, "${KEY}")
					t.Log("accepted reader frame restored authored selector facts before resolution")
				}
				_, err = l.Load(t.Context(), filepath.Join(root, "root"), manifest.NamedResource{}, nil)
				assert.NoError(t, err)
				objects, err := l.documents.censusFile(t.Context(), file, manifest.ParseDocOptions{})
				assert.NoError(t, err)
				authored := objects[0].fact.facts.refs[0].Selector.DeepCopy()
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
				assert.Diff(t, objects[0].fact.facts.refs[0].Selector, authored)
				assert.Equal(t, authored.MatchExpressions[0].Key, "${KEY}")
				return l
			}
			fresh, restored := load(false), load(true)
			want, known := fresh.Occurrence(reader)
			assert.Equal(t, known, true)
			got, known := restored.Occurrence(reader)
			assert.Equal(t, known, true)
			assert.Equal(t, want.Refs[0].Selector.MatchExpressions[0].Key, "role")
			assert.Equal(t, string(want.Refs[0].Selector.MatchExpressions[0].Operator), "NotIn")
			assert.Diff(t, want.Refs[0].Selector.MatchExpressions[0].Values, []string{"x"})
			assert.Equal(t, cmp.Diff(want.Refs, got.Refs, cmp.AllowUnexported(manifest.Reference{})), "")
			var selected [2][]manifest.NamedResource
			for i, l := range []*Loader{fresh, restored} {
				proposal, err := prepareSelection(t.Context(), t, l, selection.PrepareRequest{Invocation: true})
				assert.NoError(t, err)
				selected[i] = slices.Clone(proposal.Selected)
				assert.Equal(t, slices.Contains(proposal.Selected, reader), true)
				proposal.Retire()
			}
			assert.Diff(t, selected[1], selected[0])
		})
	}
}

func TestInputCache_ExternalFileDeclarationReplay(t *testing.T) {
	t.Parallel()
	for _, transition := range []string{"addition", "removal", "replacement"} {
		t.Run(transition, func(t *testing.T) {
			t.Parallel()
			for _, direction := range []string{"forward", "reverse"} {
				t.Run(direction, func(t *testing.T) {
					t.Parallel()
					root, cacheDir := t.TempDir(), t.TempDir()
					owner, err := cache.New(t.Context(), cacheDir)
					assert.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, owner.Close()) })
					persistent := cache.RenderPersistenceDisabledReason() == ""
					assert.Equal(t, owner.CanPersistRenders(), persistent)
					hr := testutil.HelmReleaseID("default", "demo")
					parent := testutil.KustomizationID("flux-system", "parent")
					files := externalFileHelmReleaseFiles(t)
					load := func(files fstest.MapFS, inputCache *cache.Cache, accepted bool) *Loader {
						view, _, viewErr := gittree.New(t.Context(), nil, plumbing.ZeroHash,
							gittree.ViewOptions{Mode: gittree.Direct, Current: files})
						assert.NoError(t, viewErr)
						st := store.New()
						st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
						l := New(st)
						t.Cleanup(l.Close)
						l.SourceRoot = root
						l.Options.Cache = inputCache
						configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{hr}, Changed: true})
						l.Options.Changes = change.NewSet([]string{"layer/common/child.yaml"})
						l.documents.compact = true
						l.mountSources()
						assert.NoError(t, l.restoreCachedInputs(t.Context()))
						switch {
						case !accepted:
						case persistent:
							file := filepath.Join(root, "layer", "common", "child.yaml")
							doc, known := l.documents.cache[file]
							assert.FatalIf(
								t,
								!known || doc.deferred == nil,
								"external declaration cache frame was not accepted",
							)
							assert.Equal(t, len(doc.objects), 0)
							objects, censusErr := l.documents.censusFile(t.Context(), file, manifest.ParseDocOptions{})
							assert.NoError(t, censusErr)
							assert.Equal(t, len(objects), 1)
							assert.Equal(t, objects[0].fact.Authored, testutil.KustomizationID("flux-system", "child"))
							assert.Equal(t, objects[0].fact.facts.path, "./payload/one")
							t.Log("accepted external declaration frame before authored resolution")
						default:
							_, known := l.documents.cache[filepath.Join(root, "layer", "common", "child.yaml")]
							assert.Equal(t, known, false)
						}
						_, loadErr := l.Load(t.Context(), filepath.Join(root, "site-a"), manifest.NamedResource{}, nil)
						assert.NoError(t, loadErr)
						assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
						return l
					}
					type closure struct {
						Identity, Authored manifest.NamedResource
						Path               string
						ChildProductions   []manifest.Production
						PayloadProductions []manifest.Production
						Selected           []manifest.NamedResource
					}
					snapshot := func(l *Loader, name string) closure {
						var result closure
						childID := testutil.KustomizationID("flux-system", "child")
						otherID := testutil.KustomizationID("flux-system", "other")
						_, childKnown := l.Index.Lookup(childID)
						_, otherKnown := l.Index.Lookup(otherID)
						assert.Equal(t, childKnown, name == "child")
						assert.Equal(t, otherKnown, name == "other")
						assert.Equal(t, l.Store.GetObject(childID) == nil, true)
						assert.Equal(t, l.Store.GetObject(otherID) == nil, true)
						payload, known := l.Index.Lookup(hr)
						assert.Equal(t, known, name != "")
						want := map[string]struct {
							id         manifest.NamedResource
							path, file string
							parent     manifest.NamedResource
						}{
							"child": {childID, "./payload/one", "layer/common/child.yaml", parent},
							"other": {otherID, "./payload/one", "layer/common/child.yaml", parent},
						}[name]
						id := testutil.KustomizationID("flux-system", name)
						child, found := l.Occurrence(id)
						assert.FatalIf(t, found != (name != ""), "child occurrence availability: %s", id)
						assert.Equal(t, child.Identity, id)
						assert.Equal(t, child.Authored, want.id)
						assert.Equal(t, child.Path, want.path)
						assert.Equal(t, child.Declaration.File, want.file)
						record, _ := l.Index.Lookup(id)
						assert.Equal(t, record.Parent, want.parent)
						assert.Equal(t, slices.Contains(testutil.Owners(record.Productions), parent), name != "")
						assert.Equal(t, slices.Contains(l.Index.Claims["payload/one/"], id), name != "")
						assert.Equal(t, slices.Contains(testutil.Owners(payload.Productions), id), name != "")
						result.Identity, result.Authored, result.Path = want.id, child.Authored, child.Path
						result.ChildProductions = slices.Clone(record.Productions)
						result.PayloadProductions = slices.Clone(payload.Productions)
						proposal, prepareErr := prepareSelection(
							t.Context(),
							t,
							l,
							selection.PrepareRequest{Invocation: true},
						)
						assert.NoError(t, prepareErr)
						result.Selected = slices.Clone(proposal.Selected)
						assert.Equal(t, slices.Contains(result.Selected, hr), name != "")
						proposal.Retire()
						return result
					}
					first := load(files, owner, false)
					original := snapshot(first, "child")
					assert.NoError(t, first.PersistInputs(t.Context()))
					first.Close()
					assert.NoError(t, owner.Close())
					owner, err = cache.New(t.Context(), cacheDir)
					assert.NoError(t, err)
					assert.Equal(t, owner.CanPersistRenders(), persistent)
					restored := load(files, owner, true)
					assert.Diff(t, snapshot(restored, "child"), original)
					before, after := externalFileHelmReleaseFiles(t), externalFileHelmReleaseFiles(t)
					const declaration = "layer/common/child.yaml"
					absent := &fstest.MapFile{Data: []byte(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker, namespace: flux-system}\n",
					)}
					replacement := strings.Replace(string(after[declaration].Data), "name: child", "name: other", 1)
					assert.Equal(t, len(replacement), len(before[declaration].Data))
					type side struct {
						files fstest.MapFS
						name  string
					}
					type declarationSide struct {
						name string
						file *fstest.MapFile
					}
					declarations := map[string][2]declarationSide{
						"addition":    {{"", absent}, {"child", after[declaration]}},
						"removal":     {{"child", before[declaration]}, {"", absent}},
						"replacement": {{"child", before[declaration]}, {"other", &fstest.MapFile{Data: []byte(replacement)}}},
					}[transition]
					before[declaration], after[declaration] = declarations[0].file, declarations[1].file
					pair := [2]side{{before, declarations[0].name}, {after, declarations[1].name}}
					sides := map[string][2]side{"forward": pair, "reverse": {pair[1], pair[0]}}[direction]
					for _, side := range sides {
						view, _, viewErr := gittree.New(t.Context(), nil, plumbing.ZeroHash,
							gittree.ViewOptions{Mode: gittree.Direct, Current: side.files})
						assert.NoError(t, viewErr)
						restored.Store.SetArtifact(
							manifest.BootstrapSourceID,
							&store.SourceArtifact{LocalPath: root, FS: view},
						)
						_, loadErr := restored.Load(
							t.Context(),
							filepath.Join(root, "site-a"),
							manifest.NamedResource{},
							nil,
						)
						assert.NoError(t, loadErr)
						assert.NoError(t, restored.FinalizeIndex(t.Context(), root, false))
						fresh := load(side.files, nil, false)
						assert.Diff(t, snapshot(restored, side.name), snapshot(fresh, side.name))
						construction := restored.construction
						assert.NoError(t, restored.FinalizeIndex(t.Context(), root, false))
						assert.Equal(t, restored.construction, construction)
					}
					assert.Equal(t, original.Identity, testutil.KustomizationID("flux-system", "child"))
					assert.Equal(t, original.Path, "./payload/one")
				})
			}
		})
	}
}
