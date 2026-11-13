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

//nolint:gocognit // Accepted replay MUST retain authored facts and match fresh selection in both diff directions.
func TestInputCache_ContextualSelectorReplay(t *testing.T) {
	t.Parallel()
	for _, direction := range []string{"forward", "reverse"} {
		t.Run(direction, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			if !owner.CanPersistRenders() {
				t.Fatal("input cache witness requires a clean stamped test binary")
			}
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
					assert.FatalIf(t, !known || doc.deferred == nil, "reader cache frame was not accepted")
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
			if diff := cmp.Diff(want.Refs, got.Refs, cmp.AllowUnexported(manifest.Reference{})); diff != "" {
				t.Error(diff)
			}
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

//nolint:gocognit // Accepted external frames MUST preserve both diff sides and remove stale declarations during reuse.
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
					if !owner.CanPersistRenders() {
						t.Fatal("input cache witness requires a clean stamped test binary")
					}
					hr := testutil.HelmReleaseID("default", "demo")
					parent := testutil.KustomizationID("flux-system", "parent")
					files := externalFileHelmReleaseFiles(t)
					load := func(files fstest.MapFS, cached bool, accepted bool) *Loader {
						view, _, viewErr := gittree.New(t.Context(), nil, plumbing.ZeroHash,
							gittree.ViewOptions{Mode: gittree.Direct, Current: files})
						assert.NoError(t, viewErr)
						st := store.New()
						st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
						l := New(st)
						t.Cleanup(l.Close)
						l.SourceRoot = root
						if cached {
							l.Options.Cache = owner
						}
						configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{hr}, Changed: true})
						l.Options.Changes = change.NewSet([]string{"layer/common/child.yaml"})
						l.documents.compact = true
						l.mountSources()
						assert.NoError(t, l.restoreCachedInputs(t.Context()))
						if accepted {
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
						for _, candidate := range []string{"child", "other"} {
							id := testutil.KustomizationID("flux-system", candidate)
							_, known := l.Index.Lookup(id)
							assert.Equal(t, known, candidate == name)
							assert.Equal(t, l.Store.GetObject(id) == nil, true)
						}
						payload, known := l.Index.Lookup(hr)
						assert.Equal(t, known, name != "")
						if name != "" {
							id := testutil.KustomizationID("flux-system", name)
							child, found := l.Occurrence(id)
							assert.FatalIf(t, !found, "child occurrence is absent: %s", id)
							assert.Equal(t, child.Identity, id)
							assert.Equal(t, child.Authored, id)
							assert.Equal(t, child.Path, "./payload/one")
							assert.Equal(t, child.Declaration.File, "layer/common/child.yaml")
							record, _ := l.Index.Lookup(id)
							assert.Equal(t, record.Parent, parent)
							assert.Equal(t, slices.Contains(testutil.Owners(record.Productions), parent), true)
							assert.Equal(t, slices.Contains(l.Index.Claims["payload/one/"], id), true)
							assert.Equal(t, slices.Contains(testutil.Owners(payload.Productions), id), true)
							result.Identity, result.Authored, result.Path = child.Identity, child.Authored, child.Path
							result.ChildProductions = slices.Clone(record.Productions)
							result.PayloadProductions = slices.Clone(payload.Productions)
						}
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
					first := load(files, true, false)
					original := snapshot(first, "child")
					assert.NoError(t, first.PersistInputs(t.Context()))
					first.Close()
					assert.NoError(t, owner.Close())
					owner, err = cache.New(t.Context(), cacheDir)
					assert.NoError(t, err)
					assert.Equal(t, owner.CanPersistRenders(), true)
					restored := load(files, true, true)
					assert.Diff(t, snapshot(restored, "child"), original)
					before, after := externalFileHelmReleaseFiles(t), externalFileHelmReleaseFiles(t)
					beforeName, afterName := "child", "child"
					const declaration = "layer/common/child.yaml"
					absent := []byte(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker, namespace: flux-system}\n",
					)
					switch transition {
					case "addition":
						before[declaration] = &fstest.MapFile{Data: absent}
						beforeName = ""
					case "removal":
						after[declaration] = &fstest.MapFile{Data: absent}
						afterName = ""
					case "replacement":
						data := strings.Replace(string(after[declaration].Data), "name: child", "name: other", 1)
						assert.Equal(t, len(data), len(before[declaration].Data))
						after[declaration] = &fstest.MapFile{Data: []byte(data)}
						afterName = "other"
					}
					if direction == "reverse" {
						before, after = after, before
						beforeName, afterName = afterName, beforeName
					}
					for _, side := range []struct {
						files fstest.MapFS
						name  string
					}{{before, beforeName}, {after, afterName}} {
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
						fresh := load(side.files, false, false)
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
