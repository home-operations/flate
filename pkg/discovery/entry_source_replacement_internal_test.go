package discovery

import (
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ReplacementKeepsSelfSourceMaterial(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "apps/file", "dirty local material")
	document := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindGitRepository,
		"metadata": map[string]any{"name": "self", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://fixture.invalid/self.git", "ref": map[string]any{"branch": "main"}},
	}
	parsed, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	st := store.New()
	st.AddObject(parsed.Object)
	result, err := Run(t.Context(), Config{Path: root, Store: st, SelfURLs: []string{"ssh://git@fixture.invalid/self"}})
	assert.NoError(t, err)
	t.Cleanup(result.Close)
	id := parsed.Object.Named()
	before := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	document["metadata"] = map[string]any{
		"name":      "self",
		"namespace": "apps",
		"labels":    map[string]any{"rendered": "true"},
	}
	updated, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	replacement := updated.Object
	st.AddObject(replacement)
	assert.NoError(t, constructAdmissionTest(t.Context(), result.Admission, id))
	child, material, err := result.Admission.Resolve(t.Context(), id, replacement, nil)
	assert.NoError(t, err)
	assert.Equal(t, child.Obj == replacement, true)
	assert.Equal(t, material.SameMaterial(before), true)
	assert.Equal(t, material.Reads.Valid(), true)
	data, err := fs.ReadFile(material.FS, "apps/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "dirty local material")
}

func TestAdmission_ReplacementWithdrawsSyntheticArtifact(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, supplied := range []bool{false, true} {
				t.Run(fmt.Sprintf("supplied=%t", supplied), func(t *testing.T) {
					t.Parallel()
					st := store.New()
					result, err := Run(t.Context(), Config{Path: t.TempDir(), Store: st})
					assert.NoError(t, err)
					t.Cleanup(result.Close)
					id := manifest.NamedResource{
						Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "shared",
					}
					published, err := result.Admission.Publish(
						t.Context(), id, aliasRequestReads(t, st, id),
						func(store.Child) (bool, error) { return true, nil },
					)
					assert.NoError(t, err)
					assert.Equal(t, published, true)
					assert.NoError(t, constructAdmissionTest(t.Context(), result.Admission, id))
					alias := st.GetArtifact(id, store.ArtifactContent)
					assert.Equal(t, alias != nil, true)
					actual, err := manifest.ParseDoc(map[string]any{
						"apiVersion": manifest.SourceDomain + "/v1",
						"kind":       kind,
						"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
						"spec": map[string]any{
							"url":        "https://fixture.invalid/actual",
							"endpoint":   "fixture.invalid",
							"bucketName": "actual",
						},
					}, manifest.ParseDocOptions{})
					assert.NoError(t, err)
					st.AddObject(actual.Object)
					var material *store.SourceArtifact
					if supplied {
						material = &store.SourceArtifact{FS: fstest.MapFS{}, LocalPath: t.TempDir()}
						st.SetArtifact(id, material)
					}
					assert.NoError(t, constructAdmissionTest(t.Context(), result.Admission, id))
					child, local, err := result.Admission.Resolve(t.Context(), id, actual.Object, nil)
					assert.NoError(t, err)
					assert.Equal(t, child.Obj == actual.Object, true)
					assert.Equal(t, local == nil, true)
					if supplied {
						assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == material, true)
					} else {
						assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == nil, true)
					}
				})
			}
		})
	}
}

func TestResolveEntrySources_RefreshPreservesSuppliedArtifact(t *testing.T) {
	t.Parallel()
	st := store.New()
	d := Session{cfg: Config{Path: t.TempDir(), Store: st}, loader: loader.New(st)}
	t.Cleanup(d.loader.Close)
	t.Cleanup(d.Close)
	_, err := d.resolveTestSources(t.Context(), false)
	assert.NoError(t, err)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "shared",
	}
	a := d.admission()
	reads := aliasRequestReads(t, st, id)
	published, err := a.Publish(t.Context(), id, reads, func(store.Child) (bool, error) { return true, nil })
	assert.NoError(t, err)
	assert.Equal(t, published, true)
	accepted := d.resolvedSources.snapshot()[id]
	status, ready := st.GetStatus(id)
	material := &store.SourceArtifact{FS: fstest.MapFS{}, LocalPath: t.TempDir()}
	st.SetArtifact(id, material)
	_, err = d.resolveTestSources(t.Context(), true)
	assert.NoError(t, err)
	if st.GetArtifact(id, store.ArtifactContent) != material {
		t.Fatal("discovery refresh removed the supplied artifact")
	}
	assert.Equal(t, st.GetDeclaration(id) == accepted.object, true)
	assert.Equal(t, d.resolvedSources.snapshot()[id].synthetic, true)
	assert.Equal(t, d.resolvedSources.snapshot()[id].artifact == accepted.artifact, true)
	assert.NoError(t, d.loader.FinalizeIndex(t.Context(), d.cfg.Path, true))
	_, err = d.resolveTestSources(t.Context(), true)
	assert.NoError(t, err)
	assert.Equal(t, st.GetArtifact(id, store.ArtifactContent) == material, true)
	assert.Equal(t, st.GetDeclaration(id) == accepted.object, true)
	assert.Equal(t, d.resolvedSources.snapshot()[id].synthetic, true)
	currentStatus, currentReady := st.GetStatus(id)
	assert.Equal(t, currentReady, ready)
	assert.Equal(t, currentStatus, status)
	child, artifact, err := a.Resolve(t.Context(), id, accepted.object, nil)
	assert.NoError(t, err)
	assert.Equal(t, child.Obj == nil, true)
	assert.Equal(t, artifact == nil, true)
}
