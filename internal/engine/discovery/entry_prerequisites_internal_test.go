package discovery

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryCandidateReady_UnconditionalBootstrap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		canonical     bool
		sparse        []string
		includes      []sourcev1.GitRepositoryInclude
		pinned        bool
		replaced      bool
		prerequisite  bool
		unconditional bool
	}{
		{name: "bootstrap", canonical: true, unconditional: true},
		{name: "mutable alias", unconditional: true},
		{name: "sparse alias", sparse: []string{"elsewhere"}},
		{
			name: "composed alias",
			includes: []sourcev1.GitRepositoryInclude{
				{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}},
			},
		},
		{name: "pin", pinned: true},
		{name: "replaced artifact", replaced: true},
		{name: "prerequisite", prerequisite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir(), FS: fstest.MapFS{}}
			owner := &manifest.Kustomization{
				Name: "owner", Namespace: manifest.DefaultNamespace, Path: "./missing",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository, Name: "fleet",
				},
			}
			if tc.canonical {
				owner.SourceRef.Name = manifest.BootstrapSourceID.Name
			}
			if tc.prerequisite {
				owner.DependsOn = []kustomizev1.DependencyReference{{Name: "dependency"}}
			}
			sourceID := owner.EffectiveSourceID()
			selected := entryResolution{input: input, artifact: input}
			if !tc.canonical {
				git := &manifest.GitRepository{Name: sourceID.Name, Namespace: sourceID.Namespace}
				git.SparseCheckout = tc.sparse
				object := *git
				object.Include = tc.includes
				selected.git, selected.object = git, &object
				artifact, _, err := new(Admission).entrySourceView(t.Context(), &selected)
				assert.NoError(t, err)
				selected.artifact = artifact
			}
			source := selected.artifact
			if tc.pinned {
				source.Provider, source.Content = store.ProviderCommit, "commit"
			}
			if tc.replaced {
				source = &store.SourceArtifact{
					Kind: source.Kind, LocalPath: source.LocalPath, LocalRoot: source.LocalRoot,
					FS: source.FS, Provider: store.ProviderAlias,
				}
			}
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, input)
			st.SetArtifact(sourceID, source)
			d := newDiscoverer(new(testConfig(t, Config{Store: st, Input: input})))
			t.Cleanup(d.Close)
			d.resolvedSources = &entrySourcePublisher{
				mutable: map[manifest.NamedResource]entryResolution{sourceID: selected},
			}
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			t.Cleanup(reads.ReleaseCapture)
			t.Cleanup(reads.Retire)
			assert.Equal(t, d.entryCandidateReady(t.Context(), &reads, owner), tc.unconditional)
			assert.Equal(t, d.prepared[owner.Named()] == nil, tc.unconditional)
			st.SetArtifact(sourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestRun_EntryPreparationFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const configMap = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: duplicate}\n"
	owner := &manifest.Kustomization{
		Name: "owner", Namespace: "apps", Path: "./entry",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "fleet"},
	}
	encoded, err := json.Marshal(testutil.ObjectDocument(owner))
	assert.NoError(t, err)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
		"entry/kustomization.yaml": {Data: []byte("resources: [owner.yaml, first.yaml, second.yaml]\n")},
		"entry/owner.yaml":         {Data: encoded},
		"entry/first.yaml":         {Data: []byte(configMap)},
		"entry/second.yaml":        {Data: []byte(configMap)},
	}}
	st := store.New()
	st.AddObject(owner)
	d, err := beginTest(t.Context(), testConfig(t, Config{Store: st, Input: input, Path: filepath.Join(root, "entry")}))
	assert.NoError(t, err)
	t.Cleanup(d.Close)

	result, err := d.Run(t.Context())
	assert.NoError(t, err)
	assert.FatalIf(t, result == nil || result.Binding.Failure == nil, "entry returned no terminal failure")
	assert.Equal(t, result.Binding.Producer, owner.Named())
	assert.Equal(t, result.Binding.Source, owner.EffectiveSourceID())
	assert.Equal(t, errors.Is(result.Binding.Failure, manifest.ErrFlux), true)
	assert.FatalIf(t, !strings.Contains(result.Binding.Failure.Error(), "already registered id"),
		"entry lost the underlying cause: %v", result.Binding.Failure)
	assert.Equal(t, len(d.PendingSources()), 0)

	repeated, err := d.Run(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, repeated, result)
}
