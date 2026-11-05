package discovery

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestBegin_AuthoredSourceAuthority(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		name := "full"
		if changed {
			name = "changed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := authoredSourceConfig(changed)
			st, input := cfg.Store, cfg.Input
			session, err := Begin(t.Context(), cfg)
			assert.NoError(t, err)
			id := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository,
				Namespace: "apps", Name: "self",
			}
			record := session.loader.Index.Records[id]
			if !slices.Contains(record.Productions, manifest.Production{Source: manifest.BootstrapSourceID}) {
				t.Fatalf("missing invocation: resolution=%+v record=%+v", session.resolvedSources[id], record)
			}
			assert.Equal(t, st.GetDeclaration(id), session.resolvedSources[id].object)
			assert.FatalIf(t, st.GetArtifact(id, store.ArtifactContent) == nil, "missing local content")
			assert.NoError(t, session.refreshDeclarations(t.Context(), input.LocalPath))
			result, err := session.Freeze(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent), store.Artifact(input))
			assert.Equal(t, st.GetDeclaration(id), session.resolvedSources[id].object)
			assert.Equal(t, slices.Contains(result.Index.Records[id].Productions,
				manifest.Production{Source: manifest.BootstrapSourceID}), true)
			testAuthoredSourceWithdrawal(t, st, result.Admission, id)
		})
	}
}

func testAuthoredSourceWithdrawal(t *testing.T, st *store.Store, admission *Admission, id manifest.NamedResource) {
	t.Helper()
	seed := st.GetDeclaration(id)
	owner := testutil.KustomizationID("apps", "root")
	var captured store.ReadSet
	captured.Init(st, id, nil)
	defer captured.Retire()
	assert.Equal(t, captured.GetDeclaration(id), seed)
	resolution, err := admission.Resolve(t.Context(), &captured, ResolveRequest{ID: id, Object: seed})
	assert.NoError(t, err)
	artifact, err := resolution.Resolve(t.Context(), manifest.DrainNone)
	assert.NoError(t, err)
	assert.FatalIf(t, artifact == nil || !artifact.InputAlias, "invocation did not supply the source")
	for gate := range captured.Gates() {
		t.Fatalf("bootstrap formed an owner edge: %+v", gate)
	}
	replacement := &manifest.GitRepository{
		Name:      id.Name,
		Namespace: id.Namespace,
		URL:       "https://fixture.invalid/foreign",
	}
	tasks := task.NewBounded(2)
	published := make(chan struct{})
	read := make(chan struct{})
	tasks.Go(t.Context(), "publish", func(context.Context) {
		st.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: replacement}}, Complete: true}, nil)
		close(published)
		tasks.YieldSlot(func() { <-read })
		st.RecordEmitted(owner, &store.Emission{Complete: true}, nil)
	})
	tasks.Go(t.Context(), "read", func(ctx context.Context) {
		defer close(read)
		tasks.YieldSlot(func() { <-published })
		assert.Equal(t, captured.Valid(), false)
		var fresh store.ReadSet
		fresh.Init(st, id, nil)
		defer fresh.Retire()
		assert.Equal(t, fresh.GetDeclaration(id), manifest.BaseManifest(replacement))
		candidate := admission.sourceCandidate(&fresh, &ResolveRequest{ID: id, Object: replacement})
		assert.Equal(t, candidate.bootstrap, false)
		assert.Equal(t, fresh.Valid(), true)
		resolved, resolveErr := admission.Resolve(ctx, &fresh, ResolveRequest{ID: id, Object: replacement})
		assert.NoError(t, resolveErr)
		assert.Equal(t, resolved.Object, manifest.BaseManifest(replacement))
	})
	tasks.BlockTillDone()
	assert.Equal(t, st.GetDeclaration(id), nil)
	var removed store.ReadSet
	removed.Init(st, id, nil)
	defer removed.Retire()
	resolution, err = admission.Resolve(t.Context(), &removed, ResolveRequest{ID: id})
	assert.NoError(t, err)
	assert.Equal(t, resolution.Object, nil)
	assert.Equal(t, resolution.Resolve == nil, true)
}

func authoredSourceConfig(changed bool) Config {
	const root = "/virtual/bootstrap-authority"
	const url = "https://fixture.invalid/self"
	return Config{
		Store: store.New(), Path: filepath.Join(root, "entry"), SelfURLs: []string{url},
		Selection: manifest.Selection{Changed: changed},
		Input: &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"entry/kustomization.yaml": {Data: []byte("resources: [root.yaml, source.yaml]\n")},
			"entry/root.yaml": {Data: []byte(testutil.KustomizationYAML("root", "apps",
				"  path: ./entry\n  sourceRef: {kind: GitRepository, name: self}\n"))},
			"entry/source.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
				"metadata: {name: self, namespace: apps}\nspec: {url: " + url + "}\n")},
		}},
	}
}

func TestRun_AuthoredSourceInputBinding(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"full", "changed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			cfg := authoredSourceConfig(mode == "changed")
			result, err := Run(t.Context(), cfg)
			assert.NoError(t, err)
			st := cfg.Store
			id := manifest.NamedResource{
				Group: manifest.SourceDomain, Kind: manifest.KindGitRepository,
				Namespace: "apps", Name: "self",
			}
			seed := st.GetDeclaration(id)
			assert.FatalIf(t, seed == nil, "source declaration was not admitted")
			var before store.ReadSet
			before.Init(st, id, nil)
			defer before.Retire()
			assert.Equal(t, before.GetDeclaration(id), seed)
			artifact, ok := before.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, !ok || artifact.Reads == nil, "source alias was not captured")
			st.SetIncluded(manifest.BootstrapSourceID, false)
			assert.Equal(t, before.Valid(), true)
			next := *cfg.Input
			next.Content = "changed-input"
			st.SetArtifact(manifest.BootstrapSourceID, &next)
			assert.Equal(t, before.Valid(), false)
			assert.Equal(t, artifact.Reads.Valid(), false)
			assert.Equal(t, st.GetDeclaration(id), nil)
			var fresh store.ReadSet
			fresh.Init(st, id, nil)
			defer fresh.Retire()
			candidate := result.Admission.sourceCandidate(&fresh, &ResolveRequest{ID: id, Object: seed})
			assert.Equal(t, candidate.bootstrap, false)
		})
	}
}
