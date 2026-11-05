package discovery_test

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDiscovery_FixtureEmptySourcePathOwnership(t *testing.T) {
	t.Parallel()
	var explicit *discovery.Result
	for _, variant := range []string{"explicit", "omitted"} {
		fixture := testfixture.Materialize(t, "source-bootstrap", "source-ref--empty-path-matches-root-"+variant)
		st := store.New()
		result, err := discovery.Run(t.Context(), discovery.Config{
			Path: filepath.Join(fixture.Repo, "flux"), RepoRoot: fixture.Repo, Store: st,
		})
		assert.NoError(t, err)
		parent := testutil.KustomizationID("flux-system", "root")
		child := testutil.KustomizationID("apps", "child")
		record, _ := result.Index.Lookup(child)
		assert.FatalIf(
			t,
			st.GetObject(child) == nil || record.Parent != parent,
			"source-root child parent = %v, want %v",
			record.Parent,
			parent,
		)
		owners := result.Index.OwnersOfFile("cm.yaml").Owners
		assert.FatalIf(
			t,
			!slices.Equal(owners, []manifest.NamedResource{parent}),
			"source-root ConfigMap owners = %v, want [%v]",
			owners,
			parent,
		)
		if explicit == nil {
			explicit = result
			continue
		}
		for id, got := range result.Index.Files() {
			want, ok := explicit.Index.Lookup(id)
			assert.FatalIf(t, !ok, "unexpected discovered resource %s", id)
			assert.Equal(t, got.File, want.File)
			assert.Equal(t, got.Parent, want.Parent)
		}
	}
}

func TestDiscovery_FixturePinnedConsumerReplacement(t *testing.T) {
	t.Parallel()
	for _, variant := range []struct {
		name, survivor, path, source string
	}{
		{name: "removed"},
		{name: "replaced"},
		{name: "different-name", survivor: "b-survivor", path: "./apps/broken", source: "unknown"},
		{name: "different-path", survivor: "a-held", path: "./ok", source: "unknown"},
		{name: "different-source-ref", survivor: "a-held", path: "./apps/broken", source: "flux-system"},
	} {
		t.Run(variant.name, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(
				t,
				"source-pins",
				"source-ref--pinned-tree-replaces-held-consumer-"+variant.name,
			)
			l := loader.New(store.New())
			l.SourceRoot = fixture.Repo
			_, err := l.Load(t.Context(), filepath.Join(fixture.Repo, "apps", "broken"), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			l.Admit(fixture.Repo)
			held := l.EntryFailure(t.Context(), fixture.Repo)
			assert.FatalIf(t, !errors.Is(held, manifest.ErrInput), "expected held consumer decode error, got %v", held)
			st := store.New()
			_, err = discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(fixture.Repo, "flux"), Store: st})
			assert.NoError(t, err)
			if variant.survivor == "" {
				return
			}
			id := testutil.KustomizationID("flux-system", variant.survivor)
			ks, ok := st.Get[*manifest.Kustomization](id)
			assert.FatalIf(
				t,
				!ok || ks.Path != variant.path || ks.SourceRef.Name != variant.source || ks.SourceRef.Namespace != "",
				"surviving consumer = %+v",
				ks,
			)
			if variant.survivor != "a-held" {
				id.Name = "a-held"
				assert.FatalIf(t, st.GetObject(id) != nil, "held consumer survived replacement")
			}
		})
	}
}
