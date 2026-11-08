package external_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"

	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/external"
	"github.com/home-operations/flate/internal/store"
)

func TestFetcher_NoArtifact(t *testing.T) {
	t.Parallel()
	for _, predicted := range []bool{false, true} {
		t.Run(strconv.FormatBool(predicted), func(t *testing.T) {
			t.Parallel()
			f := &external.Fetcher{}
			ea := &manifest.ExternalArtifact{Name: "ea", Namespace: "apps"}
			s := store.New()
			if predicted {
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				s.AddObject(owner)
				generator := &manifest.RawObject{
					APIVersion: "source.extensions.fluxcd.io/v1beta1",
					Kind:       "ArtifactGenerator",
					Name:       "generator",
					Namespace:  "apps",
					Spec:       map[string]any{"artifacts": []any{map[string]any{"name": ea.Name}}},
				}
				s.RecordEmitted(
					owner.Named(),
					&store.Emission{Children: []store.Child{{Obj: generator}}, Complete: true},
					nil,
				)
				assert.FatalIf(t, len(s.Productions(ea.Named(), nil)) != 1, "%s", "missing producer evidence")
			}
			assert.FatalIf(
				t,
				s.GetObject(ea.Named()) != nil || s.GetArtifact(ea.Named(), store.ArtifactContent) != nil,
				"%s",
				"prediction supplied source content",
			)

			_, err := fetchtest.Fetch(t.Context(), f.Fetch, ea, testReadSet(ea), &sourcecache.Policy{}, nil)
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
				err.Error() != "no generated content", "missing-content error = %v", err)
		})
	}
}
