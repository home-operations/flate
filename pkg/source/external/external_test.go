package external_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_NoArtifact(t *testing.T) {
	t.Parallel()
	for _, predicted := range []bool{false, true} {
		t.Run(fmt.Sprint(predicted), func(t *testing.T) {
			t.Parallel()
			f := &external.Fetcher{}
			ea := &manifest.ExternalArtifact{Name: "ea", Namespace: "apps"}
			s := store.New()
			if predicted {
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				s.AddObject(owner)
				generator := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"artifacts": []any{map[string]any{"name": ea.Name}}}}
				s.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: generator}}, Complete: true}, nil)
				if len(s.Productions(ea.Named(), nil)) != 1 {
					t.Fatal("missing producer evidence")
				}
			}
			if s.GetObject(ea.Named()) != nil || s.GetArtifact(ea.Named()) != nil {
				t.Fatal("prediction supplied source content")
			}
			_, err := f.Fetch(t.Context(), ea, testReadSet(ea), sourcecache.Policy{})
			if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("missing-content error = %v", err)
			}
		})
	}
}
