package loader_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestLoader_FixtureHeldDecodeErrors(t *testing.T) {
	t.Parallel()
	for _, variant := range []string{"oci", "bootstrap", "resolved-unavailable-tag", "non-matching-url", "two-held", "two-held-path-order"} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "source-errors", "source-ref--held-error-scope-"+variant)
			second, held := "second", "held"
			if variant == "two-held-path-order" {
				second, held = "a-second", "z-held"
			}
			l := loader.New(store.New())
			l.SourceRoot = fixture.Repo
			_, err := l.Load(t.Context(), filepath.Join(fixture.Repo, second), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			err = l.EntryFailure(t.Context(), fixture.Repo)
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput),
				"expected the second consumer decode error, got %v",
				err,
			)
			if variant == "two-held" || variant == "two-held-path-order" {
				heldLoader := loader.New(store.New())
				heldLoader.SourceRoot = fixture.Repo
				_, err := heldLoader.Load(t.Context(), filepath.Join(fixture.Repo, held), manifest.NamedResource{}, nil)
				assert.NoError(t, err)
				heldLoader.Admit(fixture.Repo)
				assert.FatalIf(
					t,
					!errors.Is(heldLoader.EntryFailure(t.Context(), fixture.Repo), manifest.ErrInput),
					"expected the held consumer decode error",
				)
			}
		})
	}
}

func TestLoader_FixturePermissionError(t *testing.T) {
	t.Parallel()
	fixture := testfixture.Materialize(t, "source-errors", "source-ref--held-error-scope-non-decode-error")
	file := filepath.Join(fixture.Repo, "second", "extra.yaml")
	if _, err := os.ReadFile(file); err == nil {
		t.Skip(
			"The OS or current user permits reading a file with no permissions. The loader owns this permission-dependent proof.",
		)
	}
	input, _, err := baseline.AdmitDirectory(t.Context(), fixture.Repo, fixture.Repo, manifest.WorkScope{})
	assert.NoError(t, err)
	st := store.New()
	obj := &manifest.GitRepository{Name: "second", Namespace: "flux-system"}
	st.AddObject(obj)
	st.SetArtifact(obj.Named(), &store.SourceArtifact{LocalPath: fixture.Repo, FS: input.FS})
	l := loader.New(st)
	l.SourceRoot = fixture.Repo
	_, err = l.Load(t.Context(), filepath.Join(fixture.Repo, "second"), manifest.NamedResource{}, nil)
	if err == nil {
		t.Skip(
			"The admitted source permits reading a file with no permissions. The loader owns this permission-dependent proof.",
		)
	}
	assert.FatalIf(t, errors.Is(err, manifest.ErrInput), "expected the second consumer read error, got %v", err)
}
