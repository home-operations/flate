package base

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestExecution_SkipClearsAlias(t *testing.T) {
	t.Parallel()
	for _, invalidArtifact := range []bool{false, true} {
		name := "valid_bootstrap_alias"
		if invalidArtifact {
			name = "invalid_acquisition"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			repository := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/repo"}
			st.AddObject(repository)
			var support store.ReadSet
			support.Init(st, repository.Named(), nil)
			defer support.Retire()
			support.GetObject(repository.Named())
			secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}
			if invalidArtifact {
				support.GetObject(secretID)
			}
			st.SetArtifact(
				repository.Named(),
				&store.SourceArtifact{FS: fstest.MapFS{}, InputAlias: true, Reads: &support},
			)
			if invalidArtifact {
				st.AddObject(&manifest.Secret{Name: "auth", Namespace: "apps"})
			}
			tasks := task.NewBounded(2)
			run := gateExecution(New(st, tasks), repository.Named())
			defer run.Close()
			run.Reads.GetObject(repository.Named())
			tasks.Go(t.Context(), "skip", func(ctx context.Context) {
				missing := &source.MissingSecretError{Owner: repository.Named(), Secret: secretID, Detail: "not found"}
				run.finishFailure(ctx, repository.Named(), manifest.NewError(manifest.ErrSourceSkipped, missing, ""))
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 {
				t.Fatal("skip panicked")
			}
			status, ok := st.GetStatus(repository.Named())
			if !ok || status.Outcome() != store.OutcomeSkipped {
				t.Fatalf("outcome = %+v", status)
			}
			if st.GetArtifact(repository.Named(), store.ArtifactContent) != nil {
				t.Fatal("source is skipped, but its valid bootstrap alias still supplies usable content")
			}
		})
	}
}
