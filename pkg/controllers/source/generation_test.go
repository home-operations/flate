package source

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

type sourceGenerator struct{ *manifest.GitRepository }

func (g *sourceGenerator) GeneratedObjects() ([]manifest.BaseManifest, error) {
	return []manifest.BaseManifest{manifest.NewConfigMap(manifest.NamedResource{Name: "source-info", Namespace: g.Namespace}, map[string]any{"url": g.URL})}, nil
}

func TestSource_GeneratedInventoryAndAcquisition(t *testing.T) {
	t.Parallel()
	fetcher := &fakeFetcher{artifact: &store.SourceArtifact{Key: "acquisition", Content: "generation", FS: fstest.MapFS{}}}
	c, st := newController(t, map[string]src.Fetcher{manifest.KindGitRepository: fetcher})
	repository := &sourceGenerator{GitRepository: &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "https://example.test/first"}}
	st.AddObject(repository)
	var previous *base.Execution
	for _, url := range []string{repository.URL, "https://example.test/second"} {
		next := *repository.GitRepository
		next.URL = url
		repository = &sourceGenerator{GitRepository: &next}
		st.AddObject(repository)
		fetcher.during = func() {
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: repository.Namespace, Name: "source-info"}
			data, _ := st.GetObject(id).(*manifest.ConfigMap)
			if data == nil || data.Data["url"] != url || !st.ReportHidden(id) {
				t.Error("source acquired content before publishing its current generated declaration")
			}
		}
		var execution *base.Execution
		c.Tasks.Go(t.Context(), "source", func(ctx context.Context) { execution = c.ReconcileNode(ctx, repository.Named(), manifest.DrainNone) })
		c.Tasks.BlockTillDone()
		if previous != nil {
			previous.Retire()
		}
		if execution == nil || !execution.Valid() || c.Tasks.Failures() != 0 {
			t.Fatal("source publication invalidated its own retained outputs")
		}
		content, _ := st.GetArtifact(repository.Named(), store.ArtifactContent).(*store.SourceArtifact)
		_, _, inventory := st.GetEmission(repository.Named())
		if content == nil || content.Reads != &execution.Reads || len(inventory.Children) != 1 {
			t.Fatalf("source content=%v inventory=%+v", content, inventory)
		}
		inventory.Finish()
		previous = execution
	}
	if fetcher.calls != 2 {
		t.Fatalf("acquisitions=%d, want one for each declaration", fetcher.calls)
	}
	previous.Close()
	for range st.Sweep() {
	}
}
